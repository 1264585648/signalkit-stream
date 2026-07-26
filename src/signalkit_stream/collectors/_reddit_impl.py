from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any, Literal, Mapping
from urllib.parse import quote

import httpx

from signalkit_stream.collectors.base import HTTPCollector, RetryPolicy
from signalkit_stream.models import SignalEvent, SignalKind
from signalkit_stream.protocol import (
    CollectorContext,
    CollectorError,
    CollectorErrorKind,
    CollectorResult,
    Cursor,
    RateLimitSnapshot,
)

RedditListing = Literal["new", "hot", "top", "rising"]
RedditTimeFilter = Literal["hour", "day", "week", "month", "year", "all"]


class RedditCollector(HTTPCollector):
    """Collect subreddit posts and optional top-level comments via Reddit OAuth.

    The adapter uses Reddit's official OAuth API. App credentials are never embedded
    in cursors or events. Listing progress uses Reddit's native ``after`` cursor plus
    a bounded seen-ID watermark so completed polling cycles return to the newest page
    without repeatedly walking old history.
    """

    TOKEN_URL = "https://www.reddit.com/api/v1/access_token"
    API_ROOT = "https://oauth.reddit.com"

    def __init__(
        self,
        subreddit: str,
        *,
        client_id: str,
        client_secret: str,
        user_agent: str,
        listing: RedditListing = "new",
        time_filter: RedditTimeFilter | None = None,
        include_comments: bool = False,
        comments_per_post: int = 0,
        seen_window: int = 500,
        instance: str | None = None,
        client: httpx.AsyncClient | None = None,
        timeout: float = 20.0,
        retry_policy: RetryPolicy | None = None,
    ) -> None:
        normalized_subreddit = subreddit.strip()
        if normalized_subreddit.lower().startswith("r/"):
            normalized_subreddit = normalized_subreddit[2:]
        if not normalized_subreddit or "/" in normalized_subreddit:
            raise ValueError("subreddit must be a name such as 'saas' or 'all'")
        if not client_id.strip():
            raise ValueError("Reddit client_id must not be empty")
        if not client_secret.strip():
            raise ValueError("Reddit client_secret must not be empty")
        if not user_agent.strip():
            raise ValueError("Reddit user_agent must not be empty")
        if listing not in {"new", "hot", "top", "rising"}:
            raise ValueError(f"unsupported Reddit listing: {listing}")
        if time_filter is not None and time_filter not in {
            "hour",
            "day",
            "week",
            "month",
            "year",
            "all",
        }:
            raise ValueError(f"unsupported Reddit time_filter: {time_filter}")

        super().__init__(
            client=client,
            timeout=timeout,
            user_agent=user_agent,
            retry_policy=retry_policy,
        )
        self.subreddit = normalized_subreddit
        self.client_id = client_id
        self.client_secret = client_secret
        self.user_agent = user_agent
        self.listing = listing
        self.time_filter = time_filter
        self.include_comments = include_comments
        self.comments_per_post = max(0, min(comments_per_post, 100))
        self.seen_window = max(50, seen_window)
        self.source = "reddit"
        self.instance = instance or f"r-{self.subreddit}-{self.listing}"
        self._access_token: str | None = None
        self._token_expires_at: datetime | None = None

    async def collect(
        self,
        *,
        context: CollectorContext | None = None,
        cursor: Cursor | None = None,
    ) -> CollectorResult:
        ctx = self.context(context)
        self.validate_cursor(cursor)

        state = dict(cursor.state) if cursor else {}
        after = _optional_text(state.get("after"))
        prior_seen = [str(value) for value in state.get("seen_ids", []) if str(value)]
        prior_seen_set = set(prior_seen)
        starting_new_cycle = after is None
        watermark_ids = (
            prior_seen if starting_new_cycle else [str(value) for value in state.get("watermark_ids", [])]
        )
        watermark = set(watermark_ids)
        count = int(state.get("count", 0)) if after else 0

        async with self.http_client() as client:
            token = await self._token(client, context=ctx)
            params: dict[str, object] = {
                "limit": min(ctx.limit, 100),
                "raw_json": 1,
                "count": count,
            }
            if after:
                params["after"] = after
            if self.time_filter is not None:
                params["t"] = self.time_filter

            response = await self.request(
                client,
                "GET",
                self._listing_url(),
                context=ctx,
                params=params,
                headers=self._api_headers(token),
            )
            payload = self._json_object(response, "Reddit listing")
            listing_data = payload.get("data")
            if not isinstance(listing_data, Mapping):
                raise self._parse_error("Reddit listing is missing data")
            children = listing_data.get("children", [])
            if not isinstance(children, list):
                raise self._parse_error("Reddit listing children must be a list")

            events: list[SignalEvent] = []
            processed_ids: list[str] = []
            primary_count = 0
            warnings: list[str] = []
            reached_watermark = False

            for child in children:
                if not isinstance(child, Mapping) or child.get("kind") != "t3":
                    warnings.append("ignored non-post Reddit listing child")
                    continue
                data = child.get("data")
                if not isinstance(data, Mapping):
                    warnings.append("ignored malformed Reddit post")
                    continue
                fullname = self._fullname(data, prefix="t3")
                if fullname in watermark:
                    reached_watermark = True
                    break

                primary_count += 1
                if fullname in prior_seen_set:
                    continue

                post = self._post_event(data, fullname=fullname)
                events.append(post)
                processed_ids.append(fullname)
                if self.include_comments and self.comments_per_post:
                    comment_events, truncated = await self._comments(
                        client,
                        token=token,
                        post=data,
                        parent_event_id=post.id,
                        context=ctx,
                    )
                    events.extend(comment_events)
                    if truncated:
                        warnings.append(
                            f"Reddit comments truncated for post {fullname}; only top-level comments are collected"
                        )

        response_after = _optional_text(listing_data.get("after"))
        next_after = None if reached_watermark else response_after
        has_more = next_after is not None
        merged_seen = self._merge_seen(
            prior_seen,
            processed_ids,
            prepend=starting_new_cycle,
        )
        next_state: dict[str, Any] = {
            "after": next_after,
            "count": count + primary_count if has_more else 0,
            "seen_ids": merged_seen,
            "listing": self.listing,
            "subreddit": self.subreddit,
        }
        if has_more:
            next_state["watermark_ids"] = watermark_ids

        return CollectorResult(
            events=events,
            cursor=Cursor(self.identity.key, next_state),
            has_more=has_more,
            primary_count=primary_count,
            rate_limit=self.rate_limit,
            warnings=warnings,
        )

    async def _token(
        self,
        client: httpx.AsyncClient,
        *,
        context: CollectorContext,
    ) -> str:
        now = datetime.now(UTC)
        if (
            self._access_token is not None
            and self._token_expires_at is not None
            and now < self._token_expires_at - timedelta(seconds=30)
        ):
            return self._access_token

        response = await self.request(
            client,
            "POST",
            self.TOKEN_URL,
            context=context,
            auth=httpx.BasicAuth(self.client_id, self.client_secret),
            data={"grant_type": "client_credentials"},
            headers={"User-Agent": self.user_agent},
        )
        payload = self._json_object(response, "Reddit OAuth token")
        token = payload.get("access_token")
        if not isinstance(token, str) or not token.strip():
            raise CollectorError(
                "Reddit OAuth response did not contain an access_token",
                kind=CollectorErrorKind.AUTH,
                source_key=self.identity.key,
                retryable=False,
                details={"error": payload.get("error")},
            )
        expires_in = _safe_float(payload.get("expires_in")) or 3600.0
        self._access_token = token
        self._token_expires_at = now + timedelta(seconds=max(0.0, expires_in))
        return token

    async def _comments(
        self,
        client: httpx.AsyncClient,
        *,
        token: str,
        post: Mapping[str, Any],
        parent_event_id: str,
        context: CollectorContext,
    ) -> tuple[list[SignalEvent], bool]:
        post_id = str(post.get("id") or "").strip()
        if not post_id:
            return [], False
        response = await self.request(
            client,
            "GET",
            f"{self.API_ROOT}/comments/{quote(post_id, safe='')}",
            context=context,
            params={
                "limit": self.comments_per_post,
                "depth": 1,
                "sort": "top",
                "raw_json": 1,
            },
            headers=self._api_headers(token),
        )
        try:
            payload = response.json()
        except ValueError as exc:
            raise self._parse_error("invalid JSON in Reddit comments response") from exc
        if not isinstance(payload, list) or len(payload) < 2 or not isinstance(payload[1], Mapping):
            raise self._parse_error("unexpected Reddit comments response shape")
        data = payload[1].get("data")
        if not isinstance(data, Mapping) or not isinstance(data.get("children", []), list):
            raise self._parse_error("Reddit comments listing is malformed")

        result: list[SignalEvent] = []
        truncated = False
        for child in data.get("children", []):
            if not isinstance(child, Mapping):
                continue
            if child.get("kind") == "more":
                truncated = True
                continue
            if child.get("kind") != "t1":
                continue
            comment = child.get("data")
            if not isinstance(comment, Mapping):
                continue
            result.append(
                self._comment_event(
                    comment,
                    parent_event_id=parent_event_id,
                    post_id=post_id,
                )
            )
            if len(result) >= self.comments_per_post:
                break
        return result, truncated

    def _post_event(self, data: Mapping[str, Any], *, fullname: str) -> SignalEvent:
        permalink = str(data.get("permalink") or "").strip()
        canonical_url = (
            f"https://www.reddit.com{permalink}"
            if permalink.startswith("/")
            else str(data.get("url") or self._listing_url())
        )
        return SignalEvent(
            id=SignalEvent.stable_id(
                self.source,
                fullname,
                SignalKind.POST,
                source_instance=self.instance,
            ),
            source=self.source,
            source_instance=self.instance,
            kind=SignalKind.POST,
            title=_optional_text(data.get("title")),
            content=str(data.get("selftext") or ""),
            author=_optional_text(data.get("author")),
            url=canonical_url,
            created_at=_timestamp(data.get("created_utc")),
            updated_at=_edited_timestamp(data.get("edited")),
            metadata={
                "external_id": fullname,
                "native_id": data.get("id"),
                "subreddit": data.get("subreddit"),
                "score": data.get("score"),
                "num_comments": data.get("num_comments"),
                "permalink": permalink,
                "domain": data.get("domain"),
                "link_url": data.get("url"),
                "is_self": data.get("is_self"),
                "locked": data.get("locked"),
                "stickied": data.get("stickied"),
                "over_18": data.get("over_18"),
                "link_flair_text": data.get("link_flair_text"),
            },
        )

    def _comment_event(
        self,
        data: Mapping[str, Any],
        *,
        parent_event_id: str,
        post_id: str,
    ) -> SignalEvent:
        fullname = self._fullname(data, prefix="t1")
        permalink = str(data.get("permalink") or "").strip()
        url = (
            f"https://www.reddit.com{permalink}"
            if permalink.startswith("/")
            else f"https://www.reddit.com/comments/{post_id}"
        )
        return SignalEvent(
            id=SignalEvent.stable_id(
                self.source,
                fullname,
                SignalKind.COMMENT,
                source_instance=self.instance,
            ),
            source=self.source,
            source_instance=self.instance,
            kind=SignalKind.COMMENT,
            content=str(data.get("body") or ""),
            author=_optional_text(data.get("author")),
            url=url,
            created_at=_timestamp(data.get("created_utc")),
            updated_at=_edited_timestamp(data.get("edited")),
            metadata={
                "external_id": fullname,
                "native_id": data.get("id"),
                "subreddit": data.get("subreddit"),
                "score": data.get("score"),
                "parent_id": data.get("parent_id"),
                "link_id": data.get("link_id"),
                "post_id": post_id,
                "parent_event_id": parent_event_id,
            },
        )

    def _listing_url(self) -> str:
        subreddit = quote(self.subreddit, safe="+_")
        return f"{self.API_ROOT}/r/{subreddit}/{self.listing}"

    def _api_headers(self, token: str) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {token}",
            "User-Agent": self.user_agent,
        }

    def _merge_seen(
        self,
        prior: list[str],
        processed: list[str],
        *,
        prepend: bool,
    ) -> list[str]:
        values = [*processed, *prior] if prepend else [*prior, *processed]
        return list(dict.fromkeys(values))[: self.seen_window]

    def _fullname(self, data: Mapping[str, Any], *, prefix: str) -> str:
        fullname = str(data.get("name") or "").strip()
        if fullname:
            return fullname
        native_id = str(data.get("id") or "").strip()
        if not native_id:
            raise self._parse_error(f"Reddit {prefix} object is missing id")
        return f"{prefix}_{native_id}"

    def _json_object(self, response: httpx.Response, label: str) -> Mapping[str, Any]:
        try:
            payload = response.json()
        except ValueError as exc:
            raise self._parse_error(f"invalid JSON in {label} response") from exc
        if not isinstance(payload, Mapping):
            raise self._parse_error(f"{label} response must be a JSON object")
        return payload

    def _parse_error(self, message: str) -> CollectorError:
        return CollectorError(
            message,
            kind=CollectorErrorKind.PARSE,
            source_key=self.identity.key,
            retryable=False,
        )

    @classmethod
    def _rate_limit_from_headers(cls, response: httpx.Response) -> RateLimitSnapshot | None:
        """Parse Reddit's decimal remaining values and relative reset seconds."""

        headers = response.headers
        remaining_value = _safe_float(headers.get("X-Ratelimit-Remaining"))
        used_value = _safe_float(headers.get("X-Ratelimit-Used"))
        limit_value = _safe_float(headers.get("X-Ratelimit-Limit"))
        reset_seconds = _safe_float(headers.get("X-Ratelimit-Reset"))
        retry_after = cls._parse_retry_after(headers.get("Retry-After"))
        if all(
            value is None
            for value in (remaining_value, used_value, limit_value, reset_seconds, retry_after)
        ):
            return None

        if limit_value is None and remaining_value is not None and used_value is not None:
            limit_value = remaining_value + used_value
        remaining = int(max(0.0, remaining_value)) if remaining_value is not None else None
        limit = int(max(0.0, limit_value)) if limit_value is not None else None
        reset_at = (
            datetime.now(UTC) + timedelta(seconds=max(0.0, reset_seconds))
            if reset_seconds is not None
            else None
        )
        if retry_after is None and remaining == 0 and reset_seconds is not None:
            retry_after = max(0.0, reset_seconds)
        return RateLimitSnapshot(
            limit=limit,
            remaining=remaining,
            reset_at=reset_at,
            retry_after=retry_after,
        )


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _safe_float(value: object) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _timestamp(value: object) -> datetime:
    seconds = _safe_float(value)
    if seconds is None:
        return datetime.now(UTC)
    return datetime.fromtimestamp(seconds, tz=UTC)


def _edited_timestamp(value: object) -> datetime | None:
    if value is False or value is None:
        return None
    seconds = _safe_float(value)
    if seconds is None or seconds <= 0:
        return None
    return datetime.fromtimestamp(seconds, tz=UTC)
