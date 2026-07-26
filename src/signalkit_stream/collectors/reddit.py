from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any, Literal, Mapping
from urllib.parse import quote

import httpx

from signalkit_stream.collectors._text import validated_seen_window
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

DEFAULT_COMMENT_CONCURRENCY = 6

RedditListing = Literal["new", "hot", "top", "rising"]
RedditTimeFilter = Literal["hour", "day", "week", "month", "year", "all"]


class RedditCollector(HTTPCollector):
    """Collect subreddit posts and optional top-level comments via Reddit OAuth.

    Authentication precedence is:

    1. a configured access token for the first request;
    2. a refresh token when a new access token is required;
    3. confidential-client ``client_credentials`` when no user token is configured.

    OAuth API requests that return HTTP 401 are re-authenticated and retried once when
    refresh credentials or app credentials are available; the token endpoint itself is
    excluded from that retry. Credentials are read from the environment by the registry,
    are kept only in memory, and are never placed in cursors or emitted events.

    Listing progress uses Reddit's native ``after`` cursor plus a bounded seen-ID
    watermark, so completed polling cycles return to the newest page instead of
    repeatedly walking old history.
    """

    TOKEN_URL = "https://www.reddit.com/api/v1/access_token"
    API_ROOT = "https://oauth.reddit.com"

    def __init__(
        self,
        subreddit: str,
        *,
        user_agent: str,
        client_id: str | None = None,
        client_secret: str | None = None,
        access_token: str | None = None,
        refresh_token: str | None = None,
        listing: RedditListing = "new",
        time_filter: RedditTimeFilter | None = None,
        include_comments: bool = False,
        comments_per_post: int = 0,
        comment_concurrency: int = DEFAULT_COMMENT_CONCURRENCY,
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

        clean_user_agent = user_agent.strip()
        clean_client_id = (client_id or "").strip()
        clean_client_secret = (client_secret or "").strip()
        clean_access_token = (access_token or "").strip()
        clean_refresh_token = (refresh_token or "").strip()

        if not clean_user_agent:
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

        if clean_refresh_token and not clean_client_id:
            raise ValueError("Reddit refresh_token requires client_id")
        if not clean_access_token and not clean_refresh_token:
            if not clean_client_id:
                raise ValueError(
                    "Reddit auth requires access_token, refresh_token + client_id, "
                    "or client_id + client_secret"
                )
            if not clean_client_secret:
                raise ValueError(
                    "Reddit client_credentials auth requires a non-empty client_secret"
                )

        super().__init__(
            client=client,
            timeout=timeout,
            user_agent=clean_user_agent,
            retry_policy=retry_policy,
        )
        self.subreddit = normalized_subreddit
        self.client_id = clean_client_id
        self.client_secret = clean_client_secret
        self.user_agent = clean_user_agent
        self.configured_access_token = clean_access_token or None
        self.refresh_token = clean_refresh_token or None
        self.listing = listing
        self.time_filter = time_filter
        self.include_comments = include_comments
        self.comments_per_post = max(0, min(comments_per_post, 100))
        if comment_concurrency < 1:
            raise ValueError("Reddit comment_concurrency must be >= 1")
        self.comment_concurrency = comment_concurrency
        self.seen_window = validated_seen_window(seen_window, label="Reddit")
        self.source = "reddit"
        self.instance = instance or f"r-{self.subreddit}-{self.listing}"
        self._access_token: str | None = None
        self._token_expires_at: datetime | None = None

    @property
    def auth_mode(self) -> str:
        if self.configured_access_token and self.refresh_token:
            return "access_token+refresh_token"
        if self.configured_access_token:
            return "access_token"
        if self.refresh_token:
            return "refresh_token"
        return "client_credentials"

    async def request(
        self,
        client: httpx.AsyncClient,
        method: str,
        url: str,
        *,
        context: CollectorContext | None = None,
        **kwargs: object,
    ) -> httpx.Response:
        """Retry one OAuth API 401 after obtaining a fresh bearer token."""

        try:
            return await super().request(
                client,
                method,
                url,
                context=context,
                **kwargs,
            )
        except CollectorError as exc:
            if not self._should_reauthenticate(url, kwargs, exc):
                raise

            ctx = self.context(context)
            token = await self._token(client, context=ctx, force=True)
            retry_kwargs = dict(kwargs)
            raw_headers = retry_kwargs.get("headers")
            headers = dict(raw_headers) if isinstance(raw_headers, Mapping) else {}
            headers["Authorization"] = f"bearer {token}"
            headers.setdefault("User-Agent", self.user_agent)
            retry_kwargs["headers"] = headers
            return await super().request(
                client,
                method,
                url,
                context=ctx,
                **retry_kwargs,
            )

    def _should_reauthenticate(
        self,
        url: str,
        kwargs: Mapping[str, object],
        error: CollectorError,
    ) -> bool:
        if url == self.TOKEN_URL:
            return False
        if error.kind is not CollectorErrorKind.AUTH or error.status_code != 401:
            return False
        raw_headers = kwargs.get("headers")
        if not isinstance(raw_headers, Mapping):
            return False
        authorization = str(raw_headers.get("Authorization") or "").lower()
        if not authorization.startswith("bearer "):
            return False
        return self._can_reauthenticate

    @property
    def _can_reauthenticate(self) -> bool:
        return bool(self.refresh_token or (self.client_id and self.client_secret))

    async def _token(
        self,
        client: httpx.AsyncClient,
        *,
        context: CollectorContext,
        force: bool = False,
    ) -> str:
        now = datetime.now(UTC)
        if not force and self._access_token is not None:
            if self._token_expires_at is None or now < self._token_expires_at - timedelta(seconds=30):
                return self._access_token

        if not force and self.configured_access_token:
            self._access_token = self.configured_access_token
            self._token_expires_at = None
            return self._access_token

        if self.refresh_token:
            auth = httpx.BasicAuth(self.client_id, self.client_secret)
            data = {
                "grant_type": "refresh_token",
                "refresh_token": self.refresh_token,
            }
            mode = "refresh_token"
        elif self.client_id and self.client_secret:
            auth = httpx.BasicAuth(self.client_id, self.client_secret)
            data = {"grant_type": "client_credentials"}
            mode = "client_credentials"
        else:
            raise CollectorError(
                "Reddit access token was rejected and no refresh/app credentials are configured",
                kind=CollectorErrorKind.AUTH,
                source_key=self.identity.key,
                retryable=False,
                details={"auth_mode": self.auth_mode},
            )

        response = await super().request(
            client,
            "POST",
            self.TOKEN_URL,
            context=context,
            auth=auth,
            data=data,
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
                details={"error": payload.get("error"), "auth_mode": mode},
            )

        expires_in = _safe_float(payload.get("expires_in")) or 3600.0
        self._access_token = token.strip()
        self._token_expires_at = now + timedelta(seconds=max(0.0, expires_in))
        return self._access_token

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

            posts: list[tuple[Mapping[str, Any], str, SignalEvent]] = []
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
                posts.append((data, fullname, post))
                processed_ids.append(fullname)

            comment_batches = await self._comment_batches(
                client,
                token=token,
                posts=posts,
                context=ctx,
            )

        events: list[SignalEvent] = []
        for (_, fullname, post), (comment_events, truncated) in zip(
            posts, comment_batches, strict=True
        ):
            events.append(post)
            events.extend(comment_events)
            if truncated:
                warnings.append(
                    f"Reddit comments truncated for post {fullname}; "
                    "only top-level comments are collected"
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

    async def _comment_batches(
        self,
        client: httpx.AsyncClient,
        *,
        token: str,
        posts: list[tuple[Mapping[str, Any], str, SignalEvent]],
        context: CollectorContext,
    ) -> list[tuple[list[SignalEvent], bool]]:
        """Fetch each post's comments concurrently, bounded and order-preserving.

        ``await self._comments(...)`` used to run inside the per-post loop, so a poll with
        ``limit=100`` serialized ~100 round trips while holding the runtime's per-source
        concurrency slot. ``comment_concurrency`` keeps the fan-out small enough for
        Reddit's per-app rate limits.
        """

        if not (self.include_comments and self.comments_per_post):
            return [([], False) for _ in posts]

        semaphore = asyncio.Semaphore(self.comment_concurrency)

        async def fetch(post: Mapping[str, Any], parent: SignalEvent) -> tuple[list[SignalEvent], bool]:
            async with semaphore:
                return await self._comments(
                    client,
                    token=token,
                    post=post,
                    parent_event_id=parent.id,
                    context=context,
                )

        results = await asyncio.gather(
            *(fetch(data, post) for data, _, post in posts),
            return_exceptions=True,
        )
        batches: list[tuple[list[SignalEvent], bool]] = []
        for result in results:
            if isinstance(result, BaseException):
                raise result
            batches.append(result)
        return batches

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


__all__ = ["RedditCollector", "RedditListing", "RedditTimeFilter"]
