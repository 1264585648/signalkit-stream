from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Literal

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

RedditListing = Literal["posts", "comments"]


@dataclass(slots=True)
class RedditOAuth:
    """OAuth credentials for an approved Reddit Data API client.

    A static access token is useful for short-lived jobs. Long-running runtimes should
    provide client ID + refresh token credentials so access tokens can be refreshed.
    Installed-app clients may use an empty client secret.
    """

    access_token: str | None = None
    client_id: str | None = None
    client_secret: str = ""
    refresh_token: str | None = None
    _cached_token: str | None = None
    _expires_at: datetime | None = None

    @property
    def can_refresh(self) -> bool:
        return bool(self.client_id and self.refresh_token)

    def invalidate(self) -> None:
        self._cached_token = None
        self._expires_at = None


class RedditCollector(HTTPCollector):
    """Collect new subreddit posts or comments through Reddit's OAuth API.

    The collector never falls back to unauthenticated scraping. The caller must have
    Reddit-approved Data API access appropriate for the intended use.
    """

    API_ROOT = "https://oauth.reddit.com"
    TOKEN_URL = "https://www.reddit.com/api/v1/access_token"

    def __init__(
        self,
        subreddit: str,
        *,
        listing: RedditListing = "posts",
        oauth: RedditOAuth,
        user_agent: str,
        instance: str | None = None,
        seen_window: int = 1000,
        client: httpx.AsyncClient | None = None,
        timeout: float = 20.0,
        retry_policy: RetryPolicy | None = None,
    ) -> None:
        subreddit = subreddit.strip().removeprefix("r/").strip("/")
        if not subreddit:
            raise ValueError("Reddit subreddit must not be empty")
        if listing not in {"posts", "comments"}:
            raise ValueError("Reddit listing must be 'posts' or 'comments'")
        user_agent = user_agent.strip()
        if not user_agent:
            raise ValueError("Reddit user_agent must not be empty")
        if not oauth.access_token and not oauth.can_refresh:
            raise ValueError(
                "Reddit OAuth requires an access token or client_id + refresh_token credentials"
            )
        if seen_window < 100:
            raise ValueError("Reddit seen_window must be >= 100")

        super().__init__(
            client=client,
            timeout=timeout,
            user_agent=user_agent,
            retry_policy=retry_policy,
        )
        self.subreddit = subreddit
        self.listing = listing
        self.oauth = oauth
        self.user_agent = user_agent
        self.seen_window = seen_window
        self.source = "reddit"
        self.instance = instance or f"r-{subreddit.lower()}-{listing}"

    async def collect(
        self,
        *,
        context: CollectorContext | None = None,
        cursor: Cursor | None = None,
    ) -> CollectorResult:
        ctx = self.context(context)
        self.validate_cursor(cursor)
        state = dict(cursor.state) if cursor else {}
        initialized = bool(state.get("initialized", False))
        seen_ids = [str(value) for value in state.get("seen_ids", [])]
        seen = set(seen_ids)
        cycle_ids = [str(value) for value in state.get("cycle_ids", [])]
        after = str(state.get("after") or "").strip() or None

        page_limit = min(ctx.limit, 100)
        async with self.http_client() as client:
            response = await self._oauth_get(
                client,
                self._listing_url(),
                context=ctx,
                params={
                    "limit": page_limit,
                    "raw_json": 1,
                    **({"after": after} if after else {}),
                },
            )

        self._rate_limit = self._reddit_rate_limit(response)
        payload = response.json()
        data = payload.get("data", {}) if isinstance(payload, dict) else {}
        children = data.get("children", []) if isinstance(data, dict) else []
        page_after = data.get("after") if isinstance(data, dict) else None

        events: list[SignalEvent] = []
        processed: list[str] = []
        reached_known = False
        for child in children:
            if not isinstance(child, dict):
                continue
            thing = child.get("data")
            if not isinstance(thing, dict):
                continue
            fullname = str(thing.get("name") or "").strip()
            if not fullname:
                continue
            if initialized and fullname in seen:
                reached_known = True
                break
            processed.append(fullname)
            events.append(self._normalize(thing))

        cycle_ids.extend(processed)
        source_exhausted = not page_after or not children

        # On the first ever poll, establish a boundary after the newest page instead
        # of crawling the entire historical subreddit. Later bursts can span pages:
        # we keep paging until a previously seen item is reached.
        cycle_complete = (not initialized) or reached_known or source_exhausted
        if cycle_complete:
            merged_seen = list(dict.fromkeys([*cycle_ids, *seen_ids]))[: self.seen_window]
            next_state = {
                "initialized": True,
                "seen_ids": merged_seen,
                "cycle_ids": [],
                "after": None,
            }
            has_more = False
        else:
            next_after = str(page_after or "").strip() or None
            next_state = {
                "initialized": True,
                "seen_ids": seen_ids,
                "cycle_ids": cycle_ids,
                "after": next_after,
            }
            has_more = bool(next_after)

        return CollectorResult(
            events=events,
            cursor=Cursor(self.identity.key, next_state),
            has_more=has_more,
            primary_count=len(processed),
            rate_limit=self.rate_limit,
        )

    def _listing_url(self) -> str:
        if self.listing == "posts":
            return f"{self.API_ROOT}/r/{self.subreddit}/new"
        return f"{self.API_ROOT}/r/{self.subreddit}/comments"

    async def _oauth_get(
        self,
        client: httpx.AsyncClient,
        url: str,
        *,
        context: CollectorContext,
        params: dict[str, object],
    ) -> httpx.Response:
        token = await self._access_token(client, context=context)
        try:
            return await self.request(
                client,
                "GET",
                url,
                context=context,
                params=params,
                headers=self._headers(token),
            )
        except CollectorError as exc:
            if exc.kind is not CollectorErrorKind.AUTH or not self.oauth.can_refresh:
                raise
            self.oauth.invalidate()
            token = await self._access_token(client, context=context, force_refresh=True)
            return await self.request(
                client,
                "GET",
                url,
                context=context,
                params=params,
                headers=self._headers(token),
            )

    async def _access_token(
        self,
        client: httpx.AsyncClient,
        *,
        context: CollectorContext,
        force_refresh: bool = False,
    ) -> str:
        if not force_refresh and self.oauth.access_token:
            return self.oauth.access_token

        now = datetime.now(UTC)
        if (
            not force_refresh
            and self.oauth._cached_token
            and self.oauth._expires_at
            and self.oauth._expires_at > now + timedelta(seconds=60)
        ):
            return self.oauth._cached_token

        if not self.oauth.can_refresh:
            raise CollectorError(
                "Reddit access token is unavailable and refresh credentials were not configured",
                kind=CollectorErrorKind.AUTH,
                source_key=self.identity.key,
                retryable=False,
            )

        response = await self.request(
            client,
            "POST",
            self.TOKEN_URL,
            context=context,
            auth=(self.oauth.client_id or "", self.oauth.client_secret),
            data={
                "grant_type": "refresh_token",
                "refresh_token": self.oauth.refresh_token or "",
            },
            headers={"User-Agent": self.user_agent},
        )
        payload = response.json()
        if not isinstance(payload, dict) or not payload.get("access_token"):
            raise CollectorError(
                "Reddit OAuth refresh response did not contain an access token",
                kind=CollectorErrorKind.AUTH,
                source_key=self.identity.key,
                retryable=False,
            )
        token = str(payload["access_token"])
        try:
            expires_in = max(60.0, float(payload.get("expires_in", 3600)))
        except (TypeError, ValueError):
            expires_in = 3600.0
        self.oauth._cached_token = token
        self.oauth._expires_at = now + timedelta(seconds=expires_in)
        return token

    def _headers(self, token: str) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {token}",
            "User-Agent": self.user_agent,
            "Accept": "application/json",
        }

    def _normalize(self, thing: dict[str, Any]) -> SignalEvent:
        if self.listing == "comments":
            return self._comment_event(thing)
        return self._post_event(thing)

    def _post_event(self, thing: dict[str, Any]) -> SignalEvent:
        external_id = self._external_id(thing, prefix="t3")
        permalink = str(thing.get("permalink") or "")
        canonical_url = (
            f"https://www.reddit.com{permalink}"
            if permalink.startswith("/")
            else str(thing.get("url") or f"https://www.reddit.com/r/{self.subreddit}")
        )
        outbound_url = str(thing.get("url") or "") or None
        return SignalEvent(
            id=SignalEvent.stable_id(
                self.source,
                external_id,
                SignalKind.POST,
                source_instance=self.instance,
            ),
            source=self.source,
            source_instance=self.instance,
            kind=SignalKind.POST,
            title=self._optional_string(thing.get("title")),
            content=str(thing.get("selftext") or ""),
            author=self._optional_string(thing.get("author")),
            url=canonical_url,
            created_at=self._timestamp(thing.get("created_utc")),
            updated_at=self._edited_timestamp(thing.get("edited")),
            metadata={
                "external_id": external_id,
                "subreddit": str(thing.get("subreddit") or self.subreddit),
                "score": thing.get("score"),
                "num_comments": thing.get("num_comments"),
                "link_flair_text": thing.get("link_flair_text"),
                "over_18": thing.get("over_18"),
                "is_self": thing.get("is_self"),
                "outbound_url": outbound_url,
            },
        )

    def _comment_event(self, thing: dict[str, Any]) -> SignalEvent:
        external_id = self._external_id(thing, prefix="t1")
        permalink = str(thing.get("permalink") or "")
        canonical_url = (
            f"https://www.reddit.com{permalink}"
            if permalink.startswith("/")
            else f"https://www.reddit.com/r/{self.subreddit}/comments"
        )
        return SignalEvent(
            id=SignalEvent.stable_id(
                self.source,
                external_id,
                SignalKind.COMMENT,
                source_instance=self.instance,
            ),
            source=self.source,
            source_instance=self.instance,
            kind=SignalKind.COMMENT,
            title=self._optional_string(thing.get("link_title")),
            content=str(thing.get("body") or ""),
            author=self._optional_string(thing.get("author")),
            url=canonical_url,
            created_at=self._timestamp(thing.get("created_utc")),
            updated_at=self._edited_timestamp(thing.get("edited")),
            metadata={
                "external_id": external_id,
                "subreddit": str(thing.get("subreddit") or self.subreddit),
                "score": thing.get("score"),
                "parent_id": thing.get("parent_id"),
                "link_id": thing.get("link_id"),
                "link_title": thing.get("link_title"),
                "link_url": thing.get("link_url"),
            },
        )

    @staticmethod
    def _external_id(thing: dict[str, Any], *, prefix: str) -> str:
        fullname = str(thing.get("name") or "").strip()
        if fullname:
            return fullname
        raw_id = str(thing.get("id") or "").strip()
        if not raw_id:
            raise CollectorError(
                "Reddit item does not contain a stable id",
                kind=CollectorErrorKind.PARSE,
                source_key="reddit",
                retryable=False,
            )
        return f"{prefix}_{raw_id}"

    @staticmethod
    def _timestamp(value: object) -> datetime:
        try:
            return datetime.fromtimestamp(float(value), tz=UTC)
        except (TypeError, ValueError, OSError, OverflowError):
            return datetime.now(UTC)

    @classmethod
    def _edited_timestamp(cls, value: object) -> datetime | None:
        if value in {None, False, 0, "0", "false", "False"}:
            return None
        return cls._timestamp(value)

    @staticmethod
    def _optional_string(value: object) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    @staticmethod
    def _reddit_rate_limit(response: httpx.Response) -> RateLimitSnapshot | None:
        remaining_raw = response.headers.get("X-Ratelimit-Remaining")
        reset_raw = response.headers.get("X-Ratelimit-Reset")
        if remaining_raw is None and reset_raw is None:
            return None

        remaining: int | None = None
        if remaining_raw is not None:
            try:
                remaining = max(0, int(float(remaining_raw)))
            except ValueError:
                remaining = None

        reset_at: datetime | None = None
        if reset_raw is not None:
            try:
                reset_seconds = max(0.0, float(reset_raw))
                reset_at = datetime.now(UTC) + timedelta(seconds=reset_seconds)
            except ValueError:
                reset_at = None

        return RateLimitSnapshot(remaining=remaining, reset_at=reset_at)
