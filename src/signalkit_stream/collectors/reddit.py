from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Literal, Mapping

import httpx

from signalkit_stream.collectors._reddit_impl import RedditCollector as _RedditCollector
from signalkit_stream.collectors.base import HTTPCollector, RetryPolicy
from signalkit_stream.protocol import CollectorContext, CollectorError, CollectorErrorKind

RedditListing = Literal["new", "hot", "top", "rising"]
RedditTimeFilter = Literal["hour", "day", "week", "month", "year", "all"]


class RedditCollector(_RedditCollector):
    """Reddit OAuth collector with static-token, refresh-token, and app-only auth.

    Authentication precedence is:

    1. a configured access token for the first request;
    2. a refresh token when a new access token is required;
    3. confidential-client ``client_credentials`` when no user token is configured.

    OAuth API requests that return HTTP 401 are re-authenticated and retried once when
    refresh credentials or app credentials are available. Tokens are kept only in memory
    and are never placed in cursors or emitted events.
    """

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
        comment_refresh_window: int = 10,
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

        HTTPCollector.__init__(
            self,
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
        self.comment_refresh_window = max(0, comment_refresh_window)
        self.seen_window = max(50, seen_window)
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


def _safe_float(value: object) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


__all__ = ["RedditCollector", "RedditListing", "RedditTimeFilter"]
