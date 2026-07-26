from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
import random

import httpx

from signalkit_stream.protocol import (
    CollectorContext,
    CollectorError,
    CollectorErrorKind,
    CollectorResult,
    Cursor,
    RateLimitSnapshot,
    SourceIdentity,
)


@dataclass(slots=True, frozen=True)
class RetryPolicy:
    max_attempts: int = 4
    base_delay: float = 0.25
    max_delay: float = 8.0
    jitter_ratio: float = 0.2
    retry_statuses: frozenset[int] = frozenset({408, 425, 429, 500, 502, 503, 504})

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if self.base_delay < 0 or self.max_delay < 0:
            raise ValueError("retry delays must be >= 0")
        if not 0 <= self.jitter_ratio <= 1:
            raise ValueError("jitter_ratio must be between 0 and 1")


class Collector(ABC):
    """Contract implemented by every source adapter."""

    source: str
    instance: str = "default"

    @property
    def identity(self) -> SourceIdentity:
        return SourceIdentity(self.source, self.instance)

    def context(self, context: CollectorContext | None) -> CollectorContext:
        return context or CollectorContext()

    def validate_cursor(self, cursor: Cursor | None) -> None:
        if cursor is not None and cursor.source_key != self.identity.key:
            raise CollectorError(
                f"cursor belongs to {cursor.source_key}, expected {self.identity.key}",
                kind=CollectorErrorKind.CURSOR,
                source_key=self.identity.key,
                retryable=False,
            )

    @abstractmethod
    async def collect(
        self,
        *,
        context: CollectorContext | None = None,
        cursor: Cursor | None = None,
    ) -> CollectorResult:
        """Collect one resumable batch and return normalized events plus a cursor."""


class HTTPCollector(Collector):
    """Collector base with shared retry/backoff and rate-limit inspection."""

    def __init__(
        self,
        *,
        client: httpx.AsyncClient | None = None,
        timeout: float = 20.0,
        user_agent: str = "signalkit-stream/0.2 (+https://github.com/1264585648/signalkit-stream)",
        retry_policy: RetryPolicy | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._client = client
        self._timeout = timeout
        self._user_agent = user_agent
        self._retry_policy = retry_policy or RetryPolicy()
        self._sleep = sleep
        self._rate_limit: RateLimitSnapshot | None = None

    @property
    def rate_limit(self) -> RateLimitSnapshot | None:
        return self._rate_limit

    @asynccontextmanager
    async def http_client(self) -> AsyncIterator[httpx.AsyncClient]:
        if self._client is not None:
            yield self._client
            return

        async with httpx.AsyncClient(
            timeout=self._timeout,
            headers={"User-Agent": self._user_agent},
            follow_redirects=True,
        ) as client:
            yield client

    async def request(
        self,
        client: httpx.AsyncClient,
        method: str,
        url: str,
        *,
        context: CollectorContext | None = None,
        **kwargs: object,
    ) -> httpx.Response:
        ctx = self.context(context)
        policy = self._retry_policy
        last_error: Exception | None = None

        for attempt in range(1, policy.max_attempts + 1):
            if ctx.deadline is not None and datetime.now(UTC) >= ctx.deadline:
                raise CollectorError(
                    "collector deadline exceeded before HTTP request",
                    kind=CollectorErrorKind.TIMEOUT,
                    source_key=self.identity.key,
                    retryable=True,
                )

            try:
                response = await client.request(method, url, **kwargs)
                snapshot = self._rate_limit_from_headers(response)
                if snapshot is not None:
                    self._rate_limit = snapshot

                if response.status_code < 400:
                    return response

                is_rate_limited = response.status_code == 429 or (
                    response.status_code == 403
                    and response.headers.get("X-RateLimit-Remaining") == "0"
                )
                retryable = response.status_code in policy.retry_statuses or is_rate_limited
                if not retryable or attempt >= policy.max_attempts:
                    raise self._http_error(
                        response,
                        retryable=retryable,
                        rate_limited=is_rate_limited,
                    )

                await self._sleep(self._retry_delay(attempt, response))
                continue
            except CollectorError:
                raise
            except httpx.TimeoutException as exc:
                last_error = exc
                if attempt >= policy.max_attempts:
                    raise CollectorError(
                        f"request timed out after {attempt} attempts: {url}",
                        kind=CollectorErrorKind.TIMEOUT,
                        source_key=self.identity.key,
                        retryable=True,
                    ) from exc
                await self._sleep(self._retry_delay(attempt, None))
            except httpx.RequestError as exc:
                last_error = exc
                if attempt >= policy.max_attempts:
                    raise CollectorError(
                        f"network request failed after {attempt} attempts: {url}",
                        kind=CollectorErrorKind.NETWORK,
                        source_key=self.identity.key,
                        retryable=True,
                    ) from exc
                await self._sleep(self._retry_delay(attempt, None))

        raise CollectorError(
            f"request failed: {url}",
            kind=CollectorErrorKind.NETWORK,
            source_key=self.identity.key,
            retryable=True,
            details={"cause": repr(last_error)},
        )

    def _retry_delay(self, attempt: int, response: httpx.Response | None) -> float:
        if response is not None:
            retry_after = self._parse_retry_after(response.headers.get("Retry-After"))
            if retry_after is not None:
                return min(retry_after, self._retry_policy.max_delay)

        base = min(
            self._retry_policy.base_delay * (2 ** max(0, attempt - 1)),
            self._retry_policy.max_delay,
        )
        if base == 0 or self._retry_policy.jitter_ratio == 0:
            return base
        spread = base * self._retry_policy.jitter_ratio
        return max(0.0, min(self._retry_policy.max_delay, base + random.uniform(-spread, spread)))

    def _http_error(
        self,
        response: httpx.Response,
        *,
        retryable: bool,
        rate_limited: bool = False,
    ) -> CollectorError:
        status = response.status_code
        if rate_limited or status == 429:
            kind = CollectorErrorKind.RATE_LIMIT
        elif status in {401, 403}:
            kind = CollectorErrorKind.AUTH
        else:
            kind = CollectorErrorKind.HTTP
        return CollectorError(
            f"HTTP {status} for {response.request.url}",
            kind=kind,
            source_key=self.identity.key,
            retryable=retryable,
            status_code=status,
            details={"response_preview": response.text[:300]},
        )

    @classmethod
    def _rate_limit_from_headers(cls, response: httpx.Response) -> RateLimitSnapshot | None:
        headers = response.headers
        raw_limit = headers.get("X-RateLimit-Limit") or headers.get("RateLimit-Limit")
        raw_remaining = headers.get("X-RateLimit-Remaining") or headers.get("RateLimit-Remaining")
        raw_reset = headers.get("X-RateLimit-Reset") or headers.get("RateLimit-Reset")
        raw_retry = headers.get("Retry-After")
        if not any((raw_limit, raw_remaining, raw_reset, raw_retry)):
            return None

        limit = cls._safe_int(raw_limit)
        remaining = cls._safe_int(raw_remaining)
        reset_at: datetime | None = None
        reset_value = cls._safe_int(raw_reset)
        if reset_value is not None:
            try:
                reset_at = datetime.fromtimestamp(reset_value, tz=UTC)
            except (OverflowError, OSError, ValueError):
                reset_at = None

        return RateLimitSnapshot(
            limit=limit,
            remaining=remaining,
            reset_at=reset_at,
            retry_after=cls._parse_retry_after(raw_retry),
        )

    @staticmethod
    def _safe_int(value: str | None) -> int | None:
        if value is None:
            return None
        try:
            return int(value)
        except ValueError:
            return None

    @staticmethod
    def _parse_retry_after(value: str | None) -> float | None:
        if not value:
            return None
        try:
            return max(0.0, float(value))
        except ValueError:
            pass
        try:
            parsed = parsedate_to_datetime(value)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=UTC)
            return max(0.0, (parsed.astimezone(UTC) - datetime.now(UTC)).total_seconds())
        except (TypeError, ValueError, OverflowError):
            return None
