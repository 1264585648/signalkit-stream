from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from functools import lru_cache
from importlib.metadata import PackageNotFoundError, version as _distribution_version
import random
import sys

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

#: Distribution name used to resolve the advertised User-Agent version.
PACKAGE_DISTRIBUTION = "signalkit-stream"
PROJECT_URL = "https://github.com/1264585648/signalkit-stream"

#: Hard cap on the number of body bytes a collector will buffer for one response.
#: 8 MiB is ~an order of magnitude above the largest first-party response and
#: bounds worst-case RSS even when the runtime polls sources concurrently.
DEFAULT_MAX_RESPONSE_BYTES = 8 * 1024 * 1024

#: Redirect hops followed for a single logical request.
DEFAULT_MAX_REDIRECTS = 3

_REDIRECT_STATUSES = frozenset({301, 302, 303, 307, 308})
_ALLOWED_REDIRECT_SCHEMES = frozenset({"http", "https"})

# Only these request headers survive a redirect that changes the origin. An
# allowlist (rather than a denylist of known secret header names) guarantees that
# operator-configured auth headers such as X-Api-Key / X-Auth-Token /
# Private-Token can never be replayed to another host.
_CROSS_ORIGIN_SAFE_HEADERS = frozenset(
    {
        "accept",
        "accept-charset",
        "accept-encoding",
        "accept-language",
        "cache-control",
        "content-type",
        "if-modified-since",
        "if-none-match",
        "pragma",
        "user-agent",
    }
)

# Headers that must never be copied verbatim onto a redirect request because the
# new request recomputes them.
_REDIRECT_DROP_HEADERS = frozenset({"host", "content-length", "transfer-encoding"})

_BODY_HEADERS = frozenset({"content-encoding", "content-length", "transfer-encoding"})

_ERROR_PREVIEW_BYTES = 300
_REDACTED = "REDACTED"


def _package_version() -> str:
    """Resolve the installed distribution version without importing the package.

    ``importlib.metadata`` reads the installed dist metadata (populated from the
    single ``[project].version`` field in ``pyproject.toml``), so it cannot create
    an import cycle with ``signalkit_stream/__init__.py`` — which imports the
    collectors. The fallback only inspects ``sys.modules`` and never triggers an
    import, so it is cycle-safe too.
    """

    try:
        return _distribution_version(PACKAGE_DISTRIBUTION)
    except PackageNotFoundError:  # pragma: no cover - source checkout without install
        module = sys.modules.get("signalkit_stream")
        return str(getattr(module, "__version__", "unknown"))


@lru_cache(maxsize=1)
def default_user_agent() -> str:
    """Default User-Agent advertised by every first-party HTTP collector."""

    return f"signalkit-stream/{_package_version()} (+{PROJECT_URL})"


def redact_url(url: httpx.URL | str) -> str:
    """Return ``url`` with credentials, fragment and query values removed.

    Parameter *names* are preserved so operators can still tell which request
    failed, while secrets passed via ``?api_key=`` or ``https://user:pass@host``
    never reach logs, checkpoints or CLI output.
    """

    try:
        parsed = url if isinstance(url, httpx.URL) else httpx.URL(str(url))
    except (httpx.InvalidURL, ValueError, TypeError):
        return "<unparsable-url>"

    safe = parsed
    if parsed.query:
        names = [name for name, _ in parsed.params.multi_items()]
        query = str(httpx.QueryParams([(name, _REDACTED) for name in names]))
        safe = safe.copy_with(query=query.encode("ascii"))
    if parsed.userinfo:
        safe = safe.copy_with(userinfo=b"")
    if parsed.fragment:
        safe = safe.copy_with(fragment=None)
    return str(safe)


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

    async def aclose(self) -> None:
        """Release long-lived resources. Idempotent; no-op by default."""

        return None

    @abstractmethod
    async def collect(
        self,
        *,
        context: CollectorContext | None = None,
        cursor: Cursor | None = None,
    ) -> CollectorResult:
        """Collect one resumable batch and return normalized events plus a cursor."""


class HTTPCollector(Collector):
    """Collector base with shared retry/backoff and rate-limit inspection.

    Networking behavior owned here:

    * One ``httpx.AsyncClient`` per collector instance, created lazily on first
      use and reused for the instance lifetime. Constructing a client costs
      hundreds of milliseconds (it builds a fresh SSLContext from the certifi
      bundle) and is fully synchronous, so per-request construction blocks the
      event loop. Call :meth:`aclose` on shutdown to release an instance-owned
      client; an injected ``client=`` is never closed here.
    * Response bodies are streamed and capped at ``max_response_bytes``.
    * Redirects are followed manually: same-origin only by default, with a
      ``max_redirects`` ceiling. Cross-origin redirects are refused unless
      ``allow_cross_origin_redirects=True``, and when they are allowed every
      request header outside a small safe allowlist (plus any ``auth=``) is
      dropped, because httpx itself only strips ``Authorization``/``Cookie``.
    * Error messages are built from redacted URLs so query-string secrets never
      reach checkpoints, logs or CLI output.
    """

    def __init__(
        self,
        *,
        client: httpx.AsyncClient | None = None,
        timeout: float = 20.0,
        user_agent: str | None = None,
        retry_policy: RetryPolicy | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        max_response_bytes: int = DEFAULT_MAX_RESPONSE_BYTES,
        max_redirects: int = DEFAULT_MAX_REDIRECTS,
        allow_cross_origin_redirects: bool = False,
    ) -> None:
        if max_response_bytes < 1:
            raise ValueError("max_response_bytes must be >= 1")
        if max_redirects < 0:
            raise ValueError("max_redirects must be >= 0")

        self._client = client
        self._owns_client = client is None
        self._timeout = timeout
        self._user_agent = user_agent or default_user_agent()
        self._retry_policy = retry_policy or RetryPolicy()
        self._sleep = sleep
        self._rate_limit: RateLimitSnapshot | None = None
        self._max_response_bytes = int(max_response_bytes)
        self._max_redirects = int(max_redirects)
        self._allow_cross_origin_redirects = bool(allow_cross_origin_redirects)

    @property
    def rate_limit(self) -> RateLimitSnapshot | None:
        return self._rate_limit

    @property
    def max_response_bytes(self) -> int:
        return self._max_response_bytes

    # ------------------------------------------------------------------ client

    def _build_client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            timeout=self._timeout,
            headers={"User-Agent": self._user_agent},
            follow_redirects=False,
        )

    def _ensure_client(self) -> httpx.AsyncClient:
        client = self._client
        if client is None:
            client = self._build_client()
            self._client = client
            self._owns_client = True
        return client

    @asynccontextmanager
    async def http_client(self) -> AsyncIterator[httpx.AsyncClient]:
        """Yield this instance's cached client, creating it on first use.

        The client outlives the ``async with`` block on purpose: closing it per
        request forces a new SSLContext (and certifi bundle load) on every poll.
        """

        yield self._ensure_client()

    async def aclose(self) -> None:
        """Close the instance-owned client. Idempotent, and safe if none exists.

        A client supplied through ``client=`` belongs to the caller and is left
        open.
        """

        if not self._owns_client:
            return
        client = self._client
        self._client = None
        if client is not None:
            await client.aclose()

    # ----------------------------------------------------------------- request

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
        safe_url = redact_url(url)

        for attempt in range(1, policy.max_attempts + 1):
            if ctx.deadline is not None and datetime.now(UTC) >= ctx.deadline:
                raise CollectorError(
                    "collector deadline exceeded before HTTP request",
                    kind=CollectorErrorKind.TIMEOUT,
                    source_key=self.identity.key,
                    retryable=True,
                )

            try:
                response = await self._send(client, method, url, kwargs)
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
                        f"request timed out after {attempt} attempts: {safe_url}",
                        kind=CollectorErrorKind.TIMEOUT,
                        source_key=self.identity.key,
                        retryable=True,
                    ) from exc
                await self._sleep(self._retry_delay(attempt, None))
            except httpx.RequestError as exc:
                last_error = exc
                if attempt >= policy.max_attempts:
                    raise CollectorError(
                        f"network request failed after {attempt} attempts: {safe_url}",
                        kind=CollectorErrorKind.NETWORK,
                        source_key=self.identity.key,
                        retryable=True,
                    ) from exc
                await self._sleep(self._retry_delay(attempt, None))

        raise CollectorError(
            f"request failed: {safe_url}",
            kind=CollectorErrorKind.NETWORK,
            source_key=self.identity.key,
            retryable=True,
            details={"cause": repr(last_error)},
        )

    # --------------------------------------------------------- send + redirect

    async def _send(
        self,
        client: httpx.AsyncClient,
        method: str,
        url: str,
        kwargs: Mapping[str, object],
    ) -> httpx.Response:
        build_kwargs = dict(kwargs)
        build_kwargs.pop("follow_redirects", None)
        auth: object = build_kwargs.pop("auth", httpx.USE_CLIENT_DEFAULT)

        request = client.build_request(method, url, **build_kwargs)  # type: ignore[arg-type]
        hops = 0
        while True:
            response = await self._send_capped(client, request, auth=auth)
            if response.status_code not in _REDIRECT_STATUSES:
                return response
            location = response.headers.get("Location")
            if not location:
                return response
            if hops >= self._max_redirects:
                raise CollectorError(
                    f"exceeded {self._max_redirects} redirects for {redact_url(request.url)}",
                    kind=CollectorErrorKind.HTTP,
                    source_key=self.identity.key,
                    retryable=False,
                    status_code=response.status_code,
                )
            request, auth = self._redirect_request(client, request, response, location, auth)
            hops += 1

    def _redirect_request(
        self,
        client: httpx.AsyncClient,
        request: httpx.Request,
        response: httpx.Response,
        location: str,
        auth: object,
    ) -> tuple[httpx.Request, object]:
        origin = request.url
        try:
            target = origin.join(location.strip())
        except (httpx.InvalidURL, ValueError) as exc:
            raise CollectorError(
                f"invalid redirect target from {redact_url(origin)}",
                kind=CollectorErrorKind.HTTP,
                source_key=self.identity.key,
                retryable=False,
                status_code=response.status_code,
            ) from exc

        if target.scheme not in _ALLOWED_REDIRECT_SCHEMES:
            raise CollectorError(
                f"refusing redirect to unsupported scheme {target.scheme!r} "
                f"from {redact_url(origin)}",
                kind=CollectorErrorKind.HTTP,
                source_key=self.identity.key,
                retryable=False,
                status_code=response.status_code,
            )

        method = self._redirect_method(request.method, response.status_code)
        headers = {
            name: value
            for name, value in request.headers.multi_items()
            if name.lower() not in _REDIRECT_DROP_HEADERS
        }
        content: bytes | None = None
        if method == request.method:
            content = self._request_body(request)

        if not self._same_origin(origin, target) and not self._is_https_upgrade(origin, target):
            if not self._allow_cross_origin_redirects:
                raise CollectorError(
                    f"refusing cross-origin redirect from {redact_url(origin)} "
                    f"to {redact_url(target)}",
                    kind=CollectorErrorKind.HTTP,
                    source_key=self.identity.key,
                    retryable=False,
                    status_code=response.status_code,
                )
            headers = {
                name: value
                for name, value in headers.items()
                if name.lower() in _CROSS_ORIGIN_SAFE_HEADERS
            }
            auth = None

        return (
            client.build_request(method, target, headers=headers, content=content),
            auth,
        )

    @staticmethod
    def _redirect_method(method: str, status_code: int) -> str:
        upper = method.upper()
        if status_code == 303 and upper != "HEAD":
            return "GET"
        if status_code in {301, 302} and upper == "POST":
            return "GET"
        return method

    @staticmethod
    def _request_body(request: httpx.Request) -> bytes | None:
        try:
            body = request.content
        except httpx.RequestNotRead:  # pragma: no cover - streaming bodies unused here
            return None
        return body or None

    @staticmethod
    def _same_origin(origin: httpx.URL, target: httpx.URL) -> bool:
        return (
            origin.scheme == target.scheme
            and origin.host == target.host
            and origin.port == target.port
        )

    @staticmethod
    def _is_https_upgrade(origin: httpx.URL, target: httpx.URL) -> bool:
        """Allow the very common ``http://host/x`` -> ``https://host/x`` upgrade."""

        return (
            origin.scheme == "http"
            and target.scheme == "https"
            and origin.host == target.host
            and origin.port is None
            and target.port is None
        )

    # ------------------------------------------------------------- body limits

    async def _send_capped(
        self,
        client: httpx.AsyncClient,
        request: httpx.Request,
        *,
        auth: object,
    ) -> httpx.Response:
        response = await client.send(
            request,
            auth=auth,  # type: ignore[arg-type]
            stream=True,
            follow_redirects=False,
        )
        try:
            declared = self._safe_int(response.headers.get("Content-Length"))
            if declared is not None and declared > self._max_response_bytes:
                raise self._too_large_error(
                    request,
                    f"declared Content-Length {declared} bytes",
                )
            body = await self._read_capped(response, request)
        finally:
            await response.aclose()
        return self._materialize(response, body)

    async def _read_capped(self, response: httpx.Response, request: httpx.Request) -> bytes:
        chunks: list[bytes] = []
        total = 0
        async for chunk in response.aiter_bytes():
            total += len(chunk)
            if total > self._max_response_bytes:
                raise self._too_large_error(request, f"streamed at least {total} bytes")
            chunks.append(chunk)
        return b"".join(chunks)

    def _too_large_error(self, request: httpx.Request, detail: str) -> CollectorError:
        return CollectorError(
            f"response exceeded the {self._max_response_bytes} byte limit "
            f"({detail}) for {redact_url(request.url)}",
            kind=CollectorErrorKind.HTTP,
            source_key=self.identity.key,
            retryable=False,
        )

    @staticmethod
    def _materialize(response: httpx.Response, body: bytes) -> httpx.Response:
        """Rebuild a fully-buffered response so ``.content``/``.json()`` work.

        Content-coding headers are dropped because ``body`` is already decoded.
        """

        headers = [
            (name, value)
            for name, value in response.headers.multi_items()
            if name.lower() not in _BODY_HEADERS
        ]
        return httpx.Response(
            response.status_code,
            headers=headers,
            content=body,
            request=response.request,
            extensions=response.extensions,
            history=response.history,
        )

    # -------------------------------------------------------------- diagnostics

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
            f"HTTP {status} for {redact_url(response.request.url)}",
            kind=kind,
            source_key=self.identity.key,
            retryable=retryable,
            status_code=status,
            details={"response_preview": self._response_preview(response)},
        )

    @staticmethod
    def _response_preview(response: httpx.Response) -> str:
        """Preview the first bytes of the body without decoding all of it."""

        try:
            raw = response.content[:_ERROR_PREVIEW_BYTES]
        except httpx.ResponseNotRead:  # pragma: no cover - bodies are read by _send
            return ""
        return raw.decode(response.encoding or "utf-8", errors="replace")

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
