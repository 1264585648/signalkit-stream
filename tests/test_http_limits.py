"""Response-size caps, redirect containment, URL redaction and User-Agent."""

from __future__ import annotations

import gzip
from collections.abc import AsyncIterator, Iterable

import httpx
import pytest

import signalkit_stream
from signalkit_stream.collectors import _base_impl
from signalkit_stream.collectors.base import Collector, HTTPCollector, RetryPolicy
from signalkit_stream.protocol import CollectorError, CollectorErrorKind, CollectorResult

SECRET = "s3cr3t-api-key"
NO_RETRIES = RetryPolicy(max_attempts=1, base_delay=0, jitter_ratio=0)


class StubCollector(HTTPCollector):
    source = "test-limits"
    instance = "default"

    async def collect(self, *, context=None, cursor=None) -> CollectorResult:
        raise NotImplementedError


class ChunkStream(httpx.AsyncByteStream):
    """Async byte stream that records how many chunks the consumer pulled."""

    def __init__(self, chunks: Iterable[bytes], pulled: list[int]) -> None:
        self._chunks = list(chunks)
        self._pulled = pulled

    async def __aiter__(self) -> AsyncIterator[bytes]:
        for chunk in self._chunks:
            self._pulled.append(len(chunk))
            yield chunk

    async def aclose(self) -> None:
        return None


class ChunkTransport(httpx.AsyncBaseTransport):
    def __init__(self, chunks: Iterable[bytes], pulled: list[int]) -> None:
        self._chunks = list(chunks)
        self._pulled = pulled

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"Content-Type": "application/xml"},
            stream=ChunkStream(self._chunks, self._pulled),
        )


def mock_client(handler, **kwargs) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler), **kwargs)


# --------------------------------------------------------------------- C2 caps


@pytest.mark.asyncio
async def test_oversized_content_length_is_rejected_before_the_body_is_read() -> None:
    pulled: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"Content-Length": "99999999", "Content-Type": "application/xml"},
            stream=ChunkStream([b"x" * 1024], pulled),
        )

    async with mock_client(handler) as client:
        collector = StubCollector(client=client, max_response_bytes=4096)
        with pytest.raises(CollectorError) as caught:
            await collector.request(client, "GET", "https://example.com/big.xml")

    assert caught.value.kind is CollectorErrorKind.HTTP
    assert caught.value.retryable is False
    assert "99999999" in str(caught.value)
    assert "4096 byte limit" in str(caught.value)
    assert pulled == []


@pytest.mark.asyncio
async def test_streamed_body_aborts_as_soon_as_the_cap_is_exceeded() -> None:
    pulled: list[int] = []
    chunks = [b"x" * 1024] * 200  # 200 KiB advertised via chunked transfer

    async with httpx.AsyncClient(transport=ChunkTransport(chunks, pulled)) as client:
        collector = StubCollector(client=client, max_response_bytes=8192)
        with pytest.raises(CollectorError, match="8192 byte limit"):
            await collector.request(client, "GET", "https://example.com/huge.xml")

    # 8 chunks fill the cap, the 9th trips it; the remaining 191 are never buffered.
    assert len(pulled) == 9
    assert sum(pulled) == 9216


@pytest.mark.asyncio
async def test_body_within_the_cap_is_returned_with_working_content_and_json() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"items": [1, 2, 3]}, request=request)

    async with mock_client(handler) as client:
        collector = StubCollector(client=client, max_response_bytes=4096)
        response = await collector.request(client, "GET", "https://example.com/feed.json")

    assert response.status_code == 200
    assert response.json() == {"items": [1, 2, 3]}
    assert response.content == b'{"items":[1,2,3]}'


@pytest.mark.asyncio
async def test_gzip_encoded_body_is_decoded_once_and_content_is_intact() -> None:
    payload = b"<rss><channel><title>gz</title></channel></rss>"
    pulled: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"Content-Encoding": "gzip", "Content-Type": "application/xml"},
            stream=ChunkStream([gzip.compress(payload)], pulled),
        )

    async with mock_client(handler) as client:
        collector = StubCollector(client=client)
        response = await collector.request(client, "GET", "https://example.com/feed.xml")

    assert response.content == payload
    assert response.text == payload.decode()


@pytest.mark.asyncio
async def test_conditional_get_304_is_still_returned_untouched() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["If-None-Match"] == '"abc"'
        return httpx.Response(304, headers={"ETag": '"abc"'}, request=request)

    async with mock_client(handler) as client:
        collector = StubCollector(client=client)
        response = await collector.request(
            client,
            "GET",
            "https://example.com/feed.xml",
            headers={"If-None-Match": '"abc"'},
        )

    assert response.status_code == 304
    assert response.headers["ETag"] == '"abc"'
    assert response.content == b""


@pytest.mark.asyncio
async def test_max_response_bytes_is_configurable_and_validated() -> None:
    assert StubCollector().max_response_bytes == _base_impl.DEFAULT_MAX_RESPONSE_BYTES
    assert StubCollector(max_response_bytes=123).max_response_bytes == 123
    with pytest.raises(ValueError, match="max_response_bytes"):
        StubCollector(max_response_bytes=0)
    with pytest.raises(ValueError, match="max_redirects"):
        StubCollector(max_redirects=-1)
    with pytest.raises(ValueError, match="cross_origin_redirects"):
        StubCollector(cross_origin_redirects="maybe")


# ---------------------------------------------------------------- C3 redirects


@pytest.mark.asyncio
async def test_credentialed_cross_origin_redirect_is_refused_not_replayed() -> None:
    """httpx strips only Authorization/Cookie, so X-Api-Key used to leak."""

    seen: list[tuple[str, str | None]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((str(request.url), request.headers.get("X-Api-Key")))
        if request.url.host == "api.example.com":
            return httpx.Response(302, headers={"Location": "https://attacker.tld/"})
        return httpx.Response(200, json={"ok": True})

    async with mock_client(handler, follow_redirects=True) as client:
        collector = StubCollector(client=client, retry_policy=NO_RETRIES)
        with pytest.raises(CollectorError) as caught:
            await collector.request(
                client,
                "GET",
                "https://api.example.com/v1/items",
                headers={"X-Api-Key": SECRET},
            )

    assert "refusing cross-origin redirect" in str(caught.value)
    assert caught.value.kind is CollectorErrorKind.HTTP
    assert caught.value.retryable is False
    assert [host for host, _ in seen] == ["https://api.example.com/v1/items"]
    assert all(header != SECRET for _, header in seen[1:])


@pytest.mark.asyncio
async def test_anonymous_cross_origin_redirect_is_followed_with_headers_stripped() -> None:
    """Mirrors the real http://blog.golang.org/feed.atom -> https://go.dev hop."""

    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(str(request.url))
        if request.url.host == "blog.example.org":
            return httpx.Response(301, headers={"Location": "https://go.example.dev/blog/feed"})
        return httpx.Response(200, text="<feed/>")

    async with mock_client(handler) as client:
        collector = StubCollector(client=client, retry_policy=NO_RETRIES)
        response = await collector.request(
            client,
            "GET",
            "https://blog.example.org/feed.atom",
            headers={"If-None-Match": '"etag"'},
        )

    assert response.status_code == 200
    assert seen == ["https://blog.example.org/feed.atom", "https://go.example.dev/blog/feed"]


@pytest.mark.asyncio
async def test_never_policy_refuses_even_an_anonymous_cross_origin_redirect() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "blog.example.org":
            return httpx.Response(301, headers={"Location": "https://go.example.dev/blog/feed"})
        return httpx.Response(200, text="<feed/>")

    async with mock_client(handler) as client:
        collector = StubCollector(
            client=client,
            retry_policy=NO_RETRIES,
            cross_origin_redirects="never",
        )
        with pytest.raises(CollectorError, match="refusing cross-origin redirect"):
            await collector.request(client, "GET", "https://blog.example.org/feed.atom")


@pytest.mark.asyncio
async def test_client_level_auth_makes_a_cross_origin_hop_credentialed() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "a.example":
            return httpx.Response(302, headers={"Location": "https://b.example/x"})
        return httpx.Response(200, json={})

    async with mock_client(handler, auth=httpx.BasicAuth("u", SECRET)) as client:
        collector = StubCollector(client=client, retry_policy=NO_RETRIES)
        with pytest.raises(CollectorError, match="refusing cross-origin redirect"):
            await collector.request(client, "GET", "https://a.example/x")


@pytest.mark.asyncio
async def test_cookie_jar_is_not_replayed_to_a_new_origin() -> None:
    seen: list[str | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.headers.get("Cookie"))
        if request.url.host == "a.example":
            return httpx.Response(302, headers={"Location": "https://b.example/x"})
        return httpx.Response(200, json={})

    async with mock_client(handler, cookies={"session": SECRET}) as client:
        collector = StubCollector(
            client=client,
            retry_policy=NO_RETRIES,
            cross_origin_redirects="always",
        )
        await collector.request(client, "GET", "https://a.example/x")

    assert seen[0] is not None and SECRET in seen[0]
    assert seen[1] is None


@pytest.mark.asyncio
async def test_always_policy_strips_every_non_safe_header_cross_origin() -> None:
    seen: list[httpx.Headers] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.headers)
        if request.url.host == "api.example.com":
            return httpx.Response(302, headers={"Location": "https://cdn.example.org/items"})
        return httpx.Response(200, json={"ok": True})

    async with mock_client(handler) as client:
        collector = StubCollector(
            client=client,
            retry_policy=NO_RETRIES,
            cross_origin_redirects="always",
        )
        response = await collector.request(
            client,
            "GET",
            "https://api.example.com/v1/items",
            headers={
                "X-Api-Key": SECRET,
                "X-Auth-Token": SECRET,
                "Private-Token": SECRET,
                "Authorization": f"Bearer {SECRET}",
                "If-None-Match": '"etag"',
            },
        )

    assert response.status_code == 200
    assert len(seen) == 2
    first, second = seen
    assert first["X-Api-Key"] == SECRET
    for leaky in ("X-Api-Key", "X-Auth-Token", "Private-Token", "Authorization"):
        assert leaky not in second
    assert SECRET not in "\n".join(f"{k}: {v}" for k, v in second.items())
    # Conditional-GET and identity headers survive because they carry no secret.
    assert second["If-None-Match"] == '"etag"'
    assert second["Host"] == "cdn.example.org"
    assert second["User-Agent"] == first["User-Agent"]


@pytest.mark.asyncio
async def test_cross_origin_redirect_drops_injected_auth_credentials() -> None:
    seen: list[str | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.headers.get("Authorization"))
        if request.url.host == "www.example.com":
            return httpx.Response(302, headers={"Location": "https://other.example.net/t"})
        return httpx.Response(200, json={"ok": True})

    async with mock_client(handler) as client:
        collector = StubCollector(
            client=client,
            retry_policy=NO_RETRIES,
            cross_origin_redirects="always",
        )
        await collector.request(
            client,
            "POST",
            "https://www.example.com/api/v1/access_token",
            auth=httpx.BasicAuth("id", SECRET),
            data={"grant_type": "client_credentials"},
        )

    assert seen[0] is not None
    assert seen[1] is None


@pytest.mark.asyncio
async def test_same_origin_redirect_is_followed_and_keeps_auth_headers() -> None:
    seen: list[tuple[str, str | None]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.url.path, request.headers.get("X-Api-Key")))
        if request.url.path == "/v1/items":
            return httpx.Response(301, headers={"Location": "/v2/items"})
        return httpx.Response(200, json={"ok": True})

    async with mock_client(handler) as client:
        collector = StubCollector(client=client, retry_policy=NO_RETRIES)
        response = await collector.request(
            client,
            "GET",
            "https://api.example.com/v1/items",
            headers={"X-Api-Key": SECRET},
        )

    assert response.status_code == 200
    assert seen == [("/v1/items", SECRET), ("/v2/items", SECRET)]


@pytest.mark.asyncio
async def test_http_to_https_upgrade_on_the_same_host_is_followed() -> None:
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(str(request.url))
        if request.url.scheme == "http":
            return httpx.Response(301, headers={"Location": "https://feeds.example.com/rss"})
        return httpx.Response(200, text="<rss/>")

    async with mock_client(handler) as client:
        collector = StubCollector(client=client, retry_policy=NO_RETRIES)
        response = await collector.request(client, "GET", "http://feeds.example.com/rss")

    assert response.status_code == 200
    assert seen == ["http://feeds.example.com/rss", "https://feeds.example.com/rss"]


@pytest.mark.asyncio
async def test_redirect_chain_is_bounded_by_max_redirects() -> None:
    hops = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal hops
        hops += 1
        return httpx.Response(302, headers={"Location": f"/hop/{hops}"})

    async with mock_client(handler) as client:
        collector = StubCollector(client=client, retry_policy=NO_RETRIES, max_redirects=2)
        with pytest.raises(CollectorError, match="exceeded 2 redirects"):
            await collector.request(client, "GET", "https://example.com/start")

    assert hops == 3


@pytest.mark.asyncio
async def test_redirect_to_a_non_http_scheme_is_refused() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(302, headers={"Location": "file:///etc/passwd"})

    async with mock_client(handler) as client:
        collector = StubCollector(client=client, retry_policy=NO_RETRIES)
        with pytest.raises(CollectorError, match="unsupported scheme"):
            await collector.request(client, "GET", "https://example.com/start")


@pytest.mark.asyncio
async def test_redirect_without_location_is_returned_as_is() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(302, request=request)

    async with mock_client(handler) as client:
        collector = StubCollector(client=client, retry_policy=NO_RETRIES)
        response = await collector.request(client, "GET", "https://example.com/start")

    assert response.status_code == 302


@pytest.mark.asyncio
async def test_303_redirect_downgrades_post_to_get_and_drops_the_body() -> None:
    seen: list[tuple[str, bytes]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.method, request.content))
        if request.url.path == "/submit":
            return httpx.Response(303, headers={"Location": "/result"})
        return httpx.Response(200, json={"ok": True})

    async with mock_client(handler) as client:
        collector = StubCollector(client=client, retry_policy=NO_RETRIES)
        await collector.request(
            client,
            "POST",
            "https://example.com/submit",
            json={"a": 1},
        )

    assert seen[0][0] == "POST"
    assert seen[1] == ("GET", b"")


@pytest.mark.asyncio
async def test_307_redirect_preserves_method_and_body() -> None:
    seen: list[tuple[str, bytes]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.method, request.content))
        if request.url.path == "/submit":
            return httpx.Response(307, headers={"Location": "/moved"})
        return httpx.Response(200, json={"ok": True})

    async with mock_client(handler) as client:
        collector = StubCollector(client=client, retry_policy=NO_RETRIES)
        await collector.request(client, "POST", "https://example.com/submit", json={"a": 1})

    assert seen[0] == ("POST", b'{"a":1}')
    assert seen[1] == ("POST", b'{"a":1}')


@pytest.mark.asyncio
async def test_collector_owned_client_disables_httpx_redirect_following() -> None:
    collector = StubCollector()
    async with collector.http_client() as client:
        assert client.follow_redirects is False
    await collector.aclose()


# --------------------------------------------------------------- C4 redaction


@pytest.mark.asyncio
async def test_http_error_message_does_not_leak_query_string_secrets() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="upstream exploded", request=request)

    async with mock_client(handler) as client:
        collector = StubCollector(client=client, retry_policy=NO_RETRIES)
        with pytest.raises(CollectorError) as caught:
            await collector.request(
                client,
                "GET",
                "https://api.example.com/v1/items",
                params={"api_key": SECRET, "since": "2026-01-01"},
            )

    message = str(caught.value)
    assert SECRET not in message
    assert "api_key=REDACTED" in message
    assert "since=REDACTED" in message
    assert "https://api.example.com/v1/items" in message
    assert caught.value.status_code == 500


@pytest.mark.asyncio
async def test_in_url_token_is_redacted_in_timeout_and_network_messages() -> None:
    url = f"https://feeds.example.com/rss?token={SECRET}"

    def timeout_handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectTimeout("boom", request=request)

    def network_handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("boom", request=request)

    async with mock_client(timeout_handler) as client:
        collector = StubCollector(client=client, retry_policy=NO_RETRIES)
        with pytest.raises(CollectorError) as timed_out:
            await collector.request(client, "GET", url)

    async with mock_client(network_handler) as client:
        collector = StubCollector(client=client, retry_policy=NO_RETRIES)
        with pytest.raises(CollectorError) as failed:
            await collector.request(client, "GET", url)

    assert timed_out.value.kind is CollectorErrorKind.TIMEOUT
    assert failed.value.kind is CollectorErrorKind.NETWORK
    for exc in (timed_out.value, failed.value):
        assert SECRET not in str(exc)
        assert "token=REDACTED" in str(exc)


def test_redact_url_strips_credentials_and_fragments() -> None:
    redacted = _base_impl.redact_url(f"https://user:{SECRET}@h.example/x?a=1&a=2&b=3#frag")
    assert SECRET not in redacted
    assert "user" not in redacted
    assert "frag" not in redacted
    assert redacted == "https://h.example/x?a=REDACTED&a=REDACTED&b=REDACTED"
    assert _base_impl.redact_url("https://h.example/x") == "https://h.example/x"


@pytest.mark.asyncio
async def test_error_preview_slices_bytes_before_decoding() -> None:
    body = b"\xff\xfe" + b"A" * 5000

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            404,
            headers={"Content-Type": "application/octet-stream"},
            content=body,
            request=request,
        )

    async with mock_client(handler) as client:
        collector = StubCollector(client=client, retry_policy=NO_RETRIES)
        with pytest.raises(CollectorError) as caught:
            await collector.request(client, "GET", "https://example.com/missing")

    preview = caught.value.details["response_preview"]
    assert len(preview) == 300
    assert preview.endswith("A")


# ------------------------------------------------------------- C5 user agent


def test_default_user_agent_reports_the_installed_package_version() -> None:
    agent = _base_impl.default_user_agent()
    assert agent.startswith(f"signalkit-stream/{signalkit_stream.__version__} ")
    assert "signalkit-stream/0.2 " not in agent


@pytest.mark.asyncio
async def test_owned_client_sends_the_versioned_user_agent_by_default() -> None:
    expected = (
        f"signalkit-stream/{signalkit_stream.__version__} "
        "(+https://github.com/1264585648/signalkit-stream)"
    )
    collector = StubCollector()
    async with collector.http_client() as client:
        # httpx merges client-level headers into every outgoing request.
        assert client.headers["User-Agent"] == expected
    await collector.aclose()


def test_explicit_user_agent_still_wins() -> None:
    collector = StubCollector(user_agent="custom/1.0")
    assert collector._user_agent == "custom/1.0"


# --------------------------------------------------------------- C6 shim gone


def test_collectors_base_is_a_plain_reexport_of_the_implementation() -> None:
    assert HTTPCollector is _base_impl.HTTPCollector
    assert Collector is _base_impl.Collector
    assert RetryPolicy is _base_impl.RetryPolicy
    # No shim subclass between the public name and the implementation.
    assert HTTPCollector.__mro__[:2] == (_base_impl.HTTPCollector, _base_impl.Collector)


@pytest.mark.asyncio
async def test_allow_statuses_shim_keyword_is_gone() -> None:
    async with mock_client(lambda request: httpx.Response(200, json={})) as client:
        collector = StubCollector(client=client, retry_policy=NO_RETRIES)
        with pytest.raises(TypeError):
            await collector.request(
                client,
                "GET",
                "https://example.com/x",
                allow_statuses={304},
            )
