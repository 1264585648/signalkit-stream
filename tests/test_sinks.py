from datetime import UTC, datetime
from io import StringIO
import json

import httpx
import pytest

from signalkit_stream.config import SinkConfig
from signalkit_stream.models import SignalEvent, SignalKind
from signalkit_stream.sinks import (
    JsonlSink,
    SinkError,
    StdoutSink,
    WebhookSink,
    default_sink_registry,
)


def event() -> SignalEvent:
    return SignalEvent(
        id="sig_sink",
        source="test",
        kind=SignalKind.POST,
        content="hello",
        url="https://example.com/1",
        created_at=datetime(2026, 7, 25, tzinfo=UTC),
    )


@pytest.mark.asyncio
async def test_stdout_and_jsonl_sinks(tmp_path) -> None:
    stream = StringIO()
    stdout = StdoutSink("out", stream=stream)
    archive = JsonlSink("archive", tmp_path / "events.jsonl")

    await stdout.send(event())
    await archive.send(event())

    assert json.loads(stream.getvalue())["id"] == "sig_sink"
    assert json.loads((tmp_path / "events.jsonl").read_text(encoding="utf-8"))["id"] == "sig_sink"


@pytest.mark.asyncio
async def test_webhook_sends_idempotency_headers_and_classifies_failure() -> None:
    requests: list[httpx.Request] = []

    def success(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(202, request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(success)) as client:
        sink = WebhookSink("brain", "https://example.com/hook", token="secret", client=client)
        await sink.send(event())

    request = requests[0]
    assert request.headers["Idempotency-Key"] == "signalkit:brain:sig_sink"
    assert request.headers["Authorization"] == "Bearer secret"

    def limited(request: httpx.Request) -> httpx.Response:
        return httpx.Response(429, headers={"Retry-After": "12"}, request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(limited)) as client:
        sink = WebhookSink("brain", "https://example.com/hook", client=client)
        with pytest.raises(SinkError) as caught:
            await sink.send(event())

    assert caught.value.retryable is True
    assert caught.value.status_code == 429
    assert caught.value.retry_after == 12


def test_default_sink_registry_validates_options(tmp_path) -> None:
    registry = default_sink_registry()
    sink = registry.create(
        SinkConfig("archive", "jsonl", options={"path": str(tmp_path / "out.jsonl")})
    )
    assert isinstance(sink, JsonlSink)
    assert registry.types == ("jsonl", "stdout", "webhook")

    with pytest.raises(ValueError, match="unknown stdout options"):
        registry.create(SinkConfig("bad", "stdout", options={"path": "x"}))
