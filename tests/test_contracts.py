from datetime import UTC, datetime

import pytest

from signalkit_stream.collectors.base import Collector
from signalkit_stream.contracts import validate_collector_result
from signalkit_stream.models import SignalEvent, SignalKind
from signalkit_stream.pipeline import run_collector
from signalkit_stream.protocol import (
    CollectorContext,
    CollectorError,
    CollectorErrorKind,
    CollectorResult,
    Cursor,
)
from signalkit_stream.storage import SQLiteSignalStore


def make_event(event_id: str = "sig_ok", *, source: str = "fake") -> SignalEvent:
    return SignalEvent(
        id=event_id,
        source=source,
        kind=SignalKind.OTHER,
        content="hello",
        url="https://example.com/item",
        created_at=datetime(2026, 7, 25, tzinfo=UTC),
    )


class FakeCollector(Collector):
    source = "fake"

    def __init__(self, result: CollectorResult) -> None:
        self.result = result

    async def collect(
        self,
        *,
        context: CollectorContext | None = None,
        cursor: Cursor | None = None,
    ) -> CollectorResult:
        return self.result


def test_valid_result_satisfies_contract() -> None:
    collector = FakeCollector(
        CollectorResult(
            events=[make_event()],
            cursor=Cursor("fake:default", {"page": 1}),
            primary_count=1,
        )
    )

    validate_collector_result(
        collector,
        collector.result,
        context=CollectorContext(limit=10),
    )


@pytest.mark.parametrize(
    ("result", "message"),
    [
        (
            CollectorResult(events=[], primary_count=11),
            "exceeds context.limit",
        ),
        (
            CollectorResult(events=[], has_more=True, cursor=None, primary_count=1),
            "requires a continuation cursor",
        ),
        (
            CollectorResult(
                events=[make_event(source="other")],
                primary_count=1,
            ),
            "belongs to 'other:default'",
        ),
        (
            CollectorResult(
                events=[make_event("same"), make_event("same")],
                primary_count=1,
            ),
            "duplicate event id",
        ),
        (
            CollectorResult(
                events=[],
                cursor=Cursor("other:default", {}),
                primary_count=0,
            ),
            "cursor belongs to",
        ),
    ],
)
def test_invalid_result_raises_nonretryable_contract_error(
    result: CollectorResult,
    message: str,
) -> None:
    collector = FakeCollector(result)

    with pytest.raises(CollectorError, match=message) as caught:
        validate_collector_result(
            collector,
            result,
            context=CollectorContext(limit=10),
        )

    assert caught.value.kind is CollectorErrorKind.INTERNAL
    assert caught.value.retryable is False


@pytest.mark.asyncio
async def test_pipeline_rejects_invalid_page_before_event_or_checkpoint_commit(tmp_path) -> None:
    collector = FakeCollector(
        CollectorResult(
            events=[make_event(source="wrong")],
            cursor=Cursor("fake:default", {"page": 1}),
            primary_count=1,
        )
    )

    with SQLiteSignalStore(tmp_path / "signals.db") as store:
        with pytest.raises(CollectorError, match="contract violation"):
            await run_collector(collector, limit=10, store=store)
        assert store.count() == 0
        assert store.get_checkpoint("fake:default") is None
