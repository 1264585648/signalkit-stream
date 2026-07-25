from datetime import UTC, datetime

import pytest

from signalkit_stream.collectors.base import Collector
from signalkit_stream.models import SignalEvent, SignalKind
from signalkit_stream.pipeline import run_collector
from signalkit_stream.protocol import CollectorContext, CollectorResult, Cursor
from signalkit_stream.storage import SQLiteSignalStore


class PagingCollector(Collector):
    source = "fake"
    instance = "paging"

    async def collect(self, *, context=None, cursor=None) -> CollectorResult:
        context = context or CollectorContext()
        self.validate_cursor(cursor)
        position = int(cursor.state.get("position", 0)) if cursor else 0
        remaining = 5 - position
        count = min(context.limit, 2, max(0, remaining))
        events = [
            SignalEvent(
                id=f"sig_{position + index}",
                source=self.source,
                source_instance=self.instance,
                kind=SignalKind.OTHER,
                content="hello",
                url=f"https://example.com/{position + index}",
                created_at=datetime(2026, 7, 1, tzinfo=UTC),
            )
            for index in range(count)
        ]
        next_position = position + count
        return CollectorResult(
            events=events,
            cursor=Cursor(self.identity.key, {"position": next_position}),
            has_more=next_position < 5,
            primary_count=count,
        )


@pytest.mark.asyncio
async def test_pipeline_pages_and_persists_checkpoint(tmp_path) -> None:
    with SQLiteSignalStore(tmp_path / "signals.db") as store:
        result = await run_collector(PagingCollector(), limit=5, store=store)
        checkpoint = store.get_checkpoint("fake:paging")

        assert result.inserted == 5
        assert result.primary_count == 5
        assert result.pages == 3
        assert store.count() == 5
        assert checkpoint is not None
        assert checkpoint.cursor.state["position"] == 5


@pytest.mark.asyncio
async def test_pipeline_resumes_from_checkpoint(tmp_path) -> None:
    with SQLiteSignalStore(tmp_path / "signals.db") as store:
        first = await run_collector(PagingCollector(), limit=2, store=store)
        second = await run_collector(PagingCollector(), limit=2, store=store)

        assert [event.id for event in first.events] == ["sig_0", "sig_1"]
        assert [event.id for event in second.events] == ["sig_2", "sig_3"]
        assert store.count() == 4
