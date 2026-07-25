from datetime import UTC, datetime

import pytest

from signalkit_stream.collectors.base import Collector
from signalkit_stream.models import SignalEvent, SignalKind
from signalkit_stream.pipeline import run_collector
from signalkit_stream.storage import SQLiteSignalStore


class FakeCollector(Collector):
    source = "fake"

    async def collect(self, *, limit: int = 100) -> list[SignalEvent]:
        return [
            SignalEvent(
                id="sig_fake",
                source=self.source,
                kind=SignalKind.OTHER,
                content="hello",
                url="https://example.com/fake",
                created_at=datetime(2026, 7, 1, tzinfo=UTC),
            )
        ][:limit]


@pytest.mark.asyncio
async def test_pipeline_persists_events(tmp_path) -> None:
    with SQLiteSignalStore(tmp_path / "signals.db") as store:
        result = await run_collector(FakeCollector(), limit=10, store=store)
        assert result.inserted == 1
        assert len(result.events) == 1
        assert store.count() == 1
