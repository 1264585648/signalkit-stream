from datetime import UTC, datetime

from signalkit_stream.collectors.base import Collector
from signalkit_stream.models import SignalEvent, SignalKind


class ForumCollector(Collector):
    source = "my-forum"

    async def collect(self, *, limit: int = 100) -> list[SignalEvent]:
        # Replace this with the forum's official API/client.
        return [
            SignalEvent(
                id=SignalEvent.stable_id(self.source, "post-123", SignalKind.POST),
                source=self.source,
                kind=SignalKind.POST,
                title="Looking for an analytics tool",
                content="We need something that works with our current stack.",
                author="example-user",
                url="https://forum.example.com/posts/123",
                created_at=datetime.now(UTC),
                metadata={"community": "example"},
            )
        ][:limit]
