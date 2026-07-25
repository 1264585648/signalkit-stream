from datetime import UTC, datetime

from signalkit_stream.collectors.base import Collector
from signalkit_stream.models import SignalEvent, SignalKind
from signalkit_stream.protocol import CollectorContext, CollectorResult, Cursor


class ForumCollector(Collector):
    source = "my-forum"
    instance = "example-community"

    async def collect(
        self,
        *,
        context: CollectorContext | None = None,
        cursor: Cursor | None = None,
    ) -> CollectorResult:
        context = self.context(context)
        self.validate_cursor(cursor)

        # Replace this with the forum's official API/client. A real adapter should use
        # the source cursor/pagination token instead of this fixed example item.
        event = SignalEvent(
            id=SignalEvent.stable_id(
                self.source,
                "post-123",
                SignalKind.POST,
                source_instance=self.instance,
            ),
            source=self.source,
            source_instance=self.instance,
            kind=SignalKind.POST,
            title="Looking for an analytics tool",
            content="We need something that works with our current stack.",
            author="example-user",
            url="https://forum.example.com/posts/123",
            created_at=datetime.now(UTC),
            metadata={"community": "example"},
        )
        events = [event][: context.limit]
        next_cursor = Cursor(self.identity.key, {"last_id": "post-123"})
        return CollectorResult(
            events=events,
            cursor=next_cursor,
            has_more=False,
            primary_count=len(events),
        )
