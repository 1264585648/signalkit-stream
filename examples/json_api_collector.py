"""Reference adapter for a cursor-paginated JSON REST API.

This file is intentionally an example rather than a universal scraper. Real APIs have
source-specific authentication, pagination, ordering, and rate-limit semantics. Copy
this adapter into your integration and make those semantics explicit.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Mapping

import httpx

from signalkit_stream.collectors.base import HTTPCollector
from signalkit_stream.models import SignalEvent, SignalKind
from signalkit_stream.protocol import CollectorContext, CollectorResult, Cursor


class ExampleJSONAPICollector(HTTPCollector):
    source = "example-api"

    def __init__(
        self,
        url: str,
        *,
        instance: str = "default",
        token: str | None = None,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        super().__init__(client=client)
        self.url = url
        self.instance = instance
        self.token = token

    async def collect(
        self,
        *,
        context: CollectorContext | None = None,
        cursor: Cursor | None = None,
    ) -> CollectorResult:
        ctx = self.context(context)
        self.validate_cursor(cursor)
        state = dict(cursor.state) if cursor else {}
        continuation = state.get("continuation")
        params: dict[str, object] = {"limit": min(ctx.limit, 100)}
        if continuation:
            params["cursor"] = str(continuation)

        headers: dict[str, str] = {"Accept": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"

        async with self.http_client() as client:
            response = await self.request(
                client,
                "GET",
                self.url,
                context=ctx,
                params=params,
                headers=headers,
            )

        payload = response.json()
        if not isinstance(payload, Mapping):
            raise ValueError("expected a JSON object")
        raw_items = payload.get("items", [])
        if not isinstance(raw_items, list):
            raise ValueError("expected an items array")

        events = [self._event(item) for item in raw_items if isinstance(item, Mapping)]
        next_value = payload.get("next_cursor")
        next_cursor = Cursor(
            self.identity.key,
            {"continuation": str(next_value) if next_value else None},
        )
        return CollectorResult(
            events=events,
            cursor=next_cursor,
            has_more=bool(next_value),
            primary_count=len(events),
            rate_limit=self.rate_limit,
        )

    def _event(self, item: Mapping[str, Any]) -> SignalEvent:
        external_id = str(item["id"])
        created = datetime.fromisoformat(str(item["created_at"]).replace("Z", "+00:00"))
        if created.tzinfo is None:
            created = created.replace(tzinfo=UTC)
        return SignalEvent(
            id=SignalEvent.stable_id(
                self.source,
                external_id,
                SignalKind.POST,
                source_instance=self.instance,
            ),
            source=self.source,
            source_instance=self.instance,
            kind=SignalKind.POST,
            title=str(item.get("title") or "") or None,
            content=str(item.get("body") or ""),
            author=str(item.get("author") or "") or None,
            url=str(item["url"]),
            created_at=created,
            metadata={"external_id": external_id},
        )
