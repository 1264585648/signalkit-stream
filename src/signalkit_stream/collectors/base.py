from __future__ import annotations

from collections.abc import Collection

import httpx

from signalkit_stream.collectors._base_impl import (
    Collector,
    HTTPCollector as _HTTPCollector,
    RetryPolicy,
)
from signalkit_stream.protocol import CollectorContext


class HTTPCollector(_HTTPCollector):
    """HTTP collector compatibility layer for explicitly accepted statuses.

    The underlying retry implementation already returns every response below 400.
    `allow_statuses` makes that contract explicit for adapters such as conditional
    GET collectors that intentionally handle HTTP 304 themselves, without leaking
    the adapter-only keyword into `httpx.AsyncClient.request`.
    """

    async def request(
        self,
        client: httpx.AsyncClient,
        method: str,
        url: str,
        *,
        context: CollectorContext | None = None,
        allow_statuses: Collection[int] | None = None,
        **kwargs: object,
    ) -> httpx.Response:
        allowed = frozenset(allow_statuses or ())
        unsupported = sorted(status for status in allowed if status >= 400)
        if unsupported:
            raise ValueError(
                "allow_statuses currently supports only statuses below 400; unsupported: "
                + ", ".join(str(status) for status in unsupported)
            )
        return await super().request(
            client,
            method,
            url,
            context=context,
            **kwargs,
        )


__all__ = ["Collector", "HTTPCollector", "RetryPolicy"]
