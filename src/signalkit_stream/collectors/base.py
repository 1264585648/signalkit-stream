from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx

from signalkit_stream.models import SignalEvent


class Collector(ABC):
    """Interface implemented by every source adapter."""

    source: str

    @abstractmethod
    async def collect(self, *, limit: int = 100) -> list[SignalEvent]:
        """Collect and normalize up to ``limit`` primary items."""


class HTTPCollector(Collector):
    """Collector base with injectable HTTP transport for testing and customization."""

    def __init__(
        self,
        *,
        client: httpx.AsyncClient | None = None,
        timeout: float = 20.0,
        user_agent: str = "signalkit-stream/0.1 (+https://github.com/1264585648/signalkit-stream)",
    ) -> None:
        self._client = client
        self._timeout = timeout
        self._user_agent = user_agent

    @asynccontextmanager
    async def http_client(self) -> AsyncIterator[httpx.AsyncClient]:
        if self._client is not None:
            yield self._client
            return

        async with httpx.AsyncClient(
            timeout=self._timeout,
            headers={"User-Agent": self._user_agent},
            follow_redirects=True,
        ) as client:
            yield client
