from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
import logging

from signalkit_stream.collectors.base import Collector, HTTPCollector
from signalkit_stream.config import SourceConfig, StreamConfig
from signalkit_stream.pipeline import CollectionResult, run_collector
from signalkit_stream.protocol import CollectorError, CollectorErrorKind, RateLimitSnapshot
from signalkit_stream.registry import CollectorRegistry, default_registry
from signalkit_stream.storage import SQLiteSignalStore, SourceHealth

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class RuntimeSource:
    config: SourceConfig
    collector: Collector
    failures: int = 0
    total_runs: int = 0
    total_events: int = 0
    last_success_at: datetime | None = None


@dataclass(slots=True, frozen=True)
class SourceRunResult:
    name: str
    source_key: str
    success: bool
    delay: float
    collection: CollectionResult | None = None
    error: str | None = None
    status: str = "healthy"


class StreamRuntime:
    """Long-running scheduler for independent collector workers."""

    def __init__(
        self,
        config: StreamConfig,
        store: SQLiteSignalStore,
        *,
        registry: CollectorRegistry | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self.config = config
        self.store = store
        self.registry = registry or default_registry()
        self._sleep = sleep
        self._now = now
        self._semaphore = asyncio.Semaphore(config.runtime.concurrency)
        self.sources = tuple(
            RuntimeSource(source, self.registry.create(source))
            for source in config.sources
            if source.enabled
        )
        keys = [runtime.collector.identity.key for runtime in self.sources]
        duplicates = sorted({key for key in keys if keys.count(key) > 1})
        if duplicates:
            raise ValueError(f"duplicate collector source identities: {', '.join(duplicates)}")
        self._restore_health()

    def _restore_health(self) -> None:
        for source in self.sources:
            health = self.store.get_source_health(source.collector.identity.key)
            if health is None:
                continue
            source.failures = health.consecutive_failures
            source.total_runs = health.total_runs
            source.total_events = health.total_events
            source.last_success_at = health.last_success_at

    async def run_once(self) -> list[SourceRunResult]:
        """Run every enabled source once, respecting global concurrency."""

        return list(await asyncio.gather(*(self._run_source(source) for source in self.sources)))

    async def run_forever(self, stop_event: asyncio.Event | None = None) -> None:
        """Run one worker per source until stopped, then cancel in-flight workers."""

        stop = stop_event or asyncio.Event()
        tasks = [
            asyncio.create_task(self._source_loop(source, stop), name=f"signalkit:{source.config.name}")
            for source in self.sources
        ]
        if not tasks:
            return
        try:
            await stop.wait()
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _source_loop(self, source: RuntimeSource, stop: asyncio.Event) -> None:
        while not stop.is_set():
            result = await self._run_source(source)
            if stop.is_set():
                break
            await self._sleep_or_stop(result.delay, stop)

    async def _sleep_or_stop(self, delay: float, stop: asyncio.Event) -> None:
        if delay <= 0:
            await asyncio.sleep(0)
            return
        sleep_task = asyncio.create_task(self._sleep(delay))
        stop_task = asyncio.create_task(stop.wait())
        done, pending = await asyncio.wait(
            {sleep_task, stop_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        for task in done:
            if task is sleep_task:
                task.result()

    async def _run_source(self, source: RuntimeSource) -> SourceRunResult:
        attempted_at = self._now()
        async with self._semaphore:
            try:
                collection = await run_collector(
                    source.collector,
                    limit=source.config.limit,
                    store=self.store,
                    resume=True,
                )
            except asyncio.CancelledError:
                raise
            except CollectorError as exc:
                return self._failure(source, attempted_at, exc)
            except Exception as exc:
                wrapped = CollectorError(
                    f"unexpected collector failure: {exc}",
                    source_key=source.collector.identity.key,
                    retryable=False,
                )
                return self._failure(source, attempted_at, wrapped)

        source.failures = 0
        source.total_runs += 1
        source.total_events += len(collection.events)
        source.last_success_at = self._now()
        delay = self._success_delay(source.config.interval, collection.rate_limit)
        self._persist_health(source, "healthy", attempted_at, None)
        logger.info(
            "source=%s status=healthy primary=%s events=%s inserted=%s updated=%s next=%.1fs",
            source.collector.identity.key,
            collection.primary_count,
            len(collection.events),
            collection.inserted,
            collection.updated,
            delay,
        )
        return SourceRunResult(
            name=source.config.name,
            source_key=source.collector.identity.key,
            success=True,
            delay=delay,
            collection=collection,
            status="healthy",
        )

    def _failure(
        self,
        source: RuntimeSource,
        attempted_at: datetime,
        error: CollectorError,
    ) -> SourceRunResult:
        source.failures += 1
        source.total_runs += 1
        status = (
            "circuit_open"
            if source.failures >= self.config.runtime.failure_threshold
            else "degraded"
        )
        delay = self._failure_delay(source, error)
        self._persist_health(source, status, attempted_at, str(error))
        logger.warning(
            "source=%s status=%s failures=%s error=%s next=%.1fs",
            source.collector.identity.key,
            status,
            source.failures,
            error,
            delay,
        )
        return SourceRunResult(
            name=source.config.name,
            source_key=source.collector.identity.key,
            success=False,
            delay=delay,
            error=str(error),
            status=status,
        )

    def _success_delay(self, interval: float, rate_limit: RateLimitSnapshot | None) -> float:
        return max(interval, self._rate_limit_delay(rate_limit))

    def _failure_delay(self, source: RuntimeSource, error: CollectorError) -> float:
        if source.failures >= self.config.runtime.failure_threshold:
            return self.config.runtime.circuit_cooldown
        rate_limit = source.collector.rate_limit if isinstance(source.collector, HTTPCollector) else None
        if error.kind is CollectorErrorKind.RATE_LIMIT:
            limited_delay = self._rate_limit_delay(rate_limit)
            if limited_delay > 0:
                return limited_delay
        exponent = max(0, source.failures - 1)
        return min(
            source.config.interval,
            self.config.runtime.failure_backoff_base * (2**exponent),
        )

    def _rate_limit_delay(self, rate_limit: RateLimitSnapshot | None) -> float:
        if rate_limit is None:
            return 0.0
        if rate_limit.retry_after is not None:
            return max(0.0, rate_limit.retry_after)
        if rate_limit.remaining == 0 and rate_limit.reset_at is not None:
            return max(0.0, (rate_limit.reset_at - self._now()).total_seconds())
        return 0.0

    def _persist_health(
        self,
        source: RuntimeSource,
        status: str,
        attempted_at: datetime,
        error: str | None,
    ) -> None:
        self.store.upsert_source_health(
            SourceHealth(
                source_key=source.collector.identity.key,
                status=status,
                updated_at=self._now(),
                last_attempt_at=attempted_at,
                last_success_at=source.last_success_at,
                last_error=error,
                consecutive_failures=source.failures,
                total_runs=source.total_runs,
                total_events=source.total_events,
            )
        )
