from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol

from signalkit_stream.collectors.base import Collector
from signalkit_stream.config import SourceConfig, StreamConfig
from signalkit_stream.health import RuntimeStateStore, SourceHealth
from signalkit_stream.pipeline import CollectionResult, run_collector
from signalkit_stream.protocol import CollectorError, CollectorErrorKind, RateLimitSnapshot
from signalkit_stream.registry import CollectorRegistry, default_registry
from signalkit_stream.storage import SignalStore


class Clock(Protocol):
    def now(self) -> datetime: ...

    async def sleep(self, seconds: float) -> None: ...


class SystemClock:
    def now(self) -> datetime:
        return datetime.now(UTC)

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(seconds)


@dataclass(slots=True, frozen=True)
class SourceRunOutcome:
    name: str
    source_key: str
    next_delay: float
    result: CollectionResult | None = None
    error: str | None = None
    skipped: bool = False
    reason: str | None = None

    @property
    def ok(self) -> bool:
        return self.error is None and not self.skipped


class StreamRuntime:
    """Long-running scheduler for configured collectors.

    One worker exists per configured source instance. Global and provider-level
    semaphores bound concurrent collection without coupling source failures.
    Circuit-breaker and rate-limit pauses are persisted through RuntimeStateStore.
    """

    def __init__(
        self,
        config: StreamConfig,
        *,
        event_store: SignalStore,
        state_store: RuntimeStateStore,
        registry: CollectorRegistry | None = None,
        clock: Clock | None = None,
    ) -> None:
        self.config = config
        self.event_store = event_store
        self.state_store = state_store
        self.registry = registry or default_registry()
        self.clock = clock or SystemClock()
        self._stop_event = asyncio.Event()
        self._global_semaphore = asyncio.Semaphore(config.runtime.global_concurrency)
        self._provider_semaphores: dict[str, asyncio.Semaphore] = {}
        self._sources: dict[str, tuple[SourceConfig, Collector]] = {}

        for source in config.enabled_sources:
            collector = self.registry.create(source)
            self._sources[source.name] = (source, collector)
            self._provider_semaphores.setdefault(
                collector.source,
                asyncio.Semaphore(config.runtime.provider_concurrency),
            )

    @property
    def source_names(self) -> tuple[str, ...]:
        return tuple(self._sources)

    def request_stop(self) -> None:
        self._stop_event.set()

    async def run_once(self) -> list[SourceRunOutcome]:
        """Run one scheduler cycle for all enabled sources without sleeping."""

        tasks = [asyncio.create_task(self.run_source(name)) for name in self._sources]
        if not tasks:
            return []
        return list(await asyncio.gather(*tasks))

    async def run_source(self, name: str) -> SourceRunOutcome:
        if name not in self._sources:
            raise KeyError(f"unknown runtime source: {name}")
        source, collector = self._sources[name]
        source_key = collector.identity.key
        now = self.clock.now()
        health = self.state_store.get_health(source_key)
        pause_delay = self._persisted_pause_delay(health, now)
        if pause_delay > 0:
            return SourceRunOutcome(
                name=name,
                source_key=source_key,
                next_delay=pause_delay,
                skipped=True,
                reason="source is paused",
            )

        self.state_store.record_attempt(source_key, at=now)
        provider_semaphore = self._provider_semaphores[collector.source]
        try:
            async with self._global_semaphore:
                async with provider_semaphore:
                    result = await run_collector(
                        collector,
                        limit=source.limit,
                        max_pages=source.max_pages,
                        store=self.event_store,
                        resume=True,
                        metadata={"runtime_source": source.name},
                    )
        except CollectorError as exc:
            return self._failure_outcome(source, collector, exc, health)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            return self._failure_outcome(source, collector, exc, health)

        completed_at = self.clock.now()
        paused_until = self._rate_limit_pause(result.rate_limit, completed_at)
        self.state_store.record_success(
            source_key,
            at=completed_at,
            rate_limit=result.rate_limit,
            paused_until=paused_until,
        )
        return SourceRunOutcome(
            name=name,
            source_key=source_key,
            result=result,
            next_delay=self._success_delay(source, result.rate_limit, completed_at),
        )

    async def run_forever(self) -> None:
        """Run all configured source workers until stop is requested or cancelled."""

        workers = [
            asyncio.create_task(self._worker(name), name=f"signalkit:{name}")
            for name in self._sources
        ]
        if not workers:
            return
        try:
            await asyncio.gather(*workers)
        except asyncio.CancelledError:
            self.request_stop()
            for worker in workers:
                worker.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
            raise
        finally:
            self.request_stop()
            for worker in workers:
                if not worker.done():
                    worker.cancel()
            await asyncio.gather(*workers, return_exceptions=True)

    async def _worker(self, name: str) -> None:
        delay = 0.0
        while not self._stop_event.is_set():
            if delay > 0 and await self._sleep_or_stop(delay):
                return
            if self._stop_event.is_set():
                return
            outcome = await self.run_source(name)
            delay = max(0.001, outcome.next_delay)

    async def _sleep_or_stop(self, seconds: float) -> bool:
        sleep_task = asyncio.create_task(self.clock.sleep(max(0.0, seconds)))
        stop_task = asyncio.create_task(self._stop_event.wait())
        done, pending = await asyncio.wait(
            {sleep_task, stop_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        return stop_task in done and self._stop_event.is_set()

    def _failure_outcome(
        self,
        source: SourceConfig,
        collector: Collector,
        exc: Exception,
        previous_health: SourceHealth | None,
    ) -> SourceRunOutcome:
        now = self.clock.now()
        source_key = collector.identity.key
        failures = (previous_health.consecutive_failures if previous_health else 0) + 1
        rate_limit = getattr(collector, "rate_limit", None)
        if not isinstance(rate_limit, RateLimitSnapshot):
            rate_limit = None

        is_rate_limit = isinstance(exc, CollectorError) and exc.kind is CollectorErrorKind.RATE_LIMIT
        if is_rate_limit:
            paused_until = self._rate_limit_pause(rate_limit, now) or (
                now + timedelta(seconds=self.config.runtime.cooldown)
            )
        elif failures >= self.config.runtime.failure_threshold:
            paused_until = now + timedelta(seconds=self.config.runtime.cooldown)
        else:
            backoff = min(
                source.interval * (2 ** max(0, failures - 1)),
                self.config.runtime.cooldown,
            )
            paused_until = now + timedelta(seconds=backoff)

        self.state_store.record_failure(
            source_key,
            str(exc),
            at=now,
            consecutive_failures=failures,
            paused_until=paused_until,
            rate_limit=rate_limit,
        )
        return SourceRunOutcome(
            name=source.name,
            source_key=source_key,
            error=str(exc),
            next_delay=max(0.001, (paused_until - now).total_seconds()),
            reason="rate limit" if is_rate_limit else "collector failure",
        )

    @staticmethod
    def _persisted_pause_delay(health: SourceHealth | None, now: datetime) -> float:
        if health is None or health.paused_until is None:
            return 0.0
        return max(0.0, (health.paused_until - now).total_seconds())

    @staticmethod
    def _rate_limit_pause(
        rate_limit: RateLimitSnapshot | None,
        now: datetime,
    ) -> datetime | None:
        if rate_limit is None:
            return None
        if rate_limit.remaining is not None and rate_limit.remaining > 0:
            return None
        candidates: list[datetime] = []
        if rate_limit.reset_at is not None and rate_limit.reset_at > now:
            candidates.append(rate_limit.reset_at)
        if rate_limit.retry_after is not None and rate_limit.retry_after > 0:
            candidates.append(now + timedelta(seconds=rate_limit.retry_after))
        return max(candidates) if candidates else None

    @classmethod
    def _success_delay(
        cls,
        source: SourceConfig,
        rate_limit: RateLimitSnapshot | None,
        now: datetime,
    ) -> float:
        pause = cls._rate_limit_pause(rate_limit, now)
        if pause is None:
            return source.interval
        return max(source.interval, (pause - now).total_seconds())
