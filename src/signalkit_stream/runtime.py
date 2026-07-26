from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
import logging

from signalkit_stream.collectors.base import Collector, HTTPCollector
from signalkit_stream.config import SourceConfig, StreamConfig
from signalkit_stream.delivery import DeliveryEngine, DeliveryResult
from signalkit_stream.pipeline import CollectionResult, run_collector
from signalkit_stream.protocol import CollectorError, CollectorErrorKind, RateLimitSnapshot
from signalkit_stream.registry import CollectorRegistry, default_registry
from signalkit_stream.sinks import SinkRegistry, default_sink_registry
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
    """Long-running collection and durable-delivery scheduler."""

    def __init__(
        self,
        config: StreamConfig,
        store: SQLiteSignalStore,
        *,
        registry: CollectorRegistry | None = None,
        sink_registry: SinkRegistry | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self.config = config
        self.store = store
        self.registry = registry or default_registry()
        self.sink_registry = sink_registry or default_sink_registry()
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

        enabled_sink_configs = tuple(sink for sink in config.sinks if sink.enabled)
        self.sinks = tuple(self.sink_registry.create(sink) for sink in enabled_sink_configs)
        for sink_config, sink in zip(enabled_sink_configs, self.sinks, strict=True):
            self.store.register_delivery_sink(sink.key, backfill=sink_config.backfill)
        self._reconcile_delivery_sinks(frozenset(sink.key for sink in self.sinks))
        self.delivery = DeliveryEngine(
            self.store,
            self.sinks,
            batch_size=config.runtime.delivery_batch,
            max_attempts=config.runtime.delivery_max_attempts,
            backoff_base=config.runtime.delivery_backoff_base,
            backoff_max=config.runtime.delivery_backoff_max,
            interval=config.runtime.delivery_interval,
            sleep=sleep,
            now=now,
        )
        self.last_delivery_results: list[DeliveryResult] = []
        self._restore_health()

    def _reconcile_delivery_sinks(self, active_keys: frozenset[str]) -> None:
        """Disable database sinks that this configuration no longer drains.

        ``register_delivery_sink`` only ever enables rows, so a sink that an operator
        removes from the config (or flips to ``enabled = false``) keeps its row at
        ``enabled = 1``. The insert/update triggers then keep queueing ``pending``
        delivery rows that no worker will ever drain, growing ``deliveries`` without
        bound. Reconciling at startup makes the database follow the config in both
        directions. Rows queued *before* the sink was removed are intentionally left
        alone: draining or pruning historical backlog is an operator decision
        (``signalkit retry-deliveries`` / maintenance), not a startup side effect.
        """

        for key in self._enabled_delivery_sink_keys():
            if key in active_keys:
                continue
            logger.warning(
                "sink=%s disabled: not present as an enabled sink in the current configuration",
                key,
            )
            self.store.disable_delivery_sink(key)

    def _enabled_delivery_sink_keys(self) -> tuple[str, ...]:
        connection = getattr(self.store, "_connection", None)
        if connection is None:  # pragma: no cover - non-SQLite stores cannot be reconciled
            return ()
        rows = connection.execute(
            "SELECT sink_key FROM delivery_sinks WHERE enabled = 1 ORDER BY sink_key"
        ).fetchall()
        return tuple(str(row[0]) for row in rows)

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
        """Run every enabled source once, then drain one delivery batch per sink."""

        results = list(await asyncio.gather(*(self._run_source(source) for source in self.sources)))
        self.last_delivery_results = await self.delivery.deliver_all_once()
        return results

    async def run_forever(self, stop_event: asyncio.Event | None = None) -> None:
        """Run independent source and sink workers until stopped.

        The supervisor waits on the workers *and* the stop event. A worker that ends on
        its own is a defect rather than normal operation -- every source iteration is
        individually guarded (see ``_source_loop``) -- so its exit is logged and the
        whole runtime shuts down and re-raises instead of leaving a process that looks
        healthy while collecting nothing. Restart is deliberately delegated to the
        process supervisor (systemd/docker/k8s), which owns restart backoff and crash
        looping policy; an in-process restart loop would hide the defect and could spin.
        """

        stop = stop_event or asyncio.Event()
        tasks = [
            asyncio.create_task(self._source_loop(source, stop), name=f"signalkit:{source.config.name}")
            for source in self.sources
        ]
        if self.sinks:
            tasks.append(asyncio.create_task(self.delivery.run_forever(stop), name="signalkit:delivery"))
        if not tasks:
            return
        stop_task = asyncio.create_task(stop.wait(), name="signalkit:stop")
        dead_worker_error: BaseException | None = None
        try:
            done, _pending = await asyncio.wait(
                {*tasks, stop_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            dead_worker_error = self._dead_worker_error(
                done,
                stop_task=stop_task,
                stopping=stop.is_set(),
            )
        finally:
            stop.set()
            stop_task.cancel()
            for task in tasks:
                task.cancel()
            await asyncio.gather(stop_task, *tasks, return_exceptions=True)
            # Collector teardown belongs here, next to sink teardown, once
            # HTTPCollector.aclose() lands (pooled httpx clients).
            await self.delivery.close()
        if dead_worker_error is not None:
            raise dead_worker_error

    def _dead_worker_error(
        self,
        done: set[asyncio.Task[None]],
        *,
        stop_task: asyncio.Task[bool],
        stopping: bool,
    ) -> BaseException | None:
        """Log every worker that finished before the stop event and return the first error."""

        first: BaseException | None = None
        for task in done:
            if task is stop_task or task.cancelled():
                continue
            error = task.exception()
            if error is not None:
                logger.error(
                    "worker=%s died with an unhandled exception; shutting down runtime",
                    task.get_name(),
                    exc_info=error,
                )
            elif stopping:
                continue
            else:
                logger.error(
                    "worker=%s ended on its own without an error; shutting down runtime",
                    task.get_name(),
                )
                error = RuntimeError(f"runtime worker {task.get_name()} ended unexpectedly")
            if first is None:
                first = error
        return first

    async def _source_loop(self, source: RuntimeSource, stop: asyncio.Event) -> None:
        """Poll one source until stopped, surviving unexpected per-iteration failures.

        Everything in an iteration can fail, not just the collector call: health
        persistence is a database write and can raise ``database is locked``. Without
        this guard such a failure would end the worker permanently while the process
        kept looking healthy. An unexpected failure is logged and retried after the
        configured poll interval -- never faster, so a repeatedly broken iteration
        cannot hammer the remote source.
        """

        while not stop.is_set():
            try:
                delay = (await self._run_source(source)).delay
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "source=%s iteration failed unexpectedly; retrying in %.1fs",
                    source.collector.identity.key,
                    source.config.interval,
                )
                delay = source.config.interval
            if stop.is_set():
                break
            await self._sleep_or_stop(delay, stop)

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
        """Return how long to wait after a failed run.

        Policy, in priority order:

        1. A ``RATE_LIMIT`` error is an explicit slow-down signal and always wins. Its
           delay is ``max(Retry-After/reset hint, source interval)``: the hint is
           honoured in full even when it exceeds the poll interval or the circuit
           cooldown, and a 429 that carries no hint still costs at least one full poll
           interval. Answering "slow down" with traffic *faster* than the configured
           interval is never correct.
        2. An open circuit waits at least ``circuit_cooldown``, but never less than an
           outstanding rate-limit hint -- cooldown is a floor for failing sources, not a
           licence to ignore an endpoint that asked for an hour.
        3. Ordinary (non rate-limited) failures keep exponential backoff clamped *down*
           to the poll interval: ``min(interval, failure_backoff_base * 2**(n-1))``.
           This is deliberately faster than the interval for long-interval sources: a
           failed poll returned no data, the request budget it would have spent is
           unused, and a source polled hourly should recover from a transient blip in
           seconds rather than an hour. Sustained failure traffic is already bounded by
           the circuit breaker in rule 2.
        """

        limited = 0.0
        if error.kind is CollectorErrorKind.RATE_LIMIT:
            rate_limit = (
                source.collector.rate_limit
                if isinstance(source.collector, HTTPCollector)
                else None
            )
            limited = max(self._rate_limit_delay(rate_limit), source.config.interval)
        if source.failures >= self.config.runtime.failure_threshold:
            return max(self.config.runtime.circuit_cooldown, limited)
        if limited > 0:
            return limited
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
