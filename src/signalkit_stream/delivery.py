from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import logging
from typing import Callable

from signalkit_stream.sinks import Sink, SinkError
from signalkit_stream.storage import SQLiteSignalStore

logger = logging.getLogger(__name__)


@dataclass(slots=True, frozen=True)
class DeliveryResult:
    sink_key: str
    attempted: int = 0
    delivered: int = 0
    failed: int = 0
    dead: int = 0


class DeliveryEngine:
    """Drain the transactional outbox into independently retryable sinks."""

    def __init__(
        self,
        store: SQLiteSignalStore,
        sinks: tuple[Sink, ...],
        *,
        batch_size: int = 100,
        max_attempts: int = 8,
        backoff_base: float = 5.0,
        backoff_max: float = 3600.0,
        interval: float = 1.0,
        sleep=asyncio.sleep,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        if batch_size < 1:
            raise ValueError("delivery batch_size must be >= 1")
        if max_attempts < 1:
            raise ValueError("delivery max_attempts must be >= 1")
        if backoff_base <= 0 or backoff_max <= 0 or interval <= 0:
            raise ValueError("delivery timing values must be > 0")
        self.store = store
        self.sinks = sinks
        self.batch_size = batch_size
        self.max_attempts = max_attempts
        self.backoff_base = backoff_base
        self.backoff_max = backoff_max
        self.interval = interval
        self._sleep = sleep
        self._now = now
        keys = [sink.key for sink in sinks]
        duplicates = sorted({key for key in keys if keys.count(key) > 1})
        if duplicates:
            raise ValueError(f"duplicate sink keys: {', '.join(duplicates)}")

    async def deliver_all_once(self) -> list[DeliveryResult]:
        return list(await asyncio.gather(*(self.deliver_once(sink) for sink in self.sinks)))

    async def deliver_once(self, sink: Sink) -> DeliveryResult:
        records = self.store.list_ready_deliveries(
            sink.key,
            limit=self.batch_size,
            now=self._now(),
        )
        delivered = failed = dead = 0
        for record in records:
            event = self.store.get(record.event_id)
            if event is None:
                self.store.mark_delivery_failure(
                    sink.key,
                    record.event_id,
                    error="event no longer exists",
                    next_attempt_at=None,
                    dead=True,
                    attempted_at=self._now(),
                )
                dead += 1
                continue

            try:
                await sink.send(event)
            except asyncio.CancelledError:
                raise
            except SinkError as exc:
                is_dead = (not exc.retryable) or record.attempts + 1 >= self.max_attempts
                next_attempt = None if is_dead else self._next_attempt(record.attempts, exc)
                self.store.mark_delivery_failure(
                    sink.key,
                    event.id,
                    error=str(exc),
                    next_attempt_at=next_attempt,
                    dead=is_dead,
                    attempted_at=self._now(),
                )
                if is_dead:
                    dead += 1
                else:
                    failed += 1
                logger.warning(
                    "sink=%s event=%s status=%s attempts=%s error=%s",
                    sink.key,
                    event.id,
                    "dead" if is_dead else "failed",
                    record.attempts + 1,
                    exc,
                )
            except Exception as exc:
                is_dead = record.attempts + 1 >= self.max_attempts
                next_attempt = None if is_dead else self._next_attempt(record.attempts, None)
                self.store.mark_delivery_failure(
                    sink.key,
                    event.id,
                    error=f"unexpected sink error: {exc}",
                    next_attempt_at=next_attempt,
                    dead=is_dead,
                    attempted_at=self._now(),
                )
                if is_dead:
                    dead += 1
                else:
                    failed += 1
            else:
                self.store.mark_delivery_success(sink.key, event.id, delivered_at=self._now())
                delivered += 1

        return DeliveryResult(
            sink_key=sink.key,
            attempted=len(records),
            delivered=delivered,
            failed=failed,
            dead=dead,
        )

    async def run_forever(self, stop_event: asyncio.Event) -> None:
        tasks = [
            asyncio.create_task(self._sink_loop(sink, stop_event), name=f"signalkit-sink:{sink.key}")
            for sink in self.sinks
        ]
        if not tasks:
            await stop_event.wait()
            return
        try:
            await stop_event.wait()
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _sink_loop(self, sink: Sink, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            result = await self.deliver_once(sink)
            if stop_event.is_set():
                break
            if result.attempted >= self.batch_size and result.delivered == result.attempted:
                await asyncio.sleep(0)
            else:
                await self._sleep_or_stop(self.interval, stop_event)

    async def _sleep_or_stop(self, delay: float, stop_event: asyncio.Event) -> None:
        sleep_task = asyncio.create_task(self._sleep(delay))
        stop_task = asyncio.create_task(stop_event.wait())
        done, pending = await asyncio.wait(
            {sleep_task, stop_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        if sleep_task in done:
            sleep_task.result()

    def _next_attempt(self, prior_attempts: int, error: SinkError | None) -> datetime:
        if error is not None and error.retry_after is not None:
            delay = min(max(0.0, error.retry_after), self.backoff_max)
        else:
            delay = min(self.backoff_base * (2**prior_attempts), self.backoff_max)
        return self._now() + timedelta(seconds=delay)

    async def close(self) -> None:
        await asyncio.gather(*(sink.close() for sink in self.sinks))
