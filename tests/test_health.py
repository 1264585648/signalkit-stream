from datetime import UTC, datetime, timedelta

from signalkit_stream.health import SQLiteRuntimeStateStore
from signalkit_stream.protocol import RateLimitSnapshot


def test_runtime_health_persists_success_failure_and_pause(tmp_path) -> None:
    database = tmp_path / "signals.db"
    now = datetime(2026, 7, 25, 12, 0, tzinfo=UTC)
    reset = now + timedelta(minutes=5)

    with SQLiteRuntimeStateStore(database) as store:
        store.record_attempt("github:leads", at=now)
        store.record_failure(
            "github:leads",
            "rate limited",
            at=now,
            consecutive_failures=2,
            paused_until=reset,
            rate_limit=RateLimitSnapshot(remaining=0, reset_at=reset),
        )
        failed = store.get_health("github:leads")

    assert failed is not None
    assert failed.consecutive_failures == 2
    assert failed.last_error == "rate limited"
    assert failed.paused_until == reset
    assert failed.rate_limit_remaining == 0

    with SQLiteRuntimeStateStore(database) as store:
        persisted = store.get_health("github:leads")
        assert persisted == failed
        store.record_success(
            "github:leads",
            at=reset,
            rate_limit=RateLimitSnapshot(remaining=10),
        )
        healthy = store.get_health("github:leads")

    assert healthy is not None
    assert healthy.status == "healthy"
    assert healthy.consecutive_failures == 0
    assert healthy.last_error is None
    assert healthy.last_success_at == reset
