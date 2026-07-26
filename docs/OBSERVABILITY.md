# Observability

SignalKit Stream persists operational state in SQLite so source and delivery health remain
inspectable after process restarts rather than existing only in memory.

## Snapshot command

Read database, source, and sink state without starting collectors or delivery workers:

```bash
python -m signalkit_stream.observability signals.db
```

JSON:

```bash
python -m signalkit_stream.observability signals.db --format json
```

Prometheus exposition text:

```bash
python -m signalkit_stream.observability signals.db --format prometheus
```

The snapshot path is read-only. It first uses the maintenance verification layer to inspect
SQLite integrity and persistent schema compatibility, then aggregates persisted runtime state.
It does not poll sources, retry deliveries, migrate the database, or advance checkpoints.

The command exits non-zero when the database schema is not current for the running build.
That makes it usable as a lightweight deployment/readiness check in addition to metrics output.

## Database metrics

Every snapshot includes:

```text
signals_total
schema_version
supported_schema_version
schema_status
```

Prometheus metric names:

```text
signalkit_signals_total
signalkit_database_schema_version
signalkit_database_schema_supported_version
signalkit_database_schema_current
```

`signalkit_database_schema_current` is `1` only when the database is on the current persistent
schema and passed the schema validation used by maintenance tooling.

## Persisted source metrics

For each source the snapshot exposes:

- current persisted health status;
- consecutive failures;
- total runtime executions;
- total events reported by the runtime;
- last attempt time;
- last success time;
- latest persisted error.

Prometheus metric names:

```text
signalkit_source_status
signalkit_source_consecutive_failures
signalkit_source_runs_total
signalkit_source_events_total
```

`signalkit_source_status` emits the actual persisted status as a label, for example:

```text
signalkit_source_status{source="reddit:saas",status="degraded"} 1
```

## Delivery metrics and sink health

All registered sinks are included, even if they currently have no delivery rows. The snapshot
reports whether the sink is enabled plus outbox rows in these states:

```text
pending
failed
dead
delivered
```

It also exposes persisted delivery-attempt totals and the most recent failed/dead error for
each sink.

Prometheus metric names:

```text
signalkit_sink_enabled
signalkit_deliveries
signalkit_delivery_attempts_total
```

These are persisted-state metrics, not volatile in-process counters. They remain meaningful
after restarts. A deployment can scrape the command output or call `read_snapshot()` from its
existing monitoring/exporter process; SignalKit Stream does not add a web server dependency
just to expose `/metrics`.

## Structured logging

`signalkit_stream.logging_utils` provides dependency-free text and JSON root logging setup:

```python
import logging

from signalkit_stream.logging_utils import configure_logging

configure_logging(level=logging.INFO, output_format="json")
```

Extra `LogRecord` fields become top-level JSON fields:

```python
logger.info(
    "source completed",
    extra={
        "event": "source.completed",
        "source_key": "github:issues",
        "events": 20,
    },
)
```

Output shape:

```json
{"timestamp":"2026-07-26T12:00:00+00:00","level":"info","logger":"signalkit.runtime","message":"source completed","event":"source.completed","source_key":"github:issues","events":20}
```

Use stable `event` names and structured fields for machine processing. Never attach access
tokens, refresh tokens, authorization headers, client secrets, or unfiltered raw source
payloads to logs.

## Monitoring recommendations

At minimum alert on persistent `degraded` / `circuit_open` source status, increasing consecutive
failures, dead deliveries, failed deliveries that never recover, sustained pending-delivery
growth, and a non-current database schema. Interpret event-rate alerts in source context: a
quiet feed can legitimately emit nothing while a high-volume community source may justify a
tighter staleness threshold.
