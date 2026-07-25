# Observability

SignalKit Stream keeps operational state in SQLite so health can be inspected after a process restart rather than existing only in memory.

## Snapshot command

Read source and delivery health without starting the runtime:

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

The command opens SQLite in read-only mode. It does not poll sources, retry deliveries, or modify checkpoints.

## Persisted source metrics

The snapshot exposes:

- stored signal count
- current source health status
- consecutive source failures
- persisted source run count
- persisted source event count
- last source success and error in JSON/table output

Prometheus metric names:

```text
signalkit_signals_total
signalkit_source_status
signalkit_source_consecutive_failures
signalkit_source_runs_total
signalkit_source_events_total
```

`signalkit_source_status` is emitted as one-hot gauges for `healthy`, `degraded`, and `circuit_open`.

## Delivery metrics and sink health

Delivery snapshots aggregate independent outbox rows by sink and status:

```text
pending
failed
dead
delivered
```

They also expose the persisted total attempt count and the most recent failed/dead error for each sink in JSON/table output.

Prometheus metric names:

```text
signalkit_deliveries
signalkit_delivery_attempts_total
```

These are persisted state metrics rather than in-process counters, so they remain meaningful after a restart. A monitoring agent can scrape the command output or call `read_snapshot()` from a small HTTP exporter if a deployment needs a `/metrics` endpoint. SignalKit Stream intentionally does not add a web server dependency just for metrics.

## Structured logging

`signalkit_stream.logging_utils` provides dependency-free text and JSON logging configuration.

```python
import logging

from signalkit_stream.logging_utils import configure_logging

configure_logging(level=logging.INFO, output_format="json")
```

Extra `LogRecord` fields become JSON fields:

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

Example output shape:

```json
{"timestamp":"2026-07-25T12:00:00+00:00","level":"info","logger":"signalkit.runtime","message":"source completed","event":"source.completed","source_key":"github:issues","events":20}
```

Use stable `event` names and structured extra fields for machine processing; keep the human message concise. Avoid attaching credentials, authorization headers, tokens, or raw source payloads to logs.

## Monitoring recommendations

At minimum alert on:

- source status remaining `degraded` or `circuit_open`
- increasing consecutive source failures
- any dead delivery rows
- failed delivery rows that do not return to pending/delivered
- sustained growth in pending delivery rows
- no successful source events over an interval appropriate for that source

Interpret counters in source context: a quiet RSS feed can legitimately produce no new events, while a frequently polled community source may warrant a tighter staleness threshold.
