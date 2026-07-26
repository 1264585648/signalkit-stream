# Intended 1.0 public API surface

This document is the maintainer-facing inventory used for the final 1.0 API review. It identifies the Python and CLI surfaces that downstream users should be able to rely on once 1.0 is tagged.

`docs/COMPATIBILITY.md` defines how these surfaces evolve after 1.0. Additive names/commands remain allowed in compatible releases; the purpose of this inventory is to prevent accidental removal or reinterpretation of already documented contracts.

## Top-level Python API

The following names are intentionally importable from `signalkit_stream`.

### Event and collector protocol

```text
SCHEMA_VERSION
PROTOCOL_VERSION
SignalEvent
SignalKind
SourceIdentity
Cursor
CollectorContext
CollectorResult
CollectorError
CollectorErrorKind
RateLimitSnapshot
validate_collector_result
```

`SignalEvent.id` is stable source-object identity; `SignalEvent.fingerprint()` identifies the source-visible version. `SignalEvent.schema_version` and `PROTOCOL_VERSION` are protocol compatibility markers, not package versions.

### Configuration

```text
RuntimeConfig
SourceConfig
SinkConfig
StreamConfig
load_config
```

Configuration remains strict: unknown TOML keys fail validation instead of being silently ignored.

### Collection/runtime

```text
CollectionResult
run_collector
SourceRunResult
StreamRuntime
```

`run_collector` owns page draining and checkpoint-safe persistence. `StreamRuntime` owns long-running scheduling and delivery-worker lifecycle.

### Storage and database compatibility

```text
SQLiteSignalStore
Checkpoint
SourceHealth
StoreWriteResult
DATABASE_SCHEMA_VERSION
DatabaseSchemaError
DatabaseSchemaTooNew
DatabaseMigrationError
get_database_schema_version
migrate_database
validate_database_schema
```

The persistent database version is independent of the event schema version. Forward migrations are supported; automatic downgrades are not.

### Durable delivery and sinks

```text
Sink
SinkError
StdoutSink
JsonlSink
WebhookSink
DeliveryEngine
DeliveryResult
DeliveryRecord
```

Delivery is at-least-once. Webhook idempotency semantics described in the architecture/compatibility documentation are part of this contract.

### Diagnostics, maintenance, observability, logging

```text
DiagnosticStatus
DiagnosticCheck
DiagnosticReport
doctor
validate_config_file
validate_stream_config
BackupResult
VerifyResult
backup_database
verify_database
SourceStatusSnapshot
SinkStatusSnapshot
StreamSnapshot
read_snapshot
format_snapshot
LogFormat
TextLogFormatter
JsonLogFormatter
configure_logging
```

JSON/Prometheus fields intended for automation are compatibility-relevant. Human table spacing is not a machine contract.

## Extension APIs

Collector authors should import the collector SDK from its documented modules:

```python
from signalkit_stream.collectors.base import Collector, HTTPCollector, RetryPolicy
from signalkit_stream.protocol import CollectorContext, CollectorResult, Cursor
from signalkit_stream.models import SignalEvent, SignalKind
```

For explicitly understood JSON list APIs:

```python
from signalkit_stream.collectors.rest import GenericRESTCollector
from signalkit_stream.rest_config import build_generic_rest_collector
```

The generic REST collector remains opt-in and is not added to `default_registry()` automatically.

Registry/sink extension points:

```python
from signalkit_stream.registry import CollectorRegistry, default_registry
from signalkit_stream.sinks import SinkRegistry, default_sink_registry
```

Modules or symbols prefixed with `_` are implementation details even when technically importable.

## CLI surface

Intended stable top-level commands:

```text
signalkit init
signalkit validate
signalkit doctor
signalkit run
signalkit collect
signalkit show
signalkit checkpoint
signalkit status
signalkit deliveries
signalkit retry-deliveries
signalkit db
```

First-party one-shot collector commands:

```text
signalkit collect rss
signalkit collect jsonfeed
signalkit collect hn
signalkit collect github
signalkit collect reddit
```

Database operations:

```text
signalkit db backup
signalkit db verify
```

Automation-facing output:

```text
validate/doctor JSON reports
status --verbose --format json
status --format prometheus
deliveries --format json
db backup/verify --format json
```

`signalkit run --log-format json` is the structured process-log surface.

## Compatibility-test policy

The deterministic test suite keeps a **required subset** of public names/commands rather than asserting that the API can never grow. Compatible releases may add exports, flags, JSON fields, or metrics. Removing/renaming a required 1.0 surface must go through the compatibility/deprecation process.

The exact 1.0 release PR should review this file together with `CHANGELOG.md`, `docs/COMPATIBILITY.md`, and the generated CLI help from the release candidate.
