# Contributing

SignalKit Stream is intentionally small: collectors retrieve source data and normalize it; downstream AI analysis belongs outside this repository.

## Development

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\\Scripts\\activate
python -m pip install -e ".[dev]"
make check
```

## Adding a collector

1. Subclass `Collector` or `HTTPCollector`.
2. Return `SignalEvent` objects from `collect()`.
3. Use `SignalEvent.stable_id()` with a source-native immutable ID.
4. Preserve source-specific fields in `metadata` rather than changing the common schema.
5. Add network-free tests using `httpx.MockTransport` or fixtures.
6. Prefer official APIs and feeds; respect authentication, rate limits, robots policies, and source terms.

A collector should not perform lead scoring, LLM classification, outreach, or business-specific filtering. Those are downstream concerns.
