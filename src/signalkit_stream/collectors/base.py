"""Public import path for the collector base classes.

`signalkit_stream.collectors.base` is the documented SDK entry point (see
`docs/COLLECTOR_SDK.md` and `examples/`), so it stays a stable re-export of the
implementation in `_base_impl`.
"""

from __future__ import annotations

from signalkit_stream.collectors._base_impl import (
    Collector,
    HTTPCollector,
    RetryPolicy,
)

__all__ = ["Collector", "HTTPCollector", "RetryPolicy"]
