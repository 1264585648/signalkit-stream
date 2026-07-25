# Generic REST adapter / SDK reference

`GenericRESTCollector` is the extension path for JSON GET APIs that look like a list of objects but do not justify a dedicated first-party adapter yet. It provides the shared SignalKit HTTP policy, normalized events, incremental recent-ID tracking, conditional requests, and common page/cursor pagination without embedding source-specific business rules.

Prefer a dedicated adapter when an API has richer semantics, special authentication, nonstandard rate limits, comment relationships, or cursor behavior that cannot be represented faithfully by this mapping layer.

## Programmatic use

```python
from signalkit_stream.collectors.rest import GenericRESTCollector
from signalkit_stream.models import SignalKind

collector = GenericRESTCollector(
    "https://api.example.com/issues",
    items_path="data.items",
    id_path="id",
    kind=SignalKind.ISSUE,
    title_path="title",
    content_path="body",
    author_path="user.login",
    url_path="html_url",
    created_at_path="created_at",
    updated_at_path="updated_at",
    metadata_paths={"state": "state", "labels": "labels"},
    pagination="cursor",
    cursor_param="after",
    next_cursor_path="data.next_cursor",
    limit_param="limit",
)
```

Paths use simple dot notation. Array indexes are also accepted, for example `data.items.0.id`. Keys containing literal dots require a dedicated adapter.

## Runtime integration

The generic mapper is deliberately **not** in the default collector registry. Register its configuration factory explicitly so a deployment makes the decision to trust a generic mapping rather than accidentally treating an unfamiliar API as a known source.

```python
import asyncio

from signalkit_stream.config import load_config
from signalkit_stream.registry import default_registry
from signalkit_stream.rest_config import build_generic_rest_collector
from signalkit_stream.runtime import StreamRuntime
from signalkit_stream.storage import SQLiteSignalStore


async def main() -> None:
    config = load_config("signalkit.toml")
    registry = default_registry()
    registry.register("rest", build_generic_rest_collector)

    with SQLiteSignalStore(config.runtime.database) as store:
        runtime = StreamRuntime(config, store, registry=registry)
        await runtime.run_forever()


asyncio.run(main())
```

Example TOML:

```toml
[[sources]]
name = "example-issues"
type = "rest"
interval = 120
limit = 100

url = "https://api.example.com/issues"
items_path = "data.items"
id_path = "id"
kind = "issue"
title_path = "title"
content_path = "body"
author_path = "user.login"
url_path = "html_url"
created_at_path = "created_at"
updated_at_path = "updated_at"

pagination = "cursor"
cursor_param = "after"
next_cursor_path = "data.next_cursor"
limit_param = "limit"
seen_window = 2000
initial_backfill = false

params = { state = "open" }
headers = { Accept = "application/json" }
metadata_paths = { state = "state", labels = "labels" }

token_env = "EXAMPLE_API_TOKEN"
token_header = "Authorization"
token_prefix = "Bearer "
```

Secrets are resolved from environment variables. `token_env` must point to an existing environment variable; the token itself should not be placed in TOML.

## Pagination modes

### `none`

The endpoint is refetched from the same URL every cycle. If one response contains more unseen items than the current collection limit, SignalKit refetches the same response and uses `cycle_ids` to drain the rest without re-emitting already processed items.

### `page`

SignalKit adds a numeric query parameter such as `page=1`, incrementing until it reaches a previously seen item or an empty page. Configuration:

```toml
pagination = "page"
page_param = "page"
page_start = 1
```

If the collection limit cuts through a response page, the same API page is refetched first; the transport page advances only after that page has been fully scanned.

### `cursor`

SignalKit reads the next API cursor from the JSON response and sends it on the next request:

```toml
pagination = "cursor"
cursor_param = "after"
next_cursor_path = "paging.next"
```

As with numeric paging, a response page is fully drained before advancing the API cursor.

## Incremental boundary

The default `initial_backfill = false` treats the first poll as the starting boundary. It emits at most the configured collection limit from the first transport page and does not walk historical transport pages. This is useful when the process should start listening from “now.”

Set:

```toml
initial_backfill = true
```

to walk transport pages/cursors until the endpoint is exhausted, subject to the normal pipeline page guards and collection limits.

After initialization, polling always starts from the newest transport position and continues until it finds an ID in the bounded `seen_ids` window or exhausts the API.

## HTTP validators

When the server returns `ETag` or `Last-Modified`, the collector sends `If-None-Match` / `If-Modified-Since` on future complete cycles. Validators are not committed while a multi-call drain is still in progress, preventing a `304 Not Modified` response from hiding un-emitted objects.

## Mapping rules

`id_path` is required and must resolve to a non-empty scalar. That value becomes the immutable external identity used by deterministic `SignalEvent` IDs.

Optional mapping paths:

```text
title_path
content_path
author_path
url_path
created_at_path
updated_at_path
```

Dates accept ISO-8601 strings or numeric Unix seconds. When no creation timestamp path is configured or the field is absent, the collector uses a stable Unix epoch timestamp rather than collection time. If a configured date field is present but malformed, collection fails with a non-retryable parse error rather than silently changing event fingerprints.

`metadata_paths` copies selected source fields into normalized event metadata. SignalKit intentionally does not store the whole raw object by default; choose the source fields downstream consumers actually need.

## Authentication and headers

Static non-secret headers can be configured through `headers`. One token can be injected from an environment variable through:

```toml
token_env = "EXAMPLE_API_TOKEN"
token_header = "Authorization"
token_prefix = "Bearer "
```

For OAuth refresh flows, request signing, multiple credentials, custom rate-limit semantics, or other source-specific authentication, create a dedicated `HTTPCollector` adapter instead of extending the generic mapper with source-specific branches.
