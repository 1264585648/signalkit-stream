# Generic REST adapter

`GenericRESTCollector` is an explicit extension helper for simple JSON APIs. It is intentionally **not** registered by `default_registry()`: arbitrary APIs do not share safe authentication, pagination, ordering, or field semantics, so SignalKit Stream requires you to opt in and describe those semantics.

For a source with a stable JSON list and page/offset pagination, this avoids writing repetitive adapter code while preserving the Stream contracts.

## Python usage

```python
from signalkit_stream.collectors.rest import GenericRESTCollector
from signalkit_stream.pipeline import run_collector

collector = GenericRESTCollector(
    "https://api.example.com/v1/items",
    item_path="data.items",
    id_field="id",
    title_field="title",
    content_field="body",
    author_field="author.name",
    url_field="links.html",
    created_at_field="created_at",
    updated_at_field="updated_at",
    source="partner-api",
    instance="open-items",
    query={"state": "open"},
    pagination="page",
    page_param="page",
    limit_param="limit",
    page_size=100,
)

result = await run_collector(collector, limit=500, store=store)
```

Dot-separated field paths such as `author.name` and `data.items` traverse JSON objects. The required `id_field` must point to an immutable source-native identifier. Do not use a title, score, mutable URL, or body as the ID.

## TOML opt-in

The generic REST adapter is not included in `default_registry()`. Register it in an embedding application:

```python
from signalkit_stream.registry import default_registry
from signalkit_stream.rest_config import register_generic_rest

registry = default_registry()
register_generic_rest(registry)
```

Then a normal `SourceConfig` / TOML source can use `type = "rest"`:

```toml
[[sources]]
name = "partner-open-items"
type = "rest"
interval = 120
limit = 200

url = "https://api.example.com/v1/items"
item_path = "data.items"
id_field = "id"
title_field = "title"
content_field = "body"
author_field = "author.name"
url_field = "links.html"
created_at_field = "created_at"
updated_at_field = "updated_at"
kind = "post"
pagination = "page"
page_param = "page"
limit_param = "limit"
page_size = 100
query = { state = "open" }
headers = { Accept = "application/json" }
token_env = "PARTNER_API_TOKEN"
token_header = "Authorization"
token_prefix = "Bearer "
```

Credential values come from environment variables. Only the environment-variable name belongs in configuration.

## Pagination

Two deterministic modes are supported:

### Page pagination

```toml
pagination = "page"
page_param = "page"
limit_param = "limit"
page_size = 100
```

The cursor advances page numbers after a page has been fully scanned.

### Offset pagination

```toml
pagination = "offset"
offset_param = "offset"
limit_param = "limit"
page_size = 100
```

The cursor advances by the number of source items returned.

If the Stream batch limit cuts through the middle of an API page, the next call deliberately refetches that same page and skips IDs already scanned in the current cycle. This costs one repeat request but avoids pretending an API has a continuation cursor that it does not provide.

## Incremental polling

A bounded `seen_ids` watermark records recently completed source IDs. On a later polling cycle, encountering a previously seen object ends the cycle. `seen_window` controls the size of that watermark.

This model assumes the endpoint is ordered newest-first or otherwise places newly created objects before the recent watermark. If your API has different ordering semantics, write a dedicated collector with an appropriate native cursor or timestamp watermark instead of using this generic helper.

## Initial backfill

`initial_backfill = true` is the default and walks pages until the endpoint is exhausted, the runtime item limit is reached, or another runtime guard stops collection.

Use `initial_backfill = false` only when embedding a policy that wants to establish a recent watermark without historical traversal. For complex bootstrap semantics, prefer a dedicated adapter.

## What this adapter deliberately does not do

It does not guess:

- OAuth flows or token refresh
- cursor-link pagination
- POST/search endpoints
- source-specific 429/reset semantics
- nested comment/thread traversal
- API-specific deletion/tombstone behavior
- custom update watermarks
- arbitrary transformation expressions

Those belong in source-specific adapters built on `HTTPCollector`. See `docs/COLLECTOR_SDK.md` and `examples/json_api_collector.py`.

## Testing

Before relying on a configured REST source, create deterministic `httpx.MockTransport` tests for its actual response shape, pagination, authentication, rate-limit behavior, and ordering assumptions. The shared collector contract is necessary but cannot prove an arbitrary API's business semantics.
