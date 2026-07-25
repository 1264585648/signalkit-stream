# JSON Feed adapter

SignalKit Stream supports JSON Feed 1.x as a first-party source adapter. The adapter uses the published JSON Feed item `id` as the immutable source identity, normalizes items into `SignalEvent`, and maintains a bounded recent-ID cursor for incremental polling.

Specification: https://www.jsonfeed.org/version/1.1/

## Runtime configuration

```toml
[[sources]]
name = "product-blog-json"
type = "jsonfeed"
url = "https://example.com/feed.json"
interval = 300
limit = 100
seen_window = 2000
```

Optional fields:

```toml
source = "company-blog"
instance = "product-blog"
seen_window = 5000
```

`source` changes the logical source name stored on events. `instance` changes the source-instance key used by deterministic event IDs and checkpoints. Keep an explicit instance stable if the feed URL itself may move.

## One-shot collection

```bash
signalkit collect jsonfeed https://example.com/feed.json --limit 100
```

The standard `--db`, `--fresh`, `--no-store`, `--format`, `--source`, and `--instance` options are available. `--seen-window` controls the retained item-ID window.

## Incremental behavior

The cursor keeps recent item IDs plus HTTP validators:

```text
seen_ids
cycle_ids
etag
last_modified
```

When a feed contains more unseen items than one collection call is allowed to emit, `cycle_ids` records the items already emitted during the current drain. The next call refetches the feed and skips those IDs until the whole current batch is drained. The adapter does not commit a new ETag/Last-Modified boundary until the drain is complete, preventing a conditional `304 Not Modified` response from hiding items that were not emitted yet.

After a complete cycle, subsequent polls send `If-None-Match` and/or `If-Modified-Since` when the server supplied validators. HTTP 304 produces no events and leaves the cursor unchanged.

The adapter also stops at a previously seen item when feeds are newest-first, so normal incremental polls only emit new items.

## Normalization

JSON Feed items become `SignalKind.ARTICLE`.

Content priority:

1. `content_text`
2. text extracted from `content_html`
3. `summary`
4. empty string

Canonical event URL priority:

1. item `url`
2. item `external_url`
3. feed `home_page_url`
4. feed `feed_url`
5. configured feed URL

JSON Feed 1.1 `authors` and legacy `author` are supported. Multiple author names are joined for the normalized `author` field while the structured author objects remain in `metadata`.

`date_published` maps to `created_at`; `date_modified` maps to `updated_at`. If neither timestamp is available, the adapter uses a stable epoch timestamp rather than collection time so repeated polling cannot create fingerprint churn.

Source-specific fields such as `external_url`, `summary`, authors, tags, language, attachments, feed title, and feed version remain under `metadata`.

## Validation and failure behavior

The adapter rejects:

- invalid JSON
- non-object feed roots
- unsupported/missing JSON Feed versions
- a non-array `items` field
- items without the required JSON Feed `id`

Network failures, timeouts, transient 5xx responses, and HTTP 429 responses use the shared `HTTPCollector` retry policy.

## Operational notes

`seen_window` defaults to 2000 and must be at least 100. Increase it for feeds with very high publication volume between polls. Feed servers that provide ETag or Last-Modified validators avoid transferring/parsing an unchanged feed on every cycle; feeds without validators still remain incremental through the item-ID window.
