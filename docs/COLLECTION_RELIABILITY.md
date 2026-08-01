# Collection reliability and freshness

SignalKit Stream uses **at-least-once collection with idempotent persistence**. A collector may intentionally replay a small overlap when that is safer than risking a gap. Stable event IDs and mutation fingerprints make those overlaps safe at the storage boundary.

## HTTP backpressure and deadlines

Every HTTP collector applies a bounded per-collector request semaphore. Fan-out collectors such as Hacker News can fetch several objects concurrently without creating an unbounded burst against an upstream service.

Retries use exponential backoff, `Retry-After`, and normalized rate-limit state. Retry sleep is refused when it would consume the remaining collector deadline, allowing the runtime to record a timeout instead of leaving a source worker asleep beyond its collection budget.

`X-RateLimit-Reset` is interpreted as an epoch timestamp. Standard `RateLimit-Reset` is interpreted as relative seconds. Reddit retains its source-specific decimal/relative rate-limit parser.

## Incremental overlap

GitHub issue search resumes with an inclusive update watermark:

```text
updated:>=<last committed update second>
```

GitHub timestamps have second-level resolution. An exclusive boundary can miss an issue updated in the same second as the last committed object. The inclusive overlap may replay an already stored event, which is safe because persistence is idempotent.

GitHub comment collection selects the newest bounded comment window and emits those comments in chronological order. A search response with `incomplete_results=true` produces an operator-visible warning.

## Active-thread comment refresh

A post or story can receive valuable replies long after the primary object was first observed. Only collecting comments with newly discovered primary objects creates a permanent freshness gap.

Hacker News and Reddit therefore revisit a bounded window of recently seen threads after the newest listing cycle is caught up:

```toml
[[sources]]
name = "hn-ask"
type = "hackernews"
feed = "askstories"
comments = 3
comment_refresh = 10

[[sources]]
name = "reddit-saas"
type = "reddit"
subreddit = "SaaS"
listing = "new"
comments = 5
comment_refresh = 10
```

`comments` controls the maximum recent top-level comments collected per primary item. `comment_refresh` controls how many recently seen primary items are revisited per caught-up polling cycle. Set `comment_refresh = 0` to disable revisits.

The refresh is intentionally bounded. It does not recursively crawl an entire reply tree. Stable comment IDs make repeated recent-window collection idempotent.

## Mutable RSS and Atom feeds

RSS/Atom feeds are often mutable newest-first snapshots rather than stable pages. An integer offset alone is unsafe when a publisher inserts or reorders entries while a multi-batch collection cycle is in progress.

The RSS collector stores the external ID of the last entry in each partial batch as a page anchor. Before resuming, it verifies that the anchor still occupies the expected position. When the feed changed, collection restarts from the beginning of the current snapshot and emits a warning instead of risking skipped entries.

This can replay earlier entries, but replay is preferable to loss and is safe at the idempotent storage boundary.

## Remaining source limitations

The reliability model does not claim exactly-once network collection. Remaining source-specific enhancements include:

- recursive/deeper reply-tree pagination beyond bounded recent top-level comments;
- explicit tombstone or deletion events where an upstream API exposes reliable deletion semantics;
- stronger historical-backfill controls for unusually large feeds without validators;
- live compatibility monitoring for upstream API policy or response-shape changes.
