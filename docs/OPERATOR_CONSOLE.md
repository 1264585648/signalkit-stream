# SignalKit Operator Console

SignalKit Stream includes a dependency-free local web console for inspecting an ingestion database. It is designed as an operator surface, not as a public multi-user application.

## Start the console

Install the project and point the console at an existing Stream database:

```bash
signalkit-console --db signals.db --open
```

The default bind is `127.0.0.1:8765` and the console is read-only.

Choose another local address or port:

```bash
signalkit-console --db data/signals.db --host 127.0.0.1 --port 9000
```

## What the console shows

- database schema compatibility and total stored signals;
- collection activity during the last hour and 24 hours;
- persisted health for every source instance;
- recent normalized signals, searchable by title, content, author, and URL;
- source and kind filters with paginated results;
- full event content, timestamps, source metadata, and the original public URL;
- sink delivery counts, recent errors, failures, and dead letters.

The signal list is ordered by `collected_at` first. An older upstream item collected or updated recently therefore remains visible in current operational activity.

## Optional delivery actions

Mutating actions are disabled by default. Enable dead-letter replay explicitly:

```bash
signalkit-console --db signals.db --allow-actions
```

The UI asks for confirmation before requeueing dead deliveries. The API also requires a non-simple `X-SignalKit-Action` header, reducing accidental cross-origin submissions.

## Network safety

The console has no built-in authentication. It refuses non-loopback binds unless `--allow-remote` is supplied:

```bash
signalkit-console --db signals.db --host 0.0.0.0 --allow-remote
```

Only use a remote bind behind a trusted network or an authenticated reverse proxy. Do not expose the console directly to the public internet.

Responses include a restrictive Content Security Policy, frame denial, referrer suppression, and MIME sniffing protection. Original event links are enabled only for `http` or `https` URLs.

## Interaction shortcuts

- `/` focuses signal search;
- `R` refreshes the console while focus is outside an input;
- `Esc` closes the signal detail drawer;
- auto refresh can be disabled or set to 15, 30, or 60 seconds.

Search filters are mirrored into the page URL so a filtered view survives refresh and can be bookmarked locally.

## Failure and empty states

The static console shell still loads if the database does not exist, is locked, is corrupt, or has an incompatible schema. API failures are returned as structured JSON and rendered as actionable error states instead of a blank screen.

An empty but current database shows setup guidance for starting source workers and registering delivery sinks.

## Implementation boundary

The console uses Python's standard-library threaded HTTP server and packaged HTML/CSS/JavaScript assets. It does not add Node, a frontend build process, a JavaScript framework, or another runtime dependency.
