# Reddit adapter

SignalKit Stream's Reddit adapter uses Reddit OAuth and the `oauth.reddit.com` API. It does
not automatically fall back to anonymous JSON endpoints, HTML scraping, browser automation,
proxy rotation, or access-workaround techniques.

Before deploying the adapter, verify that the application and intended use are approved under
Reddit's current Data API / Responsible Builder policies. API access requirements can change
independently of SignalKit Stream.

Official references:

- https://support.reddithelp.com/hc/en-us/articles/16160319875092-Reddit-Data-API-Wiki
- https://redditinc.com/policies/data-api-terms
- https://redditinc.com/policies/responsible-builder-policy
- https://www.reddit.com/dev/api

## Authentication modes

The collector supports three runtime credential patterns.

### 1. Static access token

Useful for a short-lived job or when another approved credential manager supplies bearer
tokens:

```text
REDDIT_ACCESS_TOKEN
REDDIT_USER_AGENT
```

A static token is used directly and is not persisted by Stream. If it receives HTTP 401 and no
refresh/app credentials are available, the collector surfaces an authentication failure rather
than retrying the same rejected token.

### 2. Refresh token

For a long-running process acting with an approved refresh token:

```text
REDDIT_REFRESH_TOKEN
REDDIT_CLIENT_ID
REDDIT_USER_AGENT
```

`REDDIT_CLIENT_SECRET` is optional for client types that legitimately do not have a secret.
The collector exchanges the refresh token at `/api/v1/access_token`, caches the resulting
access token in memory until shortly before expiry, and requests a new access token when
needed.

### 3. App-only client credentials

For confidential clients using application-only OAuth:

```text
REDDIT_CLIENT_ID
REDDIT_CLIENT_SECRET
REDDIT_USER_AGENT
```

The collector requests `grant_type=client_credentials` and caches the returned access token in
memory until shortly before expiry.

## Runtime configuration

A source entry can keep the default environment-variable names:

```toml
[[sources]]
name = "reddit-saas"
type = "reddit"
subreddit = "SaaS"
listing = "new"
interval = 60
limit = 100
comments = 0
```

Or override the names without placing secret values in TOML:

```toml
access_token_env = "MY_REDDIT_ACCESS_TOKEN"
refresh_token_env = "MY_REDDIT_REFRESH_TOKEN"
client_id_env = "MY_REDDIT_CLIENT_ID"
client_secret_env = "MY_REDDIT_CLIENT_SECRET"
user_agent_env = "MY_REDDIT_USER_AGENT"
```

Credential precedence is:

1. configured access token for the first request;
2. refresh token when a fresh token is required;
3. `client_credentials` when no user token is configured.

This allows a deployment to start with a supplied access token while retaining a refresh token
for rollover.

## HTTP 401 behavior

For authenticated Reddit API calls, HTTP 401 triggers at most one re-authentication and retry
when either a refresh token or confidential app credentials are available.

```text
API request with bearer token
        |
       401
        |
 invalidate/reacquire access token
        |
 retry the original API request once
```

The token endpoint itself is never recursively retried as an OAuth-refresh attempt. If fresh
credentials are rejected, the normal collector authentication error is surfaced.

## Secret boundary

Access tokens, refresh tokens, client secrets, and Authorization headers are not placed in:

- `SignalEvent` values;
- cursors/checkpoints;
- SQLite event metadata;
- diagnostic output.

Configuration stores only environment-variable names. Keep the underlying environment values
in the deployment's secret manager.

## Incremental behavior

The source adapter uses Reddit listing fullnames such as `t3_...` as immutable post identities.
It combines Reddit's native `after` cursor with a bounded recent-ID watermark so polling can
resume across pages without replaying the entire subreddit history.

Comments are normalized as separate `SignalKind.COMMENT` events and preserve their parent/link
identifiers in metadata. The current comments mode collects bounded top-level comments; deeper
thread pagination remains a separate extension area.

## Rate limits

Reddit `X-Ratelimit-*` headers are translated into Stream's `RateLimitSnapshot`. In particular,
`X-Ratelimit-Reset` is interpreted using Reddit's reset semantics rather than treated as a
source event timestamp. Shared HTTP retry behavior still handles 429, transient 5xx responses,
timeouts, and network failures.
