# Reddit adapter

SignalKit Stream's Reddit adapter is intentionally OAuth-only. It does **not** fall back to anonymous JSON endpoints, HTML scraping, browser automation, proxy rotation, or other access workarounds.

Reddit's current Data API guidance requires OAuth authentication and a unique, descriptive User-Agent, and Reddit's current builder/data terms require the appropriate approval for the intended use. Commercial use can require Reddit's written permission or a separate agreement. Before enabling this adapter, verify that your client and use case are approved under the current Reddit terms.

Official references:

- Reddit Data API Wiki: https://support.reddithelp.com/hc/en-us/articles/16160319875092-Reddit-Data-API-Wiki
- Reddit Data API Terms: https://redditinc.com/policies/data-api-terms
- Reddit Responsible Builder Policy: https://redditinc.com/policies/responsible-builder-policy
- Reddit API reference: https://www.reddit.com/dev/api

## Runtime configuration

One collector instance follows one subreddit listing. Configure posts and comments as separate source entries so each stream has its own cursor and checkpoint.

Posts:

```toml
[[sources]]
name = "reddit-saas-posts"
type = "reddit"
subreddit = "SaaS"
listing = "posts"
interval = 60
limit = 100
user_agent_env = "REDDIT_USER_AGENT"
```

Comments:

```toml
[[sources]]
name = "reddit-saas-comments"
type = "reddit"
subreddit = "SaaS"
listing = "comments"
interval = 60
limit = 100
user_agent_env = "REDDIT_USER_AGENT"
```

The default credential environment variables are:

```text
REDDIT_USER_AGENT
REDDIT_ACCESS_TOKEN
REDDIT_CLIENT_ID
REDDIT_CLIENT_SECRET
REDDIT_REFRESH_TOKEN
```

For short-lived jobs, `REDDIT_ACCESS_TOKEN` is enough. For a long-running runtime, configure `REDDIT_CLIENT_ID` and `REDDIT_REFRESH_TOKEN`; `REDDIT_CLIENT_SECRET` may be empty for client types that do not have a secret. The collector refreshes and caches access tokens automatically.

Environment variable names can be overridden per source:

```toml
access_token_env = "MY_REDDIT_ACCESS_TOKEN"
client_id_env = "MY_REDDIT_CLIENT_ID"
client_secret_env = "MY_REDDIT_CLIENT_SECRET"
refresh_token_env = "MY_REDDIT_REFRESH_TOKEN"
user_agent_env = "MY_REDDIT_USER_AGENT"
```

A direct non-secret `user_agent` option is also accepted. Do not put access tokens, refresh tokens, or client secrets directly in `signalkit.toml`.

## Incremental behavior

The adapter uses Reddit listing fullnames such as `t3_...` and `t1_...` as immutable source IDs.

The first successful poll emits the newest page and stores it as the incremental boundary instead of walking the entire historical subreddit. On later polls, the collector starts from the newest listing and follows Reddit's `after` pagination until it reaches an item in the previous seen window. If more than one page of new activity arrived between polls, the cursor preserves the in-progress cycle so the next pipeline call continues without skipping a page.

The cursor keeps:

```text
initialized
seen_ids
cycle_ids
after
```

`seen_window` defaults to 1000 and must be at least 100. Increase it for very high-volume communities or longer polling intervals.

## Normalization

Posts become `SignalKind.POST`; comments become `SignalKind.COMMENT`.

Posts preserve source-specific fields such as score, comment count, flair, NSFW flag, self-post flag, and outbound URL under `metadata`. Their canonical SignalKit URL is the Reddit permalink.

Comments preserve `parent_id`, `link_id`, link title, link URL, score, and subreddit under `metadata`.

Reddit's `created_utc` becomes the event `created_at`. A numeric `edited` timestamp becomes `updated_at`.

## Rate limits

Reddit reports rate-limit information through `X-Ratelimit-*` response headers. In particular, `X-Ratelimit-Reset` is interpreted as a number of seconds until reset rather than a Unix epoch timestamp. The adapter translates those headers into SignalKit's `RateLimitSnapshot`, allowing the runtime scheduler to delay the next poll when the source is exhausted.

HTTP 429, transient 5xx responses, timeouts, and network failures still use the shared `HTTPCollector` retry policy. OAuth 401 responses can trigger one token refresh and retry when refresh credentials are configured.

## Security and policy boundary

The adapter never logs configured tokens or client secrets. Credentials are read from environment variables and used only for Reddit OAuth/API requests.

If Reddit changes access requirements, endpoints, scopes, or rate-limit semantics, update the adapter and its live compatibility tests rather than introducing scraping as an automatic fallback.
