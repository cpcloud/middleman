# SSE + ETag Live Sync

Replace frontend polling with server-sent events for instant UI updates, and add ETag-based conditional requests to reduce GitHub API rate limit consumption.

## Background

The current sync architecture has two latency sources:

1. **GitHub to middleman:** Syncer polls GitHub on a fixed timer (default 5m). No conditional requests, so every cycle makes full API calls even when nothing changed.
2. **middleman to browser:** Frontend polls multiple endpoints on independent timers (2s/30s sync status, 15s activity, 60s detail view). Changes detected by the syncer don't reach the browser until the next poll.

## Goals

- Browser sees sync results within seconds of sync completion, not up to 60s later
- GitHub API rate limit usage drops for unchanged data (304 responses cost 0 rate limit points)
- No new external infrastructure required (stays local-first)

## Non-goals

- Reducing the GitHub poll interval (that's a config knob, orthogonal to this work)
- WebSocket support (SSE is simpler, sufficient for server-to-client push)
- Caching response bodies (ETags are used as skip signals, not for response caching)

---

## Feature 1: Server-Sent Events

### Event Hub

New file: `internal/server/event_hub.go`

`EventHub` manages SSE subscribers with fan-out broadcasting.

```go
type Event struct {
    Type string
    Data any
}

type EventHub struct {
    mu          sync.Mutex
    subscribers map[uint64]chan Event
    nextID      uint64
}
```

Methods:
- `Subscribe(ctx context.Context) <-chan Event` -- creates a buffered channel (buffer size 16), registers it, spawns a goroutine that removes the subscriber when ctx is canceled. Returns the channel.
- `Broadcast(event Event)` -- iterates subscribers under lock, non-blocking send to each channel. Drops events for slow consumers (full channel).

### Event Types

| Event | Payload | Trigger |
|-------|---------|---------|
| `sync_status` | `SyncStatus` JSON (same shape as `GET /sync/status`) | Every status change in Syncer (started, progress, complete) |
| `data_changed` | `{}` (empty object) | After `RunOnce` completes |

Note: `data_changed` fires after every sync completion, even when ETags caused all requests to return 304 and nothing in the DB actually changed. This is intentional -- the cost of an unnecessary local re-fetch (Go server to SQLite to browser) is negligible, and tracking dirty state to suppress the event adds complexity for no meaningful benefit.

### SSE Endpoint

`GET /api/v1/events` -- registered as a plain `mux.HandleFunc` on the inner mux (not Huma, since SSE doesn't fit request/response OpenAPI modeling). The existing base-path `StripPrefix` setup handles path routing automatically since this is on the inner mux like all other handlers.

Handler:
1. Set headers: `Content-Type: text/event-stream`, `Cache-Control: no-cache`, `Connection: keep-alive`
2. Assert `http.Flusher` interface, flush headers immediately
3. Subscribe to hub with `r.Context()`
4. Start a 30s keepalive ticker
5. Select loop: on channel event, marshal to SSE wire format (`event: <type>\ndata: <json>\n\n`), write, flush. On ticker, write SSE comment (`: keepalive\n\n`), flush. If write returns error, return (client disconnected). On context cancel, return.
6. Keepalive ticker is stopped via defer

SSE is exempt from CSRF checks (GET request -- the existing CSRF check in `ServeHTTP` only applies to non-GET methods).

### Syncer Integration

The Syncer gets a callback field:

```go
type Syncer struct {
    // ... existing fields ...
    onStatusChange func(*SyncStatus)
}
```

Set during server construction via a setter. No import cycle -- the github package doesn't import server.

```go
syncer.SetOnStatusChange(func(status *SyncStatus) {
    hub.Broadcast(Event{Type: "sync_status", Data: status})
    if !status.Running {
        hub.Broadcast(Event{Type: "data_changed", Data: struct{}{}})
    }
})
```

The callback fires at each `s.status.Store(...)` call site in `RunOnce`:
1. `SyncStatus{Running: true}` -- sync started (broadcasts `sync_status`)
2. `SyncStatus{Running: true, CurrentRepo: ..., Progress: ...}` -- per-repo progress (broadcasts `sync_status`)
3. `SyncStatus{Running: false, LastRunAt: ..., LastError: ...}` -- sync complete (broadcasts `sync_status` + `data_changed`)

### WriteTimeout

The server's current `WriteTimeout: 30s` will kill SSE connections after 30 seconds. Set `WriteTimeout: 0` on the main `http.Server` to allow long-lived SSE connections. The SSE handler uses 30s keepalive writes to detect dead connections via write errors.

This is safe because the server only listens on loopback (127.0.0.1). `ReadTimeout` (15s) still protects against slow request reads. `IdleTimeout` (60s) still cleans up idle HTTP/1.1 keep-alive connections (distinct from SSE connections, which are active).

---

## Feature 2: ETag Conditional Requests

### Target Endpoints

Two list endpoints benefit from ETags:

| Endpoint | Calls per cycle | ETag benefit |
|----------|----------------|-------------|
| `ListOpenPullRequests` | 1 per repo | Skip entire PR list processing when no PRs changed |
| `ListOpenIssues` | 1 per repo | Skip entire issue list processing when no issues changed |

`GetCombinedStatus` and `ListCheckRunsForRef` are excluded from ETag support. While they run unconditionally every cycle, their normalization functions (`NormalizeCIStatus`, `NormalizeCIChecks`) merge data from both sources into a single `ci_checks_json` column. Partial updates (one endpoint returns 304, the other returns 200) would require parsing and merging the existing JSON blob, adding complexity that outweighs the rate limit savings from these single-request endpoints.

All other read endpoints are already conditionally called (guarded by `UpdatedAt` comparison or in-memory caches).

### ETag Transport

New file: `internal/github/etag_transport.go`

```go
type etagTransport struct {
    base  http.RoundTripper
    cache sync.Map // URL string -> etagEntry
}

type etagEntry struct {
    etag     string
    cachedAt time.Time
}
```

**ETag TTL:** Cached ETags expire after `etagTTL` (constant, 30 minutes). Expired entries are treated as uncached — no `If-None-Match` sent, forcing an unconditional fetch. This bounds staleness for edge cases where a 304 hides changes that only affect later pages (see "Pagination and ETags" section). At the default 5-minute sync interval, this means ~6 ETag-accelerated cycles per unconditional refresh — still a large rate limit improvement over no caching.

```go
const etagTTL = 30 * time.Minute
```

`RoundTrip(req)`:
1. Check if this is a paginated later-page request: if the URL has a `page` query parameter with value > 1, skip ETag handling entirely — pass through to the base transport and return.
2. Look up `req.URL.String()` in cache. If found AND not expired (`time.Since(entry.cachedAt) < etagTTL`), clone the request and add `If-None-Match: <etag>` header (must clone to avoid mutating the original). If expired, delete the entry and proceed as uncached.
3. Call `base.RoundTrip(req)` with the (possibly modified) request.
4. On 200: if the response has an `ETag` header AND does NOT have a `Link` header containing `rel="next"` (indicating this is a single-page result), store the ETag in cache with `cachedAt: time.Now()`. If the response IS multi-page (has `Link: next`), delete any previously-cached ETag for this URL (`cache.Delete(url)`) — this evicts stale entries from when the endpoint was single-page and ensures multi-page endpoints always fetch fresh on the next cycle.
5. On 304: return response as-is (empty body, 304 status).
6. On other status: return response as-is.

**Three pagination safeguards:**
- Step 1 (page parameter check) prevents later pages from caching or using stale ETags. go-github's first page request has no `page` parameter, while subsequent pages have `page=2`, `page=3`, etc.
- Step 4 (Link: next eviction) prevents the first page of multi-page results from being cached, AND actively evicts any previously-cached ETag for that URL. This handles the single-page → multi-page transition: if a repo was single-page (ETag cached), then grows past 100 items, the first 200 response with `Link: next` evicts the stale entry.
- Step 2 (TTL expiry) bounds staleness for the reverse edge case: a cached single-page ETag that 304s indefinitely, hiding a transition to multi-page. `ListOpenPullRequests` uses GitHub's default `created desc` sort, so page 1 content can remain unchanged even when later-page items are modified. The 30-minute TTL forces periodic unconditional fetches, bounding the window during which such changes are invisible.

All three are self-contained in the transport. No syncer, client, or collectPages changes are needed.

### Not-Modified Detection

go-github treats non-2xx responses as errors. When the transport returns a 304, go-github wraps it in `*github.ErrorResponse`. Detection helper in `internal/github/etag_transport.go`:

```go
func IsNotModified(err error) bool {
    var ghErr *github.ErrorResponse
    return errors.As(err, &ghErr) && ghErr.Response.StatusCode == http.StatusNotModified
}
```

### Client Changes

`NewClient` wraps the OAuth2 HTTP client's transport with `etagTransport`:

```go
func NewClient(token string) Client {
    ts := oauth2.StaticTokenSource(&oauth2.Token{AccessToken: token})
    tc := oauth2.NewClient(context.Background(), ts)
    tc.Transport = &etagTransport{base: tc.Transport}
    return &liveClient{gh: gh.NewClient(tc)}
}
```

### Syncer Changes

#### Head SHA Cache

The Syncer needs a new in-memory cache to support CI refresh when `ListOpenPullRequests` returns 304:

```go
type Syncer struct {
    // ... existing fields ...
    headSHAs map[int64]map[int]string // repoID -> PR number -> head SHA
}
```

Populated during normal sync (when `ListOpenPullRequests` returns 200). On each 200 response, replace the entire inner map for this repoID with only the PRs from the current response -- this automatically evicts entries for closed/merged PRs that no longer appear in the open list. Lost on process restart, but so is the ETag cache, so the first sync always does a full fetch.

#### Two call sites with IsNotModified handling

**1. `doSyncRepo` -- ListOpenPullRequests:**

The 304 path must NOT return early -- `doSyncRepo` still needs to run `syncIssues` after PR handling. Structure:

```go
ghPRs, err := s.client.ListOpenPullRequests(ctx, repo.Owner, repo.Name)
if IsNotModified(err) {
    // PR list unchanged -- still refresh CI status for existing open PRs
    if ciErr := s.refreshCIForExistingPRs(ctx, repo, repoID); ciErr != nil {
        slog.Error("refresh CI for existing PRs", "repo", repoName, "err", ciErr)
    }
    // Fall through to syncIssues below
} else if err != nil {
    return fmt.Errorf("list open PRs: %w", err)
} else {
    // Normal path: process PRs, handle closures, populate headSHAs cache
    // ... existing PR processing logic ...
}

// Always sync issues regardless of PR list ETag result
if err := s.syncIssues(ctx, repo, repoID); err != nil { ... }
```

New helper `refreshCIForExistingPRs`: reads the head SHA cache for this repoID, calls `refreshCIStatus` for each entry (which still fetches CI data from the GitHub API unconditionally -- no ETags on CI endpoints). If the cache is empty (first sync after restart), returns nil (the 304 won't happen anyway since there's no cached ETag on first sync).

On normal 200 response, replace the headSHAs entry for this repoID with only the current open PRs' head SHAs.

**2. `syncIssues` -- ListOpenIssues:**
```go
ghIssues, err := s.client.ListOpenIssues(ctx, repo.Owner, repo.Name)
if IsNotModified(err) {
    return nil // issues unchanged, nothing to do
}
if err != nil {
    return fmt.Errorf("list open issues: %w", err)
}
```

Issues have no independent background updates (unlike CI), so 304 means skip entirely.

### Pagination and ETags

`ListOpenPullRequests` and `ListOpenIssues` use `collectPages`. The ETag transport handles pagination entirely on its own — no syncer, client, or `collectPages` changes needed.

**Later pages bypass ETags:** URLs with a `page` query parameter > 1 skip ETag handling entirely (no `If-None-Match` sent, no ETag stored). go-github's first-page request has no `page` parameter; subsequent pages have `page=2`, `page=3`, etc. This prevents a correctness bug: if page 1 returns 200 (data changed) but page 2 had a stale cached ETag and returned 304, `collectPages` would abort with a not-modified error, discarding the real page-1 changes.

**Multi-page first pages evict cached ETags:** The transport checks the `Link` response header for `rel="next"`. If present, the response spans multiple pages — the transport does NOT cache the ETag and explicitly deletes any previously-cached entry for that URL. This handles the single-page → multi-page transition: a repo that was under 100 items (ETag cached) then grows past 100 won't retain a stale ETag.

**TTL bounds hidden transitions:** A cached single-page ETag can legitimately 304 even after the list grows to multiple pages, because `ListOpenPullRequests` uses GitHub's `created desc` sort — page 1 content may not change when items shift to later pages. The 30-minute TTL on cached ETags forces periodic unconditional fetches, bounding the window during which such changes are invisible. At the default 5-minute sync interval, this means ~6 ETag-accelerated cycles per unconditional refresh.

When the first page returns 304, `collectPages` returns the not-modified error immediately. The caller treats the entire list as unchanged and skips processing. Single-page repos (the common case) get full ETag benefit. Multi-page repos always fetch fresh after the first unconditional cycle detects the transition.

### ETag Cache Lifetime

The `sync.Map` lives on the transport for the process lifetime. Entries are keyed by full URL (including query params). Only single-page first-page URLs are cached (later pages and multi-page first pages are excluded), so cache size is bounded by repos * 2 list endpoints. For a typical setup (5 repos), this is at most 10 entries. Entries expire after `etagTTL` (30 minutes) and are lazily deleted on lookup. No background eviction needed.

---

## Feature 3: Frontend SSE Client

### Events Store

New file: `frontend/src/lib/stores/events.svelte.ts`

Central SSE connection manager:

```typescript
let eventSource: EventSource | null = null;
let connected = $state(false);

export function connect(): void
export function disconnect(): void
export function isSSEConnected(): boolean
```

**`connect()` implementation:**
- Constructs URL using `getBasePath()` from router store: `${basePrefix}/api/v1/events`
- Creates `EventSource` at that URL
- Registers event listeners:
  - `sync_status`: parse JSON payload, call `updateSyncFromSSE(status)` on sync store
  - `data_changed`: call refresh functions based on current view (see below)
  - `open`: set `connected = true`, call `disablePolling()` on all stores with polling (sync, activity, detail, pulls, issues)
  - `error`: set `connected = false`, call `enablePolling()` on all stores with polling (sync, activity, detail, pulls, issues). EventSource handles reconnection automatically.

**View-aware refresh on `data_changed`:**

The events store imports `getPage()` from the router store to determine which views are active:

| Current page | View | Actions on `data_changed` |
|-------------|------|--------------------------|
| `pulls` | list | `loadPulls()` to refresh sidebar/list. If a PR detail is selected (`getSelectedPR()` is non-null), also call `refreshDetail()` with that PR's owner/name/number. |
| `pulls` | board | `loadPulls({ state: "open" })` (board always shows open PRs). If the board drawer is open, also call `refreshDetail()` for the drawer's PR. |
| `issues` | - | `loadIssues()` to refresh list. If an issue is selected, also call `refreshFromSSE()` on the issues store for the selected issue's detail. |
| `activity` | - | `pollNewItems()` for incremental activity feed update. |
| `settings` | - | No data refresh needed. |

**Board view specifics:** `KanbanBoard.svelte` has its own drawer state (`drawerPR`) that is local to the component, not in the pulls store. The events store cannot directly access this. Two options: (a) move `drawerPR` into the pulls store so the events store can check it, or (b) have the board component register a refresh callback with the events store on mount and unregister on destroy. Option (b) is simpler and doesn't require restructuring the board's local state.

`loadPulls()` is always called on `data_changed` regardless of current page, since the sidebar PR count badges are visible on all pages. When on the board view, the events store calls `loadPulls({ state: "open" })` instead of the generic `loadPulls()` to match the board's filter. The `getView()` helper from the router store distinguishes list from board.

### Store Changes

**Prerequisite: move component-level polling into stores.** Currently, `PullList.svelte`, `IssueList.svelte`, and `KanbanBoard.svelte` each have their own 15s `setInterval` timers that call `loadPulls()`/`loadIssues()` directly. These must be moved into their respective stores so the SSE events store can centrally control them. Without this, SSE would disable store-level polling but component-level timers would keep firing.

**`pulls.svelte.ts`:**
- New `startListPolling(overrides?)` / `stopListPolling()` functions managing a 15s timer that calls `loadPulls(overrides)`. The optional `overrides` parameter lets callers lock the timer to specific filters (e.g., `{ state: "open" }` for the board view). Replaces the `setInterval` in `PullList.svelte` and `KanbanBoard.svelte`.
- New `enablePolling()` / `disablePolling()` to gate polling on SSE connection state. `startListPolling` checks the `pollingEnabled` flag before creating the timer.
- Events store calls `loadPulls()` on `data_changed`.

**`issues.svelte.ts`:**
- New `startListPolling()` / `stopListPolling()` functions managing a 15s timer that calls `loadIssues()`. Replaces the `setInterval` in `IssueList.svelte`.
- New `enablePolling()` / `disablePolling()` with same pattern.
- New `refreshFromSSE(owner: string, name: string, number: number)` that calls the existing issue detail refresh.
- Events store calls `loadIssues()` on `data_changed` when issues page is active.

**`sync.svelte.ts`:**
- New `updateSyncFromSSE(status: SyncStatus)` function: sets state directly and fires `onSyncComplete` callback when detecting running-to-idle transition. Same logic as `refreshSyncStatus` but without the HTTP call.
- New `enablePolling()` / `disablePolling()` functions. `disablePolling` clears the interval. `enablePolling` restarts it at the current interval. `startPolling` and `stopPolling` still exist for lifecycle (mount/unmount) but check a `pollingEnabled` flag before creating timers.

**`activity.svelte.ts`:**
- New `enablePolling()` / `disablePolling()` functions with same pattern.
- New `refreshFromSSE()` that calls the existing `pollNewItems()`.

**`detail.svelte.ts`:**
- New `enablePolling()` / `disablePolling()` functions with same pattern.
- New `refreshFromSSE(owner: string, name: string, number: number)` that calls the existing `refreshDetail()`.

### Connection Lifecycle

```
App mount      -> connect()
SSE open       -> set connected, disable all polling timers
SSE error      -> clear connected, enable all polling timers
                  (EventSource auto-reconnects with backoff)
SSE reconnect  -> set connected, disable polling again
App unmount    -> disconnect() (close EventSource)
```

`connect()` is called from `App.svelte`'s `onMount`. `disconnect()` is called from `onDestroy`.

**No event replay:** The SSE endpoint does not use event IDs or maintain a replay buffer. Events emitted during a brief SSE disconnection are lost. This is acceptable because: (a) polling re-enables immediately on disconnect and catches up within seconds, and (b) the next `data_changed` event after reconnection triggers a full refresh of all active views.

### Fallback Behavior

When SSE is disconnected (fallback):
- Sync status polling: 2s while syncing, 30s idle (current behavior, unchanged)
- Activity polling: 15s (current behavior, unchanged)
- Detail polling: 60s (current behavior, unchanged)

When SSE is connected:
- All polling timers disabled
- `data_changed` triggers immediate refresh of active views
- `sync_status` updates sync state directly (no HTTP call)

---

## Testing

### Go Tests

**`internal/server/event_hub_test.go`:**
- Subscribe returns a channel that receives broadcast events
- Unsubscribe on context cancel (no goroutine leak)
- Concurrent broadcast safety (multiple goroutines broadcasting)
- Slow consumer (full channel) doesn't block other subscribers

**`internal/github/etag_transport_test.go`:**
- 200 response stores ETag from header
- Subsequent request to same URL includes `If-None-Match` header
- 304 response returned as-is (status preserved)
- Different URLs get independent ETag entries
- Request without cached ETag has no `If-None-Match` header
- Requests with `page` query parameter > 1 bypass ETag handling (no `If-None-Match` sent, no ETag stored)
- 200 response with `Link: rel="next"` evicts any previously-cached ETag for that URL
- Single-page → multi-page → single-page transition: ETag cached on single-page, evicted on multi-page detection, re-cached when back to single-page
- Expired ETag entries (older than `etagTTL`) are treated as uncached
- `IsNotModified` returns true for 304 errors, false for other errors

**Integration tests:**
- SSE endpoint: returns `text/event-stream` content type, receives events after broadcast, connection closes cleanly on client disconnect
- Syncer with `onStatusChange`: callback fires for started/progress/complete transitions
- Syncer with `IsNotModified`: verify PR processing is skipped on 304, CI refresh still runs with cached head SHAs, issue sync still runs on PR list 304

### Frontend Tests

- Events store: `connect()` creates EventSource with correct URL, `disconnect()` closes it
- Polling toggle: `disablePolling()` clears timers, `enablePolling()` restarts them
- `updateSyncFromSSE` updates state and fires completion callback
- View-aware refresh: `data_changed` triggers correct store functions based on current page

---

## Files Changed

### New Files
| File | Purpose |
|------|---------|
| `internal/server/event_hub.go` | SSE subscriber management and fan-out |
| `internal/server/event_hub_test.go` | Hub unit tests |
| `internal/github/etag_transport.go` | HTTP transport with ETag injection + `IsNotModified` helper |
| `internal/github/etag_transport_test.go` | Transport unit tests |
| `frontend/src/lib/stores/events.svelte.ts` | SSE client and connection management |

### Modified Files
| File | Change |
|------|--------|
| `internal/server/server.go` | Add `EventHub` field, register `GET /api/v1/events` handler, set `WriteTimeout: 0`, wire syncer callback via `SetOnStatusChange` |
| `internal/github/client.go` | Wrap OAuth2 transport with `etagTransport` in `NewClient` |
| `internal/github/sync.go` | Add `onStatusChange` callback + setter, `headSHAs` cache, `IsNotModified` checks at 2 call sites, `refreshCIForExistingPRs` helper, refactor `refreshCIStatus` to take `number int, headSHA string` instead of `*gh.PullRequest` |
| `frontend/src/lib/stores/sync.svelte.ts` | Add `updateSyncFromSSE`, `enablePolling`/`disablePolling`, `pollingEnabled` flag |
| `frontend/src/lib/stores/activity.svelte.ts` | Add `refreshFromSSE`, `enablePolling`/`disablePolling`, `pollingEnabled` flag |
| `frontend/src/lib/stores/detail.svelte.ts` | Add `refreshFromSSE`, `enablePolling`/`disablePolling`, `pollingEnabled` flag |
| `frontend/src/lib/stores/issues.svelte.ts` | Add `refreshFromSSE`, `enablePolling`/`disablePolling`, `pollingEnabled` flag |
| `frontend/src/lib/stores/pulls.svelte.ts` | Add `startListPolling`/`stopListPolling`, `enablePolling`/`disablePolling`, `pollingEnabled` flag |
| `frontend/src/lib/components/sidebar/PullList.svelte` | Remove 15s `setInterval`, call `startListPolling`/`stopListPolling` from pulls store |
| `frontend/src/lib/components/sidebar/IssueList.svelte` | Remove 15s `setInterval`, call `startListPolling`/`stopListPolling` from issues store |
| `frontend/src/lib/components/kanban/KanbanBoard.svelte` | Remove 15s `setInterval`, call `startListPolling({ state: "open" })`/`stopListPolling` from pulls store |
| `frontend/src/App.svelte` | Call `connect()` on mount, `disconnect()` on destroy |
