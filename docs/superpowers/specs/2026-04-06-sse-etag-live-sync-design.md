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

Set during server construction via a setter: `syncer.SetOnStatusChange(func(status *SyncStatus) { hub.Broadcast(...) })`. No import cycle -- the github package doesn't import server.

The callback fires at each `s.status.Store(...)` call site in `RunOnce`:
1. `SyncStatus{Running: true}` -- sync started
2. `SyncStatus{Running: true, CurrentRepo: ..., Progress: ...}` -- per-repo progress (fires once per repo)
3. `SyncStatus{Running: false, LastRunAt: ..., LastError: ...}` -- sync complete

After the final status store (sync complete), also broadcast `Event{Type: "data_changed", Data: struct{}{}}`.

### WriteTimeout

The server's current `WriteTimeout: 30s` will kill SSE connections after 30 seconds. Set `WriteTimeout: 0` on the main `http.Server` to allow long-lived SSE connections. The SSE handler uses 30s keepalive writes to detect dead connections via write errors.

This is safe because the server only listens on loopback (127.0.0.1). `ReadTimeout` (15s) still protects against slow request reads. `IdleTimeout` (60s) still cleans up idle HTTP/1.1 keep-alive connections (distinct from SSE connections, which are active).

---

## Feature 2: ETag Conditional Requests

### Target Endpoints

Four endpoints benefit from ETags (the only ones called unconditionally every sync cycle):

| Endpoint | Calls per cycle | ETag benefit |
|----------|----------------|-------------|
| `ListOpenPullRequests` | 1 per repo | Skip entire PR list processing when no PRs changed |
| `ListOpenIssues` | 1 per repo | Skip entire issue list processing when no issues changed |
| `GetCombinedStatus` | 1 per open PR | Skip CI status write when status unchanged for this SHA |
| `ListCheckRunsForRef` | 1 per open PR | Skip check runs write when runs unchanged for this SHA |

All other read endpoints are already conditionally called (guarded by `UpdatedAt` comparison or in-memory caches).

### ETag Transport

New file: `internal/github/etag_transport.go`

```go
type etagTransport struct {
    base  http.RoundTripper
    cache sync.Map // URL string -> etagEntry
}

type etagEntry struct {
    etag string
}
```

`RoundTrip(req)`:
1. Look up `req.URL.String()` in cache. If found, clone the request and add `If-None-Match: <etag>` header (must clone to avoid mutating the original).
2. Call `base.RoundTrip(req)` with the (possibly modified) request.
3. On 200: store the response `ETag` header value in cache (if present). Return response.
4. On 304: return response as-is (empty body, 304 status).
5. On other status: return response as-is.

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

Populated during normal sync (when `ListOpenPullRequests` returns 200). Each PR's head SHA is stored after processing. Lost on process restart, but so is the ETag cache, so the first sync always does a full fetch.

#### Four call sites with IsNotModified handling

**1. `doSyncRepo` -- ListOpenPullRequests:**
```go
ghPRs, err := s.client.ListOpenPullRequests(ctx, repo.Owner, repo.Name)
if IsNotModified(err) {
    // PR list unchanged -- still refresh CI status for existing open PRs
    return s.refreshCIForExistingPRs(ctx, repo, repoID)
}
if err != nil {
    return fmt.Errorf("list open PRs: %w", err)
}
```

New helper `refreshCIForExistingPRs`: reads the head SHA cache for this repoID, calls `refreshCIStatus` for each entry. If the cache is empty (first sync after restart), falls through to the error path which triggers a full fetch on the next cycle (the 304 won't happen anyway since there's no cached ETag).

On normal 200 response, store each PR's head SHA in the cache after processing.

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

**3. `refreshCIStatus` -- GetCombinedStatus:**
```go
combined, err := s.client.GetCombinedStatus(ctx, repo.Owner, repo.Name, headSHA)
if IsNotModified(err) {
    combined = nil // mark as unchanged
    err = nil
}
if err != nil {
    slog.Warn("get combined status failed", ...)
    return nil
}
```

**4. `refreshCIStatus` -- ListCheckRunsForRef:**
```go
checkRuns, err := s.client.ListCheckRunsForRef(ctx, repo.Owner, repo.Name, headSHA)
if IsNotModified(err) {
    checkRuns = nil // mark as unchanged
    err = nil
}
if err != nil {
    slog.Warn("list check runs failed", ...)
    return nil
}

// Only update DB if at least one source returned new data
if combined == nil && checkRuns == nil {
    return nil // both unchanged, skip DB write entirely
}
```

When one is nil (304) and the other has data (200), the DB update uses the new data for the changed source and preserves existing data for the unchanged source. Read the existing values from DB before the partial update.

### Pagination and ETags

`ListOpenPullRequests` and `ListOpenIssues` use `collectPages`. The ETag transport caches per-URL, so each page gets its own ETag.

When page 1 returns 304 (via `collectPages` returning the not-modified error from go-github), the caller treats the entire list as unchanged. When page 1 returns 200, all pages are fetched normally.

**Known limitation for multi-page results:** `ListOpenPullRequests` sorts by creation date (GitHub default). If a PR beyond page 1 is updated (e.g., gets a new comment), page 1's content and ETag may not change, causing the update to be missed for one sync cycle. This is acceptable for middleman's target use case (small repo set -- most repos have <100 open PRs, fitting in a single page). `ListOpenIssues` sorts by `updated_at DESC`, so the most recently updated issue always appears on page 1 and this limitation does not apply.

### ETag Cache Lifetime

The `sync.Map` lives on the transport for the process lifetime. Entries are keyed by full URL (including query params). Cache size is bounded by the number of unique URLs hit: (repos * 2 list endpoints) + (open PRs * 2 CI endpoints). For a typical setup (5 repos, 20 open PRs each), this is ~210 entries.

Stale entries (e.g., CI status URLs for merged PRs) are harmless -- tiny memory, never matched again. No eviction needed.

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
  - `open`: set `connected = true`, call `disablePolling()` on sync/activity/detail stores
  - `error`: set `connected = false`, call `enablePolling()` on sync/activity/detail stores. EventSource handles reconnection automatically.

**View-aware refresh on `data_changed`:**

The events store imports `getPage()` from the router store to determine which views are active:

| Current page | Actions on `data_changed` |
|-------------|--------------------------|
| `pulls` | `loadPulls()` to refresh sidebar/list. If a PR detail is selected (`getSelectedPR()` is non-null), also call `refreshDetail()` with that PR's owner/name/number. |
| `issues` | `loadIssues()` to refresh list. If an issue is selected, also call `refreshFromSSE()` on the issues store for the selected issue's detail. |
| `activity` | `pollNewItems()` for incremental activity feed update. |
| `settings` | No data refresh needed. |

`loadPulls()` is always called on `data_changed` regardless of current page, since the sidebar PR count badges are visible on all pages. (If this becomes a performance concern, it can be gated to pulls/activity pages only.)

### Store Changes

**`sync.svelte.ts`:**
- New `updateSyncFromSSE(status: SyncStatus)` function: sets state directly and fires `onSyncComplete` callback when detecting running-to-idle transition. Same logic as `refreshSyncStatus` but without the HTTP call.
- New `enablePolling()` / `disablePolling()` functions. `disablePolling` clears the interval. `enablePolling` restarts it at the current interval. `startPolling` and `stopPolling` still exist for lifecycle (mount/unmount) but check a `pollingEnabled` flag before creating timers.

**`activity.svelte.ts`:**
- New `enablePolling()` / `disablePolling()` functions with same pattern.
- New `refreshFromSSE()` that calls the existing `pollNewItems()`.

**`detail.svelte.ts`:**
- New `enablePolling()` / `disablePolling()` functions with same pattern.
- New `refreshFromSSE(owner: string, name: string, number: number)` that calls the existing `refreshDetail()`.

**`issues.svelte.ts`:**
- New `enablePolling()` / `disablePolling()` functions with same pattern (for issue detail polling).
- New `refreshFromSSE(owner: string, name: string, number: number)` that calls the existing issue detail refresh.
- Events store calls `loadIssues()` on `data_changed` when issues page is active.

**`pulls.svelte.ts`:**
- No polling to disable (already on-demand).
- Events store calls `loadPulls()` directly.

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
- `IsNotModified` returns true for 304 errors, false for other errors

**Integration tests:**
- SSE endpoint: returns `text/event-stream` content type, receives events after broadcast, connection closes cleanly on client disconnect
- Syncer with `onStatusChange`: callback fires for started/progress/complete transitions
- Syncer with `IsNotModified`: verify PR processing is skipped on 304, CI refresh still runs with cached head SHAs

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
| `internal/github/sync.go` | Add `onStatusChange` callback + setter, `headSHAs` cache, `IsNotModified` checks at 4 call sites, `refreshCIForExistingPRs` helper |
| `frontend/src/lib/stores/sync.svelte.ts` | Add `updateSyncFromSSE`, `enablePolling`/`disablePolling`, `pollingEnabled` flag |
| `frontend/src/lib/stores/activity.svelte.ts` | Add `refreshFromSSE`, `enablePolling`/`disablePolling`, `pollingEnabled` flag |
| `frontend/src/lib/stores/detail.svelte.ts` | Add `refreshFromSSE`, `enablePolling`/`disablePolling`, `pollingEnabled` flag |
| `frontend/src/lib/stores/issues.svelte.ts` | Add `refreshFromSSE`, `enablePolling`/`disablePolling`, `pollingEnabled` flag |
| `frontend/src/lib/stores/pulls.svelte.ts` | No structural changes (events store calls `loadPulls` directly) |
| `frontend/src/App.svelte` | Call `connect()` on mount, `disconnect()` on destroy |
