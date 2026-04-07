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
    mu             sync.Mutex
    subscribers    map[uint64]chan Event
    nextID         uint64
    lastSyncStatus *Event // most recent sync_status event, cached for new subscribers
}
```

Methods:
- `Subscribe(ctx context.Context) <-chan Event` -- under `mu`, creates a buffered channel (buffer size 16), pre-loads `lastSyncStatus` into the channel if non-nil, registers the channel, and releases the lock. Spawns a goroutine that removes the subscriber and closes the channel when ctx is canceled. Returns the channel.
- `Broadcast(event Event)` -- under `mu`, updates `lastSyncStatus` if `event.Type == "sync_status"` (stored by value), then iterates subscribers with a non-blocking send to each channel. Drops events for slow consumers (full channel).

**Ordering guarantee:** `Subscribe` and `Broadcast` share the same `mu`, so the channel observed by a new subscriber always begins with the cached `lastSyncStatus` (if any) followed strictly by events broadcast after `Subscribe` returned. A transition broadcast that lands between a naive snapshot read and a subscribe can never sneak in ahead of the initial event, so the client cannot regress from a newer snapshot to an older buffered transition.

**Priming and startup order:** Broadcasting with no subscribers is cheap (the subscriber loop is empty) and still updates `lastSyncStatus`. Priming the hub on startup must happen **before** `syncer.Start(ctx)`, or a newer callback-driven broadcast from an already-running sync could be overwritten by a later prime reading an older `Syncer.Status()` value. The required ordering is:

1. Create the syncer (`NewSyncer`) — not yet started, `Status()` returns the zero value.
2. Construct the server (`server.NewWithConfig`) — this creates the `EventHub`, registers the `onStatusChange` callback on the syncer via `SetOnStatusChange`, and primes the hub with `Broadcast(Event{Type: "sync_status", Data: syncer.Status()})`. Because the syncer has not started yet, no callback broadcasts can race with this prime.
3. Call `syncer.Start(ctx)` — from this point forward, any status change flows through the already-wired callback, and the hub's `lastSyncStatus` stays monotonically current under the broadcast mutex.
4. Call `srv.Listen(addr)` (synchronously prepares `httpSrv`) and then `srv.Serve()` in a goroutine, so the server only begins accepting requests after the syncer is running.

This ordering also guarantees that the first HTTP request on `/api/v1/events` cannot land before the prime, because the server isn't listening yet.

To make this ordering testable and resistant to regression in `cmd/middleman/main.go`, extract the bootstrap logic into a shared helper with two distinct entry points:

```go
// cmd/middleman/app.go

type App struct {
    Server *server.Server
    Syncer *ghclient.Syncer
    DB     *db.DB
}

// Bootstrap performs steps 1–2 above (create syncer, construct server which
// primes the hub and wires the callback) without starting the syncer or
// serving HTTP. Tests use this directly to inspect hub state mid-sync.
// configPath is required because server.NewWithConfig uses it to persist
// PUT /api/v1/settings and repo add/remove writes back to the same file
// the CLI loaded.
func Bootstrap(cfg *config.Config, configPath string, ghClient ghclient.Client) (*App, error)

// Run is the only entry point `cmd/middleman/main.go` may use. It calls
// Bootstrap, starts the syncer, synchronously binds the listening
// socket via Server.Listen(addr) (returning any bind error to the
// caller directly), runs Server.Serve() in a goroutine, then selects
// on ctx.Done() (triggering a graceful shutdown) or a server error.
// Returns nil only if Serve returned http.ErrServerClosed (normal
// shutdown); any other error from Listen, Serve, or Shutdown is
// returned wrapped.
func Run(ctx context.Context, cfg *config.Config, configPath string, ghClient ghclient.Client, addr string) error {
    app, err := Bootstrap(cfg, configPath, ghClient)
    if err != nil {
        return err
    }
    defer app.DB.Close()
    app.Syncer.Start(ctx)
    defer app.Syncer.Stop()

    // Synchronously bind the TCP listener and prepare *http.Server
    // BEFORE spawning the serve goroutine. A bind error here is
    // returned directly and cannot be masked by a later ctx cancel,
    // and Shutdown is guaranteed to see a live httpSrv + listener
    // even if ctx fires before the serve goroutine is scheduled.
    if err := app.Server.Listen(addr); err != nil {
        return fmt.Errorf("listen: %w", err)
    }

    errCh := make(chan error, 1)
    go func() {
        err := app.Server.Serve()
        if errors.Is(err, http.ErrServerClosed) {
            errCh <- nil
            return
        }
        errCh <- err
    }()

    select {
    case <-ctx.Done():
        shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
        defer cancel()
        if err := app.Server.Shutdown(shutdownCtx); err != nil {
            // Drain the serve goroutine to avoid leaking it, but
            // report the shutdown error which caused the failure.
            <-errCh
            return fmt.Errorf("server shutdown: %w", err)
        }
        // Wait for Serve to observe the shutdown and return.
        if err := <-errCh; err != nil {
            return fmt.Errorf("server: %w", err)
        }
        return nil
    case err := <-errCh:
        if err != nil {
            return fmt.Errorf("server: %w", err)
        }
        return nil
    }
}
```

`Run` preserves graceful shutdown while fixing three holes the old inline `main.go` pattern had:

1. **Bind errors are surfaced synchronously.** `Server.Listen(addr)` actually calls `net.Listen("tcp", addr)` and returns any resulting error directly to the caller, before any goroutine is spawned and before any `ctx.Done()` branch can fire. This eliminates the race where a cancellation beats the serve goroutine into `ListenAndServe`, `Shutdown` marks the server closed, and the later `ListenAndServe` call returns `http.ErrServerClosed` without ever attempting the bind — silently masking "address already in use" and similar startup failures. With this split, `Serve()` calls `httpSrv.Serve(listener)` against the already-bound listener, so there is no `net.Listen` call inside the goroutine.
2. **No Shutdown-vs-Listen race on `httpSrv`:** because `Listen` is synchronous, by the time the select statement runs, both `s.httpSrv` and `s.listener` are guaranteed non-nil, so `Shutdown` can always reach a real server.
3. **Shutdown waits for Serve to exit:** after `Shutdown` returns, the select branch blocks on `errCh` until the serve goroutine has observed the shutdown and returned its final error. Any non-`ErrServerClosed` error from `Serve` is propagated instead of silently dropped. A `nil` return from `Run` truly implies `Serve` returned `http.ErrServerClosed`.

This requires the following changes to `*server.Server`:

- Add `httpSrv *http.Server` and `listener net.Listener` fields.
- Add `Listen(addr string) error`: creates the `*http.Server` (SSE-friendly `WriteTimeout: 0`, existing `ReadTimeout: 15s`, `IdleTimeout: 60s`, handler = `s`), then calls `net.Listen("tcp", addr)` and stores the resulting listener. Returns any error from `net.Listen` directly to the caller. Must be called exactly once before `Serve` or `Shutdown`.
- Add `Serve() error`: calls `s.httpSrv.Serve(s.listener)` and returns its result. Does **not** call `net.Listen` — the listener must already be bound by a prior `Listen` call, so `Serve` cannot encounter a bind error.
- Add `Shutdown(ctx context.Context) error`: calls `s.httpSrv.Shutdown(ctx)` **first**, capturing its return value, then calls `s.listener.Close()` ignoring the returned error (closing an already-closed listener returns a harmless "use of closed network connection"), then returns the stored `httpSrv.Shutdown` error. The ordering matters: `http.Server.Shutdown` atomically sets its internal `inShutdown` flag before attempting to close its tracked listeners, and `http.Server.Serve` checks that flag via `trackListener` at its very first step. So regardless of whether `Serve` has started yet when `Shutdown` is called, `Shutdown` runs its normal path correctly:
  - **Post-`Serve` case:** `Serve` has already adopted `s.listener`. `httpSrv.Shutdown(ctx)` closes the tracked listener, which makes the running `Serve`'s `Accept` return, `Serve` returns `http.ErrServerClosed`, and then `Shutdown` drains active connections until `ctx` fires. Our subsequent explicit `s.listener.Close()` is a no-op on the already-closed listener.
  - **Pre-`Serve` case:** the serve goroutine has not yet entered `httpSrv.Serve(s.listener)`. `httpSrv.Shutdown(ctx)` has no tracked listeners to close but still sets `inShutdown`. Any subsequent `Serve()` call sees the flag in `trackListener` and returns `http.ErrServerClosed` immediately without touching `Accept`. `httpSrv.Shutdown` does NOT close the unadopted listener, so we must follow up with an explicit `s.listener.Close()` to release the bound socket — otherwise an early `ctx` cancellation between `Listen` and the serve goroutine being scheduled would leak the port. Closing the listener before the goroutine runs is safe because the flag already guarantees `Serve()` will return `ErrServerClosed` without racing on `Accept`.

  Previously-attempted orderings that are wrong and rejected: (a) close `s.listener` **before** calling `httpSrv.Shutdown` — in the post-Serve case, the running `Serve`'s `Accept` returns a raw listener-close error before `httpSrv` marks itself as shutting down, and `Serve` propagates that raw error instead of `http.ErrServerClosed`, making `Run`'s error branch treat a normal graceful shutdown as a server error. (b) Skip the explicit `s.listener.Close()` in the post-Serve case with an "already adopted" flag — correct but more state to track than the idempotent ordering above.

  For the test path where `*Server` is wired purely as an `http.Handler` via `httptest.NewServer` (no `Listen` call), both `httpSrv` and `listener` are nil and `Shutdown` is a no-op returning `nil`.
- The old `ListenAndServe(addr string) error` method is **removed** (not renamed or kept as a wrapper) to make the AST test enforceable: there must be no caller of the combined form anywhere in `cmd/middleman`.

`cmd/middleman/main.go`'s `run` function is reduced to loading config, constructing the GitHub client, setting up signal-cancellation, and calling `Run`. It does not reference `app.Syncer` or `app.Server` directly; the `Run` helper sequences them.

To prevent a future edit from reintroducing the bypass (calling `Bootstrap` and then wiring `syncer.Start` / `Server.Serve` inline in `main.go`, in a new helper file, or even in a new function inside `app.go` itself), add a type-aware regression test at `cmd/middleman/main_ast_test.go` that inspects the **entire `cmd/middleman` package including `app.go`**, but scoped by enclosing function:

```go
// TestOnlyAppRunStartsServerAndSyncer loads cmd/middleman with
// go/packages (with type info) and walks every *ast.SelectorExpr in
// every non-test source file — NOT just selectors that are immediate
// call-site callees. Walking every selector catches method-value
// aliases like `serve := app.Server.Serve` where the later `serve()`
// call is a plain *ast.Ident and the forbidden reference only shows
// up at the earlier selector. For each selector, it resolves via
// pkg.TypesInfo.Selections[sel] (NOT Uses: method references are
// recorded in Selections; Uses only holds plain identifiers) and
// accepts BOTH `types.MethodVal` (normal `x.M`) AND `types.MethodExpr`
// (the `(*server.Server).Serve` form) so that neither bypasses the
// guardrail. It tracks the enclosing FuncDecl for each selector and
// fails if any selector resolves to a method in the forbidden set
// UNLESS the enclosing FuncDecl is the **specific** package-level
// `Run` function node found during a prior pass over app.go.
//
// Forbidden set (receiver type + method name):
//   - (*github.com/wesm/middleman/internal/github.Syncer).Start
//   - (*github.com/wesm/middleman/internal/server.Server).Listen
//   - (*github.com/wesm/middleman/internal/server.Server).Serve
//   - (*github.com/wesm/middleman/internal/server.Server).Shutdown
//
// No file in cmd/middleman may reference these methods, with ONE
// exception: the body of the single package-level `Run` function in
// app.go (FuncDecl.Recv == nil, name == "Run", declared at file scope
// in app.go, matching the expected signature). Bootstrap, App methods,
// any future helper inside app.go itself, and any method named `Run`
// on some other receiver type are all forbidden from touching this
// set — everything must go through the specific package-level Run.
// main.go is therefore forced to call Run.
func TestOnlyAppRunStartsServerAndSyncer(t *testing.T) {
    cfg := &packages.Config{
        Mode: packages.NeedName | packages.NeedFiles |
              packages.NeedSyntax | packages.NeedTypes |
              packages.NeedTypesInfo,
        Dir:  ".",
    }
    pkgs, err := packages.Load(cfg, ".")
    // First pass: locate the unique package-level Run FuncDecl.
    //   var runDecl *ast.FuncDecl
    //   for each non-test syntax file f in pkgs[0]:
    //     if filepath.Base(pkg.Fset.File(f.Pos()).Name()) != "app.go" { continue }
    //     for each top-level decl in f.Decls:
    //       fd, ok := decl.(*ast.FuncDecl); if !ok continue
    //       if fd.Recv != nil { continue }           // must not be a method
    //       if fd.Name.Name != "Run" { continue }
    //       if runDecl != nil { t.Fatalf("multiple Run decls") }
    //       runDecl = fd
    //   if runDecl == nil { t.Fatalf("no package-level Run in app.go") }
    //   // Validate signature: (ctx context.Context, cfg *config.Config,
    //   //   configPath string, ghClient ghclient.Client, addr string) error
    //   // using types info from runDecl.Name's *types.Func so a rename
    //   // or signature drift cannot silently disable the guardrail.
    //
    // Second pass: walk every non-test file and check every selector.
    //   for each non-test syntax file in pkgs[0]:
    //     var enclosing *ast.FuncDecl
    //     ast.Inspect(f, func(n ast.Node) bool {
    //       if fd, ok := n.(*ast.FuncDecl); ok { enclosing = fd; return true }
    //       // Check EVERY SelectorExpr, not just call-site callees.
    //       // A bare `serve := app.Server.Serve` produces a selector
    //       // that is a forbidden method reference even though the
    //       // later `serve()` call site is an *ast.Ident and carries
    //       // no selection information of its own.
    //       sel, ok := n.(*ast.SelectorExpr); if !ok { return true }
    //       selection := pkg.TypesInfo.Selections[sel]
    //       if selection == nil { return true }
    //       // Accept BOTH MethodVal (normal `x.M` form, whether called
    //       // or taken as a method value) AND MethodExpr (the method-
    //       // expression form like `(*server.Server).Serve`). Either
    //       // Kind bypass would otherwise satisfy a narrower check.
    //       if selection.Kind() != types.MethodVal &&
    //          selection.Kind() != types.MethodExpr {
    //         return true
    //       }
    //       recv := selection.Recv()
    //       key := recv.String() + "." + selection.Obj().Name()
    //       if _, forbidden := forbiddenSet[key]; !forbidden { return true }
    //       // Pointer-identity check against the validated Run node.
    //       // Matching on fd.Name.Name == "Run" would be spoofable by
    //       // `func (h helper) Run(...)` elsewhere in the package.
    //       if enclosing != runDecl { t.Fatalf(...) }
    //       return true
    //     })
}
```

Why the `Run`-only exemption is pinned to a specific FuncDecl (not a file-level `app.go` exemption, and not a name-only check): exempting the entire file would let a future edit add a second helper function next to `Run` inside `app.go` that does the inline wiring, and have `main.go` call that helper instead. Scoping the exemption by bare name "Run" would let a future edit add a method like `func (h helper) Run(...)` anywhere in the package that inlines `Syncer.Start` / `Server.Listen` / `Server.Serve` / `Server.Shutdown` — the enclosing FuncDecl's name would still be "Run", defeating the guardrail. Pinning the exemption to the **specific AST node** identified by a first pass (file = `app.go`, `Recv == nil`, `Name == "Run"`, validated signature) forces all startup wiring through exactly that one package-level function. Signature validation guards against a rename or drift that would leave a same-named but wrong-shaped `Run` silently disabling the check.

Why the second pass walks every `*ast.SelectorExpr` rather than the callees of `*ast.CallExpr` only: a method-value alias like `serve := app.Server.Serve; serve()` separates the forbidden method reference (the `app.Server.Serve` selector) from the call site (a plain identifier `serve` that carries no `TypesInfo.Selections` entry). A call-only walk would see the identifier and skip it because it is not a selector; the forbidden reference would hide at the earlier selector expression. Walking every `SelectorExpr` node catches the reference regardless of whether it is the callee of an immediate call, taken as a method value, or used as the operand of a method expression — all three resolve via `TypesInfo.Selections` and all three are checked.

Combined with the Bootstrap regression test, this gives two independent guardrails: `app_test.go` verifies the helper correctly orders prime and start, and `main_ast_test.go` verifies every caller of the forbidden method set in the entire `cmd/middleman` package is inside the body of `Run`.

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
2. Obtain an `*http.ResponseController` via `http.NewResponseController(w)`. This exposes a `Flush() error` method (unlike the bare `http.Flusher` interface, whose `Flush()` returns no error), so the handler can detect flush failures and return promptly. Call `rc.Flush()` to push headers; if it returns an error, return immediately (flushing unsupported or the underlying writer is broken).
3. Subscribe to hub with `r.Context()`. Because the hub pre-loads the cached `lastSyncStatus` into the new channel under the broadcast lock (see Event Hub), the very first receive from the channel in step 5's select loop is a `sync_status` event carrying the current state. The handler doesn't need to fetch `Syncer.Status()` separately, and there is no window between snapshot read and subscribe.
4. Start a 30s keepalive ticker
5. Select loop: on channel event, marshal to SSE wire format (`event: <type>\ndata: <json>\n\n`), write, call `rc.Flush()`. If `Write` returns an error or `rc.Flush()` returns an error, return immediately (client disconnected or flush failed). On ticker, write SSE comment (`: keepalive\n\n`), call `rc.Flush()`, return on write or flush failure. On context cancel, return. The very first iteration of this loop will drain the cached `sync_status` from the channel and flush it out, so the client receives the current state immediately after the headers — no separate "write then flush" step outside the loop is required.
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
5. On 304: return response as-is (empty body, 304 status). Do NOT update `cachedAt` — the entry must age out so the TTL can eventually force an unconditional fetch.
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

| Current page | View | Actions on `data_changed` (in addition to global refreshes below) |
|-------------|------|--------------------------|
| `pulls` | list | If a PR detail is selected (`getSelectedPR()` is non-null), call `refreshDetail()` with that PR's owner/name/number. |
| `pulls` | board | If the board drawer is open, call `refreshDetail()` for the drawer's PR. |
| `issues` | - | If an issue is selected, call `refreshFromSSE()` on the issues store for the selected issue's detail. |
| `activity` | - | `loadActivity()` for full feed refresh (not `pollNewItems()` — incremental append wouldn't update existing rows whose title/state changed during sync). If an activity detail drawer is open, fire the registered refresh callback (see below). |
| `settings` | - | No additional refresh needed. |

**Global refreshes:** `loadPulls()` AND `loadIssues()` are always called on `data_changed` regardless of current page. The status bar (`StatusBar.svelte`) is part of the layout chrome and visible on every page; it reads `getPulls().length`, `getIssues().length`, and a `repoCount()` derived from both stores. Without global refreshes, the counts would go stale on `activity`, `pulls`, and `settings` after background syncs change issue state. When on the board view, the events store calls `loadPulls({ state: "open" })` instead of the generic `loadPulls()` to match the board's filter. The `getView()` helper from the router store distinguishes list from board.

**Board view specifics:** `KanbanBoard.svelte` has its own drawer state (`drawerPR`) that is local to the component, not in the pulls store. The events store cannot directly access this. Two options: (a) move `drawerPR` into the pulls store so the events store can check it, or (b) have the board component register a refresh callback with the events store on mount and unregister on destroy. Option (b) is simpler and doesn't require restructuring the board's local state.

**Activity drawer specifics:** Activity selection lives in `App.svelte` local state (plus the `?selected=...` query parameter), not in the router store. Activity items can be either PRs or issues, so the refresh path must branch: PR items call `detail.refreshFromSSE(owner, name, number)`, issue items call `issues.refreshFromSSE(owner, name, number)`. Same callback pattern as the board: `App.svelte` registers a refresh callback with the events store on mount that checks the current selection type and calls the appropriate store. Unregisters on destroy.

### Store Changes

**Prerequisite: move component-level polling into stores.** Currently, `PullList.svelte`, `IssueList.svelte`, and `KanbanBoard.svelte` each have their own 15s `setInterval` timers that call `loadPulls()`/`loadIssues()` directly. These must be moved into their respective stores so the SSE events store can centrally control them. Without this, SSE would disable store-level polling but component-level timers would keep firing.

**Polling gate rule (applies to all stores below):** Every store maintains a module-level `pollingEnabled` flag. `disablePolling()` sets the flag to false and clears any active interval. `enablePolling()` sets the flag to true and recreates any timer whose active state is recorded (see per-store sections). Critically, every `start*Polling()` function must also check this flag: if `pollingEnabled` is false (SSE is connected), the start function records its active state (flag + overrides/target) but skips creating the interval. When SSE later disconnects, `enablePolling()` walks the recorded state and creates the appropriate timers. This ensures mount-time `start*Polling()` calls from a newly-navigated-to view don't defeat SSE by creating fallback timers while SSE is connected.

**`pulls.svelte.ts`:**
- New `startListPolling(overrides?)` / `stopListPolling()` functions managing a 15s timer that calls `loadPulls(overrides)`. The optional `overrides` parameter lets callers lock the timer to specific filters (e.g., `{ state: "open" }` for the board view). `startListPolling` stores the active overrides AND sets a boolean `listPollingActive` flag to true. `stopListPolling` clears the interval, the stored overrides, AND sets the flag to false (component unmount — timer should not revive). Replaces the `setInterval` in `PullList.svelte` and `KanbanBoard.svelte`.
- New `enablePolling()` / `disablePolling()` to gate polling on SSE connection state. These are distinct from start/stop: `disablePolling` clears the interval but preserves both the stored overrides and the `listPollingActive` flag. `enablePolling` checks the flag — if true, recreates the timer with the stored overrides (so board polling restarts with `{ state: "open" }`, and plain sidebar polling restarts with no overrides). If false (component unmounted via `stopListPolling`), `enablePolling` is a no-op for the list timer.
- Events store calls `loadPulls()` on `data_changed`.

**`issues.svelte.ts`:**
- New `startListPolling()` / `stopListPolling()` functions managing a 15s timer that calls `loadIssues()`. Same `listPollingActive` flag pattern as pulls store. Replaces the `setInterval` in `IssueList.svelte`.
- Existing `startIssueDetailPolling` / `stopIssueDetailPolling` gain the same lifecycle/toggle semantics as `detail.svelte.ts`: `startIssueDetailPolling` stores the current target (owner/name/number) and sets `issueDetailActive` flag. `stopIssueDetailPolling` clears the interval, stored target, and flag. `enablePolling` / `disablePolling` check the flag — `disablePolling` preserves target, `enablePolling` recreates the timer if the flag is true. This ensures issue detail polling restarts correctly after SSE reconnect (for both the issues page and the activity drawer's issue items).
- New `refreshFromSSE(owner: string, name: string, number: number)` that calls the existing issue detail refresh.
- Events store calls `loadIssues()` on `data_changed` when issues page is active.

**`sync.svelte.ts`:**
- New `updateSyncFromSSE(status: SyncStatus)` function: sets state directly, fires `onSyncComplete` callback when detecting running-to-idle transition, AND runs the same `adjustPollingSpeed` logic as `refreshSyncStatus` to update `currentIntervalMs` (2s while running, 30s idle). This ensures `currentIntervalMs` tracks SSE-delivered transitions so that when SSE later disconnects, `enablePolling` recreates the timer at the correct adaptive cadence. Same as `refreshSyncStatus` but without the HTTP call.
- New `enablePolling()` / `disablePolling()` functions following the polling gate rule. `disablePolling` clears the interval but preserves the current `currentIntervalMs` (the adaptive 2s-while-syncing or 30s-idle value). `enablePolling` recreates the timer at the preserved `currentIntervalMs` so a sync that was running during SSE disconnect resumes at 2s, not the 30s default.
- `startPolling` and `stopPolling` still exist for lifecycle (mount/unmount). `startPolling` records the `syncPollingActive` flag but checks `pollingEnabled` before creating the timer. When `pollingEnabled` is false, `startPolling` must NOT touch `currentIntervalMs` — the preserved adaptive value must survive mount-during-SSE. When `pollingEnabled` is true, `startPolling` uses `currentIntervalMs` (defaulting it only if unset). `stopPolling` clears the interval, the flag, and resets `currentIntervalMs` to the default.

**`activity.svelte.ts`:**
- New `enablePolling()` / `disablePolling()` functions following the polling gate rule. `startActivityPolling` / `stopActivityPolling` maintain an `activityPollingActive` flag; start records state and checks the gate before creating the timer.
- New `refreshFromSSE()` that calls `loadActivity()` (full refresh, not incremental `pollNewItems()`).

**`detail.svelte.ts`:**
- New `enablePolling()` / `disablePolling()` functions. `startDetailPolling` already stores the current target (owner/name/number) in module-level state. `stopDetailPolling` clears both the interval AND the stored target (component unmount). `disablePolling` clears the interval but preserves the stored target. `enablePolling` recreates the timer for the stored target (so detail polling restarts for the correct PR/issue after SSE reconnect). If no target is stored (detail already closed via `stopDetailPolling`), `enablePolling` is a no-op for the detail timer.
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
- `Broadcast` with a `sync_status` event updates the cached `lastSyncStatus`; subsequent `Subscribe` receives that event as the first channel value
- `Broadcast` with non-`sync_status` events (e.g. `data_changed`) does NOT update `lastSyncStatus`
- With no prior broadcast, a new `Subscribe` returns a channel with no pre-loaded event (nil cache)
- Ordering: subscriber A connects, receives cached X, then broadcaster sends Y under the same lock → A's channel contains [X, Y] in that order
- Mid-sync connect: broadcast sync_status T1 (seeds cache), broadcast sync_status T2 (updates cache), Subscribe → new subscriber's first event is T2, never T1

**`internal/github/etag_transport_test.go`:**
- 200 response stores ETag from header
- Subsequent request to same URL includes `If-None-Match` header
- 304 response returned as-is (status preserved), does NOT refresh `cachedAt` timestamp
- Different URLs get independent ETag entries
- Request without cached ETag has no `If-None-Match` header
- Requests with `page` query parameter > 1 bypass ETag handling (no `If-None-Match` sent, no ETag stored)
- 200 response with `Link: rel="next"` evicts any previously-cached ETag for that URL
- Single-page → multi-page → single-page transition: ETag cached on single-page, evicted on multi-page detection, re-cached when back to single-page
- Expired ETag entries (older than `etagTTL`) are treated as uncached
- TTL-driven multi-page detection: cached single-page ETag, one or more 304s (which must NOT refresh `cachedAt`), then after `etagTTL` the next request omits `If-None-Match`, gets a 200 with `Link: rel="next"`, and evicts the cache entry
- `IsNotModified` returns true for 304 errors, false for other errors

**Integration tests:**
- SSE endpoint: returns `text/event-stream` content type, receives events after broadcast, connection closes cleanly on client disconnect
- SSE endpoint sends initial `sync_status` event: server startup primes the hub via `Broadcast(sync_status)` from `Syncer.Status()`; a new subscription's very first received event is a `sync_status` frame with that snapshot, flushed to the client before any transition-driven broadcast
- SSE endpoint mid-sync connect: prime the hub, simulate a sync start broadcast T1, simulate a mid-sync progress broadcast T2, then open a new subscription. The first frame the client receives is T2 (the most recent), and the client never receives T1 or any older snapshot afterward. Regression guard for the "subscribe then queued older event" race.
- SSE endpoint startup with in-progress sync (regression for priming race): this is guarded by **two independent tests**. (1) `cmd/middleman/app_test.go` drives the production `Bootstrap(cfg, configPath, mockClient)` helper with a mock GitHub client whose first list call blocks on a channel so a `RunOnce` can be stopped mid-cycle. After `Bootstrap` returns, the test calls `app.Syncer.Start(ctx)`, lets `RunOnce` broadcast its initial `Running: true` state to the callback, then opens a new SSE subscription through `app.Server`. The first frame the client receives is `{running: true}` (the most recent cached broadcast), not the zero-value snapshot used during priming. (2) `cmd/middleman/main_ast_test.go` loads the entire `cmd/middleman` package with `go/packages` (type info enabled). A first pass over `app.go` locates the unique package-level `Run` FuncDecl (`Recv == nil`, `Name == "Run"`, validated signature), failing if none or more than one exists. A second pass walks every `*ast.SelectorExpr` in every non-test file (including `app.go`) — not just the callees of `*ast.CallExpr` — so that a method-value alias like `serve := app.Server.Serve; serve()` is caught at the earlier selector reference where `TypesInfo.Selections` still resolves it. It accepts both `types.MethodVal` (which covers normal calls, method-value assignments, and any other `x.M` reference) and `types.MethodExpr` (which covers the method-expression form like `(*server.Server).Serve(app.Server)`), and tracks the enclosing `FuncDecl`. It fails if any such selector resolves to a method in `{(*Syncer).Start, (*Server).Listen, (*Server).Serve, (*Server).Shutdown}` from a `FuncDecl` that is NOT pointer-identical to the `Run` node found in the first pass. Pointer identity defeats spoofing via `func (h helper) Run(...)` defined elsewhere; handling `MethodExpr` defeats spoofing via `(*server.Server).Serve(app.Server)`; walking all `SelectorExpr` nodes defeats spoofing via `serve := app.Server.Serve; serve()`. Together: `app_test.go` verifies the helper's ordering is correct, and `main_ast_test.go` verifies every reference to the forbidden method set in the entire `cmd/middleman` package sits inside the body of exactly the validated package-level `Run` FuncDecl in `app.go`.

- `Run` bind-error propagation: the test creates an already-bound TCP listener on an ephemeral port, then calls `Run(ctx, cfg, cfgPath, mockClient, boundAddr)`. `Run` must return a wrapped bind error (the `"listen: …"` prefix from the synchronous `Server.Listen` call), not `nil`. Verifies that `Listen` actually calls `net.Listen` synchronously and that bind errors cannot be masked by a later `ctx.Done()`.
- `Run` serve-error propagation after cancel: in an environment where the listener can be programmatically closed mid-serve, trigger a `Serve()` error simultaneously with `ctx` cancellation. `Run` must wait for the serve goroutine to exit and propagate the non-`ErrServerClosed` error from `errCh` instead of silently returning `nil` from the shutdown branch.
- `Run` shutdown happy path: bind to `127.0.0.1:0`, cancel `ctx`, verify `Run` returns `nil` and the listener is closed (subsequent `net.Dial` to the bound address fails).
- Pre-`Serve` shutdown regression test (direct `*Server` lifecycle, not through `Run`): in `internal/server/server_test.go`, construct a `*Server`, call `Listen("127.0.0.1:0")` to bind the socket, capture the bound address via `s.listener.Addr().String()`, then call `Shutdown(ctx)` **before** `Serve()` is ever invoked. Assert (1) `Shutdown` returns `nil`, (2) the bound port is released — a fresh `net.Listen("tcp", boundAddr)` on the same address must succeed (fail fast if the port is still held), (3) a subsequent call to `Serve()` returns `http.ErrServerClosed` (not a raw listener-close error), proving the `httpSrv.Shutdown` step set `inShutdown` atomically even though no listener was adopted. This is the regression guard for the shutdown-ordering bug where closing the listener before `httpSrv.Shutdown` would leak the port (first version) or where closing the listener first in the post-`Serve` case would cause `Serve` to return a raw close error (second version). Note: because port reuse is subject to `TIME_WAIT` on some systems, the test may need to enable `SO_REUSEADDR` on the probing listener or use a dial-failure check instead (`net.DialTimeout` to the address returns an error within a short deadline).
- SSE handler flush-on-error: wrap an `httptest.ResponseRecorder` with a custom writer whose `FlushError() error` method (the Go 1.20+ hook used by `http.NewResponseController(w).Flush()`) succeeds for the first N calls and returns a synthetic error on a later call. Drive the handler so that the first `rc.Flush()` (header flush in step 2) and the cached initial `sync_status` flush in the first select-loop iteration both succeed, then trigger a broadcast that causes the NEXT flush (either an event flush from step 5 or a keepalive flush from the ticker) to fail. Verify the handler returns promptly on that later flush error rather than looping on stale state. This ensures implementations cannot ignore post-write flush failures and still pass the test.
- Syncer with `onStatusChange`: callback fires for started/progress/complete transitions
- Syncer with `IsNotModified`: verify PR processing is skipped on 304, CI refresh still runs with cached head SHAs, issue sync still runs on PR list 304

### Frontend Tests

- Events store: `connect()` creates EventSource with correct URL, `disconnect()` closes it
- Polling toggle: `disablePolling()` clears timers, `enablePolling()` restarts them with preserved config
- Lifecycle vs toggle: `stopListPolling()` then `enablePolling()` does NOT revive list timer (flag cleared on stop). Same for `stopDetailPolling()` / `stopIssueDetailPolling()` then `enablePolling()`
- Plain `startListPolling()` (no overrides) survives `disablePolling()` / `enablePolling()` cycle — timer restarts because `listPollingActive` flag is true even though overrides are undefined
- Issue detail polling: `startIssueDetailPolling` through disable/enable preserves target, `stopIssueDetailPolling` then `enablePolling` does not revive
- Mount while SSE connected: after `disablePolling()`, calling `startListPolling()` / `startDetailPolling()` / `startIssueDetailPolling()` / `startActivityPolling()` / sync store `startPolling()` records active state but does NOT create a timer. Subsequent `enablePolling()` creates the timer from the recorded state
- Sync interval preservation through mount: sync is running at 2s, SSE connects (`disablePolling()` preserves `currentIntervalMs: 2s`), sync store `startPolling()` is called from a newly-mounted view (must NOT clobber `currentIntervalMs` back to 30s), then SSE errors (`enablePolling()` recreates the timer at 2s, not 30s)
- `updateSyncFromSSE` updates state and fires completion callback
- `updateSyncFromSSE` idle-to-running transition: `pollingEnabled = false` (SSE connected), `currentIntervalMs = 30000`, SSE delivers `{ running: true }` → `currentIntervalMs` updates to 2000. Calling `enablePolling()` after this creates the fallback timer at 2000ms, not 30000ms
- `updateSyncFromSSE` running-to-idle transition: `pollingEnabled = false`, `currentIntervalMs = 2000`, SSE delivers `{ running: false }` → `currentIntervalMs` updates to 30000 AND `onSyncComplete` callback fires. Calling `enablePolling()` after this creates the fallback timer at 30000ms
- View-aware refresh: `data_changed` triggers correct store functions based on current page
- Global refresh: `data_changed` calls both `loadPulls()` AND `loadIssues()` on every page (pulls, issues, activity, settings) to keep status-bar counts current
- Initial sync prime: SSE opens while sync store has `syncState = null` and `pollingEnabled = false` (because `open` fires disablePolling on all stores); the events store receives a `sync_status` frame as the first message on the EventSource (that frame is the hub's cached snapshot) and calls `updateSyncFromSSE`, which populates `syncState` and updates `currentIntervalMs` even though polling stays disabled. Subsequent `enablePolling()` would then create the fallback timer at the correct cadence.

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
| `cmd/middleman/app.go` | `App` struct, `Bootstrap(cfg, configPath, ghClient)` helper that creates syncer + constructs server (primes hub, wires callback) without starting the syncer or binding, and `Run(ctx, cfg, configPath, ghClient, addr)` helper that calls `Bootstrap`, starts the syncer, synchronously binds via `Server.Listen(addr)` (returning any bind error directly), runs `Server.Serve()` in a goroutine, and selects on `ctx.Done()` to invoke `Server.Shutdown` (5s deadline, waiting for the serve goroutine to exit afterward) or on a server-error channel. `Run` is the **only** function in the entire `cmd/middleman` package that may reference `Syncer.Start`, `Server.Listen`, `Server.Serve`, or `Server.Shutdown`; `Bootstrap` is exposed separately so tests can inspect hub state between construction and start but must not call any of those lifecycle methods. |
| `cmd/middleman/app_test.go` | Startup-ordering integration tests that drive `Bootstrap` directly with a mock GitHub client that blocks mid-`RunOnce`, asserting the cached `lastSyncStatus` reflects the in-progress state for new subscribers. Also contains the `Run` bind-error, serve-error, and shutdown happy-path tests described in the Testing section. |
| `cmd/middleman/main_ast_test.go` | Type-aware regression test using `go/packages`: loads the entire `cmd/middleman` package with type info. A first pass locates the unique package-level `Run` FuncDecl in `app.go` (`Recv == nil`, `Name == "Run"`, validated signature) — failing if none or more than one exists. A second pass walks every `*ast.SelectorExpr` in every non-test file **including `app.go`** (not just selectors at call sites), tracks the enclosing `FuncDecl`, resolves each selector via `TypesInfo.Selections[sel]`, accepts both `types.MethodVal` and `types.MethodExpr` kinds, and fails the build if any selector referring to `(*Syncer).Start`, `(*Server).Listen`, `(*Server).Serve`, or `(*Server).Shutdown` occurs inside a `FuncDecl` that is not pointer-identical to the validated `Run` node. Pointer identity prevents bypass via a method such as `func (h helper) Run(...)` defined elsewhere; handling `MethodExpr` prevents bypass via the method-expression form `(*server.Server).Serve(app.Server)`; walking every `SelectorExpr` (not just call-expression callees) prevents bypass via method-value aliases such as `serve := app.Server.Serve; serve()`. Prevents bypass via a new sibling helper file, a new function added inside `app.go` itself, a same-named method on another receiver, method-expression call syntax, or method-value alias assignment. |

### Modified Files
| File | Change |
|------|--------|
| `internal/server/server.go` | Add `EventHub` field, register `GET /api/v1/events` handler using `http.NewResponseController(w)` for flushable writes, wire syncer callback via `SetOnStatusChange`, prime the hub with `Broadcast(sync_status)` from `syncer.Status()` during server construction. **Remove** the old `ListenAndServe(addr)` method and replace it with three explicit lifecycle methods: `Listen(addr string) error` synchronously creates the `*http.Server` (SSE-friendly `WriteTimeout: 0`, existing `ReadTimeout: 15s`, `IdleTimeout: 60s`) AND calls `net.Listen("tcp", addr)` AND stores the resulting listener into `s.listener` — returning any bind error directly; `Serve() error` calls `s.httpSrv.Serve(s.listener)` against the already-bound listener (never attempts a bind itself); `Shutdown(ctx context.Context) error` calls `s.httpSrv.Shutdown(ctx)` (which closes the listener) and is a no-op when `Listen` was never called. Binding in `Listen` and removing `ListenAndServe` entirely ensures bind errors surface synchronously to `Run` and cannot be masked by an early `ctx` cancellation. |
| `cmd/middleman/main.go` | Replace inline wiring with a single call to `Run(ctx, cfg, configPath, ghClient, addr)`. `main.go` does not reference `app.Syncer` or `app.Server` directly — `Run` sequences `Bootstrap` → `Syncer.Start` → `Server.Listen` → `Server.Serve` (goroutine) → `Server.Shutdown` on ctx cancel. Enforced by `main_ast_test.go`. |
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
