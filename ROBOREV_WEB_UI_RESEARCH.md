# Roborev Web UI Research

Research for building a roborev web experience inside middleman as an
alternative to the Bubbletea TUI.

## Roborev Architecture Summary

Roborev is a multi-agent code review system with a daemon, worker pool,
and SQLite storage. The daemon listens on port 7373 and exposes a JSON
HTTP API. Workers claim queued jobs, invoke AI agents (Claude Code, Codex,
Gemini, etc.), and store review output. A Bubbletea TUI connects to the
daemon API for interactive use.

```
CLI (roborev) --> HTTP API (daemon :7373) --> Worker Pool --> Agents
                       |                          |
                    SQLite DB               Git worktrees (fix jobs)
                       |
                  SSE event stream
```

## Core Data Model

### Entities

**ReviewJob** -- the central entity. Represents a unit of work.

| Field | Type | Notes |
|-------|------|-------|
| id | int64 | Primary key |
| repo_id | int64 | FK to repos |
| git_ref | string | SHA, "start..end", or label |
| branch | string | Git branch |
| agent | string | e.g. "claude-code", "codex" |
| model | string | Effective model used |
| reasoning | string | fast, standard, medium, thorough, maximum |
| status | string | queued, running, done, failed, canceled, applied, rebased |
| job_type | string | review, range, dirty, task, insights, compact, fix |
| review_type | string | "", "security", "design" |
| error | string | Error message if failed |
| enqueued_at | time | When queued |
| started_at | time | When worker claimed it |
| finished_at | time | When completed |
| parent_job_id | int64 | For fix jobs: the review being fixed |
| patch | string | Unified diff for fix jobs |
| agentic | bool | Whether agent can edit files |
| token_usage | string | JSON blob |

**Review** -- agent output for a completed job (1:1 with job).

| Field | Type | Notes |
|-------|------|-------|
| id | int64 | Primary key |
| job_id | int64 | FK to review_jobs (UNIQUE) |
| agent | string | Agent that produced it |
| output | string | Full review text (markdown) |
| closed | bool | User marked as addressed |
| verdict_bool | int | 1=pass, 0=fail, NULL=unknown |

**Response** -- developer comment on a review.

| Field | Type | Notes |
|-------|------|-------|
| id | int64 | Primary key |
| job_id | int64 | FK to review_jobs |
| responder | string | Who commented |
| response | string | Comment text |
| created_at | time | When posted |

**Repo** -- registered repository.

| Field | Type | Notes |
|-------|------|-------|
| id | int64 | Primary key |
| root_path | string | Absolute path (UNIQUE) |
| name | string | Short name |

**DaemonStatus** -- runtime state (not persisted).

| Field | Type | Notes |
|-------|------|-------|
| version | string | Daemon version |
| queued/running/completed/failed/canceled/applied/rebased_jobs | int | Counts |
| active_workers | int | Currently busy |
| max_workers | int | Pool size |

### Job Status Flow

```
queued --> running --> done | failed | canceled
                       |
                  (fix workflow)
                       |
                  applied | rebased
```

### Verdict

Parsed deterministically from review output using severity labels and
pass phrases. Stored as verdict_bool (1=pass, 0=fail). No NLP involved.

## Roborev HTTP API

All endpoints at `/api/` on the daemon (default :7373).

### Job Management

| Method | Path | Purpose |
|--------|------|---------|
| GET | /api/jobs | List jobs (paginated, filterable) |
| POST | /api/enqueue | Create a new review job |
| POST | /api/job/cancel | Cancel a queued/running job |
| POST | /api/job/rerun | Re-enqueue a completed/failed job |
| GET | /api/job/output | Get job output lines (snapshot) |
| GET | /api/job/log | SSE stream of live output |

**GET /api/jobs** query params: repo_id, status, branch, limit (default
50), offset, job_type, reverse, hide_closed. Returns `{ jobs, stats,
has_more }`.

### Review Operations

| Method | Path | Purpose |
|--------|------|---------|
| GET | /api/review | Get review + responses for a job |
| POST | /api/review/close | Toggle closed state |
| POST | /api/comment | Add a developer comment |
| GET | /api/comments | List comments for a job |

### Fix Workflow

| Method | Path | Purpose |
|--------|------|---------|
| POST | /api/job/fix | Enqueue a fix job for a review |
| GET | /api/job/patch | Get the patch from a completed fix |
| POST | /api/job/applied | Mark fix as applied |
| POST | /api/job/rebased | Mark fix as rebased |

### Infrastructure

| Method | Path | Purpose |
|--------|------|---------|
| GET | /api/status | Daemon status and queue counts |
| GET | /api/health | Component health check |
| GET | /api/repos | List registered repos |
| GET | /api/branches | List branches for a repo |
| GET | /api/stream/events | SSE: review.completed, job.status_changed |
| GET | /api/summary | Aggregate stats (verdicts, agents, durations) |

## TUI Views and What They Show

### 1. Queue View (default)

Table with 14 columns: selection, ID, ref, branch, repo, agent, queued
time, elapsed, status, verdict, handled (closed), session ID, requested
model, requested provider.

- Filterable by repo and branch (tree-based filter modal)
- Toggle hide-closed
- Column visibility and ordering customizable
- Color-coded status (yellow=queued, blue=running/done, orange=failed,
  gray=canceled) and verdict (green=pass, red=fail, cyan=closed)

Actions: select (Enter), cancel (x), rerun (r), view log (l), view
prompt (p), comment (c), copy output (y), commit message (m), fix (F),
close/open (a), column options (o).

### 2. Review View

Full review detail with:
- Title: Review #ID, repo name, agent/model
- Location: repo path, git ref, branch
- Verdict badge, closed marker, token usage
- Markdown-rendered review output (glamour)
- Comments section with responder and timestamp
- Optional inline fix panel (F key) with editable prompt

Actions: prompt (p), comment (c), close (a), copy (y), fix (F),
prev/next review (j/k).

### 3. Filter View (modal)

Tree structure: All > Repos (with job counts) > Branches.
- Lazy-loaded branches (fetched on expand)
- Search across all entries
- Keyboard navigation with expand/collapse

### 4. Log View

Real-time streaming output from a running job:
- SSE-fed line buffer
- Scroll + follow mode (auto-scroll)
- Streaming indicator

### 5. Tasks View

Table of background fix jobs:
- Similar to queue but for fix type jobs
- Actions: select, create patch, apply patch, save to file

### 6. Patch View

Unified diff viewer for fix job output:
- Scrollable
- Save-to-file option

### 7. Other Views

- **Prompt view**: read-only scrollable prompt text
- **Comment modal**: multiline text input
- **Commit message view**: formatted commit message(s)
- **Column options**: toggleable column visibility
- **Help**: context-sensitive shortcut reference

## Middleman Frontend Architecture

### Stack

Svelte 5 with runes, TypeScript, Vite, Bun. Monorepo with a shared
`@middleman/ui` package exporting views, stores, and components.

### Routing

Hash/path-based routing in `frontend/src/lib/stores/router.svelte.ts`.
Routes parsed from URL pathname. Views rendered conditionally in
`App.svelte`.

### State Management Pattern

Factory functions returning getter/setter/action objects with `$state`
and `$effect` runes:

```typescript
export function createFooStore(opts) {
  let items = $state<Item[]>([]);
  let loading = $state(false);
  function getItems() { return items; }
  async function loadItems() { ... }
  return { getItems, loadItems, ... };
}
```

Stores created in `Provider.svelte`, distributed via Svelte context.

### Existing Views

| View | Layout | Pattern |
|------|--------|---------|
| PRListView | Sidebar list + detail pane | Two-panel |
| IssueListView | Sidebar list + detail pane | Two-panel |
| KanbanBoardView | Multi-column board | Kanban |
| ActivityFeedView | Full-height feed + drawer | Feed + overlay |
| DiffViewWrapper | File tree + diff content | Two-panel |

### Design System

CSS custom properties for colors, spacing, typography. Light/dark theme
via `.dark` class on root. Semantic color tokens (bg-primary, bg-surface,
accent-blue, etc.). Fonts: Inter (sans), JetBrains Mono (mono).

### API Client

Type-safe via openapi-fetch + generated types from OpenAPI spec. Base
URL: `/api/v1`. CSRF protection on mutations.

## Integration Points

### Adding Roborev to Middleman

1. **New route type** in router store (e.g. `/reviews`, `/reviews/:id`)
2. **New stores** in `@middleman/ui` for roborev data (jobs, reviews)
3. **New views** following existing patterns (two-panel for queue+detail)
4. **New API proxy** -- middleman Go server proxies to roborev daemon
5. **Nav entry** in AppHeader for the reviews section
6. **SSE bridge** -- proxy or relay roborev's event stream

### Architecture Decision: Proxy vs Direct

The middleman Go server would need to either:
- **Proxy**: Forward `/api/roborev/*` to roborev daemon on :7373
- **Direct**: Frontend calls roborev daemon directly (CORS issues)

Proxy is the natural choice -- keeps the SPA single-origin and lets
middleman handle auth/config.

### View Mapping: TUI to Web

| TUI View | Web Component | Layout |
|----------|---------------|--------|
| Queue | ReviewJobList (sidebar) + detail | Two-panel like PRListView |
| Review detail | ReviewDetail (main pane) | Markdown + comments |
| Filter | Filter controls in sidebar header | Dropdowns/search |
| Log | LogViewer (tab or modal) | SSE-fed scrollable |
| Tasks | Fix jobs section (tab or separate view) | Table |
| Patch | DiffView (reuse existing) | File tree + diff |
| Comment | CommentBox (reuse existing) | Inline form |
| Prompt | Collapsible section in detail | Accordion |

### Reusable Middleman Components

These existing components map directly to roborev needs:
- **DiffView/DiffFile/DiffLine** -- for patch viewing
- **EventTimeline** -- adaptable for review responses
- **CommentBox** -- for adding responses
- **Markdown rendering** -- for review output
- **Syntax highlighting** -- for code in reviews
- **Search/filter patterns** -- for job queue filtering

### Data Type Alignment

Roborev jobs share structural similarity with middleman PRs:
- Both have status, timestamps, repo association
- Both have a detail view with markdown content
- Both support comments/responses
- The sidebar list + detail pane pattern works for both

Key differences:
- Jobs have verdict (pass/fail) -- no PR equivalent
- Jobs have agent/model/reasoning -- unique to roborev
- Fix workflow (parent job -> fix job -> patch) is new
- Real-time log streaming is new

## Open Questions for Spec

1. **Scope of V1**: Full TUI parity or subset? Likely start with
   queue + review detail + basic filtering.

2. **Fix workflow**: Include in V1 or defer? The fix -> patch -> apply
   flow is complex.

3. **Log streaming**: Essential for monitoring running jobs. SSE proxy
   needed.

4. **Job creation**: Should the web UI support enqueuing new reviews,
   or just viewing? The TUI itself doesn't enqueue -- that's done via
   CLI.

5. **Column customization**: The TUI has 14 columns with visibility
   toggles. Web equivalent could be simpler.

6. **Keyboard shortcuts**: The TUI is keyboard-heavy. How much keyboard
   navigation for the web UI?

7. **Connection management**: How does the web UI discover/connect to
   the roborev daemon? Config in middleman's TOML?

8. **Multi-daemon**: Could a user have multiple roborev daemons? Or
   always one local instance?
