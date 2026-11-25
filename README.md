# Window Commit Lab

A local, offline multi-tenant event-window engine with:

- deterministic JSON snapshot ingestion;
- tumbling, sliding, and session window planning;
- checked int64 fixed-point aggregation at scale `1,000,000`;
- a global/per-tenant concurrency and accounted-time-budget scheduler;
- deterministic `SHED` degradation and later re-enqueue;
- SQLite WAL persistence with tenant-versioned atomic commits;
- durable start/commit/shed intents and startup crash recovery;
- an interactive browser editor plus JSON HTTP APIs;
- a reproducible full-run / budget-run / SIGKILL-recovery demo.

No Docker, remote service, or network dependency is used. `modernc.org/sqlite` is vendored in `vendor/` and the build is exercised with `GOPROXY=off`.

## Quick Start

```bash
go version                    # verified with go1.27.4; go.mod requires >=1.22
sqlite3 --version             # verified with 3.46.1

export GOFLAGS=-mod=vendor
export GOPROXY=off
go build ./...
go test ./...

go run ./cmd/windowctl generate -seed 242 -out samples/deterministic.json
go run ./cmd/windowd -db artifacts/window-engine.db -addr 127.0.0.1:18099
```

Open `http://127.0.0.1:18099/`. The UI can load the built-in sample, edit/validate/commit the snapshot, add a domain event draft, trigger recalculations, query aggregates and tenant state, and inspect scheduler, SHED, intent, and recovery evidence.

The complete scripted demonstration is:

```bash
./scripts/demo.sh
```

It writes its database, logs, JSON responses, SQLite spot checks, and summary under `artifacts/demo-<timestamp>/`.

### Snapshot JSON shape

```json
{
  "snapshot_id": "example-v1",
  "tenants": [
    {"id": "tenant-a", "name": "Tenant A", "priority": 10}
  ],
  "window_specs": [
    {"id": "hourly", "type": "tumbling", "width_ms": 3600000, "boundary": "LEFT_CLOSED_RIGHT_OPEN"},
    {"id": "overlap", "type": "sliding", "width_ms": 3600000, "slide_ms": 1800000, "boundary": "LEFT_CLOSED_RIGHT_OPEN"},
    {"id": "session", "type": "session", "width_ms": 3600000, "session_gap_ms": 900000, "boundary": "LEFT_CLOSED_RIGHT_OPEN"}
  ],
  "events": [
    {"event_id": "e1", "tenant_id": "tenant-a", "group_key": "alpha", "timestamp_ms": 1704067200000, "seq": 1, "measure": "12.500000"}
  ]
}
```

`event_id` is optional and is generated deterministically from input position when omitted. A missing or `null` `measure` is allowed. The implemented sample lives at `samples/deterministic.json`.

## API

All failures use the same envelope:

```json
{
  "error": {
    "code": "E_SNAPSHOT_INVALID",
    "message": "tenant id must be unique",
    "context": {
      "field": "tenants.id",
      "tenant_id": "tenant-a"
    }
  }
}
```

Business codes implemented:

- `E_SNAPSHOT_INVALID`
- `E_NUMERIC_OVERFLOW`
- `E_RECALC_QUEUE_FULL`
- `E_STALE_VERSION`
- `E_INVALID_TRANSITION`
- `E_WINDOW_ZERO_DURATION`
- `E_WINDOW_RANGE_OVERFLOW`
- plus `E_NOT_FOUND`, `E_BAD_REQUEST`, and `E_INTERNAL`.

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/` | Embedded interactive editor |
| `GET` | `/api/health` | Engine/SQLite health and intent counts |
| `GET` | `/api/sample?seed=242` | Built-in deterministic sample |
| `POST` | `/api/validate` | Parse and plan without changing state |
| `GET` | `/api/snapshots` | Snapshot load versions |
| `GET` | `/api/snapshot?snapshot_id=` | Committed raw snapshot |
| `POST` | `/api/snapshots` | Atomically load a snapshot (direct snapshot JSON body) |
| `GET` | `/api/tenants?snapshot_id=` | Tenant priorities and committed versions |
| `POST` | `/api/recalc` | Run one bounded recalculation round |
| `GET` | `/api/aggregates` | Query windows/aggregates for one tenant |
| `GET` | `/api/tenant-state` | Tenant intermediate sums/group index |
| `GET` | `/api/schedule` | Round, tasks, trace, and SHED basis |
| `GET` | `/api/rounds` | Recent rounds |
| `GET` | `/api/recovery` | Latest/requested durable recovery report |
| `GET` | `/api/recovery-reports` | Recovery report history |
| `GET` | `/api/intents` | Intent log, optionally filtered by status |

### Recalculation request

```json
{
  "snapshot_id": "det-sample-seed-242",
  "budgets": {
    "global_concurrency": 4,
    "tenant_concurrency": 2,
    "time_budget_ms": 50,
    "queue_capacity": 512,
    "task_delay_ms": 0
  },
  "tenant_ids": [],
  "spec_ids": [],
  "group_keys": [],
  "force": false,
  "expected_snapshot_version": 1
}
```

`force: true` recomputes already committed windows. Without force, only `PENDING`, `SHED`, and `FAILED` windows are selected. Filters are optional.

### Aggregate query

```text
GET /api/aggregates?snapshot_id=...&tenant_id=tenant-a&window_id=...&spec_id=...&group_key=...&version=...
```

The optional `version` is an expected current tenant version. A mismatch returns `E_STALE_VERSION`. The response includes:

- window state and half-open `[window_start_ms, window_end_ms)` bounds;
- event `count`;
- `sum_scaled`, `sum_fixed`, `min_scaled`, `min_fixed`, `max_scaled`, `max_fixed`;
- `sample_size`, `scale`, and `rounding_mode`;
- the tenant intermediate state and group index from the same read transaction.

## Domain Semantics

### Snapshot validation

- Snapshot, tenant, spec, and event ids are validated for presence/non-empty values.
- Tenant ids must be unique; explicit event ids must be unique.
- Event timestamps must be `>= 0`; sequence numbers must be positive.
- Each event must reference a declared tenant and have a non-empty group key.
- `width_ms` must be positive for every spec.
- Sliding windows require positive `slide_ms`; session windows require positive `session_gap_ms`.
- Measurements accept a decimal JSON string or number with at most six fractional digits. They are converted exactly to a scaled int64. Missing/null is allowed.
- Any parse/validation failure returns `E_SNAPSHOT_INVALID` before a write transaction starts; previously committed snapshots are untouched.
- Snapshot load is one transaction and receives a monotonically increasing `load_version`.

### Boundary and window rules

The engine explicitly declares `LEFT_CLOSED_RIGHT_OPEN`: start inclusive, end exclusive. This is returned on every window and aggregate.

| Type | Rule |
|---|---|
| Tumbling | `[floor(ts / width) * width, start + width)` |
| Sliding | starts are multiples of `slide_ms`, including negative pre-epoch starts when a non-negative event lies within `width_ms` |
| Session | same tenant/group events split when `delta >= session_gap_ms`; end is `last_ts + session_gap_ms` |

Session `width_ms` remains a required positive model field but does not cap a session. An optional `max_session_ms` explicitly enables an upper bound. Empty windows are never materialized. Defensive planning rejects a zero-duration result with `E_WINDOW_ZERO_DURATION`; a valid request cannot normally reach that path because all durations are positive.

An event on a tumbling or sliding end boundary does not enter that window. An event exactly one gap after a session's last timestamp starts a new session.

### Aggregation

For each tenant/group/window:

- `count` is the number of unique events; missing measures still count.
- `sample_size` is the number of unique events with a present measure.
- `sum`, `min`, and `max` use only present measures. With no present measures, `sum = 0` and min/max are `null`.
- The sum uses checked int64 fixed-point addition at every step. Failure returns `E_NUMERIC_OVERFLOW` with tenant, window, group, running sum, and next value.
- Values are never converted to floating point.
- `rounding_mode` is `NONE`; no division or rounding occurs.
- Duplicate sequence numbers in one window/group are resolved deterministically: greatest timestamp, then present measure over missing, then greatest scaled value, then lexicographically greatest `event_id`.

### Sliding overlap

An event in multiple overlapping sliding windows is aggregated in every corresponding window. Task identity is `(snapshot_id, window_id, group_key)`.

## Scheduler

### Ordering and budgets

Priority is numeric ascending: lower values are more important. Reservation order is:

1. tenant priority ascending;
2. window start ascending;
3. tenant id, spec id, group key, window id as deterministic tie-breakers.

Each task declares a deterministic accounted cost:

```text
cost_ms = max(1, candidate event count for that window/group task)
```

The scheduler reserves the longest prefix whose summed cost fits `time_budget_ms`. All tasks in that prefix are expected to dispatch; the remaining suffix is `SHED`. Queue capacity applies to the selected prefix after accounting, so a small budget does not turn a would-be SHED suffix into a queue-full error.

If budget-selected tasks beyond currently available concurrency slots exceed `queue_capacity`, the round is rejected with `E_RECALC_QUEUE_FULL` and no round/task row is written.

The scheduler tracks a wall clock too, but budget accounting is deterministic task cost rather than nondeterministic wall-clock execution time. `task_delay_ms` is a bounded demo/test hook that holds a task after its durable start intent; it does not alter budget ordering.

### Concurrency

- One round admission at a time (`roundMu`), so the engine-wide global ceiling cannot be exceeded by overlapping HTTP triggers.
- Within the round, tasks execute in parallel up to `global_concurrency`.
- Per-tenant running counts never exceed `tenant_concurrency`.
- The dispatch order is strict; if the next priority task's tenant is at its limit, the scheduler waits rather than dispatching a lower-priority task out of order.
- The database additionally has a partial unique index preventing two active `QUEUED`/`RUNNING` tasks for the same `(snapshot_id, window_id, group_key)`.
- Observed maximum global/per-tenant concurrency is persisted on the round.

### Deterministic SHED

The unselected suffix is shed in reverse reservation order: lowest priority first, then latest window start, then reverse id tie-breakers. Each decision records:

- `decision_order`;
- priority and window start;
- task cost;
- consumed budget and remaining budget;
- the rule and reservation rank.

Started tasks are allowed to finish. SHED tasks record `consumed_ms = 0`; their windows move to `SHED` and remain eligible for a later round without double-charging budget.

## Consistency Model

### Staging and atomic promotion

Aggregate computation never mutates visible committed state. The task lifecycle is:

1. `RECALC_START` intent plus task `QUEUED -> RUNNING` in one transaction.
2. Compute entirely from the immutable snapshot and task bounds.
3. Persist the result in `window_staging`.
4. Durably append a `RECALC_COMMIT` intent.
5. Acquire the per-tenant writer lock.
6. In one transaction:
   - upsert the window aggregate with the next tenant version;
   - rebuild `tenant_intermediate` from all committed windows;
   - replace the tenant's `group_index` rows;
   - mark the window and task committed;
   - mark all pending intents for the task `APPLIED`;
   - append `commit_log`.

Thus there is no visible state where a window is committed but tenant sums or group indexes are stale. Any reader starts one SQLite read transaction and reads version, windows, aggregates, intermediate state, and group index from that same snapshot. Multiple readers run concurrently under WAL. The per-tenant writer lock serializes shared-state promotion even when multiple tasks for that tenant finish together.

Force-recomputing an already committed window keeps the prior committed aggregate visible until the replacement transaction promotes atomically. A crash rolls back to that prior committed value, not to a partial replacement.

### SQLite tables

- `snapshots`, `tenants`, `window_specs`, `events`
- `windows` — planned window/group rows and state
- `recalc_tasks`, `rounds`, `scheduler_trace`, `shed_decisions`
- `window_staging` — uncommitted computed aggregates
- `window_aggregates` — committed visible aggregates
- `tenant_intermediate`, `group_index` — tenant shared state
- `intents` — durable action log
- `commit_log`, `recovery_reports`

The engine opens SQLite with WAL, `synchronous=FULL`, a busy timeout, and foreign keys. A global store writer plus per-tenant locks make write order explicit and avoid SQLite writer contention.

## Crash Recovery

Recovery runs once at server startup, before serving HTTP.

For every pending `RECALC_START`, `RECALC_COMMIT`, or `RECALC_SHED` intent:

- nonterminal tasks are marked `ABORTED`;
- staged rows are marked `ROLLED_BACK`;
- if the window has no committed aggregate it is reset to `PENDING`;
- if a prior committed aggregate exists it remains committed;
- pending intents are finalized as `ROLLED_BACK` or `APPLIED` according to durable task state.

Startup also aborts orphaned `QUEUED`/`RUNNING` tasks, marks interrupted rounds `INTERRUPTED`, and writes a `recovery_reports` row. The report contains the pre-recovery pending intents, aborted task ids, reset window ids, finalized intent ids, and interrupted rounds.

The report is derived only from committed SQLite rows. The engine never promises that its process will generate an exit-time report; a `SIGKILL` can interrupt any action. The next startup report states exactly what durable evidence was found and repaired.

## Verification

Automated tests:

```bash
export GOFLAGS=-mod=vendor
export GOPROXY=off
go test ./... -count=1
go test -race ./... -count=1
go vet ./...
```

Coverage includes:

- fixed-point parsing and int64 overflow;
- all three window types, epoch/year boundaries, negative sliding starts, session gap boundaries;
- duplicate-sequence winners, missing measures, left/right boundary behavior;
- full recalculation and tenant-state/aggregate reconciliation;
- bounded queue rejection without a round write;
- deterministic budget SHED and later re-enqueue with zero double charge;
- full and per-tenant observed concurrency limits;
- concurrent readers seeing only coherent tenant versions;
- invalid snapshot no-state-change behavior;
- API error envelopes, stale versions, queue full, and numeric overflow rollback;
- pending start+commit intent recovery with staged aggregate rollback.

### Latest local run

The checked-in deterministic sample is `samples/deterministic.json` (seed `242`, 3 tenants, 4 specs, 70 events, 60 planned group/window tasks).

The scripted run recorded under `artifacts/` demonstrated:

- full round: `60` committed, `0` shed, `350 ms` accounted;
- constrained round: `5 ms` budget, `4 ms` accounted, `2` committed, `58` SHED, remaining `1 ms`;
- SQLite spot check: `60` committed aggregates, `5` group-index rows, no pending intents after round completion;
- real `SIGKILL`: startup recovery observed pending intents, aborted all nonterminal tasks, reset uncommitted windows, and recorded interrupted round evidence;
- post-recovery re-enqueue: `60` tasks committed with no failures or SHED.

Latest recorded run: `artifacts/demo-20261125-093839/`. See its `SUMMARY.md`, `recovery-report.json`, and `sqlite-recovery-check.txt` for exact ids and SQLite results. The consolidated build/test/race/vet/demo transcript is `artifacts/verification-20261125-093839.log`.

## Repository Layout

```text
cmd/windowd/             HTTP server and startup recovery
cmd/windowctl/           deterministic sample generator/validator
internal/domain/         models, validation, fixed point, planner, aggregation
internal/store/          SQLite schema, task lifecycle, reads, recovery
internal/engine/         budgets, dispatch, SHED, consistency orchestration
internal/httpapi/        JSON API and embedded interactive UI
internal/sample/         fixed-seed generator
samples/                 checked-in deterministic sample
scripts/demo.sh          full, budget, and SIGKILL recovery demonstration
vendor/                  offline Go dependencies
```

## Operational Notes

- The HTTP listener defaults to loopback `127.0.0.1:18099`; the demo chooses a free loopback port.
- SQLite is the durable single-node authority. Do not run two independent mutating server processes against the same database file.
- Rounds are admitted serially; tasks within a round are parallel. This deliberately favors a hard global concurrency invariant over overlapping round admission.
- Only `LEFT_CLOSED_RIGHT_OPEN` is accepted, and the selected boundary is echoed explicitly in every result.
- The sample and all engine paths are local and deterministic except wall-clock timestamps, random id suffixes, and the actual order in which concurrently completing tasks append their committed/failed trace rows.
