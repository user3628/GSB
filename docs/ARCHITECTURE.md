# Technical Architecture

## 1. State model

Every successful policy commit or scaling round creates one immutable `revision`.
A revision is one atomic, query-visible snapshot:

```text
revision
├── policy_version
├── placements(replica_id, service_id, node_id, memory_bytes, compute_milli)
├── node_states(node_id, healthy, used_memory_bytes, used_compute_milli)
└── service_states(service_id, state, current, desired, reason, degraded_domains)
```

Readers open a SQLite read transaction and select all three collections before
commit. A revision can therefore never expose half of a scale-down plus its
scale-up admission. `policy_services` and `policy_nodes` retain normalized
policy facts; `policies.snapshot_json` retains the exact submitted snapshot.

## 2. Write protocol

### Policy update

1. Quickly validate submitted `version`; if `<= latest`, return
   `E_STALE_SNAPSHOT` without any write.
2. Parse and normalize all units and topology.
3. Build the next state draft from existing placements (or bootstrap state).
4. Commit a `PENDING` intent in its own transaction.
5. In one `BEGIN IMMEDIATE` transaction: insert policy rows, revision rows,
   placements, node usage and service states; advance `latest_revision`; mark
   intent committed.

### Scaling round

1. Under the process write lock, check `round_id` and request SHA-256.
2. Derive fixed-point targets; any semantic error is a JSON business error.
3. Persist `rounds.status=RUNNING` and a `PENDING` `ROUND_COMMIT` intent.
4. Plan scale-down, migration and scale-up entirely in memory. Scale-down
   releases are immediately visible to subsequent admission in this draft.
5. In one `BEGIN IMMEDIATE` transaction, insert the new revision and all three
   state collections, then set round and intent to `COMMITTED`.

If planning fails (for example bounded queue overflow), the round is marked
`ABORTED` and no revision is inserted. An abort response is cached against the
request hash, so replay is deterministic.

## 3. Recovery

On application startup:

1. Record the latest committed revision from `meta`.
2. Find all `rounds.status=RUNNING` and orphan `intents.status=PENDING`.
3. Mark rounds/intents `ABORTED` and insert `recovery_reports` rows.
4. Do not synthesize a placement delta: SQLite already rolled back any
   uncommitted revision transaction.

The report says only what is present in committed rows. It explicitly notes that
a force-killed process cannot emit a graceful exit report.

## 4. Scheduler order

`plan_round` executes deterministic phases:

1. **Scale-down**: remove replicas down to desired count. Unhealthy replicas and
   over-concentrated domains are removed first. Resources are reclaimed in the
   planning state.
2. **Admission task ordering**: migrations and scale-ups are ordered by
   `(service priority desc, observation arrival asc, migration-before-scaleup,
   service id, replica id)`.
3. **Budget**: `max_parallel_scales * floor(time_budget // placement_cost)`
   admission slots, grouped into deterministic virtual waves of at most
   `max_parallel_scales`. Each attempt consumes a slot and results carry a
   `parallel_wave`. A successful placement also updates node usage and
   service/domain counts before the next task.
4. **Node selection**: only healthy nodes with enough memory *and* compute;
   prefer domains still under the per-service cap, then lower domain
   concentration, then more free memory, then node id.
5. **Queue**: all un-attempted deficits are grouped in `queue`; exceeding
   `wait_queue_limit` raises `E_SCALE_QUEUE_FULL` before any revision write.
6. **Final service state**: unhealthy placement -> `BLOCKED`; capacity rejection
   -> `BLOCKED`; domain overage -> `DEGRADED`; current below desired ->
   `SCALING_UP`; current above desired -> `SCALING_DOWN`; otherwise `STABLE`.

The virtual clock is deterministic. This is intentional: acceptance output must
be reproducible across machines rather than depending on OS scheduling latency.

## 5. Concurrency

- uvicorn runs one worker and one `ScalingControlService` instance.
- `threading.RLock` serializes all mutations in the process.
- FastAPI read routes are synchronous and run in Starlette's threadpool, so
  multiple SQLite readers execute concurrently.
- SQLite `journal_mode=WAL` and a read transaction per response provide a stable
  snapshot while a writer commits a new revision.
- A repeated concurrent `round_id` blocks on the writer lock, then observes the
  committed row. Same request hash returns the stored result; another hash
  returns `E_CONFLICTING_ROUND`.

## 6. Data invariants checked

The test suite verifies:

- `used = reserved + sum(managed placement demand)` for every node.
- `free_memory >= 0` and `free_compute >= 0` in every committed state.
- `service.current_replicas == len(service.placements)`.
- Queue-full, stale, transition and observation failures leave revision
  unchanged.
- Forced process exit leaves no partial revision and startup recovery marks the
  round aborted.
- Concurrent readers all return the same complete revision.
