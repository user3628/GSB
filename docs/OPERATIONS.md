# Operations and Internal Design

## 1. Components

| Module | Responsibility |
|---|---|
| `slo_budget/binary_format.py` | Binary header/record codec, endian support, CRC and offsets |
| `slo_budget/jsonl_format.py` | JSONL manifest/record codec and microsecond normalization |
| `slo_budget/compat.py` | Explicit format/version compatibility matrix |
| `slo_budget/events.py` | Request-event parsing, filtering and bucket aggregation |
| `slo_budget/calculator.py` | Window rollups, SLI, budget burn, quantiles and alert grading |
| `slo_budget/storage.py` | SQLite schema, conflict resolution, intents, recovery and atomic state |
| `slo_budget/service.py` | Application orchestration and stable response envelopes |
| `slo_budget/server.py` | Local `http.server` JSON API |
| `slo_budget/cli.py` | Operational CLI and forced-exit crash probe |

## 2. Write Path and Intent Journal

Every business mutation follows this sequence:

```text
1. BEGIN; INSERT intent_log(status='pending'); COMMIT
2. Parse and validate input without changing business tables.
3. BEGIN IMMEDIATE
4. Validate again under the write lock where state matters.
5. Apply all rows + import/replacement report.
6. UPDATE intent_log SET status='committed'  -- same transaction
7. COMMIT
8. Swap the in-memory active index (snapshot replacement only).
```

Failure handling:

- Parse, validation, compatibility or sequence failure: no window rows are committed.
- The intent is changed to `aborted`; an `ABORTED` import/replacement report is persisted.
- Conflict rejection: the transaction inserts only the rollback report and changes the intent to `rolled_back`; all window state remains unchanged.
- Process kill: SQLite rolls back any uncommitted transaction. On the next startup, `recover_pending_intents()` marks every remaining `pending` intent `aborted` and writes a recovery report referencing the last complete snapshot sequence/count.

The forced-exit probe deliberately leaves an uncommitted `record_count=999999` update and exits with code `17`. Reopening the database preserves the prior record count and active sequence.

## 3. SQLite Schema

| Table | Contents |
|---|---|
| `snapshot_state` | Single active snapshot sequence, format/version, generation time, activation time, record count |
| `windows` | Active normalized records keyed by `(service, sli_type, window_start_ms)` |
| `snapshot_versions` | Historical snapshot activations, checksums and `active`/`retired` status |
| `import_reports` | Full import/replacement report, decisions, degradation and rounding details |
| `alerts` | Last persisted grade/result per snapshot sequence/service/SLI/as-of |
| `intent_log` | Pending/committed/rolled_back/aborted intent records |
| `recovery_reports` | Restart recovery actions and preserved state |

SQLite runs in WAL mode with `synchronous=FULL` and `busy_timeout=5000`.

## 4. Snapshot Replacement

Whole-snapshot replacement requires:

```text
incoming.snapshot_sequence > active.snapshot_sequence
```

Replacement is executed while holding the storage `RLock`:

1. Parse and normalize all records.
2. Build a prospective memory index.
3. `BEGIN IMMEDIATE`, re-check sequence monotonicity, mark the old version retired.
4. Insert the new version, delete old active rows, insert all new rows.
5. Atomically update `snapshot_state` and the report.
6. Change the write intent to `committed` and `COMMIT`.
7. Assign the new memory index and clear derived caches before releasing the lock.

On any failure before commit, the old SQLite rows and old memory index remain active. The API reports `E_SNAPSHOT_INVALID` and includes whether the old snapshot was preserved.

**Deployment constraint:** use one active API process per database. The in-memory index is authoritative for reads in that process. Multiple read-only OS processes are possible, but they must not independently write, because their memory indexes would not observe one another's swaps.

## 5. Import Conflict Resolution

Identity key: `(service, sli_type, window_start_ms)`.

Ordered tie-breakers:

1. Higher `format_version` wins; lower is skipped.
2. Higher `late_count` wins; lower is skipped.
3. Later `last_write_ms` wins; earlier is skipped.
4. If all tie-breakers are equal and business fields are identical, skip as idempotent.
5. If all tie-breakers are equal but business fields differ, reject as ambiguous.

Any `REJECT` in a batch causes the whole batch to roll back. The report retains all decisions:

| Action | Meaning |
|---|---|
| `INSERT` | No existing record |
| `OVERWRITE` | Deterministically newer record replaces existing |
| `SKIP` | Existing wins or records are idempotent |
| `REJECT` | No deterministic winner |

## 6. Calculation Semantics

All configured windows are evaluated against the active normalized records:

- Include records fully contained in `[as_of-window, as_of)`.
- Reject overlapping snapshot records with `E_INCONSISTENT_READINGS`; counts cannot be de-duplicated safely.
- Sum good/total/late counts over selected buckets.
- `SLI = good_events / total_events`.
- `bad_ratio = bad_events / total_events`.
- `burn_rate = bad_ratio / (1 - slo_target)`.
- `consumed_ratio` equals `burn_rate`; `remaining_ratio = 1 - burn_rate`.

Zero total is deterministic:

```text
sli = 1.000000
bad_ratio = 0.000000
burn_rate = 0.000000
sample_status = NO_DATA
business_code = E_INSUFFICIENT_SAMPLES
```

If `total_events` is nonzero but below `min_samples`, metrics are still returned deterministically with `sample_status=INSUFFICIENT` and `E_INSUFFICIENT_SAMPLES`. HTTP returns status 422 with the evaluation in error details.

### Alert Grading

Default thresholds, most severe first:

| Grade | Short + long burn threshold |
|---|---:|
| `CRITICAL` | both `>= 6.0` |
| `WARNING` | both `>= 2.0` |
| `WATCH` | both `>= 1.0` |
| `OK` | neither exceeds the lowest threshold jointly |
| `UNKNOWN` | short or long has insufficient samples |

The alert pair is 5m and 1h. The 30m window is always calculated and reported. Divergence classification:

| Class | Meaning |
|---|---|
| `CONSISTENT_ABOVE` | both above the lowest threshold |
| `CONSISTENT_BELOW` | both below it |
| `SHORT_ONLY` | recent regression only inside the short window |
| `LONG_ONLY` | older errors remain in the long window |

### Event/Snapshot Consistency

When `events_path` is supplied, the service separately rolls up raw events and compares each window with snapshot counts.

- Exact count match: `attribution=exact_match`.
- Difference within the selected records' `late_count`: `attribution=late_event_policy` and both readings are shown.
- Larger/unattributable difference: `E_INCONSISTENT_READINGS` with snapshot counts, event counts, deltas and max attributable late count. No side is silently selected.

## 7. HTTP API

Start:

```bash
.venv/bin/python -m slo_budget.server \
  --db artifacts/review.db --host 127.0.0.1 --port 8765
```

For repeatable metadata use `--fixed-clock-ms 1767229333456` or set `SLO_FIXED_NOW_MS`.

### Preflight

```bash
curl -s -X POST http://127.0.0.1:8765/v1/preflight \
  -H 'Content-Type: application/json' \
  -d '{"path":"samples/windows/legacy-v1.bin","format":"auto"}'
```

### Replace and Query

```bash
curl -s -X POST http://127.0.0.1:8765/v1/snapshots/replace \
  -H 'Content-Type: application/json' \
  -d '{"path":"samples/windows/current-v2.bin","batch_id":"review-001"}'

curl -s 'http://127.0.0.1:8765/v1/sli?service=checkout-api&sli_type=availability&as_of_ms=1767229200000&events_path=samples/events/requests.jsonl'
```

### Export

```bash
curl -s -X POST http://127.0.0.1:8765/v1/export \
  -H 'Content-Type: application/json' \
  -d '{"path":"artifacts/export.bin","format":"binary","endian":"little"}'
```

### Recovery

```bash
curl -s http://127.0.0.1:8765/v1/recovery
```

## 8. Business Codes

| Code | HTTP | Meaning |
|---|---:|---|
| `E_RECORD_CORRUPT` | 422 | Truncation, CRC, malformed record, field bounds |
| `E_FORMAT_UNSUPPORTED` | 415 | Unknown format/version/required extension |
| `E_COUNT_INVALID` | 422 | Good/late count exceeds total or invalid length |
| `E_INSUFFICIENT_SAMPLES` | 422 | Sample gate failed; deterministic metrics remain in details |
| `E_SNAPSHOT_INVALID` | 409 | Snapshot sequence regression or invalid load |
| `E_IMPORT_CONFLICT` | 409 | Ambiguous conflict; batch rolled back |
| `E_INCONSISTENT_READINGS` | 409 | Overlap or unattributable event/snapshot mismatch |
| `E_CONFIG_INVALID` | 400 | Invalid policy/truncation/endian |
| `E_REQUEST_INVALID` | 400 | Invalid HTTP/CLI request |
| `E_NOT_FOUND` | 404 | Missing endpoint or report |
| `E_IO` | 500 | Local file read/write failure |
| `E_INTERNAL` | 500 | Unexpected internal failure |

## 9. Local-Only Security and Scope

- The server binds to `127.0.0.1` by default and has no authentication because it is a local operator tool.
- Paths are local filesystem paths supplied by the operator.
- The demo connects only to localhost.
- No Docker, container runtime, public monitoring API, credentials or external accounts are used.
