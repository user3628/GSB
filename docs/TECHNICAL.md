# Technical design and verification

## Architecture

Immutable facts are separated from complete, versioned derived projections:

```text
HTTP JSON
   |
   v
single writer mutex + durable intent row (autocommit)
   |
   v
one SQLite transaction
   +-- validate request and read committed facts
   +-- recompute mid-state and all callers of that state
   +-- insert new ledger_version + normalized tenant/pool rows
   +-- insert fact rows and reconstruction audit
   +-- commit request idempotency response and intent status
   |
   v
readers: one committed state_json row / version (WAL snapshot)
```

The writer recomputes the full projection in one transaction. This is the cascade
invalidation mechanism: principal, interest, credit, pool ratio, overcommit risk
and cross-tenant exposure are all invalidated and replaced at one new ledger
version. Readers never inspect partially rebuilt normalized tables; `/v1/state`
reads one complete `ledger_versions.state_json` row.

## Data model

Raw facts:

- `snapshots`: full replacement declaration snapshot, canonical digest and
  effective minute.
- `events`: unique `settle_seq`, tenant, type, shares, timestamp and canonical
  content digest.

Commit control:

- `intents`: durable write-ahead intent before the main transaction.
- `requests`: idempotency key, request digest, stored status/body and version.
- `ledger_versions`: complete committed projection, state digest and as-of data.

Inspection products:

- `tenant_states`, `pool_states`: normalized projection for `sqlite3` review.
- `settlement_reports`: detailed per-minute settlement output.
- `reconstruction_audit`: affected tenant and dependent pool products.
- `recovery_runs`: reports built only from committed SQLite facts.

## Snapshot validation

- Snapshot versions must strictly increase; otherwise `E_STALE_SNAPSHOT`.
- JSON fields are strictly typed and unknown fields are rejected by the API.
- Pool total/reserve, threshold, tolerance, tenant base/credit, rate-tier and
  priority fields are range-checked.
- Supported bounds are share/credit values up to `10^12` milli-shares, rates up to
  `10^6` ppm per minute and timestamps through Unix epoch year 9999. These bounds
  keep all interest numerators inside signed 64-bit arithmetic.
- Every tenant must reference a declared pool and rate tier.
- Sum of tenant base shares per pool must not exceed pool total shares.
- A snapshot cannot remove a tenant that already has committed events.
- The supplied SHA-256 digest must match the canonical normalized snapshot. An
  empty digest is filled during submission; a mismatch is `E_LEDGER_INVALID`.
- Every validation failure rolls back the whole transaction and leaves the last
  committed version unchanged.

## Event ingestion

- Batches are atomic. Any invalid event rejects the complete batch.
- `settle_seq` is globally unique and is the content idempotency key for events.
- Replaying the same sequence with the same canonical content is ignored.
- Replaying it with different content returns `E_CONFLICTING_SETTLE` and keeps the
  first committed content.
- Per-tenant committed timestamps must not regress. Within a request, each
  tenant's new events must also be monotonic. Equal timestamps are ordered by
  `settle_seq` during projection.
- A borrowed amount must fit in
  `credit_limit - principal_outstanding - unsettled_interest`; otherwise
  `E_CREDIT_EXCEEDED` includes the exact remaining micro-share credit.
- An event referencing a tenant or snapshot not effective at its minute is
  `E_EVENT_INVALID`.
- A regular usage event may create pool overcommit risk; only an explicit borrow
  request is rejected by pool/priority admission. This permits replay of already
  consumed capacity while adjudicating new borrowing.

## Minute and interest rules

All timestamps are Unix epoch milliseconds. The effective minute is:

```text
m = ceil(timestamp_ms / 60000)
```

An exactly aligned timestamp stays in its minute. Event effects begin at `m`.
Interest interval `[m, m+1)` uses state entering the interval and is charged when
`m < as_of_minute`. There is no partial-minute proration.

A snapshot generated at `t` is effective from `ceil(t / 60000)` for minute-based
decisions. A rate-tier change at such a boundary splits interest exactly there.

For each loan lot and minute:

```text
exact_nanos  = principal_milli * rate_ppm_per_minute
numerator    = carry_nanos + exact_nanos
delta_micros = floor(numerator / 1000)
carry_nanos  = numerator mod 1000
interest     = interest + delta_micros
```

The output includes every step, rate segment (`rate_tier`, rate, start/end minute),
exact nano interest, materialized micro interest and residual carry. A lot's final
carry is its cumulative rounding error and is strictly below `1000` nano-shares.
Sums and adjudication use integer arithmetic.

For the fixed sample, tenant A has `standard` at 10 ppm/minute for `[0,6)` with
`29600` micro interest and `peak` at 25 ppm/minute for `[6,10)` with `47749` micro
interest. Its principal is `475004` milli and residual is `1100` nano.

## Repayment priority

Each borrow lot captures the tenant's repayment priority at opening time. A return
is applied to open principal in this order:

1. lower numeric `repayment_priority` first (1 is highest);
2. earlier opened minute;
3. lexicographic `settle_seq`.

If the return exceeds all open principal, the remainder is applied to unsettled
interest in the same lot order. A partial principal return changes that lot at the
return minute, so all later minute intervals use the reduced principal. Return
allocations are included in tenant state and settlement output.

## Pool risk and exposure

For each pool:

```text
capacity_micros  = (total_shares_milli - reserve_shares_milli) * 1000
tenant_exposure  = principal_milli * 1000 + unsettled_interest_micros
pool_used        = sum(tenant_exposure)
cross_exposure   = sum(max(0, tenant_exposure - base_shares_milli * 1000))
available_credit = sum(max(0, credit_limit_milli * 1000 - tenant_exposure))
```

The fixed ratio uses round-half-up at six decimal places from exact integer input.
The hard threshold and tolerance limits are:

```text
threshold_limit = floor(capacity_micros * threshold_ppm / 1_000_000)
tolerance_limit = floor(capacity_micros * threshold_ppm
                        * (1_000_000 + relative_tolerance_ppm) / 10^12)
```

Risk is true only when `used_micros > tolerance_limit`; equality is inside the
tolerance. When risk is true, excess is measured against the hard threshold. The
trigger tenant is the largest exposure with lexicographically smallest tenant ID
breaking ties.

The pool also reports a Neumaier compensated `float64` aggregate with its naive
sum, compensation and standard `gamma_n = n*u/(1-n*u)` absolute/relative error
bound. Adjudication never uses floating point.

## Borrow admission

The candidate is projected at its request minute using the committed snapshot
schedule and facts visible at that minute. Checks are ordered:

1. Event shape, timestamp monotonicity and duplicate settle sequence.
2. Tenant/rate/snapshot validity and `E_CREDIT_EXCEEDED` from the exact ledger.
3. Pool overcommit with tolerance: `E_POOL_OVERCOMMIT`.
4. Priority protection: `E_PRIORITY_CONFLICT`.

Priority protection reserves remaining credit of tenants with strictly higher
repayment priority in the same pool. A lower-priority tenant may not make projected
exposure exceed `capacity - higher_priority_unused_credit`. This is a separate,
stable policy code from the hard pool threshold.

Accepted borrows are inserted and the full projection is rebuilt in the same
transaction. Rejected borrows do not create a ledger version. Replaying an
identical borrow `settle_seq` under a different `request_id` is a successful no-op
and does not create another ledger version; different content returns
`E_CONFLICTING_SETTLE` and preserves the first committed event.

## Idempotency and concurrency

- A process-local writer mutex serializes all mutating requests.
- Every request ID is checked against `requests`. A different kind or payload
  digest returns `E_CONFLICTING_REQUEST`; a matching request reuses the stored
  status/body after waiting for the writer.
- SQLite WAL permits concurrent readers. State reads one complete committed row.
  The state digest lets a reader verify the complete projection.
- A new request ID can create a new ledger version with the same numeric state, but
  `calculation_digest` excludes request/version metadata and remains stable for
  the same as-of facts and cutoff.

## Intent and recovery protocol

Before the main transaction:

1. Insert an `intents` row in autocommit mode with target version and payload digest.
2. Start the main transaction.
3. Rebuild and write all derived state, facts, request response and committed intent
   status in that transaction.
4. Commit.

On process death during step 3, SQLite rolls back the open transaction while the
intent remains durable. Startup `Recover()` scans pending/processing intents:

- if a matching committed version is durable, reconcile it as committed;
- otherwise mark it `aborted` and record the last complete version and digest.

The recovery report contains only committed SQLite facts: intent actions, last
complete version/digest, persisted snapshot/event/version counts and normalized row
counts.

The fault-injection endpoint deliberately rebuilds into an open transaction, deletes
the uncommitted pool rows and exits. The verifier observes connection loss and a
nonzero exit, restarts the server and checks state equality.

## HTTP error contract

Success:

```json
{"ok":true,"request_id":"...","ledger_version":4,"data":{}}
```

Failure:

```json
{"ok":false,"request_id":"...","ledger_version":4,
 "error":{"code":"E_POOL_OVERCOMMIT","message":"...","context":{}}}
```

Business errors use their specific code/context. Parse failures, unknown routes and
internal failures also use JSON.

## Actual validation record

Recorded on 2026-11-24 in the delivery workspace.

The delivery container is Linux and has no PowerShell runtime, so
`scripts\verify.cmd` was not executed here. The same server binary and HTTP flow
were exercised with a local deterministic harness, and both Windows `amd64`
binaries were cross-compiled successfully. The checked-in PowerShell verifier is
the Windows entry point for the same assertions.

### Automated Go tests

```text
$ go test -mod=vendor ./...
ok   quota-ledger/internal/ledger
```

Covered scenarios include an actual child-process termination/restart, concurrent
idempotent settlement with four concurrent readers, snapshot/event atomic rollback,
pool tolerance equality, lexicographic trigger tie-breaking and stable calculation
digest under reversed fact ordering.

### End-to-end HTTP values

Sequence: snapshot v1, snapshot v2 (rate tier change at minute 6), seven events,
settlement through minute 10.

| Tenant | Principal milli | Interest micro | Rounding nano | Segments |
|---|---:|---:|---:|---|
| tenant-a | 475004 | 77349 | 1100 | standard [0,6), peak [6,10) |
| tenant-b | 294998 | 26950 | 60 | standard [1,10) |
| tenant-c | 80003 | 6400 | 240 | standard [2,10) |

Pool:

```text
used_micros                   = 850115699
used_ratio_fixed              = 0.944573
overcommit_risk               = true
excess_over_threshold_micros  = 40115699
trigger_tenant_id             = tenant-a
cross_tenant_exposure_micros  = 420106299
calculation_digest            = sha256:3c20404ce24ca9d3708c59511aab74c296ec8152d233d5fadff4b12953a150a1
```

Borrow rejection:

```text
HTTP 409
code                          = E_POOL_OVERCOMMIT
projected_used_micros         = 860115699
projected_used_ratio_fixed    = 0.955684
excess_over_threshold_micros  = 50115699
trigger_tenant_id             = tenant-c
```

The rejected borrow left ledger version 4 and its state digest unchanged.

Forced termination/recovery:

```text
target aborted intent version = 5
last complete version         = 4
last state digest             = sha256:4f1c07087de7a1d3b85f19f07da8dc8852b54c810122c86d49f542ab5d40117b
aborted_intent_count          = 1
before/after full state       = identical
```

Post-recovery settlement through minute 12:

```text
ledger_version = 5
state_digest   = sha256:95703e1907e69f95eeb0e253c03a8334dba740309b9970d21ba2a0b07666fc8d
calculation_digest = sha256:afcf246383963fb922e27849735ef4d30e8dce36f6050a0dade96ebd972b2e9d
tenant interest micro = a:101099, b:32850, c:8000
pool used_micros       = 850146949
pool used_ratio_fixed  = 0.944608
```

### Operational scope

The service is designed for a local single-process writer. SQLite WAL supports
concurrent readers, but cross-process writer coordination is not implemented beyond
SQLite locking. There is no authentication or TLS; bind to loopback only. The
committed fixed samples and synthetic events are the only inputs used by the
verification tooling.
