# Multi-tenant quota ledger and settlement verifier

This repository is a local, deterministic Go implementation of the shared-compute
quota borrowing and budget-settlement pipeline described in the task. It uses a
single SQLite database, a local HTTP API, fixed JSON fixtures and a Windows
end-to-end verification entry point.

## Included components

- Versioned full ledger snapshots with canonical SHA-256 integrity digests.
- Milli-share quotas, tenant credit limits, rate tiers, pools and reserves.
- Minute-granular borrow/return ingestion with atomic batch validation.
- Integer/fixed-point interest with explicit per-minute flooring, carried
  nano-share residuals and rate-tier segmentation.
- Priority-ordered repayment of open loan lots.
- Exact pool risk, fixed-decimal ratios and Neumaier compensated-sum error bounds.
- Atomic cascade rebuilds across tenant ledgers and pool-derived products.
- Serialized single writer, concurrent version-consistent readers and idempotent
  request IDs.
- Durable write-ahead intents, startup recovery scanning and a fault-injection
  endpoint used by the verification script.
- Fixed samples, expected outputs, Go tests and a Windows `cmd`/PowerShell verifier.

## Requirements

- Go 1.22 or newer.
- SQLite 3 command-line tool on `PATH` for database inspection.
- Windows 10/11 with Windows PowerShell 5.1 or PowerShell 7 for `scripts\verify.cmd`.
- No Docker. Dependencies are vendored under `vendor/` so the build is offline.

## Run the Windows verification

From the repository root in `cmd.exe`:

```bat
scripts\verify.cmd
```

The script:

1. Builds `ledger-server.exe` with `-mod=vendor`.
2. Starts a local server on a free loopback port.
3. Submits two snapshots, the fixed event sequence and settlement through minute 10.
4. Prints every segmented interest step and pool risk result.
5. Asserts an over-threshold borrow returns `E_POOL_OVERCOMMIT` without changing the ledger.
6. Kills the process during a later settlement/rebuild transaction and restarts it.
7. Asserts recovery reports the aborted intent, the last complete version is intact,
   and the complete derived ledger before/after restart is identical.
8. Runs a post-recovery settlement and checks deterministic outputs.
9. Prints selected `sqlite3` ledger rows.

Generated evidence is written under `artifacts\verify\`.

## Build and run manually

```bat
go build -mod=vendor -o ledger-server.exe .\cmd\ledger-server
go build -mod=vendor -o ledgerctl.exe .\cmd\ledgerctl

ledger-server.exe -db data\ledger.db -addr 127.0.0.1:18080 -enable-crash-endpoint
```

The crash endpoint is disabled unless `-enable-crash-endpoint` is supplied. Use it
only for the local fault-injection test.

Create or refresh a snapshot digest:

```bat
ledgerctl.exe stamp -file samples\snapshot-v1.json
ledgerctl.exe digest -file samples\snapshot-v1.json
```

## HTTP API

All responses are JSON. Failures contain `error.code`, `error.message` and
`error.context`. Every mutating request has an idempotency key in `request_id`.

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/v1/snapshots` | Submit `{request_id, snapshot}` |
| `POST` | `/v1/events` | Submit `{request_id, events:[...]}` |
| `POST` | `/v1/settle` | Submit `{request_id, through_ms}` |
| `POST` | `/v1/borrow-requests` | Adjudicate and optionally commit a borrow |
| `GET` | `/v1/state` | Read the latest complete version |
| `GET` | `/v1/state?version=N` | Read one committed version |
| `GET` | `/v1/settlements/{request_id}` | Read a stored settlement report |
| `GET` | `/v1/recovery` | Read the latest startup recovery report |
| `GET` | `/healthz` | Health and latest version |
| `POST` | `/v1/admin/crash-during-settle` | Fault injection, only with the flag |

Example settlement request:

```json
{
  "request_id": "settle-10m",
  "through_ms": 1767226200000
}
```

## Units and time

- All shares and credit limits are integer milli-shares (`1000` = one share).
- Interest is materialized in micro-shares (`1_000_000` = one share).
- One minute of exact interest in nano-shares is
  `principal_milli * rate_ppm_per_minute`.
- Each minute floors exact nano-shares to micro-shares and carries the residual
  below one micro-share for that loan lot.
- Event and rate effective minute is `ceil(timestamp_ms / 60000)`. An exactly
  aligned timestamp stays in its minute; partial minutes are not prorated.
- Interval `[m, m+1)` is charged when `m < as_of_minute`.

The complete formulas, ordering rules and failure semantics are in
[`docs/TECHNICAL.md`](docs/TECHNICAL.md).

## Business error codes

`E_STALE_SNAPSHOT`, `E_LEDGER_INVALID`, `E_EVENT_INVALID`,
`E_CONFLICTING_SETTLE`, `E_CREDIT_EXCEEDED`, `E_POOL_OVERCOMMIT`,
`E_PRIORITY_CONFLICT`, `E_CONFLICTING_REQUEST`, `E_STALE_SETTLEMENT`,
`E_BAD_REQUEST`, `E_NOT_FOUND`, `E_INTERNAL`.

## Fixed verification result

The checked-in samples produce:

- Tenant A: principal `475004` milli, unsettled interest `77349` micro,
  rounding residual `1100` nano; segments are `standard` minutes `[0,6)` and
  `peak` minutes `[6,10)`.
- Tenant B: principal `294998` milli, interest `26950` micro.
- Tenant C: principal `80003` milli, interest `6400` micro.
- Pool used `850115699` micro, fixed ratio `0.944573`, overcommit excess
  `40115699` micro, trigger tenant `tenant-a`.
- The new borrow is rejected with `E_POOL_OVERCOMMIT`, projected used
  `860115699` micro and excess `50115699` micro.
- Forced termination leaves a durable intent at target version 5; restart aborts it
  and restores version 4 with state digest
  `sha256:4f1c07087de7a1d3b85f19f07da8dc8852b54c810122c86d49f542ab5d40117b`.
- The post-recovery settlement produces version 5, digest
  `sha256:95703e1907e69f95eeb0e253c03a8334dba740309b9970d21ba2a0b07666fc8d`.

These values are asserted by the Go tests and the Windows verifier. See
[`docs/TECHNICAL.md`](docs/TECHNICAL.md#actual-validation-record) for the recorded
run.

## Tests

```bat
go test -mod=vendor .\...
```

Tests cover snapshot atomicity/staleness, event rollback and conflicts, credit
exhaustion, rate-tier splitting, rounding, priority repayment, pool tolerance
boundaries, deterministic ordering, concurrent idempotent settlement, JSON error
responses and an actual child-process kill/recovery cycle.
