# NTFS ACL Inheritance Audit and Controlled Writeback

A deterministic, local-only engine for auditing NTFS-style DACL inheritance, reconstructing effective access, classifying migration conflicts, taking versioned SQLite snapshots, and applying minimal repairs to a real on-disk tree.

The project is pure Go (no CGO) and uses `modernc.org/sqlite`. It builds on Windows and Linux. The default fixture is produced entirely by the program: real directories/files plus per-node placeholder security-descriptor metadata. No domain account, network, Windows API, Docker, or external database is required.

## Quick start

Requirements: Go 1.22 or newer.

```sh
go build -o bin/ntfsacl ./cmd/ntfsacl

WORK=$(mktemp -d)
bin/ntfsacl init   --workdir "$WORK" --reset
bin/ntfsacl audit  --workdir "$WORK" --output audit.json
bin/ntfsacl writeback --workdir "$WORK" --output writeback.json
bin/ntfsacl conflicts --workdir "$WORK" --output conflicts-after.json
bin/ntfsacl report --workdir "$WORK" --before 2 --after 3 --output compare.json
```

All writes stay below `--workdir`. User-supplied subtree and output paths must be relative and cannot escape the workspace. Symlink components inside the sample tree are rejected.

## Commands

| Command | Purpose |
|---|---|
| `init` | Create the deterministic tree and ACL metadata. `--malformed flags\|empty\|mask` injects a validation fault. `--reset` removes only generated `tree/` and `.ntfsacl/` directories first. |
| `audit` | Audit the full tree or repeated `--subtree` sessions. Commits one contiguous audit snapshot. Configurable `--concurrency` and `--node-budget`. |
| `replay` | Emit the exact committed snapshot bytes for `--version`. On a missing/gapped version, emits the nearest healthy snapshot bytes, reports `E_SNAPSHOT_VERSION`, and exits 3. |
| `conflicts` | Emit the stable conflict closure, source chains, first deviations, and current snapshot chain. |
| `writeback` | Generate and execute a minimal plan only for conflict nodes, with writeback-before/after snapshots and per-node revisions. |
| `recover` | Recover unfinished writeback sessions after forced termination. It deliberately does not commit or promise an exit report. |
| `report` | Export a stable before/after snapshot comparison with node ACL changes and conflict summaries. |

Useful fault/writeback parameters:

```text
--fail-node REL                 inject a write failure after prior nodes were applied
--crash-at N                    forced termination after plan node N (1-based)
--crash-point after-pending     kill after DB intent commit, before descriptor write
--crash-point after-write       kill after descriptor write, before commit
--crash-point after-commit      kill after node commit
```

Exit codes: `0` success, `1` operational failure/writeback rollback, `2` malformed ACL, `3` snapshot version fallback, `4` recovery incomplete, `137` injected forced termination.

## Deterministic fixture

The tree is under `tree/`; placeholder security descriptors are under `.ntfsacl/acl/`; the policy/principal registry and SQLite database are under `.ntfsacl/`.

```text
.
├── finance/
│   └── budget.txt
├── docs/
│   ├── plan.txt
│   └── archive/
│       └── old.txt
└── legacy/
    └── orphan.txt
```

Principal registry: `SIDIntern`, `SIDFinance`, `SIDDocs`. `SIDOrphan` is intentionally absent.

The sample produces exactly one instance of each required conflict:

| Code | First deviation | Evidence |
|---|---|---|
| `E_CONFLICT_DENY_OVERRIDE` | `finance/budget.txt`, ACE 1 | Raw DACL has explicit ALLOW then explicit DENY; the deny also overrides an inherited allow. |
| `E_INHERIT_BROKEN` | `docs/plan.txt`, ACE 2 | Declared source `.:2` is absent because `docs` is protected; the child drifts to `NOT_GRANTED`. |
| `E_FLAG_INCOMPATIBLE` | `legacy/orphan.txt`, ACE 0 | Explicit copy has `OI`, while the inherited ACE of the same lineage has no propagation flags. |
| `E_ORPHAN_SID` | `legacy/orphan.txt`, ACE 1 | `S-1-5-21-424242-9999` is not in the fixture principal registry. |

## ACL semantics

Validation happens before inheritance reconstruction. `E_ACL_MALFORMED` includes node path and ACE index for:

- unknown ACE type or non-SID subject;
- absent or empty DACL;
- zero/out-of-range access mask;
- `IO` without `OI`/`CI`, `NP` without `OI`/`CI`, or `IO|NP`;
- `CI` on a file object;
- duplicate declared intent, invalid access name or expected decision.

Inheritance reconstruction:

- explicit ACEs are evaluated at their node;
- `CI` propagates to directory children, `OI` to file children;
- `IO` applies to children but not to the ACE's own node;
- `NP` applies to the immediate child, clears all propagation bits, and stops there;
- protected DACLs accept no parent inherited ACEs and propagate none;
- canonical order is explicit before inherited, DENY before ALLOW before AUDIT, then inheritance depth/source path/source index;
- effective masks accumulate all matching DENY/ALLOW ACEs. Any denied requested bit wins; all requested bits must be allowed otherwise;
- each decision carries every contributing ACE's original node path, ACE index, inheritance hop, and source path.

Raw `ALLOW` before `DENY` is retained as an explicit ordering anomaly and reported as `E_CONFLICT_DENY_OVERRIDE`, even though canonical effective evaluation puts DENY first. This exposes the historical ordering defect rather than hiding it.

## Snapshots and collision-free replay

Every snapshot is committed in one SQLite `BEGIN IMMEDIATE` transaction with:

- contiguous version and parent version;
- parent chain hash;
- SHA-256 of exact payload bytes;
- chain hash = `SHA-256(version || parent_chain_hash || payload_sha256)`;
- content fingerprint used for change classification.

Snapshot kinds are `audit`, `writeback_before`, and `writeback_after`. Old payload bytes are kept verbatim. `replay` writes exactly those bytes; it does not decode and re-encode them. Chain loading also verifies the normalized content fingerprint, so a content hash edit is treated as chain damage.

Chain loading verifies sequence, parent links, payload hash, and chain hash. A gap or damaged link yields `E_SNAPSHOT_VERSION`; replay returns the longest healthy prefix and writes those exact bytes. Snapshot comparison classifies transitions as `append`, `unchanged`, `incremental`, or `whole_replace` using normalized node identity + ACL hashes, not version numbers alone.

## Controlled writeback and recovery

Repair plan rules are intentionally minimal and only touch nodes with conflicts:

| Conflict | Repair |
|---|---|
| `E_CONFLICT_DENY_OVERRIDE` | remove the conflicting explicit DENY ACE |
| `E_INHERIT_BROKEN` | add the missing declared-inheritance ACE to the conflict node |
| `E_FLAG_INCOMPATIBLE` | clear the incompatible propagation flags on the copied explicit ACE |
| `E_ORPHAN_SID` | remove the orphan ACE |

Before planning, the engine re-audits the current tree and refuses a stale audit snapshot with `E_STALE_AUDIT`. Before execution, a `writeback_before` snapshot is committed. Each node then uses this durable sequence:

1. acquire SQLite-backed node lock;
2. verify current bytes equal the plan's before hash, or detect an identical already-applied plan and skip it;
3. commit a `PENDING` operation containing both before bytes and intended after bytes, plus a versioned ACL revision;
4. atomically replace the descriptor metadata;
5. verify the written hash;
6. commit node `COMMITTED` and an after revision.

A normal failure restores every applied node from its before revision, records restore failures, commits a `writeback_after` snapshot of the restored state, and returns `E_WRITEBACK_ROLLBACK` with the affected scope.

After forced termination:

- `COMMITTED` nodes remain committed;
- a `PENDING` operation matching the before state is discarded;
- a `PENDING` operation matching the intended after state is restored to before;
- both are removed from progress and returned in the requeue set;
- a descriptor matching neither before nor intended after is left untouched, listed as residual, and reported as `E_RECOVERY_INCOMPLETE`;
- no later exit report is committed.

The node lock table serializes concurrent writers to the same node. Lock diagnostics record subtree, node path, owner, and measured wait time.

## Concurrency and budgets

`audit --subtree` creates independent sessions. A bounded worker pool queues excess sessions instead of rejecting them. Results are merged in input order and sorted by relative path/ACE index, so concurrency setting does not alter report bytes.

`--node-budget N` stops a session after N deterministic breadth-first nodes. The completed prefix is retained and the session gets `E_NODE_BUDGET` with `partial: true`. The report itself records the code and partial status.

## Determinism

- Reports contain no absolute paths, timestamps, random IDs, or map iteration order.
- Nodes sort by relative path; ACEs by canonical sequence; conflicts by code rank/path/ACE/SID.
- Repeated audits of identical fixtures in fresh workspaces produce byte-identical snapshot payloads.
- Replaying the same committed version produces byte-identical files.
- Live concurrent writeback exposes measured lock wait milliseconds; these are operational diagnostics and by definition depend on scheduling. Lock-free repeated writebacks record zero wait.

## Verification

Run:

```sh
go test ./...
go vet ./...
GOOS=windows GOARCH=amd64 go build -o /tmp/ntfsacl.exe ./cmd/ntfsacl
./scripts/verify.sh
```

Test coverage includes: malformed descriptors, OI/CI/IO/NP propagation, partial allow-mask combination, deny dominance and ordering inversion, all four conflict classes, node budgets/queued sessions, exact replay, gap fallback, whole-replacement classification, successful repair, stale-audit rejection, rollback, all three crash points, mixed committed/pending recovery, residual intermediate state, scoped node locking, concurrent identical writebacks, CLI path confinement, CLI byte replication, report comparison, and Windows compilation.

### Recorded verification

Environment: `go version go1.27.4 linux/amd64`, module target Go 1.22, Windows cross-build `GOOS=windows GOARCH=amd64`.

```text
$ go test ./...
ok  example.com/ntfsacl/cmd/ntfsacl
ok  example.com/ntfsacl/internal/acl
ok  example.com/ntfsacl/internal/audit
ok  example.com/ntfsacl/internal/store
ok  example.com/ntfsacl/internal/writeback

$ go test -race ./...
ok  all packages (no data races)

$ go vet ./...
(no output)

$ GOOS=windows GOARCH=amd64 go build -o /tmp/ntfsacl-final.exe ./cmd/ntfsacl
/tmp/ntfsacl-final.exe: PE32+ executable for MS Windows 10.00 (console), x86-64
```

Latest `scripts/verify.sh` result:

```text
PASS  fresh-workspace audit JSON is byte-identical
PASS  fixture classified all four conflict codes
PASS  replay reproduces exact committed bytes
PASS  missing version returns E_SNAPSHOT_VERSION and exact latest bytes
PASS  controlled writeback committed 3 conflict nodes with no null arrays
PASS  post-writeback audit is conflict-free and snapshots compare incrementally
PASS  comparison includes full node/source data, conflicts, and business codes
PASS  version gap falls back to nearest contiguous snapshot with exact bytes
PASS  failure rolled back prior committed node and restored tree hash
PASS  forced termination recovered pending write and requeued node
PASS  malformed ACL rejected before inheritance and snapshot creation
```

See [docs/DESIGN.md](docs/DESIGN.md) for the state machine and SQLite schema.

## Portability note

Windows ACL APIs cannot be exercised without a Windows host and real security principals. This repository therefore implements a deterministic, cross-platform security-descriptor provider: real files/directories plus atomic placeholder ACL metadata. The audit semantics, inheritance flags, masks, SID identities, snapshot/rollback protocol, and writeback journal are fully exercised locally. The same core can later be paired with a Windows ACL adapter behind the fixture store boundary without changing report or recovery logic.
