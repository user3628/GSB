# Design Notes

## Package boundaries

- `internal/domain`: portable descriptor, ACE, intent, decision, conflict, and canonical JSON model.
- `internal/acl`: validation, inheritance propagation, canonical ordering, effective decision, conflict detection.
- `internal/fixture`: deterministic real tree plus placeholder ACL metadata and principal policy.
- `internal/audit`: subtree discovery, bounded concurrent sessions, node budgets, merged deterministic results.
- `internal/store`: pure-Go SQLite migrations, snapshot chain, exact payload replay, node locks, writeback journals, SQLite transactions.
- `internal/writeback`: minimal repair planning, execution, rollback, and forced-termination recovery.
- `internal/report`: deterministic audit report schema.
- `cmd/ntfsacl`: CLI pipeline and stable JSON export.

## Descriptor storage

A node descriptor is canonical JSON at:

```text
<workdir>/.ntfsacl/acl/<base64url(relative-path)>.json
```

The real file/directory always exists at `<workdir>/tree/<relative-path>`. Writeback verifies the logical node exists and rejects symlinks before atomically replacing the metadata file. This boundary isolates NTFS mechanics from the deterministic audit/recovery core.

## Validation gate

`acl.RebuildTree` loops over every descriptor and calls `ValidateDescriptor` before any parent/child propagation. Any failure returns `*acl.MalformedError`; no report is rendered and the CLI does not open/commit a snapshot.

## Inheritance algorithm

Nodes are processed depth-first by path order: depth ascending, then path. For each node:

1. If protected, inherited seed is empty.
2. Otherwise transform each parent effective ACE:
   - require `CI` for directory children or `OI` for file children;
   - clear `IO` so an inherit-only ACE becomes effective on its child;
   - for `NP`, clear all propagation bits after the immediate child;
   - append a source-chain hop.
3. Append explicit ACEs.
4. Sort by origin, type rank, inheritance step, source path, original index; assign canonical sequence.
5. Evaluate declared intents against effective deny/allow masks.

## Conflict rules

- Orphan SID: explicit ACE SID absent from the policy registry.
- Flag incompatible: an explicit ACE and inherited ACE with the same type/SID/mask have different flag sets.
- Deny override: raw explicit ALLOW-before-DENY inversion, or an explicit DENY causing an expected-ALLOW intent to evaluate DENY while an inherited ALLOW exists.
- Inherit broken: declared `expected_source` is absent from the resulting source chain, or an expected non-NOT_GRANTED decision evaluates to NOT_GRANTED.

A conflict key is `(code,node,ACE,SID)`, preventing duplicates while canonical sorting keeps output stable.

## SQLite schema

```sql
snapshots(
  version PRIMARY KEY,
  kind,
  parent_version,
  parent_chain_hash,
  payload BLOB,          -- exact replay bytes
  payload_sha256,
  chain_hash,
  content_hash
)

writeback_sessions(
  id PRIMARY KEY,
  audit_version,
  plan_hash,
  status,
  code,
  before_snapshot_version,
  after_snapshot_version,
  created_seq
)

acl_revisions(
  revision PRIMARY KEY AUTOINCREMENT,
  session_id,
  node_path,
  phase,                 -- before / after
  payload BLOB,
  payload_sha256
)

writeback_ops(
  id PRIMARY KEY AUTOINCREMENT,
  session_id,
  seq,
  node_path,
  phase,                 -- PENDING / COMMITTED / ROLLED_BACK
  before_revision,
  before_payload,
  before_sha256,
  after_revision,
  after_payload,
  after_sha256,
  error,
  UNIQUE(session_id, seq)
)

node_locks(
  path PRIMARY KEY,
  owner,
  subtree,
  acquired_unix_nano
)
```

All multi-row snapshot and journal transitions use `BEGIN IMMEDIATE`; SQLite WAL and busy timeout provide cross-process serialization.

## Writeback states

```text
PENDING session
  -> RUNNING
     -> per node: PENDING op -> COMMITTED op
     -> COMPLETED + writeback_after snapshot

RUNNING + normal error
  -> restore applied nodes to before revisions
  -> ROLLED_BACK + writeback_after snapshot + E_WRITEBACK_ROLLBACK

RUNNING + process death
  -> recover
     -> committed ops preserved
     -> pending before-state or intended-after-state discarded
     -> RECOVERED, pending nodes requeued
     -> neither state: RECOVERY_INCOMPLETE + residual paths, no exit report
```

An identical repair already present when a serialized writer acquires a node lock is recorded as `ALREADY_APPLIED`; this makes concurrent identical writebacks idempotent while preserving per-node serialization.

## Snapshot chain formulas

```text
payload_sha256 = SHA256(exact_payload_bytes)
chain_hash = SHA256(decimal(version) + "\n" + parent_chain_hash + "\n" + payload_sha256)
content_hash = SHA256(tree_hash + "\0" + sorted(node_path + "|" + acl_hash + "\0"))
```

Replay uses `payload` directly. It never reconstructs JSON from the parsed model.

## Determinism controls

- no timestamps in report payloads;
- no absolute workspace paths;
- fixed fixture IDs and SIDs;
- report array defaults are `[]`, never `null`;
- stable sort keys for nodes, ACEs, conflicts, and comparison nodes;
- deterministic session IDs and worker result positions;
- lock wait durations limited to operational writeback diagnostics.
