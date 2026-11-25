# HTTP API Examples

All request bodies use `application/json`. Responses are JSON. Error responses always include `code`, `message`, `context`, and `request_id`.

## Commit a Full Policy

```bash
curl -sS -X POST -H 'Content-Type: application/json' \
  --data-binary @/tmp/policy.json \
  http://127.0.0.1:8080/v1/policies
```

Success: `201 Created`

```json
{
  "policy_version": 1,
  "old_version": 0,
  "commit_seq": 1,
  "operation": "full_replace",
  "rebuild": {
    "atomic": true,
    "rebuilt_reservoir_count": 0,
    "rebuilt_hotspot": true,
    "sample_count": 0
  },
  "modes": {"exact_services": 0, "approximate_services": 0}
}
```

Same or older version: `409 E_STALE_SNAPSHOT`.

## Commit an Increment

```json
{
  "base_version": 1,
  "channels": {"cpu": {"target_permille": 500}},
  "services": {"checkout": {"reservoir_capacity": 16, "exact_limit": 8}}
}
```

The new version is always `base_version + 1`. Optional `version` must equal that value or the endpoint returns `409 E_CONFLICTING_REQUEST`. If `base_version` is not current, it returns `409 E_STALE_BASE` without changing state.

## Ingest Samples

```bash
curl -sS -X POST -H 'Content-Type: application/json' \
  --data-binary @/tmp/chunk.json \
  http://127.0.0.1:8080/v1/samples
```

```json
{
  "batch_id": "demo-large",
  "policy_version": 1,
  "commit_seq": 3,
  "total_observations": 43,
  "accepted_unique_samples": 40,
  "new_samples": 40,
  "duplicate_existing_samples": 0,
  "duplicate_observations": 3,
  "mode_by_service": {"checkout": "approximate"},
  "modes": {"exact_services": 0, "approximate_services": 1},
  "reservoir_count": 4,
  "elapsed_ms": 1,
  "within_budget": true,
  "idempotent_replay": false
}
```

Repeating the identical request returns `200 OK` with `idempotent_replay: true`. Reusing the batch ID with different content returns `409 E_CONFLICTING_REQUEST`.

## Strongly Consistent Snapshot

```bash
curl -sS 'http://127.0.0.1:8080/v1/versions/1/snapshot?service=checkout&limit=5'
```

The envelope contains one `commit_seq`; `reservoirs` and `hotspots` were read in the same SQLite read transaction. `hotspots.commit_seq` must equal the envelope value.

## Hotspot Result Fields

Every service contains:

- `path`: exact/approximate mode, reason, statistic, whether reservoir was used.
- `channels`: declared target sampling permille and bias correction.
- `strata`: layer keys, depth prefixes, population/selected counts, reservoirs.
- `approximation_basis`: populated for approximate mode.
- `bias_upper_bound`, `bias_bound_formula`, `confidence_method`.
- `frames[]`: rank, tie group, fixed-point estimate, estimated/exact share, absolute error, bound, interval, exact/selected counts.
- `conclusion`: top tie group, leader frame ID, tolerance ppm.

`GET /v1/versions/{version}/differences` returns the same exact-vs-estimated frame values in a compact audit-oriented shape. `GET /v1/versions/{version}/modes` returns only path decisions.

## Recovery Report

```json
{
  "recovered_at": "2026-11-25T17:56:56Z",
  "last_complete_commit_seq": 4,
  "current_policy_version": 2,
  "scanned_intents": 1,
  "rolled_back_intents": 1,
  "repaired_committed_intents": 0,
  "orphans_removed": {
    "reservoirs": 0,
    "hotspots": 0,
    "samples": 0,
    "batch_items": 0,
    "sample_batches": 0,
    "policies": 0,
    "channel_hits": 0
  },
  "integrity_check": "ok",
  "rollback_target": "state_meta.commit_seq",
  "complete": true
}
```
