# Technical Design

## 1. Scope and non-goals

The system is a deterministic, model-free local pipeline for small stick-figure pose shards. It intentionally does not estimate pose from pixel content: heatmaps are generated around manifest ground-truth visible keypoints. The image preprocessing path exists to exercise grayscale, normalization, contrast perturbation, stage hashing, and deterministic artifacts. All metrics use manifest truth and explicit decoded candidates only.

No implementation path reads network data, model weights, GPU APIs, or timestamps. The generated images are still real RGB PNG inputs and are copied into the workspace.

## 2. Module boundaries

| Module | Responsibility |
|---|---|
| `generator.py` | Fixed-seed PNG/manifest/ground-truth generation |
| `models.py` | Manifest parsing, coordinate convention, truth/index digests |
| `preprocess.py` | RGB decode, grayscale/contrast, Gaussian heatmaps, invisible markers |
| `peaks.py` | Local maxima, deterministic ordering, candidate cap, subpixel centroid |
| `assembly.py` | Candidate graph, connected components, conflicts, geometry filtering |
| `validation.py` | Truth/prediction matching, errors, RMSE, intervals, grouped report |
| `store.py` | Immutable artifacts, atomic records, chain recovery, file lock publication |
| `engine.py` | Import, fingerprints, invalidation planning, resumable orchestration, exports |
| `cli.py` / `http_api.py` | Interaction surfaces |

## 3. Workspace layout

```text
workspace/
  inputs/manifest.json              imported canonical manifest
  inputs/images/*.png               imported shard bytes
  artifacts/<shard>/<stage>/<sha>.json
  checkpoints/<12-digit-seq>-<digest-prefix>.json
  .state/input_index.json           last planned input identity
  .state/config_snapshot.json       last planned config
  .state/invalidations.json         last build-start invalidation report
  .state/errors.json                persisted business errors
  .state/exports.json               export validity/digests
  .state/run_status.json            deterministic run status
  .state/attempts/                  committed/rejected publication attempts
  .state/publish.lock               real inter-process lock file
  .state/stop.request               graceful stop marker
  exports/poses.json
  exports/report.json
```

Artifacts are content-addressed and immutable. A rejected or completed-but-uncommitted artifact remains readable at its digest path, but no summary path can reach it unless an accepted checkpoint references that digest.

## 4. Artifact shape and digesting

Every stage payload ends with `artifact_digest = SHA256(canonical(payload without artifact_digest))`. Canonical JSON is UTF-8, sorted keys, compact separators, and `allow_nan=false`.

The preprocess artifact embeds:

- image dimensions and image SHA-256;
- contrast-adjusted grayscale as little-endian `float32`;
- each keypoint heatmap as little-endian `float32`;
- `invisible`, `zero`, `visible_instance_count` markers;
- stage/config/input fingerprints.

Arrays use base64 to keep artifacts inspectable without NumPy object serialization or archive metadata.

## 5. Stage fingerprints and invalidation

Fingerprint composition is deliberately stage-shaped:

```text
preprocess = H(image_sha, dimensions, generation, truth coords+visibility,
               keypoint names, preprocess-config-version)

decode     = H(committed preprocess artifact digest, decode-config-version)

assembly   = H(committed decode artifact digest, skeleton, keypoint names,
               image dimensions, coordinate convention, assembly-config-version)

validation = H(committed assembly artifact digest, truth, source label,
               expected intervals, coordinate convention, validation-config-version)
```

`round_digits` belongs only to the validation configuration. Decode and assembly artifacts always use a fixed internal precision of 10 decimal places, so changing presentation precision cannot create a hybrid result that differs from a fresh build.

The build planner compares these from stage 1 to 4. Once a stage is stale/missing/unavailable, all later stages are blocked. The first non-current stage is the earliest affected stage. This gives exact cases such as:

| Change | Earliest affected |
|---|---|
| image bytes, truth, visibility, seed/render parameters, radius, contrast | `preprocess` |
| candidate cap, peak threshold, subpixel radius | `decode` |
| skeleton or geometric constraints | `assembly` |
| expected confidence interval, source label, validation threshold | `validation` |

The persisted `input_index.json` also enables human-readable reasons (`image_content_changed`, `skeleton_definition_changed`, etc.) rather than only generic fingerprint mismatches.

## 6. Candidate assembly details

Each candidate is a node. For every skeleton edge `(u, v)`, candidate pairs are connected if their Euclidean pixel distance is at most:

```text
association_max_edge_fraction * hypot(width - 1, height - 1)
```

Connected components become pose candidates. A component with duplicate keypoint names means two distinct instances were merged, so the stage raises `E_POSE_INVALID` with `reason=multi_instance_conflict`. Components below the valid-keypoint minimum also raise `E_POSE_INVALID`. Instance centroid overlap is another conflict check.

Geometry is evaluated only on present keypoints:

- edge lengths must exceed `min_edge_length`;
- each angle between adjacent skeleton edges must be within configured bounds;
- configured symmetric edge pairs must have relative length deviation at most the tolerance.

Geometry failures exclude the instance and are recorded; they are not silently dropped. If no instances survive, validation still produces a truthful all-missing summary rather than inventing coordinates.

## 7. Truth matching and metrics

Predicted and truth instances are matched with a deterministic dynamic program over a bitmask of predictions. Assignment cost is mean squared normalized keypoint error plus a missing-keypoint penalty; ties prefer lower prediction index.

For each truth keypoint:

- invisible: `ignored_invisible`, no spatial error;
- visible and predicted: `matched`, compute pixel/normalized error and interval membership;
- visible without prediction: `missing_candidate`, no imputed error.

RMSE uses only matched visible keypoints. Hit rate uses all visible truth keypoints as denominator, so missing candidates count as misses while contributing no synthetic error value. Grouped distributions similarly use only explicit matched errors.

## 8. Checkpoint chain and crash recovery

Checkpoint record fields include global `sequence`, per-shard stage ordinal `shard_sequence`, shard/stage, input fingerprint, config version, artifact digest, upstream digest, predecessor record digest, and self digest.

Publication algorithm under the lock:

1. Reconstruct the longest valid chain starting at sequence 1.
2. Compare caller's expected sequence with `head.sequence + 1`; mismatch -> `E_STALE_CHECKPOINT`.
3. Verify optional expected shard/stage; mismatch -> `E_STALE_CHECKPOINT`.
4. For stage > 1, require the latest previous-stage checkpoint and exact upstream artifact digest; otherwise `E_INVALID_STAGE`.
5. Reject a repeated input fingerprint for the same shard/stage; otherwise write a new atomic record.

Scanning ignores invalid JSON, missing fields, bad self digests, duplicate sequence records, and records not linked to the current chain. New records may reuse an ignored sequence number because filenames include the record digest, while the accepted chain remains strictly continuous.

## 9. Stop semantics

The unit of stoppage is one `(shard, stage)` computation and checkpoint publication. The stop marker is checked before each unit. If a stop arrives during a unit, that unit completes and is committed; the next unit does not start. `resume` clears the marker and plans from the accepted chain. Exports are invalidated while the pipeline is incomplete, preventing old summaries from masquerading as current.

## 10. Concurrency

`FileLock` uses `fcntl.flock` on POSIX and `msvcrt.locking` on Windows. Artifact writes and checkpoint writes use `fsync` plus `os.replace`, both standard library operations. Two workers may compute the same artifact concurrently; only one can publish the expected next sequence. The loser records a rejected attempt and receives `E_STALE_CHECKPOINT`; its immutable artifact remains readable for diagnosis but is not referenced by the accepted chain.

## 11. Determinism boundary

Determinism is defined for the same Python/NumPy/Pillow versions and same input/configuration. The final exports exclude checkpoint sequence numbers, timestamps, absolute paths, process IDs, and job identifiers. Checkpoint sequence order is deterministic for the stage-major build order, so even checkpoint record bytes match between clean and resumed runs in the acceptance tests.
