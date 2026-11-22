# Technical Design

## 1. Scope and invariants

The workbench is a local, deterministic batch engine for small RGB PNG shards. Its invariants are:

1. The same input pixels, task declaration, normalized chain and config produce the same RGB output on the same platform.
2. Accepted task estimates never exceed the global step budget; no logical worker exceeds its step quota.
3. Checkpoint sequence is per task and strictly contiguous. Final reports are independent of process scheduling.
4. A task is only executed after a current, successful `parsed` checkpoint exists.
5. Every downstream stage requires a fingerprint-matching predecessor and a valid predecessor artifact.
6. Recovery never trusts a malformed, incomplete, duplicated or non-contiguous checkpoint.

## 2. Module map

| Module | Responsibility |
|---|---|
| `models.py` | JSONL task model, field aliases and input-path safety |
| `config.py` | YAML-backed scheduler config and cost model |
| `chain.py` | ordered-chain extraction, reachability validation and normalized operations |
| `operators.py` | deterministic NumPy kernels and kernel version |
| `imaging.py` | RGB loading, canonical pixel digest, deterministic PNG encoder |
| `scheduler.py` | priority/task-id ordering and three-dimensional budget allocation |
| `versioning.py` | stage fingerprint components and changed-component analysis |
| `checkpoint.py` | atomic checkpoint journal, checksum, sequence and lock protocol |
| `runner.py` | four-stage task execution, reuse decisions and worker entrypoint |
| `workbench.py` | orchestration, invalidation history, process launch, final reports |
| `server.py` | local standard-library HTTP API |
| `cli.py` | command-line entrypoint |
| `demo.py` | fixed-seed shards, tasks, config and expected summaries |

## 3. Preparation and validation

For each task:

1. Resolve the shard path under `inputs/` and reject path escape.
2. Decode to RGB `uint8`; compute `SHA-256("RGB8\0" + width + height + pixels)`.
3. Extract the ordered chain from `operations` / `operator_chain` / `operators`.
4. Validate in order while carrying the current image dimensions:
   - operator exists in the registry;
   - operator is present in `allowed_operators` when that list is non-empty;
   - chain length does not exceed `max_chain_length`;
   - crop has positive requested size and non-empty intersection;
   - resize has positive dimensions and stays below `max_dimension`;
   - resize mode is `nearest` or `bilinear`;
   - rotate angle is finite and within configured bounds;
   - color-shift channels are finite;
   - every intermediate dimension remains positive.
5. Normalize crop to its in-bounds intersection. Store explicit normalized parameters in the chain fingerprint.

`E_CHAIN_INVALID` includes `task_id`, `position`, `reason`, and structured details. Invalid chains receive a `parsed` checkpoint with `ok=false` and are never scheduled.

## 4. Scheduler

The scheduler sorts valid and invalid tasks by `(-priority, task_id)`. For every valid chain it estimates:

```text
max(min_task_steps,
    chain_length * steps_per_operator +
    ceil(input_width * input_height / pixels_per_step))
```

The estimate deliberately uses both chain length and input size, and is charged for every task even when multiple tasks reuse one shard.

A task is accepted only when:

- `estimated_steps <= global_step_budget - accepted_steps`; and
- a logical worker slot exists with `slot.used_steps + estimated_steps <= worker_step_quota`.

The chosen slot is the fitting slot with the smallest current usage, then the lowest index. This makes concurrency and acceptance reproducible. Rejections carry machine-readable reason codes and human-readable explanations.

## 5. Execution model

Accepted tasks are grouped by logical slot. Parallel mode starts one spawn-based process per non-empty slot. Sequential mode executes the same jobs in one process, in accepted task order. Each process runs its slot's tasks one at a time, so the logical quota also acts as a physical upper bound.

Worker side effects are persisted as they complete:

- artifacts are written atomically;
- checkpoints are published under a real file lock;
- a worker result JSON is atomically written for each task.

The coordinator joins workers without treating an external timeout as a reason to claim the child is dead. Per-task time budget is checked cooperatively inside the worker; an over-budget task returns a structured error and is not force-killed. A genuinely exited worker with no result produces `E_WORKER_PROCESS_EXITED`, while results from sibling workers remain valid.

Graceful stop writes `control/STOP`; workers check it before each task. Once a task starts, all its remaining stages finish before the process exits. No final report promise is made for a stopped run.

## 6. Deterministic kernels

All arrays are `uint8` RGB and all intermediate arithmetic is `float64`.

- **crop**: integer in-bounds slice.
- **resize/nearest**: center mapping, then `floor(source + 0.5)` and clamp.
- **resize/bilinear**: center mapping, clamp to image, four-neighbor weights, then half-up quantization.
- **rotate**: inverse source-coordinate mapping around the pixel-center midpoint, nearest sample, edge clamp, same output dimensions. Exact cosine/sine values are used for 0/90/180/270 degrees.
- **color_shift**: per-channel addition, clamp `[0,255]`, then `floor(value + 0.5)`.
- **PNG**: canonical RGB IHDR, filter type 0 rows, zlib level 9, no metadata. This removes encoder metadata and filter-choice variance from exported bytes.

The task-level expected digest is the canonical pixel digest, not the PNG file hash. Checkpoint records retain both the pixel digest and the artifact file SHA-256.

## 7. Checkpoint journal

Layout:

```text
checkpoints/<task_id>/seq-00000001-parsed.json
checkpoints/<task_id>/seq-00000002-executed.json
...
```

Publication algorithm under `.locks/checkpoints/<task_id>.lock`:

1. Scan matching files.
2. Validate required fields, task id, stage/index, filename consistency and record checksum.
3. Build a journal from sequence 1 until the first missing, malformed, mismatched or duplicated sequence.
4. Quarantine all records at or after the first bad sequence.
5. Compute `next_seq = len(consistent_records) + 1`.
6. Reject a caller-supplied stale expected sequence with `E_STALE_CHECKPOINT`.
7. Validate predecessor stage record, upstream fingerprint and predecessor artifact.
8. Verify the artifact path is inside the workspace, its filename stem equals the task id, and its SHA-256 matches.
9. Build the record checksum and atomically replace the new record file.

POSIX uses `fcntl.flock(LOCK_EX)`; Windows uses `msvcrt.locking` on a one-byte lock file. The lock is OS-level and cross-process.

## 8. Four stages

| Stage | Work | Success condition |
|---|---|---|
| `parsed` | normalized chain and parsed artifact | chain valid, artifact published |
| `executed` | apply chain, encode internal PNG | output pixels generated |
| `size_checked` | compare actual and declared size | exact width/height match |
| `exported` | copy/export PNG, decode again, compare declared pixel digest | digest and decode checks pass |

If size checking fails, export is not attempted. If export verification fails, the exported artifact remains available and the failure is recorded, but the task status is `failed`.

## 9. Versioning and invalidation

Fingerprint inputs are canonical JSON hashed with SHA-256. Upstream fingerprints are embedded downstream:

- `parsed`: input digest, instruction, allowed operators, normalized chain, output size, full scheduling/reachability config, config version and parser version.
- `executed`: parsed fingerprint, input digest and kernel version.
- `size_checked`: executed fingerprint and expected size.
- `exported`: size fingerprint, expected digest, export format and PNG encoder version.

Before rewriting checkpoints, the orchestrator compares expected fingerprints to recorded ones and appends deduplicated invalidation events. The earliest mismatching stage determines the affected suffix. Unaffected upstream records retain their sequence and artifact.

Examples:

- instruction, pixels, chain, budget or reachability changes -> `parsed` and downstream;
- declared size change -> `size_checked` and downstream only;
- declared digest change -> `exported` only.

## 10. Final report reduction

Every final report is canonical pretty JSON with sorted keys and a trailing newline. Reports intentionally exclude:

- time stamps;
- process ids or worker scheduling order;
- checkpoint sequence numbers;
- reuse/build flags;
- traceback text.

They include stable stage fingerprints, decisions, expected/actual sizes and digests, artifact paths and payload hashes. Therefore parallel and sequential runs, and a full resumed run compared with a clean run, produce byte-identical `schedule.json`, `verification.json`, `versions.json` and `run_summary.json`.

The summary stores:

- `verification_sha256`: SHA-256 of the exact verification report bytes;
- `summary_sha256`: SHA-256 of canonical summary content excluding this field;
- `run_summary.json` file bytes remain directly hashable by callers.

## 11. Portability and safety

- Python 3.10+ syntax only; spawn context is explicit.
- No POSIX-only path or shell assumptions in runtime code.
- File locking has POSIX and Windows implementations.
- Input paths cannot escape the configured shard directory; task ids are restricted names.
- HTTP is local-only by default and has no authentication. Do not expose it to untrusted networks.
- No network download, container or external image generation is used.
