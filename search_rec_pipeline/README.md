# Local Search Recommendation Feature & Sample Pipeline

This project implements a fully local, deterministic, Windows-runnable feature and
training-sample pipeline for search/recommendation experiments. It does not use a
model, a remote service, a database, a GPU, Docker, or network downloads at run time.
The only runtime inputs are the committed JSONL samples, YAML/JSON configuration,
NumPy, and PyYAML.

The four persisted stages are:

```text
normalize -> decision -> encode -> assemble
```

`decision` is placed before `encode` because the encoding and assembly strategies
must be chosen before feature vectors are emitted. The four stages otherwise map
directly to the requested business phases: normalization/bucketing, algorithm
selection, online encoding, and assembly/label statistics.

## Setup

Requires Python 3.10+ on Windows, Linux, or macOS.

```bash
cd search_rec_pipeline
python -m pip install -r requirements.txt
```

No other installation is required. Run commands from the project directory, or use
the wrapper scripts, which add the project root to `sys.path` automatically.

## Quick Start

```bash
python scripts/generate_samples.py
python scripts/run_cli.py import
python scripts/run_cli.py run --scale all
python scripts/run_cli.py status --scale all
python scripts/run_cli.py decision --scale medium
python scripts/run_cli.py budget --scale large
python scripts/run_cli.py export --scale small --kind samples --out small_samples.jsonl
python scripts/run_cli.py export --scale small --kind stats --out small_stats.json
```

Run the complete reproducible demonstration, including stop/resume and version
invalidation:

```bash
python scripts/demo.py
```

Run all tests:

```bash
python -m unittest discover -s tests -v
```

## Project Layout

```text
search_rec_pipeline/
  config/
    pipeline.yaml                  # main config and budget model
    generated/                     # demo-generated config variants (after demo)
  samples/
    small.jsonl                    # 600 deterministic samples
    medium.jsonl                   # 6,000 deterministic samples
    large.jsonl                    # 30,000 deterministic samples
    *.declaration.json             # explicit count/cardinality/cross/budget
  workdir/
    import_manifest.json           # created by `import`/first run
    checkpoints/<scale>/           # atomic stage records with monotonic seq
    locks/                         # real OS file locks
    scales/<scale>/<stage>/        # stage artifacts
    demo/                          # demo outputs and demo_summary.json
  scripts/
    generate_samples.py            # deterministic sample generator
    run_cli.py                     # command-line entry point
    run_server.py                  # local HTTP entry point
    demo.py                        # full/stop-resume/version demo
  recsys_pipeline/                 # implementation package
  tests/test_pipeline.py           # 13 automated checks
  docs/DESIGN.md                   # detailed algorithms and formats
  VERIFICATION.md                  # actual local verification results
```

## Scale Declarations

The declarations are committed next to the samples and are the source of truth for
the decision stage.

| Scale | Samples | Categorical cardinality | Numeric bucket slots | Total cardinality observation | Cross cap | Budget units |
|---|---:|---:|---:|---:|---:|---:|
| small | 600 | 31 | 42 | 73 | 4 | 250,000 |
| medium | 6,000 | 59 | 42 | 101 | 8 | 2,600,000 |
| large | 30,000 | 94 | 42 | 136 | 12 | 8,000,000 |

Each sample includes `user`, `item`, `context`, `source`, `lang`, exposure/click/
conversion labels, Chinese and English values, periodic missing numeric fields, and
periodic missing labels. Missing numeric values are represented by an explicit
missing bucket; samples with missing labels are inventoried in `discarded.jsonl` and
are not used for statistics.

## Determinism

The following rules make repeated runs byte-identical:

- Sample generation uses SHA-256-derived decisions with a fixed seed; it does not
  depend on `hash()` or process randomization.
- JSON artifacts use canonical UTF-8 JSON with sorted keys. JSONL records are emitted
  in source order.
- Category hashing uses `blake2b` with a fixed namespace, salt, digest size, and
  modulo dimension.
- No timestamps or host-specific values are written into samples, stats, decisions,
  checkpoints, or exported summaries.
- All stage artifacts and checkpoint records are written through a same-directory
  temporary file, fsynced, then atomically replaced.
- `stats.json` includes a `content_digest` computed from the statistics body.

## Feature and Encoding Contract

Normalization applies Unicode NFKC, whitespace collapsing, trimming, and lowercase to
categorical values. Empty values and empty `user`/`item`/`context` regions raise
`E_INVALID_FEATURE` with `sample_id` and `field`; samples are never silently dropped.
Numeric bucket boundaries must be strictly increasing. A missing numeric value gets
`bucket_index = -1` and an explicit configured missing label.

The fixed vector is:

```text
[numeric one-hot buckets including missing] + [category hash buckets] + [cross hash buckets]
```

With the shipped config the vector length is `42 + 64 + 32 = 138`. Cross definitions
are consumed in config order, get stable `cross_index` values, and may not exceed the
declaration's `cross_limit` or `features.crossing_limit_hard`. Category and cross
collision counts, occupied slots, collision pairs, and rates are written to
`encoding_summary.json`, both per category field/cross definition and for the shared
category/cross vector slot space.

## Algorithm Selection and Budget

The decision stage evaluates the Cartesian product of:

- Encoding: `full_exact`, `batched_approx`, `sampled_features`
- Assembly: `exact_materialize`, `chunked_stream`, `stratified_reservoir`

For each candidate it records predicted cost, expected loss upper bound, score,
eligibility, and an elimination reason. Cost depends on sample count, declared feature
cardinality, active categorical fields, numeric bucket slots, selected cross count,
the strategy multiplier, assembly mode, and normalization work. Score is:

```text
0.80 * (estimated_cost / budget) + 0.20 * (loss_upper_bound / max_acceptable_loss)
```

A candidate is rejected if it exceeds the budget or loss bound. A small-scale guard
allows only full exact encoding plus exact materialization at `small`, preventing
batch/sampling strategies from being selected merely because they are cheap. The
selected combination is always checked against the declared budget both at decision
time and in `budget_ledger.json` during assembly.

The current selection is:

| Scale | Encoding | Assembly | Selected crosses | Observed reason |
|---|---|---|---:|---|
| small | full exact | exact materialize | 4 | small-scale guard; highest accuracy |
| medium | batched approximate | chunked stream | 8 | lower cost with 0.0045 loss upper bound |
| large | feature sampling | chunked stream | 8 | all full/batched candidates exceed budget |

## Checkpoints, Stop, Recovery, and Concurrency

Each completed stage writes an atomic record at
`workdir/checkpoints/<scale>/000N_<stage>.json` containing stage, scale, monotonic
sequence, raw-input fingerprint, config version, stage config fingerprint, stage
fingerprint, artifact aggregate digest, and per-artifact SHA-256/size. Records have
their own checksum. Readers only reuse a contiguous stage sequence from sequence 1;
corrupt, partial, duplicated, non-contiguous, or cross-scale records are ignored.

Publication uses a real OS file lock (`fcntl.flock` on POSIX, `msvcrt.locking` on
Windows). The next record must have exactly `current_sequence + 1`, the expected stage,
and valid prerequisite artifacts. A dedicated per-scale run lock also prevents two
workers from executing the same scale simultaneously. Stale, duplicate, behind,
non-contiguous, or cross-scale publication returns `E_STALE_CHECKPOINT`; an illegal
stage jump or missing prerequisite returns `E_INVALID_STAGE`.

A stop request is a JSON file (`workdir/STOP_REQUEST`). A worker consumes it only after
finishing the current indivisible stage/scale unit. Planned stops can also be requested
with `run --stop-after-stage <stage>`. Resume reruns the earliest invalid stage and all
downstream stages; upstream valid artifacts are reused.

## Version Fingerprints and Invalidation

Every stage has an `input_fingerprint`, a `config_fingerprint`, and a chained
`stage_fingerprint`. The earliest mismatch determines invalidation:

| Change | Earliest affected stage |
|---|---|
| sample JSONL bytes | normalize |
| normalization config, numeric boundaries, categorical fields, label mapping | normalize |
| declared budget, feature cardinality, sample count, cross cap, cross candidates/hard cap, scoring and strategy parameters | decision |
| hash dimensions, vector length, hash salt | encode |
| statistics grouping or assembly-only configuration | assemble |

The `status` and `fingerprints` CLI/HTTP queries expose expected and stored
fingerprints, invalidation reasons, and the earliest invalid stage. Rebuilding removes
the affected and downstream artifacts/checkpoints while preserving valid upstream
artifacts.

## CLI

The CLI prints UTF-8 JSON. Business errors exit with status 2; unexpected local
failures exit with status 3.

```bash
python scripts/run_cli.py import
python scripts/run_cli.py run --scale small
python scripts/run_cli.py run --scale small --stop-after-stage normalize
python scripts/run_cli.py resume --scale small
python scripts/run_cli.py stop --scale small
python scripts/run_cli.py status --scale small
python scripts/run_cli.py fingerprints --scale small
python scripts/run_cli.py decision --scale small
python scripts/run_cli.py encoding --scale small
python scripts/run_cli.py budget --scale small
python scripts/run_cli.py export --scale small --kind all
```

`export` accepts `samples`, `discarded`, `stats`, `normalized`, `encoded`, `decision`,
`budget`, `assembly_summary`, `encoding_summary`, or `all`.

## HTTP Service

Start the local service:

```bash
python scripts/run_server.py --host 127.0.0.1 --port 8765
```

The service uses the same `PipelineManager`, config, and workdir as the CLI.

| Method | Endpoint | Purpose |
|---|---|---|
| GET | `/health` | health/config/workdir |
| POST | `/import` | validate and import samples/declarations |
| POST | `/run` | body `{"scale":"all","stop_after_stage":null}` |
| POST | `/resume` | resume incomplete scales |
| POST | `/stop` | body `{"scope":"small"}` or `{"scope":"all"}` |
| GET | `/status?scale=small` | stage states, invalidation, fingerprints |
| GET | `/fingerprints?scale=small` | compact version fingerprint view |
| GET | `/decision?scale=small` | candidate scores and eliminated reasons |
| GET | `/encoding?scale=small` | collision counts and encoding plan |
| GET | `/budget?scale=small` | predicted and actual budget ledger |
| GET | `/export?scale=small&kind=samples` | download JSONL |
| GET | `/export?scale=small&kind=stats` | download JSON summary |

Example:

```bash
curl -X POST http://127.0.0.1:8765/run \
  -H 'Content-Type: application/json' \
  -d '{"scale":"small"}'
curl http://127.0.0.1:8765/status?scale=small
curl -o samples.jsonl 'http://127.0.0.1:8765/export?scale=small&kind=samples'
```

## Stable Error Codes

| Code | Meaning |
|---|---|
| `E_INVALID_FEATURE` | empty region/category, invalid numeric value, or non-monotonic boundaries |
| `E_HASH_DIM_ZERO` | category or cross hash dimension is zero |
| `E_CROSS_LIMIT` | requested crosses exceed the declaration or hard cap |
| `E_VECTOR_LENGTH_MISMATCH` | computed vector length differs from config declaration |
| `E_BUDGET_EXCEEDED` | no feasible or selected plan fits the declared budget |
| `E_INVALID_STAGE` | illegal jump, missing prerequisite/artifact, or declaration/actual count mismatch |
| `E_STALE_CHECKPOINT` | stale, duplicate, behind, non-contiguous, or cross-scale publish |
| `E_NOT_FOUND` | requested scale/stage/artifact is unavailable |
| `E_INVALID_REQUEST` | malformed CLI/HTTP request |

## Actual Verification

See [`VERIFICATION.md`](VERIFICATION.md) for the commands run locally, the 13 passing
tests, the three-scale decision summary, stop/resume byte-equality results, version
invalidation results, and final artifact digests.
