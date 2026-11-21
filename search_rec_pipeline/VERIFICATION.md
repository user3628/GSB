# Verification Record

Execution date: 2026-11-22

Local environment used for this record:

```text
OS class:    Linux (the code selects fcntl/flock here; Windows uses msvcrt.locking)
Python:      3.14.4
NumPy:       2.5.5
PyYAML:      6.0.3
Network:     not used
Containers:  not used
```

All samples and configurations were generated locally before validation. The project
is declared for Python 3.10+ and contains a Windows file-lock branch; this run itself
was executed on the available Linux host.

## Commands and Result

### 1. Compile

```bash
python3 -m compileall -q recsys_pipeline scripts tests
```

Result: `COMPILE_OK`.

### 2. Automated tests

```bash
python3 -m unittest discover -s tests -v
```

Result:

```text
Ran 13 tests in 3.478s
OK
```

Covered checks:

- Three-scale declarations and algorithm decisions.
- Unicode NFKC/whitespace/lowercase normalization and byte-identical output.
- `E_INVALID_FEATURE` context for empty regions and non-monotonic boundaries.
- `E_HASH_DIM_ZERO`, `E_CROSS_LIMIT`, and `E_VECTOR_LENGTH_MISMATCH`.
- Category/cross collision observability and deterministic encoding.
- `E_BUDGET_EXCEEDED` and small-scale strategy guard.
- Statistics reconstructed from explicit assembled records.
- Graceful stop request, resume, and corrupt-checkpoint recovery.
- Byte-for-byte equality of resumed and fresh sample/stat artifacts.
- Decision-parameter change: normalize reuse and downstream invalidation.
- Sample-content change: normalize invalidation.
- Declaration/actual sample-count mismatch: `E_INVALID_STAGE`.
- Real file-lock race: one publisher wins, one receives `E_STALE_CHECKPOINT`.
- Rejection of cross-scale publication and illegal stage jumps.
- HTTP health/import/run/status/export/error-code smoke test.

### 3. Main three-scale run

```bash
python3 scripts/run_cli.py --project . run --scale all
```

Result: `state=completed`, with four valid checkpoints per scale.

| Scale | State | Executed | Checkpoint seq | Samples SHA-256 | Stats content digest |
|---|---|---|---:|---|---|
| small | completed | normalize, decision, encode, assemble | 4 | `bc9a4c9ebd283dceccb67623a7843e4facfdbbb8458628d4006ce0587176769a` | `b73ef6dd2205b636edfa7a687dcda6debe4a40873307f8650abcc83a05c95b77` |
| medium | completed | normalize, decision, encode, assemble | 4 | `6a1183657b9415e19c686325286d411a23ca75470490cc9e65011ab3c4995cf9` | `8097bf5fb54786c7770740c39ba74a834d369e34622ad580d2ad2b05d694d50b` |
| large | completed | normalize, decision, encode, assemble | 4 | `c8b3300f9b915fd10e89f81f18a5e316110dce44ecf191db403bd8ad2a288edf` | `620b01e8a1177e5294e5d8c535b4c80e251da5e693e907b5bef2775b7a55f54d` |

### 4. Reproducible demonstration

```bash
python3 scripts/demo.py
```

Result: exit 0, `all_checks_passed=true`. Summary:
`workdir/demo/demo_summary.json` (8,913 bytes).

The demo was executed a second time from the same committed samples/config. The
summary SHA-256 was unchanged (`ceb0f919d364cf6bab82c0ee64dc357dac1c1feb27590864f9c392d64d68e6db`),
and all six full-run sample/stats artifact hashes were unchanged as well.

The demo ran full, stop/resume, and version-change paths in isolated demo workdirs.
Each stop was after a different stage, then resumed:

| Scale | Stop after | Resume state | Reused stages | Samples byte-equal to full | Stats byte-equal to full |
|---|---|---|---|---|---|
| small | normalize | completed | normalize | yes | yes |
| medium | decision | completed | normalize, decision | yes | yes |
| large | encode | completed | normalize, decision, encode | yes | yes |

The resumed sample SHA-256 and stats SHA-256/`content_digest` values exactly matched
the corresponding full-run values listed above.

### 5. Scale selection and budget consumption

Observed in `workdir/demo/full/`:

| Scale | Encoding | Assembly | Crosses | Loss bound | Cost / budget | Remaining | Assembled / discarded |
|---|---|---|---:|---:|---:|---:|---:|
| small | full_exact | exact_materialize | 4 | 0.0 | 160,590 / 250,000 | 89,410 | 552 / 48 |
| medium | batched_approx | chunked_stream | 8 | 0.0045 | 1,608,708 / 2,600,000 | 991,292 | 5,529 / 471 |
| large | sampled_features | chunked_stream | 8 | 0.0455 | 6,994,320 / 8,000,000 | 1,005,680 | 27,650 / 2,350 |

Every selected plan remained within its declared budget. The actual modeled ledger
(`budget_ledger.json`) matched the selected-plan prediction and reported
`within_budget=true` for all scales.

Encoding collision observations:

| Scale | Vector length | Encoded | Category collisions | Cross collisions | Audit mode |
|---|---:|---:|---:|---:|---|
| small | 138 | 600 | 2 | 20 | full_scan |
| medium | 138 | 6,000 | 3 | 361 | bounded_batch_probe |
| large | 138 | 30,000 | 11 | 1,554 | full_scan |

Collision counts for the shared vector slot spaces (which also include cross-field
category collisions) were: small category 4 / cross 51, medium category 17 / cross
546, large category 43 / cross 1,776.

All discarded samples in this deterministic corpus had the explicit reason
`missing_label`; the assembly implementation also supports `invalid_label`,
`encoding_failure`, and `assembly_strategy_sampling` reasons.

### 6. Version invalidation

Boundary variant:

```text
config/generated/pipeline_boundary_change.yaml
user.age boundaries: [18,25,35,50,65] -> [18,25,34,50,65]
```

Observed:

- Earliest invalid stage: `normalize`.
- Invalidated stages: `normalize, decision, encode, assemble`.
- Rebuilt stages: all four.
- Normalized SHA-256 before: `ba8a26d814cce3099be663a5678cf4afa728712e4af18cf0bf08b7965346c351`.
- Normalized SHA-256 after: `d3080b98f7ae28a42a55c4f53999d0704fc9dfcc6e7f84d5bb5b8c75780b1143`.
- Final status: fully complete.

Decision-parameter variant:

```text
config/generated/pipeline_decision_change.yaml
decision.score_weights.cost: 0.80 -> 0.81
```

Observed:

- Earliest invalid stage: `decision`.
- `normalize` remained valid and was reused.
- Invalidated/executed: `decision, encode, assemble`.
- Normalized SHA-256 before and after: `ba8a26d814cce3099be663a5678cf4afa728712e4af18cf0bf08b7965346c351`.
- Final status: fully complete.

Config versions observed:

```text
config/pipeline.yaml                                  f7f4c785fe3078044bf08c16d181377b8958dc2d610535487be07819e3b47535
config/generated/pipeline_boundary_change.yaml       e2486f8e8251b6d8890588f33deafcbfabfc100b04a1d0b46f9f3879f46f45f6
config/generated/pipeline_decision_change.yaml       62980b5af7010351f0806bb5c9d1c164b943bef53a812d4ad52083afb11c354d
```

### 7. Real server launcher smoke check

```bash
python3 scripts/run_server.py --port 18977
curl http://127.0.0.1:18977/health
curl 'http://127.0.0.1:18977/status?scale=small'
```

Result: health HTTP 200 and status HTTP 200. The live service reported the same
config version (`f7f4c785fe3078044bf08c16d181377b8958dc2d610535487be07819e3b47535`),
`fully_complete=true` for `small`, and checkpoint sequence 4 in the shared main
workdir. The wrapper was then stopped with a normal process termination.

## Acceptance Summary

- Three local scales exist with explicit sample count, cardinality, cross cap, and
  budget declarations.
- Feature normalization/bucketing, deterministic hashing/crossing, collision counts,
  fixed vector length, assembly, and source/language statistics are implemented.
- Small/medium/large select different budget-aware strategy combinations; no declared
  budget is exceeded.
- Graceful stop/resume is byte-identical to a fresh full run, including after a
  deliberately corrupted checkpoint.
- Sample/config changes rebuild the earliest affected stage and all downstream stages
  while preserving unaffected upstream artifacts.
- Real OS file locking, contiguous sequence checks, cross-scale rejection, and stable
  business error codes are covered by tests.
- CLI and local HTTP interfaces exercise the same manager/config/workdir, and the HTTP
  smoke test passed.
