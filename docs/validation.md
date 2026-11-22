# Validation Record

## Environment

- Date: 2026-11-23
- OS: local Linux workspace; code paths also implement Windows `msvcrt` locking with `pathlib`
- Python: 3.14.4
- NumPy: 2.2.1
- pytest: 8.3.4
- No network, container, or GPU required

## Commands run

```bash
python scripts/generate_samples.py --out samples
pytest -q
python -m embedding_gather codec-check \
  --manifest samples/main/manifest_v1.json --root samples/main
python -m embedding_gather errors \
  --manifest samples/main/manifest_v2.json --root samples/main \
  --workspace /tmp/eg_errors --reset
python -m embedding_gather demo --state /tmp/eg_demo_state
```

## Results

- Sample generation is byte-deterministic across two independent output directories.
- v1 import corrects dimension `16 → 8`, converts percentages to rows `[0,32)/[32,64)/[64,96)`, derives offsets `0/32/64`, and recovers pad ranges `[28,32)` and `[92,96)`.
- v1 → v2 → internal and v1 → v1 compatibility-extension roundtrips have equal semantic signatures.
- `pytest -q`: `38 passed`.
- Normal batches produce fixed-point outputs with observed error no greater than the recorded bound.
- Stop after two batches followed by resume produced the same `bundle.npz` bytes and digest as a fresh full run:

```text
08a7261810b45d41b95ec696c293c597c93bc0511028728a7c01b2f8ecbe1d82
```

- Re-running with `PYTHONHASHSEED=1` and `PYTHONHASHSEED=987654` produced the same bundle digest above.

- Error injection returned:

```text
batch-empty       E_EMPTY_BATCH
batch-oob         E_INDEX_OOB
batch-noninteger  E_INDEX_NONINTEGER
batch-zero-weight E_NUMERIC
```

- A changed weight shard invalidated only `gather` and downstream stages for affected batches; `validate` remained reusable. A batch that did not hit the changed shard stayed fully valid.
- A quantization-width change invalidated only `quantize` and `verify`.
- A pad-range change invalidated starting from `validate`.
- Checkpoint tests verified real inter-process lock exclusion, atomic-write crash cleanup, duplicate publish rejection, missing-stage rejection, tamper detection, rollback detection, and cross-partition rejection.
