# Design Notes

## Four-stage pipeline

Each linked index batch is indivisible at the stop boundary and runs four deterministic stages:

1. `validate`: dtype, emptiness, index resolution, negative policy, pad/row-offset lookup.
2. `gather`: fixed-point scale-domain weighted accumulation, hit trace, high-precision reference.
3. `quantize`: final `int64` output at `output_scale`, per-element error bound, observed error.
4. `verify`: checks observed error against bound and writes the final artifact with a verification digest.

Artifacts are deterministic NumPy `.npz` files with sorted entries and fixed ZIP metadata. Checkpoint records are canonical JSON.

## Stage fingerprints

The earliest invalidated stage is determined from component maps:

- `validate`: manifest structure version, manifest semantic fingerprint, IDs digest/dtype/shape, batch metadata, negative-index policy.
- `gather`: validate fingerprint, actual content digests of shards hit by the batch, aggregation weights, gather axis, output scale.
- `quantize`: gather fingerprint, declared quantization width, rounding mode.
- `verify`: quantize fingerprint, verification tolerance and scheme.

Consequences:

- Weight bytes or scale changes start at `gather`; unaffected batches and all `validate` records remain reusable.
- IDs or batch-link changes start at `validate`.
- Pad marker/ranges or manifest structure version changes start at `validate`.
- Quantization width changes start at `quantize`, reusing `gather`.
- Verification policy changes start at `verify`, reusing `quantize`.

`inspect()` reports expected and recorded component maps/fingerprints, earliest invalid stage, reused prefix, and prior invalidation events.

## Atomicity and ordering

`atomic_write_bytes` writes to a same-directory temporary file, flushes and `fsync`s it, then uses `os.replace`, followed by a directory `fsync` on POSIX. A crash before replace leaves no target record; a crash after replace leaves a complete record.

Stage artifacts are written before their stage records. A record always identifies the product digest. If a crash occurs between the two writes, recovery recomputes or rewrites identical deterministic bytes and then publishes the missing record.

## File locking and sequence

Each batch has a real OS file lock (`fcntl.flock` on POSIX, `msvcrt.locking` on Windows). Publishing re-reads and verifies the ledger inside that lock. New records must have sequence `head + 1`, valid prerequisite stages, matching batch/partition id, non-regressing manifest version, and a valid digest chain.

The separate anchor under `checkpoints/.anchors/` stores the high watermark, head digest, and manifest version. It detects a replaced ledger directory:

- fewer records or lower sequence than the anchor: `rollback`
- same sequence but different head digest: `tamper`
- lower manifest version: `rollback`
- foreign batch/partition record: `cross_shard`
- repeated publish: `duplicate`
- missing prerequisite artifact: `E_INVALID_STAGE`

After a crash where a record was written but the anchor was not yet updated, a later locked read repairs the anchor only when the digest chain is valid.

## Deterministic output

The final `bundle.npz` is assembled from verified batch artifacts in sorted batch-id order. Fixed ZIP timestamps, sorted keys, canonical JSON metadata, integer output tensors, and ordered hit details make full-run and resumed-run bundles byte-identical.
