# Design Notes

## Stage model

Each shard moves through four immutable checkpoint stages:

| Order | Stage | Main input | Main output |
| --- | --- | --- | --- |
| 1 | `feature` | image bytes, caption, feature config | raw visual/text features and per-item digests |
| 2 | `projection_quant` | feature artifact, projection matrices, 4/8-bit config | 64-D high-precision references, fixed-point codes, scales, zero points, error bounds |
| 3 | `index` | quantized artifact, ranking config | dual visual/text inverted postings and item key lists |
| 4 | `recall_validation` | index artifact, recall config | deterministic local top-k probes and digest |

The invalidation rule uses the earliest changed stage and rebuilds every downstream
stage. Feature artifacts are content-addressed, so unchanged shards are reused without
rewriting their bytes. Per-vector quantization is shard-local. Per-dimension
quantization is corpus-wide, so a feature change forces all quantized shards to rebuild,
which is required for a byte-identical full rebuild.

## Visual descriptors

Each RGB image is split into `grid_rows × grid_cols` blocks. Every block contributes:

- three channel means,
- three channel standard deviations,
- an 8-bin unsigned gradient-orientation histogram weighted by gradient magnitude.

With the default 4x4 grid and 8 bins, the visual descriptor is
`4 · 4 · (3 + 3 + 8) = 224` dimensions. Pixel values are scaled to `[0, 1]`; the
descriptor is normalized before projection. Block boundaries, means, standard
deviations, histograms and descriptor slices are retained in the feature artifact.

## Text features

Captions are processed with Unicode NFKC, lowercasing and whitespace collapse. Word
unigrams and character 2/3/4-grams are extracted. Each occurrence records:

- normalized source `offset_start` and exclusive `offset_end`,
- feature weight,
- deterministic SHA-256 hash bucket in `[0, text_hash_dim)`.

The default text feature dimension is 512. Hash buckets are summed, then the vector is
L2-normalized. The feature artifact contains all occurrence records as well as the
normalized caption and vector digest.

## Projection

Both modalities project to exactly 64 dimensions and are L2-normalized. Unless explicit
matrices are supplied, deterministic SHAKE-256 streams create the random base matrices.
A fixed bilingual lexicon and a hand-built visual concept basis add structured
correlation for colors, brightness, orientation, texture and shapes. The actual matrices
and their digest are part of every projection artifact, and replaced matrices are
validated for shape and finiteness.

## Quantization and error propagation

For bit width `b`, `qmin=0`, `qmax=2^b-1`, and `L=qmax-qmin`. Calibration selects a
no-clip affine interval:

- range crossing zero: `scale=(max-min)/L`, `zero=round(-min/scale)`,
- strictly positive range: `zero=0`, `scale=max/L`,
- strictly negative range: `zero=L`, `scale=-min/L`.

`per_dim` fits each dimension over all selected vectors; `per_vector` fits each vector
independently. A zero scale raises `E_QUANTIZATION_ZERO_SCALE` and stops the build.
Every stored vector records all per-dimension scales, zero points, codes, clipped dims,
per-dimension error bounds, L2 step bound and reference-checked L2 error.

For a unit high-precision vector `u` and raw dequantized vector `v`, define
`e = max(||u-v||, per-dim error bound L2)`. The conservative step-only normalized
perturbation is `2e/||v||`. The implementation also computes the exact
reference-checked perturbation `||u - v/||v||||`. For two routes with unit dequantized
vectors, the cosine bound is the sum of the two reference-checked perturbations. This
satisfies:

```text
|u_q · u_x - v_q · v_x| <= ||u_q-v_q|| + ||u_x-v_x||
```

The result reports both the tighter reference-checked bound and the worst-case
step-only bound. Effective digits are `floor(-log10(bound))`. A result above the
configured maximum raises `E_NUMERIC` with item, stage and bound context.

Corpus-level aggregation uses root-sum-square of explicit per-result bounds grouped by
shard prefix. The response includes each cumulative shard count and bound, so the error
estimate is auditable as corpus size grows.

## Similarity and deterministic order

For each candidate, the visual and text route cosines are computed against the
high-precision and quantized query representations. The score is:

```text
cosine_weight * (visual_weight * visual_cosine +
                 text_weight * text_cosine) +
inverted_hit_weight * hit_ratio
```

Route weights depend on query direction and are configurable. Inverted hit ratio comes
from the per-shard visual/text postings. Sort order is descending score, then ascending
`item_id`. Responses include both routes' weighted log contributions, similarity
components, quantization bounds, effective digits, source shard and invariance state.

## Checkpoint protocol

Checkpoint records are immutable JSON files named by zero-padded global sequence. Each
record contains the shard, stage, shard-local sequence, cycle, plan identity, input
digest, config version, quant-parameter digest, content-addressed artifact digest,
previous-record digest and self digest. Writes use same-directory temporary files,
`fsync`, atomic `os.replace` and directory `fsync`. Artifact objects are written before
records, so an interrupted artifact write can only leave an unreferenced object.

Publication is protected by a real `fcntl.flock` lock. A plan defines the exact
`(shard, stage, shard_seq, cycle)` order. Only the next global unit can publish. Stale,
duplicate, cross-shard out-of-order or artifact-digest-changed publishes raise
`E_STALE_CHECKPOINT`; a missing predecessor stage raises `E_INVALID_STAGE`.

Recovery follows the longest valid prefix, verifies each self digest and previous-record
link, and ignores corrupt or incomplete files. `checkpoint-audit` classifies the state:

- `advance`: plan/head/chain and artifacts agree,
- `rollback`: the head sequence is behind the valid record prefix,
- `tamper`: chain, plan unit metadata or artifact digest does not match.

`recover` rebuilds the head from the valid prefix. A publication path rejects a head
that points past the valid chain, spans a multi-record rollback or references another
plan. A one-record lag is accepted as the normal crash window between atomic record and
head replacement.

## Determinism boundaries

All content digests exclude timestamps and absolute workspace paths. Feature summaries,
quantization records, index artifacts, recall probes and search responses use canonical
JSON with exact float bit patterns in digest inputs. The sample generator is seeded and
does not access the network. Byte identity assumes the same Python/NumPy/Pillow lock and
the same local dataset/config.
