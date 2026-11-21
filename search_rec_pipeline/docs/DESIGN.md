# Design Notes

## 1. Stage Model

The persisted order is `normalize`, `decision`, `encode`, `assemble`. The decision
stage is before encoding because the vector still has a fixed length across strategies;
the plan controls active fields, selected crosses, batch size, and assembly mode.

### Artifacts

| Stage | Artifacts |
|---|---|
| normalize | `normalized.jsonl`, `normalize_summary.json` |
| decision | `decision.json` |
| encode | `encoded.jsonl`, `encoding_summary.json` |
| assemble | `samples.jsonl`, `discarded.jsonl`, `stats.json`, `assembly_summary.json`, `budget_ledger.json` |

All artifacts live under `workdir/scales/<scale>/NN_<stage>/`.

## 2. Sample and Normalization Rules

Input sample:

```json
{
  "sample_id": "small-000001",
  "user": {"age": 27, "activity": 0.43, "city": "上海", "device": "iOS", "interest": "数码科技"},
  "item": {"price": 399.0, "rating": 4.3, "category": "手机", "brand": "Nova"},
  "context": {"hour": 14, "distance": 3.4, "page": "search", "query_lang": "zh"},
  "label_exposure": 1,
  "label_click": 1,
  "label_conversion": 0,
  "source": "app_search",
  "lang": "zh-CN"
}
```

- `sample_id` must be a unique non-empty string in its scale.
- `user`, `item`, and `context` must exist and be non-empty objects.
- Categorical values must be strings and remain non-empty after NFKC, whitespace
  collapsing, trimming, and lowercasing.
- Numeric missing means absent, `null`, or an empty string; it gets the configured
  explicit missing bucket. Non-finite/non-numeric present values fail.
- Boundaries must be finite and strictly increasing. Validation errors carry both
  `sample_id` and the full dotted `field` name.
- Labels are preserved as present/missing; missing or invalid labels are handled at
  assembly and never inferred.

## 3. Bucketing, Hash Slots, and Crosses

Numeric fields are laid out in sorted dotted-field order. Each field has
`len(boundaries) + 1` numeric bins plus one explicit missing slot, so the shipped six
fields use 42 slots. `bisect_right` assigns boundary-equal values to the upper bin.

The category hash namespace is:

```text
<hash_salt>|category|<dotted-field>
```

and the bucket is `blake2b(namespace + U+001F + normalized-value, size=8) % hash_dim`.
Cross values are serialized as `field=value|field=value`; the namespace includes the
ordered field pair. Crosses retain config order and stable zero-based `cross_index`.

Collision auditing groups distinct raw normalized values by hashed slot. Observable
metrics are `distinct_values`, `occupied_slots`, `collision_count`,
`collision_pairs`, and `collision_rate`, separately for each category field and each
cross definition.

## 4. Cost and Score Model

For sample count `n`, declared cardinality `C`, numeric slots `S`, active categorical
fields `K`, selected crosses `X`, encoding multiplier `e`, and assembly per-sample cost
`a`:

```text
normalize = n * 10
encode = n * e * (12 + S + 14*K + 22*X + 0.05*C)
assemble = rows * a
total = normalize + encode + assemble
rows = n * 0.85 for stratified reservoir, otherwise n
```

The candidate score uses normalized budget utilization and normalized expected loss:

```text
score = 0.80*(total/budget) + 0.20*(loss_upper_bound/max_acceptable_loss)
```

The small-scale guard marks every non-full-exact/non-exact combination ineligible.
Feasible candidates are sorted by `(score, deterministic strategy order)`.

## 5. Version Fingerprints

For each stage:

```text
config_fingerprint = SHA256(canonical(stage dependency slice))
input_fingerprint  = raw sample hash (normalize) or previous stage fingerprint
stage_fingerprint  = SHA256(canonical(stage, scale, input, config))
```

Changing a downstream stage dependency cannot invalidate upstream fingerprints.
Changing an upstream stage necessarily changes all downstream expected fingerprints
because they are chained.

Checksum-valid checkpoints are scanned into a contiguous sequence. Artifact existence,
size, individual SHA-256, and the aggregate artifact digest are checked before reuse.
A record whose dependencies or artifacts do not match the current expectation marks
its stage as the earliest invalid stage; later stages are reported as blocked.

## 6. Checkpoint Atomicity

`write_json_atomic` performs:

1. Create a temporary file in the destination directory.
2. Write all bytes, flush, and `fsync` the file.
3. Atomically `os.replace` the temporary path onto the final path.
4. `fsync` the containing directory on POSIX.

Stage artifacts use the same pattern, including streamed JSONL. A checkpoint is only
published after every stage artifact is complete. A crash before publication therefore
leaves no reusable checkpoint and the stage is rebuilt. A temp file or malformed final
record is ignored by the next scan.

## 7. Locking and Sequence Rules

- Per-scale run lock: serializes full pipeline execution for that scale.
- Per-scale checkpoint lock: protects scan + sequence comparison + atomic publish.
- Import lock: protects import manifest writes.
- Stop lock: protects stop-request create/consume.

Under the checkpoint lock, publication requires:

- `expected_seq == current_contiguous_sequence + 1`;
- `stage == STAGES[current_contiguous_sequence]`;
- the checkpoint's scale equals the lock scale;
- all prerequisite artifacts are intact.

Violations of sequence/scale/repetition use `E_STALE_CHECKPOINT`. Stage jumps or
prerequisite failures use `E_INVALID_STAGE`.

## 8. Assembly and Statistics

Normalized and encoded records are joined strictly by `sample_id`. Missing encoded
records become `encoding_failure` discards. Present labels must be numeric 0/1; missing
labels become `missing_label`, and other values become `invalid_label`.

`stratified_reservoir` performs deterministic multi-stratum selection over
`(source, lang)`, ranked by salted SHA-256 of `sample_id`, then records any
non-selected eligible sample as `assembly_strategy_sampling`. The two other strategies
preserve every eligible row.

Statistics are computed only from `samples.jsonl` in memory and include overall,
by-source, by-language, and by-source-and-language groups. Each group includes sample
count, positive count, positive rate, and 0/1 distribution for all three labels. A
`content_digest` is added after the body is built.
