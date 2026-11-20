# Architecture and contracts

## Pipeline

1. **Request validation**: required fields, `query_id`, locale allow-list, and fault payload shape.
2. **Normalization**: length limit, Unicode NFKC, whitespace collapse, lowercase, and tokenization. Latin
   alphanumeric runs and individual CJK characters become tokens. `start`/`end` are offsets into the normalized
   string and satisfy `normalized_query[start:end] == token.text`.
3. **Intent scoring**: a multinomial naive Bayes model is estimated from local labeled JSONL. For each class, the
   response exposes prior, conditional likelihood, feature score, class weight, weighted components, total score,
   and softmax probability. Ties resolve by the fixed class order.
4. **Slot extraction**: every dictionary type performs longest matching at every normalized character position.
   Cross-type overlaps are resolved by `slots.type_priority`, then length, start offset, type, and term. Suppressed
   candidates remain in `slot_diagnostics.suppressed` with their blocker and dictionary version.
5. **Policy**: the primary class is accepted at or above `policy.confidence_threshold`; otherwise history prior,
   channel default, and final default are attempted in that order. Every attempted/skipped level is in the trace.

## Stage fingerprints

| Stage | Fingerprint inputs |
| --- | --- |
| `normalization` | normalization configuration |
| `intent` | normalization fingerprint, classifier configuration, training JSONL bytes |
| `slots` | normalization fingerprint, slot configuration, dictionary bytes and version |
| `decision` | intent fingerprint, slot fingerprint, policy configuration |

The cache stores each stage result with the full version snapshot used to build it. Every parse compares the newly
computed fingerprints before reuse. A changed upstream fingerprint therefore invalidates downstream stages while
independent stages remain reusable. Responses include `cache.stage_status`, `cache.invalidated_stages`,
`cache.reused_stages`, and all stage fingerprints. Cached data is never returned when its fingerprint differs.

## Fault injection

`faults` is an optional object on the CLI, HTTP request, engine API, or JSONL wrapper:

| Fault | Effect |
| --- | --- |
| `invalid_locale: true` / `locale: "xx-XX"` | `E_INVALID_LOCALE`, stage `validation` |
| `force_query_length: N` | `E_QUERY_TOO_LONG`, stage `normalization` when `N` exceeds the limit |
| Actual empty/punctuation-only `raw_query` | `E_EMPTY_QUERY`, stage `normalization` |
| `dict_version: "wrong"` | `E_DICT_VERSION`, stage `slots` |
| `missing_dictionary: true` | `E_DICT_MISSING`, stage `slots` |
| `nan_confidence: true` | `E_NUMERIC`, stage `intent` |
| `overlap_slot: {...}` | Injects a candidate into normal overlap resolution; it is never silently discarded |

No code path returns a partial success object after an error. Errors always include `code`, `stage`, `message`,
`query_id`, and `details`.

## Determinism

- JSON is canonicalized with sorted keys, compact separators, UTF-8, and rejected NaN/Infinity output.
- Class and token ordering is fixed; floating-point values are rounded to 12 decimals only at stable boundaries.
- Experiment summaries include the seed and a SHA-256 run fingerprint built from seed, configuration, dataset, and
  bootstrap settings. Bootstrap intervals use `random.Random(seed)`.
- The cache key is a canonical hash of the full query and faults, while version reuse is gated separately by stage.
