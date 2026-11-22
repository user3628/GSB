# Manifest Formats

## Internal model

`IndexProject` is the common representation used by the kernel and checkpoint pipeline. A shard contains:

- `shard_id`
- half-open global row interval `[row_start, row_end)`
- `row_offset`, the global row represented by weight array row zero
- per-shard `embedding_dim`
- `segment_digest`
- `weight_file`
- zero or more pad ranges and an explicit floating `pad_marker`
- source notes for compatibility conversions

Index batches are linked to the table through `BatchLink`, which records the batch id, table id, file, batch number, and optional default gather axis.

## Version 2

```json
{
  "schema_version": 2,
  "table_id": "demo-table",
  "embedding_dim": 8,
  "quantization_bits": 12,
  "shards": [
    {
      "shard_id": "shard-000",
      "row_range": [0, 32],
      "row_offset": 0,
      "embedding_dim": 8,
      "segment_digest": "…",
      "weight_file": "weights/shard-000.npz",
      "pad_ranges": [[28, 32]],
      "pad_marker": 1.0
    }
  ],
  "index_batches": [
    {
      "batch_id": "batch-ok",
      "table_id": "demo-table",
      "file": "index_batches/batch-ok.npz",
      "batch_number": 0,
      "gather_axis": 1
    }
  ]
}
```

Version 2 is lossless for the internal model. Encoding and decoding preserves row intervals, offsets, dimensions, pad ranges, and the marker.

## Version 1

Version 1 describes row intervals as integer percentages:

```json
{
  "schema_version": 1,
  "table_id": "demo-table",
  "embedding_dim": 16,
  "shards": [
    {
      "shard_id": "shard-000",
      "row_start_pct": 0,
      "row_end_pct": 33,
      "embedding_dim": 16,
      "segment_digest": "…",
      "weight_file": "weights/shard-000.npz"
    }
  ]
}
```

It has no `row_offset` and no pad field, so pad rows are counted as normal rows. Import performs these explicit actions:

1. Resolve percentage boundaries with `percentage_rounding = floor|ceil|round` and a required or inferred `total_rows`.
2. Derive offsets with `offset_strategy = row_start|contiguous`, unless a compatibility supplement supplies exact offsets.
3. Replace manifest-declared dimensions with dimensions declared by the actual weight shard metadata.
4. Recover pad ranges from weight metadata or `_compat`, and append provenance notes.
5. Validate the upgraded project; a conversion that moves a pad range outside its resolved interval fails with `E_PAD_OVERLAP`.

## Lossless v1 downgrade extension

When encoding v1, the main schema remains faithful to legacy semantics: no row offset, no pad field, percentages in integer units, and pad rows included in normal row ratios. A `_compat` object supplements exact information:

```json
{
  "_compat": {
    "format": "embedding-gather-v1-lossless-downgrade",
    "total_rows": 96,
    "exact_row_ranges": [[0, 32], [32, 64], [64, 96]],
    "row_offsets": [0, 32, 64],
    "embedding_dims": {"shard-000": 8},
    "pad_ranges": {"shard-000": [[28, 32]]},
    "quantization_bits": 12
  }
}
```

Re-import uses this extension when `prefer_compat_extension=true`, and records its use in provenance. This satisfies lossless downgrade without pretending legacy v1 had those fields.
