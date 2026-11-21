# GridPipe Verification

- Overall: PASS
- Seed: `20261122`
- Run id: `run-4280d047f803fe78213cba0c`
- Checkpoint sequence: stopped at `2`, completed at `16`
- Full export SHA-256: `8865f8e4ca99cf67da2143e4ffb3d494e29224af1c1a09ebec091c02c287a27c`
- Table region IoU mean: `1.0`
- Chart region IoU mean: `1.0`
- Cell row/column accuracy mean: `1.0`
- Chart series MAE mean: `0.089076`
- Accepted shards: `4`; unprocessed: `1`

## Checks

- `full_run_completed`: `true`
- `serial_run_completed`: `true`
- `stop_was_observed`: `true`
- `resume_completed`: `true`
- `parallel_equals_serial_export`: `true`
- `stop_resume_equals_full_export`: `true`
- `all_derived_files_equal`: `true`
- `invalid_region_code`: `true`
- `table_structure_code`: `true`
- `chart_axis_code`: `true`
- `chart_partial_series_preserved`: `true`
- `stale_checkpoint_code`: `true`
- `invalid_stage_code`: `true`
- `budget_within_limit`: `true`
- `unprocessed_reason_present`: `true`

## Error Codes

- `E_INVALID_REGION`: `E_INVALID_REGION`
- `E_TABLE_STRUCTURE`: `E_TABLE_STRUCTURE`
- `E_CHART_AXIS`: `E_CHART_AXIS`
- `E_STALE_CHECKPOINT`: `E_STALE_CHECKPOINT`
- `E_INVALID_STAGE`: `E_INVALID_STAGE`

All experiments are local, deterministic and use no network, container or GPU.
