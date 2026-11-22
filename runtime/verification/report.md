# Deterministic Storyboard Pipeline Verification

- Pipeline version: `shotcheck-1.0.0`
- Seed: `20261123`
- Overall: **PASS**

## Tier Results

### small (PASS)

- Shots: `14`
- Verdicts: `{'pass': 3, 'rejected': 9, 'review': 2}`
- Errors: `{'E_DURATION_PARSE': 3, 'E_EMPTY_PROMPT': 2, 'E_FORBIDDEN_SUBJECT': 1, 'E_SEMANTIC_CONFLICT': 2}`
- Unit types: `{'constraint': 3, 'duration': 12, 'scene_description': 14, 'subject_description': 13}`
- Reference emitted unit types: `{'constraint': 3, 'duration': 12, 'scene_description': 14, 'subject_description': 13}`
- Reference unit types: `{'constraint': 3, 'duration': 12, 'scene_description': 16, 'subject_description': 13}`
- Duration sources: `{'arabic_unit': 6, 'cn_quantifier': 2, 'frames': 3}`
- N/A components: `{'scene_coverage': 1}`
- Strategy: `exhaustive` (small_scale_prefers_exact_exhaustive)
- Result digest: `2e4b67a2811e3e916d3af9bea55687f450d4a857290b07bfefd8e7f992d8d845`

- `units_exact`: PASS
- `stage1_emitted_units_exact`: PASS
- `duration_values_exact`: PASS
- `coverage_exact`: PASS
- `verdicts_exact`: PASS
- `error_codes_exact`: PASS
- `error_locations_present`: PASS
- `error_distribution_exact`: PASS
- `na_distribution_exact`: PASS
- `unit_distribution_consistent`: PASS
- `strategy_parity`: PASS
- `small_scale_not_truncated`: PASS

### large (PASS)

- Shots: `51`
- Verdicts: `{'pass': 39, 'rejected': 9, 'review': 3}`
- Errors: `{'E_DURATION_PARSE': 3, 'E_EMPTY_PROMPT': 2, 'E_FORBIDDEN_SUBJECT': 1, 'E_SEMANTIC_CONFLICT': 2}`
- Unit types: `{'constraint': 20, 'duration': 49, 'scene_description': 87, 'subject_description': 50}`
- Reference emitted unit types: `{'constraint': 20, 'duration': 49, 'scene_description': 87, 'subject_description': 50}`
- Reference unit types: `{'constraint': 20, 'duration': 49, 'scene_description': 89, 'subject_description': 50}`
- Duration sources: `{'arabic_unit': 25, 'cn_quantifier': 11, 'frames': 12}`
- N/A components: `{'scene_coverage': 1}`
- Strategy: `chunk_incremental` (balanced_cost_and_exactness)
- Result digest: `5cc8c95309d8e9ca832263158aa7e8a1e95eac6637a3632523a67af96f36fd62`

- `units_exact`: PASS
- `stage1_emitted_units_exact`: PASS
- `duration_values_exact`: PASS
- `coverage_exact`: PASS
- `verdicts_exact`: PASS
- `error_codes_exact`: PASS
- `error_locations_present`: PASS
- `error_distribution_exact`: PASS
- `na_distribution_exact`: PASS
- `unit_distribution_consistent`: PASS
- `strategy_parity`: PASS
- `small_scale_not_truncated`: PASS

## Checkpoint Concurrency

- `two_writers_one_winner`: PASS
- `duplicate_publish_rejected`: PASS
- `invalid_stage_jump_rejected`: PASS
- `upstream_lineage_mismatch_rejected`: PASS

## Incremental Recompute

- `earliest_stage_is_duration`: PASS
- `normalize_stage_reused`: PASS
- `unaffected_shots_byte_identical`: PASS
- `incremental_equals_fresh_full`: PASS
- `sequence_advanced_only_downstream`: PASS
- Changed shot: `large-001`
- Duration: `3500` -> `3200` ms

## Graceful Resume

- `stopped_after_current_shot`: PASS
- `no_partial_stage_publish`: PASS
- `resume_reuses_upstream`: PASS
- `resume_equals_fresh_full`: PASS

## Version Fingerprint Matrix

- `prompt_change`: PASS (changed: `['normalize', 'strategy']`, earliest: `normalize`)
- `duration_phrase_change`: PASS (changed: `['duration']`, earliest: `duration`)
- `subject_table_change`: PASS (changed: `['normalize', 'coverage', 'strategy']`, earliest: `normalize`)
- `negative_terms_change`: PASS (changed: `['normalize', 'coverage', 'strategy']`, earliest: `normalize`)
- `weights_change`: PASS (changed: `['coverage']`, earliest: `coverage`)
- `budget_change`: PASS (changed: `['strategy']`, earliest: `strategy`)
- `split_config_change`: PASS (changed: `['normalize']`, earliest: `normalize`)
