# Deterministic samples

Run `python samples/generate_samples.py` from the project root. The generator uses
seed `20261123` and performs no network I/O.

- `stats_v2.bin`: little-endian compact binary snapshot, format v2.
- `stats_v2.jsonl`: manifest + JSON Lines snapshot, format v2, semantically equal to
  `stats_v2.bin` (same snapshot version and digest).
- `stats_v1_legacy.bin`: big-endian legacy binary v1 with float32 NDV ratios and no
  correlation coefficients.
- `stats_v1_legacy.jsonl`: legacy JSON Lines v1 with percent integer NDV ratios and
  omitted correlation coefficients.
- `stats_v3_unsupported.jsonl`: format v3 fixture that must be rejected with
  `E_FORMAT_UNSUPPORTED` and recorded as a rejected import report.
- `stats_replacement.bin` / `stats_replacement.jsonl`: next monotonic snapshot
  version (1002), used to verify whole-snapshot replacement and version binding.
- `predicates.json`: equality, range, range-join, and self-join predicates over six
  relations.
- `manifest.json`: fixed seed, versions, semantic digests, and expected decisions.

`stats_v1_legacy.*` versions are intentionally older. Import them into an empty
store first if you want to inspect degraded acceptance; a store that already has v1001
will correctly return `E_VERSION_RETIRED` due to the monotonic-version contract.

