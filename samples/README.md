# Deterministic samples

- observations.json: human-readable, intentionally not delivery-ordered observations.
- observations.jsonl: current JSON Lines v1 input, intentionally not delivery-ordered.
- observations.bin: current compact binary v1 representation of the same observations.
- legacy.jsonl: JSON Lines v0 with second timestamps, integer interval shorthand, tilde positions and no reported version.
- legacy.bin: compact binary v0 with second timestamps and no reported version.
- compat.json: explicit compatibility matrix enabling the legacy forms.

The canonical source UUID is 3e11fa47-71ca-11e1-9e33-c80aa9429562; a second source
exercises multi-source interval merging. Times and write timestamps are fixed.
