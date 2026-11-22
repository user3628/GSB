# Configuration

`default.yaml` is the complete, deterministic application configuration. The
64x64 projection matrix is stored separately in `projection_matrix.json` so it
can be edited independently. Matrix values are rounded to 12 decimal places.

Any change to the matrix invalidates the projection stage and every downstream
stage. Changes to fusion weights, thresholds, operator confidence or language
metadata invalidate fusion and aggregation. A change to `structure_version`
invalidates all four stages.
