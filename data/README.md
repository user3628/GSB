# Data directory

Run `python scripts/generate_samples.py --output data` to recreate the corpus.
All matrix files are uncompressed NumPy `.npy` float64 arrays generated with
fixed PCG64 seed offsets. `manifest.json` and `shape_list.json` are the machine
readable shape/record manifests; `SHAPES.md` is a human-readable summary.

The `config_store/` and `cache/` directories are runtime, versioned state. The
configuration high-water anchor is stored as `.config_store.anchor.json` next to
`config_store/` so wholesale replacement of the store directory is detectable.
