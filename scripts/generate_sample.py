#!/usr/bin/env python3
"""Generate the deterministic local sample CSV and JSON config."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from xfpipe.sample_gen import DEFAULT_ROWS, DEFAULT_SEED, generate_sample_csv, write_config

if __name__ == "__main__":
    workdir = os.path.abspath(sys.argv[1] if len(sys.argv) > 1 else ".")
    sample = generate_sample_csv(os.path.join(workdir, "data", "sample.csv"),
                                 rows=DEFAULT_ROWS, seed=DEFAULT_SEED)
    config = write_config(os.path.join(workdir, "data", "config.json"))
    print(f"sample={sample}")
    print(f"config={config}")
