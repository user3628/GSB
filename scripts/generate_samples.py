#!/usr/bin/env python3
"""CLI wrapper for the deterministic bundled sample generator."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from linreg_workbench.samples import generate_samples


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", default="data", help="output directory")
    args = parser.parse_args()
    declaration = generate_samples(Path(args.output))
    for dataset in declaration["datasets"]:
        print(
            f"{dataset['dataset_id']}: rows={dataset['rows']} cols={dataset['columns']} "
            f"sha256={dataset['sha256']}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
