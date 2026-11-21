#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from sumengine.samples import write_samples


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workspace", default=".")
    parser.add_argument("--seed", type=int, default=20261122)
    args = parser.parse_args()
    sample_dir = Path(args.workspace).resolve() / "samples"
    result = write_samples(sample_dir, seed=args.seed)
    print(result)


if __name__ == "__main__":
    main()
