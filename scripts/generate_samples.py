#!/usr/bin/env python3
"""Generate the deterministic local sample dataset."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from detpipe.generate import write_dataset


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate deterministic samples")
    parser.add_argument("--out", required=True)
    parser.add_argument("--seed", type=int, default=20260520)
    parser.add_argument("--num-images", type=int, default=6)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    dataset = write_dataset(args.out, seed=args.seed, num_images=args.num_images)
    print(f"wrote {len(dataset['images'])} images to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
