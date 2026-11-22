#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from feature_workbench.generator import generate_demo


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate deterministic demo CSV shards")
    parser.add_argument("--output", default="data", help="Output directory (default: data)")
    parser.add_argument("--seed", type=int, default=20260317)
    parser.add_argument("--small-rows", type=int, default=600)
    parser.add_argument("--medium-rows", type=int, default=6000)
    parser.add_argument("--small-shards", type=int, default=2)
    parser.add_argument("--medium-shards", type=int, default=3)
    args = parser.parse_args()
    manifest = generate_demo(
        Path(args.output),
        seed=args.seed,
        small_rows=args.small_rows,
        medium_rows=args.medium_rows,
        small_shards=args.small_shards,
        medium_shards=args.medium_shards,
    )
    print(f"generated {len(manifest['files'])} shards in {args.output}")
    for tier, info in manifest["tiers"].items():
        print(f"  {tier}: {info['rows']} rows x {info['columns']} columns, {info['shards']} shards")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

