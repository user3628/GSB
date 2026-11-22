#!/usr/bin/env python3
"""Regenerate the checked-in fixed-seed demo corpus."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dep_engine.generator import generate_demo_data


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=".", help="workspace root (default: .)")
    parser.add_argument("--seed", type=int, default=20261123)
    args = parser.parse_args()
    manifest = generate_demo_data(Path(args.root), seed=args.seed)
    print(json.dumps(manifest, ensure_ascii=False, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
