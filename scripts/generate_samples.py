#!/usr/bin/env python3
"""Generate the deterministic local demonstration samples."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from motion_engine.sample_gen import generate_samples  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default=str(ROOT / "data"), help="output directory")
    parser.add_argument("--seed", type=int, default=20261121)
    args = parser.parse_args()
    result = generate_samples(args.out, seed=args.seed)
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
