#!/usr/bin/env python3
"""Generate the fixed local demo dataset."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from image_workbench.demo import DEMO_SEED, generate_demo


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="demo")
    parser.add_argument("--seed", type=int, default=DEMO_SEED)
    args = parser.parse_args()
    print(json.dumps(generate_demo(args.out, seed=args.seed), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
