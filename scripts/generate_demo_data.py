#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from topk_pipeline.demo_data import generate_demo_data


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate the deterministic local demo corpus")
    parser.add_argument("--out", default=str(ROOT / "data"))
    args = parser.parse_args()
    result = generate_demo_data(args.out)
    print(json.dumps(result, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
