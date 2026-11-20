#!/usr/bin/env python3
"""Convenience wrapper for the fixed-seed local sample generator."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from eval_engine.errors import EngineError
from eval_engine.generator import generate_workspace


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workspace", default=".")
    parser.add_argument("--seed", type=int, default=20261121)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    try:
        result = generate_workspace(
            Path(args.workspace), seed=args.seed, force=args.force
        )
    except EngineError as exc:
        print(json.dumps(exc.as_dict(), ensure_ascii=False, sort_keys=True))
        return 2
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
