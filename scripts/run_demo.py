#!/usr/bin/env python3
"""Run the full deterministic acceptance demonstration."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from audioq.demo import run_demo  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=".", help="workspace root")
    parser.add_argument("--keep", action="store_true", help="keep prior checkpoint directories")
    args = parser.parse_args()
    summary = run_demo(args.root, force=not args.keep)
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if summary["acceptance_ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
