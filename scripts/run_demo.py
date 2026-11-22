#!/usr/bin/env python3
"""Run the deterministic acceptance demo from the project root."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sonic_sep.demo import run_demo
from sonic_sep.workspace import Workspace


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=".")
    parser.add_argument("--output", default=None)
    parser.add_argument("--no-reset", action="store_true")
    args = parser.parse_args()
    report = run_demo(
        Workspace(Path(args.root)),
        output_path=args.output,
        reset=not args.no_reset,
    )
    print(json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2))
    return 0 if report.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())

