#!/usr/bin/env python3
"""Re-run the full demonstration in a fresh workspace and compare fixed summaries."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from linreg_workbench.demo import verify_demo


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace", default=None)
    parser.add_argument("--data-dir", default="data")
    args = parser.parse_args()
    report = verify_demo(
        workspace=args.workspace,
        data_dir=args.data_dir,
    )
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if report.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
