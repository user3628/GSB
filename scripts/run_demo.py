#!/usr/bin/env python3
"""Run the deterministic end-to-end demonstration."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from linreg_workbench.demo import run_demo


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace", default="runtime/demo")
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--write-expected", action="store_true")
    args = parser.parse_args()
    report = run_demo(
        workspace=args.workspace,
        data_dir=args.data_dir,
        write_expected=args.write_expected,
    )
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if report.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
