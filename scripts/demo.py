#!/usr/bin/env python3
"""Run the BlockLab deterministic demo from the repository root."""

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from blocklab.demo import run_demo  # noqa: E402


if __name__ == "__main__":
    result = run_demo(root=ROOT, workdir="demo_work", report_path=ROOT / "artifacts" / "demo_report.json")
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    raise SystemExit(0 if result["ok"] else 1)

