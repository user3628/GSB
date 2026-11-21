#!/usr/bin/env python3
"""Run the deterministic acceptance demonstration."""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from rope_diag.demo import run_demo


if __name__ == "__main__":
    result = run_demo(ROOT, reset=True)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    raise SystemExit(0 if result["all_checks_passed"] else 1)
