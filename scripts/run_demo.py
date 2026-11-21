#!/usr/bin/env python3
"""Thin script wrapper for the reproducible demonstration."""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from ctr_cvr_workbench.demo import run_demo


if __name__ == "__main__":
    raise SystemExit(0 if run_demo("artifacts/demo").get("ok") else 2)
