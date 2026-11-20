#!/usr/bin/env python3
"""Convenience wrapper for deterministic local NPZ generation."""

from __future__ import annotations

import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from gsb.data import generate_datasets  # noqa: E402


if __name__ == "__main__":
    report = generate_datasets(PROJECT_ROOT / "data", force="--force" in sys.argv)
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
