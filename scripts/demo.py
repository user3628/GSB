#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from gridpipe.demo import run_demo


if __name__ == "__main__":
    result = run_demo(Path(__file__).resolve().parents[1])
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2))
    raise SystemExit(0 if result["passed"] else 2)
