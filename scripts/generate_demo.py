#!/usr/bin/env python3
"""Generate the fixed-seed demo shards and config."""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from llm_pipeline.generator import generate_demo_assets  # noqa: E402


if __name__ == "__main__":
    output = Path(sys.argv[1]) if len(sys.argv) > 1 else ROOT / "demo"
    print(json.dumps(generate_demo_assets(output), ensure_ascii=False, indent=2, sort_keys=True))
