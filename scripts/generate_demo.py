#!/usr/bin/env python3
"""Generate deterministic WAV shards, manifest and v1/v2 configs."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from audioq.demo import generate_demo_assets  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=".", help="workspace root")
    parser.add_argument("--base", default="demo", help="asset directory relative to root")
    args = parser.parse_args()
    root = Path(args.root).resolve()
    assets = generate_demo_assets(args.base, root=root)
    payload = {
        "ok": True,
        "manifest": assets["manifest"].relative_to(root).as_posix(),
        "config_v2": assets["config_v2"].relative_to(root).as_posix(),
        "config_v1": assets["config_v1"].relative_to(root).as_posix(),
        "samples": [path.relative_to(root).as_posix() for path in assets["samples"]],
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
