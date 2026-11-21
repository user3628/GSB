#!/usr/bin/env python3
"""Convenience loopback server launcher."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import uvicorn

from rope_diag.api import create_app


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workspace", default=str(ROOT / "artifacts" / "demo_workspace"))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()
    uvicorn.run(create_app(args.workspace), host=args.host, port=args.port, log_level="info")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
