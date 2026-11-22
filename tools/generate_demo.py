#!/usr/bin/env python3
"""Convenience wrapper for deterministic demo generation."""

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tracklab.cli import main  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(main(["--root", str(ROOT), "generate", "--force"]))
