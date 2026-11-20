#!/usr/bin/env python3
"""Convenience wrapper for scripts/demo.py."""

from pathlib import Path
import runpy


if __name__ == "__main__":
    runpy.run_path(str(Path(__file__).resolve().parent / "scripts" / "demo.py"), run_name="__main__")
