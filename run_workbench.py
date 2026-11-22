#!/usr/bin/env python3
"""Convenience entry point for environments that do not install the package."""

from __future__ import annotations

from segval.cli import main


if __name__ == "__main__":
    raise SystemExit(main())

