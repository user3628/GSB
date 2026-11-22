#!/usr/bin/env python3
"""Convenience entry point: python pipeline.py <command> ..."""

from llm_pipeline.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
