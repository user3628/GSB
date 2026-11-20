#!/usr/bin/env python3
"""Inject a graceful stop request into a running workspace from another process.

This is the independent-process variant of the stop mechanism: it simply
creates the workspace ``STOP`` file.  A running pipeline observes the file
between shards, finishes the current indivisible shard and exits; rerunning
the pipeline resumes from the last consistent checkpoint.
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from detpipe.checkpoint import CheckpointStore


def main() -> int:
    parser = argparse.ArgumentParser(description="Inject STOP file into a workspace")
    parser.add_argument("--workspace", required=True)
    parser.add_argument("--delay", type=float, default=0.0)
    args = parser.parse_args()
    if args.delay:
        time.sleep(args.delay)
    CheckpointStore(args.workspace).request_stop()
    print(f"stop requested at {args.workspace}/STOP")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
