"""Test-only worker that intentionally dies inside a rebuild transaction."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .service import CapacityLedger


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True)
    parser.add_argument("--intent-dir", required=True)
    parser.add_argument("--increment", required=True)
    args = parser.parse_args()
    payload = json.loads(Path(args.increment).read_text(encoding="utf-8"))
    ledger = CapacityLedger(db_path=args.db, intent_dir=args.intent_dir)
    print("crash-worker: intent will be persisted before the simulated kill", flush=True)
    ledger.commit_increment(payload, crash_point="after_partial_rebuild")
    print("crash-worker: ERROR: crash point was not reached", flush=True)
    raise SystemExit(87)


if __name__ == "__main__":
    main()

