"""Deliberately terminate a writer between intent persistence and commit.

This module is only used by the local recovery drill and tests. It writes real
SQLite rows, commits the intent record, enters the rebuild transaction, and then
calls os._exit before the transaction can commit.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from app.models import FullSnapshotInput, IncrementalSnapshotInput
from app.store import Store


ROOT = Path(__file__).resolve().parent.parent


def main() -> None:
    parser = argparse.ArgumentParser(description="Forced-termination recovery worker")
    parser.add_argument("--db", required=True)
    parser.add_argument("--fault", default="mid_rebuild", choices=["after_intent", "mid_rebuild"])
    args = parser.parse_args()

    store = Store(args.db)
    store.initialize()
    if store.current_version() == 0:
        payload = json.loads((ROOT / "samples" / "snapshot_v1.json").read_text(encoding="utf-8"))
        store.submit_full(FullSnapshotInput.model_validate(payload))
    if store.current_version() == 1:
        payload = json.loads((ROOT / "samples" / "snapshot_v2.json").read_text(encoding="utf-8"))
        store.submit_full(FullSnapshotInput.model_validate(payload))
    if store.current_version() != 2:
        raise SystemExit(
            f"recovery worker needs a fresh database at snapshot v2, got v{store.current_version()}"
        )

    store.fault_point = args.fault
    payload = json.loads(
        (ROOT / "samples" / "increment_image.json").read_text(encoding="utf-8")
    )
    store.submit_incremental(IncrementalSnapshotInput.model_validate(payload))
    # os._exit in Store._maybe_fault prevents this line during the drill.
    raise SystemExit("fault point was not reached")


if __name__ == "__main__":
    os.environ.setdefault("PYTHONUNBUFFERED", "1")
    main()
