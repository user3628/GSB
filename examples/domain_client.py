#!/usr/bin/env python3
"""Minimal in-process integration example for a sync client."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

from upload_index import UploadIndexService


def sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def main() -> int:
    db_path = Path("data/example.db")
    for suffix in ("", "-wal", "-shm"):
        candidate = Path(str(db_path) + suffix)
        if candidate.exists():
            candidate.unlink()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    service = UploadIndexService(db_path)
    service.initialize()
    try:
        session = service.create_session(
            {
                "session_id": "example-session",
                "object_key": "sync/example.bin",
                "conflict_policy": "reject",
            }
        )
        parts = [
            {"seq": 2, "size": 20, "sha256": sha256("beta"), "arrival_ms": 2},
            {"seq": 1, "size": 10, "sha256": sha256("alpha"), "arrival_ms": 1},
            {"seq": 2, "size": 20, "sha256": sha256("beta"), "arrival_ms": 22},
        ]
        batch = service.submit_batch(
            session["session_id"], {"batch_id": "example-batch-1", "parts": parts}
        )
        view = service.get_session_view(session["session_id"], batch["current_version"])
        merged = service.merge_session(
            session["session_id"],
            {
                "generation": session["generation"],
                "total_size": 30,
                "parts": [parts[1], parts[0]],
            },
        )
        print(
            json.dumps(
                {
                    "session": session,
                    "commit_versions": batch["versions"],
                    "ranges": view["index"]["contiguous_ranges"],
                    "dedup": view["index"]["dedup_refs"],
                    "object": merged,
                    "final_version": service.current_version,
                },
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    finally:
        service.shutdown()
        service.close()


if __name__ == "__main__":
    raise SystemExit(main())

