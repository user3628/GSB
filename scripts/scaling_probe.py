#!/usr/bin/env python3
"""Deterministic NumPy-based larger-batch probe for the domain service."""

from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path

import numpy as np

from upload_index.service import UploadIndexService


ROOT = Path(__file__).resolve().parents[1]
SPEC_PATH = ROOT / "samples" / "large_batch_spec.json"
DATA_DIR = ROOT / "data"
ARTIFACTS = ROOT / "artifacts"


def sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def main() -> int:
    spec = json.loads(SPEC_PATH.read_text(encoding="utf-8"))
    count = int(spec["part_count"])
    seed = int(spec["seed"])
    size = int(spec["part_size"])
    duplicate_every = int(spec["duplicate_every"])
    rng = np.random.default_rng(seed)
    order = (rng.permutation(count) + 1).tolist()

    canonical = {
        seq: {
            "seq": seq,
            "size": size,
            "sha256": sha256(f"large-part-{seq}"),
            "arrival_ms": 10_000 + seq,
        }
        for seq in range(1, count + 1)
    }
    submitted: list[dict[str, object]] = []
    for seq in order:
        submitted.append(canonical[seq])
        if seq % duplicate_every == 0:
            submitted.append(dict(canonical[seq]))

    sizes = np.asarray([part["size"] for part in submitted], dtype=np.int64)
    checksum = int(np.sum(sizes, dtype=np.int64))
    if checksum != size * len(submitted):
        raise AssertionError("NumPy int64 aggregate overflow or unexpected size sum")

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    db_path = DATA_DIR / "scaling.db"
    for suffix in ("", "-wal", "-shm"):
        candidate = Path(str(db_path) + suffix)
        if candidate.exists():
            candidate.unlink()

    service = UploadIndexService(db_path)
    service.initialize()
    try:
        session = service.create_session(
            {
                "session_id": "sess-large-001",
                "object_key": spec["object_key"],
                "conflict_policy": "reject",
            }
        )
        started = time.perf_counter()
        submitted_result = service.submit_batch(
            session["session_id"], {"batch_id": "large-0001", "parts": submitted}
        )
        commit_seconds = time.perf_counter() - started
        final_version = submitted_result["current_version"]
        before_view = service.get_session_view(session["session_id"], final_version)
        before_compact = {
            "intervals": before_view["index"]["contiguous_ranges"],
            "parts": before_view["parts"],
            "dedup_ref_count_visible": len(before_view["index"]["dedup_refs"]),
        }
        segments_before = service.get_session_segments(session["session_id"])
        compact_started = time.perf_counter()
        compaction = service.compact_session(session["session_id"])
        compact_seconds = time.perf_counter() - compact_started
        segments_after = service.get_session_segments(session["session_id"])
        after_view = service.get_session_view(session["session_id"], final_version)

        checks = {
            "all_600_segments_contiguous": before_view["index"]["contiguous_ranges"] == [[1, count]],
            "duplicate_commits_counted": before_view["session"]["duplicate_commits"]
            == len(submitted) - count,
            "dedup_records_equal_unique_content": len(before_view["index"]["dedup_refs"]) == count,
            "compaction_reduced_segments": segments_after["segment_count"]
            < segments_before["segment_count"],
            "compaction_preserved_parts": after_view["parts"] == before_view["parts"],
            "compaction_preserved_intervals": after_view["index"]["contiguous_ranges"]
            == before_view["index"]["contiguous_ranges"],
        }
        if not all(checks.values()):
            raise AssertionError(f"scaling checks failed: {checks}")

        result = {
            "ok": True,
            "spec": spec,
            "submitted_events": len(submitted),
            "unique_parts": count,
            "duplicate_events": len(submitted) - count,
            "versions_after_batch": final_version,
            "commit_seconds": round(commit_seconds, 6),
            "events_per_second": round(len(submitted) / commit_seconds, 3),
            "segments_before_compaction": segments_before["segment_count"],
            "segments_after_compaction": segments_after["segment_count"],
            "compact_seconds": round(compact_seconds, 6),
            "contiguous_ranges": before_view["index"]["contiguous_ranges"],
            "dedup_records": len(before_view["index"]["dedup_refs"]),
            "numpy_size_checksum": checksum,
            "checks": checks,
            "database": str(db_path),
        }
        (ARTIFACTS / "scaling_result.json").write_text(
            json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
        return 0
    finally:
        service.shutdown()
        service.close()


if __name__ == "__main__":
    raise SystemExit(main())

