"""Generate deterministic local sample data (no randomness, no network)."""
from __future__ import annotations

import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(ROOT, "data")

sys.path.insert(0, ROOT)
from wckpt.util import sha256_hex  # noqa: E402


def digest(page_no: int, step: int) -> str:
    return sha256_hex({"page_no": page_no, "step": step, "kind": "content"})[:16]


def build_workload(num_pages: int, modifications: int, seed_tag: str):
    """Round-robin deterministic modifications across a fixed page set."""
    records = []
    state = {}
    for step in range(modifications):
        page_no = step % num_pages
        before = state.get(page_no)  # None for the very first touch of a page
        after = digest(page_no, step)
        records.append(
            {
                "seq": step + 1,
                "page_no": page_no,
                "before_digest": before,
                "after_digest": after,
                "txn_id": f"txn-{(step // 7) + 1:04d}",
                "ts": 1_700_000_000 + step,
            }
        )
        state[page_no] = after
    return records, state


def main():
    os.makedirs(DATA_DIR, exist_ok=True)

    initial_pages = {
        str(p): {
            "page_no": p,
            "digest": None,
            "version": 0,
            "last_lsn": 0,
        }
        for p in range(16)
    }
    with open(os.path.join(DATA_DIR, "initial_pages.json"), "w", encoding="utf-8") as fh:
        json.dump(
            {
                "description": "Deterministic initial page table (8 pages, version 0).",
                "pages": initial_pages,
            },
            fh,
            indent=2,
            ensure_ascii=False,
        )

    small_records, small_final = build_workload(8, 240, "small")
    with open(os.path.join(DATA_DIR, "wal_sequence.json"), "w", encoding="utf-8") as fh:
        json.dump(
            {
                "description": "Deterministic WAL modification sequence (small workload).",
                "records": small_records,
                "expected_final_digests": {str(k): v for k, v in sorted(small_final.items())},
            },
            fh,
            indent=2,
            ensure_ascii=False,
        )

    large_records, large_final = build_workload(16, 20000, "large")
    with open(os.path.join(DATA_DIR, "wal_sequence_large.json"), "w", encoding="utf-8") as fh:
        json.dump(
            {
                "description": "Deterministic WAL modification sequence (large workload).",
                "records": large_records,
                "expected_final_digests": {str(k): v for k, v in sorted(large_final.items())},
            },
            fh,
            indent=2,
            ensure_ascii=False,
        )

    config = {
        "description": "Checkpoint / WAL component configuration.",
        "segment_max_bytes": 4096,
        "replay_record_threshold": 1000,
        "replay_dirty_page_threshold": 200,
        "replay_batch_size": 500,
        "flush_group_size": 256,
        "background_flush": False,
        "background_flush_interval_ms": 200,
        "fsync_wal": True,
        "fsync_dirs": True,
        "checkpoint_schedule": [60, 130, 200, 240],
        "corrupt_injection_segment_index": 2,
    }
    with open(os.path.join(DATA_DIR, "checkpoint_config.json"), "w", encoding="utf-8") as fh:
        json.dump(config, fh, indent=2, ensure_ascii=False)

    print("generated sample files in", DATA_DIR)
    for name in sorted(os.listdir(DATA_DIR)):
        print(" -", name, os.path.getsize(os.path.join(DATA_DIR, name)), "bytes")


if __name__ == "__main__":
    main()
