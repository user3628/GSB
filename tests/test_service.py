from __future__ import annotations

import base64
import copy
import tempfile
import time
import unittest
from pathlib import Path
from typing import Any

from colscan.errors import BusinessError
from colscan.reference import reference_scan_file
from colscan.service import ColumnarService
from colscan.snapshot import build_snapshot_document, snapshot_checksum, write_snapshot_file


def build_fixture(version: int = 1, *, rows: int = 32) -> dict[str, Any]:
    ids = []
    names = []
    statuses = []
    scores = []
    active = []
    for index in range(rows):
        ids.append(index)
        names.append(None if index % 13 == 0 else ["alice", "bob", "carol"][index % 3])
        statuses.append(None if index % 17 == 0 else ("active" if index % 4 else "archived"))
        scores.append(None if index % 11 == 0 else float(index * 2))
        active.append(index % 3 != 0)
    return build_snapshot_document(
        version=version,
        generated_at="2026-01-01T00:00:00Z",
        block_size=8,
        columns=[
            {"name": "id", "dtype": "int64", "encoding": "fixed", "segment_rows": 4, "values": ids},
            {"name": "name", "dtype": "string", "encoding": "dict", "segment_rows": 4, "values": names},
            {"name": "status", "dtype": "string", "encoding": "rle", "segment_rows": 4, "values": statuses},
            {"name": "score", "dtype": "float64", "encoding": "fixed", "segment_rows": 4, "values": scores},
            {"name": "active", "dtype": "bool", "encoding": "rle", "segment_rows": 4, "values": active},
        ],
    )


PREDICATE = {
    "op": "and",
    "predicates": [
        {"column": "score", "op": "ge", "value": 10.0},
        {"column": "status", "op": "ne", "value": "archived"},
    ],
}
COLUMNS = ["id", "name", "status", "score", "active"]


def wait_complete(service: ColumnarService, query_id: str, timeout: float = 8.0) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        status = service.query_status(query_id)
        if status["status"] in ("COMPLETE", "FAILED"):
            return status
        time.sleep(0.02)
    raise AssertionError(f"query {query_id} did not finish")


class ServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.snapshot_path = self.root / "snapshot.csnap"
        write_snapshot_file(self.snapshot_path, build_fixture())
        self.service = ColumnarService(self.root / "state.db")
        self.service.load_snapshot(self.snapshot_path)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def request(
        self,
        query_id: str,
        *,
        memory: int = 1_048_576,
        concurrency: int = 2,
        queue_capacity: int = 64,
        offset: int = 0,
        limit: int | None = None,
    ) -> dict[str, Any]:
        return {
            "query_id": query_id,
            "columns": COLUMNS,
            "predicate": PREDICATE,
            "block_start": 0,
            "block_end": 4,
            "offset": offset,
            "limit": limit,
            "budget": {
                "max_concurrent_tasks": concurrency,
                "per_task_memory_bytes": memory,
                "round_time_budget_ms": 8.0,
                "queue_capacity": queue_capacity,
                "degrade_mode": "split",
            },
        }

    def test_full_and_degraded_scan_match_reference(self) -> None:
        self.service.submit_query(self.request("full"), wait_seconds=0)
        full_status = wait_complete(self.service, "full")
        self.assertEqual(full_status["status"], "COMPLETE")
        full = self.service.query_result("full", offset=0, limit=None)
        reference = reference_scan_file(
            self.snapshot_path,
            columns=COLUMNS,
            predicate=PREDICATE,
            block_start=0,
            block_end=4,
        )
        self.assertEqual(full["rows"], reference["all_rows"])
        self.service.submit_query(self.request("degraded", memory=1200), wait_seconds=0)
        degraded_status = wait_complete(self.service, "degraded")
        self.assertEqual(degraded_status["status"], "COMPLETE")
        degraded = self.service.query_result("degraded", offset=0, limit=None)
        self.assertEqual(degraded["rows"], full["rows"])
        decisions = self.service.query_decisions("degraded")
        self.assertTrue(any(item["action"] == "split" for item in decisions))
        self.assertTrue(any(item["reason"] == "estimated_memory_exceeds_per_task_budget" for item in decisions))
        self.assertFalse(any(item["action"] == "admit_forced_overrun" for item in decisions))

    def test_pagination_is_stable_and_non_overlapping(self) -> None:
        self.service.submit_query(self.request("page"), wait_seconds=0)
        wait_complete(self.service, "page")
        whole = self.service.query_result("page", offset=0, limit=None)
        page_a = self.service.query_result("page", offset=0, limit=5)
        page_b = self.service.query_result("page", offset=5, limit=7)
        self.assertEqual(page_a["rows"] + page_b["rows"], whole["rows"][:12])
        self.assertEqual(len({tuple(row) for row in page_a["rows"] + page_b["rows"]}), len(page_a["rows"]) + len(page_b["rows"]))

    def test_query_limit_and_offset_window(self) -> None:
        self.service.submit_query(self.request("window", offset=3, limit=4), wait_seconds=0)
        wait_complete(self.service, "window")
        whole = self.service.query_result("window", offset=0, limit=None)
        default_window = self.service.query_result("window")
        self.assertEqual(default_window["rows"], whole["rows"][3:7])

    def test_duplicate_query_id_executes_once(self) -> None:
        first, created = self.service.submit_query(self.request("once"), wait_seconds=0)
        self.assertTrue(created)
        wait_complete(self.service, "once")
        generation = self.service.query_status("once")["commit_generation"]
        second, created_again = self.service.submit_query(self.request("once"), wait_seconds=2)
        self.assertFalse(created_again)
        self.assertEqual(first["query_id"], second["query_id"])
        self.assertEqual(self.service.query_status("once")["commit_generation"], generation)
        changed = self.request("once")
        changed["columns"] = ["id"]
        with self.assertRaises(BusinessError) as caught:
            self.service.submit_query(changed)
        self.assertEqual(caught.exception.code, "E_QUERY_CONFLICT")

    def test_bounded_queue_returns_queue_full(self) -> None:
        with self.assertRaises(BusinessError) as caught:
            self.service.submit_query(self.request("queue", concurrency=1, queue_capacity=1))
        self.assertEqual(caught.exception.code, "E_SCAN_QUEUE_FULL")

    def test_corrupt_block_fails_query_with_block_id(self) -> None:
        document = build_fixture(version=2)
        payload = bytearray(base64.b64decode(document["columns"][1]["blocks"][0]["payload_b64"]))
        payload[-1] ^= 0x7F
        document["columns"][1]["blocks"][0]["payload_b64"] = base64.b64encode(bytes(payload)).decode("ascii")
        document.pop("checksum")
        document["checksum"] = snapshot_checksum(document)
        corrupted_path = self.root / "corrupt.csnap"
        write_snapshot_file(corrupted_path, document)
        self.service.load_snapshot(corrupted_path)
        request = self.request("corrupt")
        request["columns"] = ["id", "name"]
        request["predicate"] = None
        request["block_end"] = 1
        self.service.submit_query(request)
        status = wait_complete(self.service, "corrupt")
        self.assertEqual(status["status"], "FAILED")
        self.assertEqual(status["error"]["code"], "E_BLOCK_CORRUPT")
        with self.assertRaises(BusinessError) as caught:
            self.service.query_result("corrupt")
        self.assertEqual(caught.exception.code, "E_BLOCK_CORRUPT")
        self.assertEqual(caught.exception.details["block_id"], "name:b0000")
        attempts_before = self.service.query_shards("corrupt")[0]["attempts"]
        duplicate_status, created = self.service.submit_query(request, wait_seconds=2)
        self.assertFalse(created)
        self.assertEqual(duplicate_status["status"], "FAILED")
        self.assertEqual(self.service.query_shards("corrupt")[0]["attempts"], attempts_before)


if __name__ == "__main__":
    unittest.main()

