from __future__ import annotations

import copy
import json
import tempfile
import unittest
from pathlib import Path

from colscan.errors import BusinessError
from colscan.service import ColumnarService
from colscan.snapshot import build_snapshot_document, snapshot_checksum, write_snapshot_file


def make_document(version: int, *, value_offset: int = 0) -> dict:
    return build_snapshot_document(
        version=version,
        generated_at=f"2026-01-01T00:00:{version:02d}Z",
        block_size=4,
        columns=[
            {
                "name": "id",
                "dtype": "int64",
                "encoding": "fixed",
                "segment_rows": 2,
                "values": [value_offset + index for index in range(12)],
            },
            {
                "name": "label",
                "dtype": "string",
                "encoding": "dict",
                "segment_rows": 2,
                "values": ["a", "b", None, "a", "c", "b", "a", "b", "c", "a", "b", "c"],
            },
        ],
    )


class SnapshotTests(unittest.TestCase):
    def test_invalid_snapshot_does_not_change_active_version(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            valid_path = root / "v1.csnap"
            write_snapshot_file(valid_path, make_document(1))
            service = ColumnarService(root / "state.db")
            service.load_snapshot(valid_path)
            invalid = make_document(2)
            invalid["generated_at"] = "tampered"
            invalid_path = root / "bad.csnap"
            write_snapshot_file(invalid_path, invalid)
            with self.assertRaises(BusinessError) as caught:
                service.load_snapshot(invalid_path)
            self.assertEqual(caught.exception.code, "E_SNAPSHOT_INVALID")
            self.assertEqual(service.current_snapshot()["version"], 1)

    def test_version_must_strictly_increase(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            v1_path = root / "v1.csnap"
            v2_path = root / "v2.csnap"
            write_snapshot_file(v1_path, make_document(1))
            write_snapshot_file(v2_path, make_document(2, value_offset=100))
            service = ColumnarService(root / "state.db")
            service.load_snapshot(v1_path)
            with self.assertRaises(BusinessError) as caught:
                service.load_snapshot(v1_path)
            self.assertEqual(caught.exception.code, "E_VERSION_RETIRED")
            service.load_snapshot(v2_path)
            self.assertEqual(service.current_snapshot()["version"], 2)
            with self.assertRaises(BusinessError) as caught:
                service.load_snapshot(v1_path)
            self.assertEqual(caught.exception.code, "E_VERSION_RETIRED")
            self.assertEqual(service.current_snapshot()["version"], 2)

    def test_checksum_is_deterministic(self) -> None:
        first = make_document(7)
        second = copy.deepcopy(first)
        second.pop("checksum")
        second["checksum"] = snapshot_checksum(second)
        self.assertEqual(first["checksum"], second["checksum"])
        reparsed = json.loads(json.dumps(first, sort_keys=True))
        self.assertEqual(snapshot_checksum(reparsed), first["checksum"])


if __name__ == "__main__":
    unittest.main()

