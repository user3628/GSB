"""Snapshot loading, validation, versioning and error codes."""

import json
import os
import shutil
import tempfile
import unittest

from qopt.errors import (SnapshotInvalidError, SnapshotNotLoadedError,
                         SnapshotVersionMismatchError, StatsMissingError)
from qopt.planner import plan_query
from qopt.stats import SnapshotRegistry, parse_snapshot


def valid_doc():
    return {
        "format": "qopt-snapshot/1",
        "version": "v1",
        "generated_at": "2026-11-20T00:00:00Z",
        "tables": {
            "t": {"row_count": 100, "pages": 2,
                  "columns": {"a": {"ndv": 10, "null_frac": 0.0}}},
        },
        "indexes": [],
        "correlations": [],
    }


class ValidationTest(unittest.TestCase):
    def test_valid_load(self):
        snap = parse_snapshot(valid_doc())
        self.assertEqual(snap.version, "v1")
        self.assertEqual(len(snap.fingerprint), 64)

    def test_missing_format(self):
        doc = valid_doc()
        del doc["format"]
        with self.assertRaises(SnapshotInvalidError):
            parse_snapshot(doc)

    def test_bad_histogram_shape(self):
        doc = valid_doc()
        doc["tables"]["t"]["columns"]["a"]["histogram"] = {
            "boundaries": [0, 1, 2], "counts": [1], "ndv_per_bucket": [1],
        }
        with self.assertRaises(SnapshotInvalidError):
            parse_snapshot(doc)

    def test_bad_null_frac(self):
        doc = valid_doc()
        doc["tables"]["t"]["columns"]["a"]["null_frac"] = 1.5
        with self.assertRaises(SnapshotInvalidError):
            parse_snapshot(doc)

    def test_bad_correlation_reference(self):
        doc = valid_doc()
        doc["correlations"] = [{"table": "t", "columns": ["a", "ghost"],
                                "kind": "coefficient", "value": 0.5}]
        with self.assertRaises(SnapshotInvalidError):
            parse_snapshot(doc)

    def test_error_code(self):
        try:
            parse_snapshot({})
        except SnapshotInvalidError as err:
            self.assertEqual(err.code, "E_SNAPSHOT_INVALID")
        else:
            self.fail("expected SnapshotInvalidError")


class RegistryTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.good = os.path.join(self.dir, "good.json")
        self.bad = os.path.join(self.dir, "bad.json")
        with open(self.good, "w") as fh:
            json.dump(valid_doc(), fh)
        doc = valid_doc()
        del doc["generated_at"]
        with open(self.bad, "w") as fh:
            json.dump(doc, fh)

    def tearDown(self):
        shutil.rmtree(self.dir)

    def test_atomic_replace_keeps_old_on_failure(self):
        reg = SnapshotRegistry()
        reg.load(self.good)
        with self.assertRaises(SnapshotInvalidError):
            reg.load(self.bad)
        self.assertIsNotNone(reg.current)
        self.assertEqual(reg.current.version, "v1")  # old snapshot preserved

    def test_version_require(self):
        reg = SnapshotRegistry()
        with self.assertRaises(SnapshotNotLoadedError):
            reg.require()
        reg.load(self.good)
        self.assertEqual(reg.require("v1").version, "v1")
        with self.assertRaises(SnapshotVersionMismatchError):
            reg.require("v9")

    def test_missing_table_stats(self):
        reg = SnapshotRegistry()
        snap = reg.load(self.good)
        with self.assertRaises(StatsMissingError) as ctx:
            plan_query({"tables": ["nope"]}, snap)
        self.assertEqual(ctx.exception.code, "E_STATS_MISSING")


if __name__ == "__main__":
    unittest.main()
