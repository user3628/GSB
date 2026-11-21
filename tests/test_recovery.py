"""Stop between stages, checkpoint recovery, resume consistency."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from voxel_engine.checkpoints import CheckpointStore
from voxel_engine.errors import E_STOPPED, EngineError
from voxel_engine.serialization import canonical_json, read_file_bytes

from tests.helpers import make_dataset, make_engine


class RecoveryTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.data = make_dataset(self.root / "data")

    def tearDown(self):
        self._tmp.cleanup()

    def _digests(self, engine):
        return engine._collect_digests()["_summary"]

    def test_stop_recover_resume_matches_full(self):
        reference = make_engine(self.root / "ws_ref", self.data)
        reference.build(mode="full")
        reference_digests = self._digests(reference)

        engine = make_engine(self.root / "ws", self.data)
        report = engine.build(mode="full", stop_after="voxelize:view_01")
        self.assertEqual(report["status"], "stopped")
        self.assertEqual(report["stopped_at"], "voxelize:view_02")
        published = [
            s["instance"] for s in engine.status()["stages"] if s["status"] == "published"
        ]
        self.assertEqual(len(published), 6)

        with self.assertRaises(EngineError) as ctx:
            engine.build(mode="incremental")
        self.assertEqual(ctx.exception.code, E_STOPPED)

        recovery = engine.recover()
        self.assertEqual(recovery["resumed_seq"], 6)
        self.assertEqual(recovery["ignored_records"], [])

        resumed = engine.build(mode="resume")
        self.assertEqual(resumed["status"], "completed")
        self.assertEqual(self._digests(engine), reference_digests)

    def test_recovery_ignores_corrupted_records(self):
        engine = make_engine(self.root / "ws", self.data)
        engine.build(mode="full")
        store = CheckpointStore(engine.workspace)
        latest = sorted(store.dir.glob("*.json"))[-1]
        record = json.loads(read_file_bytes(latest).decode("utf-8"))
        record["artifact_digest"] = "0" * 64
        latest.write_bytes(canonical_json(record))
        report = engine.recover()
        self.assertEqual(report["resumed_seq"], 9)
        self.assertEqual(len(report["ignored_records"]), 1)
        self.assertEqual(
            report["ignored_records"][0]["reason"], "checksum_mismatch"
        )
        rebuilt = engine.build(mode="incremental")
        self.assertEqual(rebuilt["rebuilt"], ["sparsify"])
        reference = make_engine(self.root / "ws_ref", self.data)
        reference.build(mode="full")
        self.assertEqual(self._digests(engine), self._digests(reference))

    def test_published_artifacts_not_recomputed_after_recovery(self):
        engine = make_engine(self.root / "ws", self.data)
        engine.build(mode="full")
        engine.recover()
        report = engine.build(mode="incremental")
        self.assertEqual(report["rebuilt"], [])
        self.assertEqual(len(report["reused"]), 10)

    def test_stop_request_halts_between_stages(self):
        engine = make_engine(self.root / "ws", self.data)
        engine.stop()
        with self.assertRaises(EngineError) as ctx:
            engine.build(mode="incremental")
        self.assertEqual(ctx.exception.code, E_STOPPED)
        report = engine.build(mode="resume")
        self.assertEqual(report["status"], "completed")


if __name__ == "__main__":
    unittest.main()
