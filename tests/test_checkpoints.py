"""Checkpoint store: atomic publish, scan validation, recovery semantics."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from voxel_engine.checkpoints import CheckpointStore, fold_chain, make_record
from voxel_engine.errors import (
    E_INVALID_STAGE,
    E_STALE_CHECKPOINT,
    EngineError,
)
from voxel_engine.serialization import canonical_json, read_file_bytes


def _publish(store, *, instance, seq, upstreams=None, fp=None, stage="projection"):
    fp = fp or f"fp-{instance}-{seq}"
    return store.publish(
        stage=stage,
        instance=instance,
        view_id=None,
        fingerprint=fp,
        input_fingerprint="in-" + instance,
        config_version="cfg-" + instance,
        config={"stage": stage},
        upstream_fingerprints=upstreams or {},
        artifact_bytes=f"artifact-{instance}-{fp}".encode(),
        artifact_rel_path=f"artifacts/{instance.replace(':', '_')}.{fp[:8]}.bin",
        expected_prev_seq=seq,
    )


class CheckpointTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.store = CheckpointStore(Path(self._tmp.name) / "ws")

    def tearDown(self):
        self._tmp.cleanup()

    def test_publish_and_scan_chain(self):
        _publish(self.store, instance="projection:v0", seq=0)
        rec = _publish(self.store, instance="projection:v1", seq=1)
        self.assertEqual(rec["seq"], 2)
        chain, ignored, truncation = self.store.scan()
        self.assertEqual(len(chain), 2)
        self.assertEqual(ignored, [])
        self.assertIsNone(truncation)

    def test_stale_sequence_rejected(self):
        _publish(self.store, instance="projection:v0", seq=0)
        with self.assertRaises(EngineError) as ctx:
            _publish(self.store, instance="projection:v1", seq=0)
        self.assertEqual(ctx.exception.code, E_STALE_CHECKPOINT)
        with self.assertRaises(EngineError) as ctx:
            _publish(self.store, instance="projection:v1", seq=5)
        self.assertEqual(ctx.exception.code, E_STALE_CHECKPOINT)

    def test_missing_prerequisite_raises_e_invalid_stage(self):
        with self.assertRaises(EngineError) as ctx:
            _publish(
                self.store,
                instance="voxelize:v0",
                seq=0,
                upstreams={"projection:v0": "fp-x"},
                stage="voxelize",
            )
        self.assertEqual(ctx.exception.code, E_INVALID_STAGE)
        self.assertEqual(ctx.exception.details["missing_upstream"], "projection:v0")

    def test_cross_stage_misalignment_raises_e_stale_checkpoint(self):
        rec = _publish(self.store, instance="projection:v0", seq=0)
        with self.assertRaises(EngineError) as ctx:
            _publish(
                self.store,
                instance="voxelize:v0",
                seq=1,
                upstreams={"projection:v0": "wrong-fp"},
                stage="voxelize",
            )
        self.assertEqual(ctx.exception.code, E_STALE_CHECKPOINT)
        self.assertEqual(ctx.exception.details["published"], rec["fingerprint"])

    def test_duplicate_identical_publish_rejected(self):
        rec = _publish(self.store, instance="projection:v0", seq=0)
        with self.assertRaises(EngineError) as ctx:
            self.store.publish(
                stage="projection",
                instance="projection:v0",
                view_id=None,
                fingerprint=rec["fingerprint"],
                input_fingerprint="in-projection:v0",
                config_version="cfg-projection:v0",
                config={"stage": "projection"},
                upstream_fingerprints={},
                artifact_bytes=b"different bytes same fp",
                artifact_rel_path="artifacts/other.bin",
                expected_prev_seq=1,
            )
        self.assertEqual(ctx.exception.code, E_STALE_CHECKPOINT)

    def test_corrupted_record_ignored_on_scan(self):
        _publish(self.store, instance="projection:v0", seq=0)
        _publish(self.store, instance="projection:v1", seq=1)
        target = next((self.store.dir).glob("00000001_*.json"))
        record = json.loads(read_file_bytes(target).decode("utf-8"))
        record["fingerprint"] = "tampered-fingerprint"
        target.write_bytes(canonical_json(record))
        chain, ignored, truncation = self.store.scan()
        self.assertEqual(len(chain), 0)
        self.assertEqual(len(ignored), 1)
        self.assertEqual(ignored[0]["reason"], "checksum_mismatch")

    def test_duplicate_seq_record_ignored(self):
        _publish(self.store, instance="projection:v0", seq=0)
        _publish(self.store, instance="projection:v1", seq=1)
        forged = make_record(
            seq=2, stage="projection", instance="projection:forged", view_id=None,
            fingerprint="fp-forged", input_fingerprint="in", config_version="cfg",
            config={}, upstream_fingerprints={},
            artifact_path="artifacts/none.bin", artifact_digest="0" * 64, prev_seq=1,
        )
        (self.store.dir / "00000002_ffffffff.json").write_bytes(canonical_json(forged))
        chain, ignored, truncation = self.store.scan()
        reasons = [entry["reason"] for entry in ignored]
        self.assertIn("duplicate_seq", reasons)
        self.assertEqual(len(chain), 2)

    def test_missing_artifact_truncates_chain(self):
        _publish(self.store, instance="projection:v0", seq=0)
        _publish(self.store, instance="projection:v1", seq=1)
        first = json.loads(
            read_file_bytes(next(self.store.dir.glob("00000001_*.json"))).decode("utf-8")
        )
        artifact = Path(self._tmp.name) / "ws" / first["artifact_path"]
        artifact.unlink()
        chain, ignored, truncation = self.store.scan()
        self.assertEqual(len(chain), 0)
        self.assertIsNotNone(truncation)
        self.assertEqual(truncation["reason"], "artifact_missing")

    def test_no_tmp_files_left_after_publish(self):
        _publish(self.store, instance="projection:v0", seq=0)
        workspace = Path(self._tmp.name) / "ws"
        tmp_files = list(workspace.rglob("*.tmp"))
        self.assertEqual(tmp_files, [])

    def test_recover_rebuilds_snapshot_and_reports(self):
        rec0 = _publish(self.store, instance="projection:v0", seq=0)
        _publish(
            self.store,
            instance="voxelize:v0",
            seq=1,
            upstreams={"projection:v0": rec0["fingerprint"]},
            stage="voxelize",
        )
        snapshot = Path(self._tmp.name) / "ws" / "state" / "published.json"
        snapshot.unlink()
        report = self.store.recover()
        self.assertEqual(report["resumed_seq"], 2)
        self.assertEqual(
            sorted(report["effective_instances"]), ["projection:v0", "voxelize:v0"]
        )
        self.assertTrue(snapshot.is_file())
        data = json.loads(snapshot.read_text())
        self.assertEqual(data["last_seq"], 2)

    def test_superseded_instance_orphans_downstream(self):
        rec0 = _publish(self.store, instance="projection:v0", seq=0)
        _publish(
            self.store,
            instance="voxelize:v0",
            seq=1,
            upstreams={"projection:v0": rec0["fingerprint"]},
            stage="voxelize",
        )
        rec2 = _publish(
            self.store, instance="projection:v0", seq=2, fp="fp-new-version"
        )
        chain, _ignored, _trunc = self.store.scan()
        effective, orphaned = fold_chain(chain)
        self.assertEqual(effective["projection:v0"]["fingerprint"], "fp-new-version")
        self.assertNotIn("voxelize:v0", effective)
        self.assertEqual([r["instance"] for r in orphaned], ["voxelize:v0"])
        self.assertEqual(rec2["seq"], 3)


if __name__ == "__main__":
    unittest.main()
