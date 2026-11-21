"""Incremental vs from-scratch full build byte-consistency (req. 6)."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from voxel_engine import artifacts
from voxel_engine.checkpoints import CheckpointStore, make_record
from voxel_engine.errors import E_INCREMENTAL_MISMATCH, EngineError
from voxel_engine.serialization import canonical_json, read_file_bytes, sha256_hex

from tests.helpers import make_dataset, make_engine


class IncrementalConsistencyTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        root = Path(self._tmp.name)
        self.data = make_dataset(root / "data")
        self.engine = make_engine(root / "ws", self.data)
        self.engine.build(mode="full")

    def tearDown(self):
        self._tmp.cleanup()

    def test_replace_view_incremental_matches_full(self):
        self.engine.replace_view("view_02", self.data / "shards_alt" / "view_02.png")
        self.engine.build(mode="incremental")
        report = self.engine.verify_consistency()
        self.assertTrue(report["match"])

    def test_config_change_incremental_matches_full(self):
        self.engine.update_config({"voxel_resolution": 0.4})
        self.engine.build(mode="incremental")
        report = self.engine.verify_consistency()
        self.assertTrue(report["match"])

    def test_multiple_consecutive_increments_match_full(self):
        self.engine.replace_view("view_02", self.data / "shards_alt" / "view_02.png")
        self.engine.build(mode="incremental")
        self.assertTrue(self.engine.verify_consistency()["match"])
        self.engine.update_config({"voxel_resolution": 0.4})
        self.engine.build(mode="incremental")
        self.assertTrue(self.engine.verify_consistency()["match"])
        self.engine.update_config({"sparsity_threshold": 0.1})
        self.engine.build(mode="incremental")
        self.assertTrue(self.engine.verify_consistency()["match"])
        self.engine.update_config({"fusion_radius": 2.0})
        self.engine.build(mode="incremental")
        report = self.engine.verify_consistency()
        self.assertTrue(report["match"])
        summary = report["summary"]
        for key in (
            "contribution_digest",
            "fused_field_digest",
            "global_field_digest",
            "sparsity_stats_sha256",
        ):
            self.assertIn(key, summary)

    def test_tampered_state_detected_as_e_incremental_mismatch(self):
        workspace = self.engine.workspace
        store = CheckpointStore(workspace)
        chain, _ignored, _trunc = store.scan()
        record = next(r for r in chain if r["instance"] == "sparsify")
        artifact_path = workspace / record["artifact_path"]
        payload = artifacts.decode_sparse_field(read_file_bytes(artifact_path))
        coord = sorted(payload["field"].keys())[0]
        color, weight = payload["field"][coord]
        payload["field"][coord] = ((color[0] + 1.0, color[1], color[2]), weight)
        forged = artifacts.encode_sparse_field(
            {"field": payload["field"], "stats": payload["stats"]}
        )
        artifact_path.write_bytes(forged)
        # Repair the checkpoint digest so the log itself stays self-consistent;
        # only the content diverges from a from-scratch build.
        record_file = next(store.dir.glob(f"{record['seq']:08d}_*.json"))
        record["artifact_digest"] = sha256_hex(forged)
        fixed = make_record(
            seq=record["seq"], stage=record["stage"], instance=record["instance"],
            view_id=record["view_id"], fingerprint=record["fingerprint"],
            input_fingerprint=record["input_fingerprint"],
            config_version=record["config_version"], config=record["config"],
            upstream_fingerprints=record["upstream_fingerprints"],
            artifact_path=record["artifact_path"],
            artifact_digest=record["artifact_digest"], prev_seq=record["prev_seq"],
        )
        record_file.write_bytes(canonical_json(fixed))
        with self.assertRaises(EngineError) as ctx:
            self.engine.verify_consistency()
        error = ctx.exception
        self.assertEqual(error.code, E_INCREMENTAL_MISMATCH)
        self.assertEqual(error.details["stage"], "sparsify")
        self.assertEqual(error.details["voxel"], [int(c) for c in coord])


if __name__ == "__main__":
    unittest.main()
