from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from segval.canonical import canonical_bytes
from segval.checkpoints import CheckpointStore
from segval.config import DEFAULT_CONFIG, validate_config
from segval.decode import decode_shard
from segval.demo_generator import generate_shard
from segval.errors import WorkbenchError
from segval.shards import import_shard, shard_path
from segval.workspace import create_workspace


class CheckpointTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.workspace = create_workspace(Path(self.temp.name))
        self.config = validate_config(DEFAULT_CONFIG)
        self.shards = [generate_shard(index) for index in (1, 2)]
        for shard in self.shards:
            import_shard(self.workspace.root, shard)
        self.store = CheckpointStore(self.workspace.root)

    def tearDown(self):
        self.temp.cleanup()

    def _publish_decode(self, shard_index):
        shard = self.shards[shard_index - 1]
        shard = __import__("segval.shards", fromlist=["load_shard_file"]).load_shard_file(
            shard_path(self.workspace.shards_dir, shard_index)
        )
        artifact = decode_shard(shard, self.config)
        from segval.engine import _publish_decode

        return _publish_decode(self.store, self.workspace, shard, self.config, artifact)

    def test_missing_prior_stage_and_duplicate_are_structured(self):
        with self.assertRaises(WorkbenchError) as missing:
            self.store.publish(
                shard_id="shard-0001",
                shard_index=1,
                stage="match",
                input_fingerprint="x",
                config_fingerprint="x",
                config_version="1",
                artifact={"artifact_digest": "x"},
                upstream_digest="missing",
            )
        self.assertEqual(missing.exception.code, "E_INVALID_STAGE")
        self._publish_decode(1)
        with self.assertRaises(WorkbenchError) as duplicate:
            self._publish_decode(1)
        self.assertEqual(duplicate.exception.code, "E_STALE_CHECKPOINT")

    def test_expected_predecessor_sequence_is_enforced(self):
        first = self._publish_decode(1)
        shard = generate_shard(2)
        artifact = decode_shard(shard, self.config)
        with self.assertRaises(WorkbenchError) as stale:
            self.store.publish(
                shard_id="shard-0002",
                shard_index=2,
                stage="decode",
                input_fingerprint="probe",
                config_fingerprint="probe",
                config_version="1",
                artifact=artifact,
                expected_prev_sequence=0,
            )
        self.assertEqual(stale.exception.code, "E_STALE_CHECKPOINT")
        second = self.store.publish(
            shard_id="shard-0002",
            shard_index=2,
            stage="decode",
            input_fingerprint="probe-2",
            config_fingerprint="probe",
            config_version="1",
            artifact=artifact,
            expected_prev_sequence=first["sequence"],
        )
        self.assertEqual(second["sequence"], first["sequence"] + 1)

    def test_journal_and_current_snapshot_are_complete_json(self):
        record = self._publish_decode(1)
        journal_files = list((self.workspace.checkpoint_dir / "_journal").glob("*.json"))
        self.assertEqual(len(journal_files), 1)
        payload = journal_files[0].read_bytes()
        self.assertEqual(payload, canonical_bytes(record))
        self.assertFalse(list(self.workspace.checkpoint_dir.rglob("*.tmp-*")))

    def test_old_worker_cannot_publish_after_shard_update_even_without_prior_stage(self):
        old_shard = generate_shard(1)
        old_artifact = decode_shard(old_shard, self.config)
        changed = generate_shard(1)
        changed["samples"][0]["predictions"][0]["confidence"] = 0.111111
        import_shard(self.workspace.root, changed)
        with self.assertRaises(WorkbenchError) as stale:
            self.store.publish(
                shard_id="shard-0001",
                shard_index=1,
                stage="decode",
                input_fingerprint="old-input",
                config_fingerprint="old-config",
                config_version="1",
                artifact=old_artifact,
            )
        self.assertEqual(stale.exception.code, "E_STALE_CHECKPOINT")


if __name__ == "__main__":
    unittest.main()

