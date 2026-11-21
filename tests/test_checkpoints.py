from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest import mock

from kde_drift.checkpoints import CheckpointStore
from kde_drift.errors import WorkbenchError


def publish_demo_bandwidth(store: CheckpointStore, *, revision: int = 0):
    return store.publish(
        stage="bandwidth",
        payload={"stage": "bandwidth", "columns": [], "diagnostics": {"ok": True}},
        expected_previous_revision=revision,
        config_version="config-v1",
        stage_input_fingerprint="input-v1",
        dataset_fingerprint="dataset-v1",
        stage_config={"kernel": "gaussian"},
        upstream={},
        accumulator={"kind": "demo", "digest": "d"},
    )


class CheckpointTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.workdir = Path(self.temp.name)
        self.store = CheckpointStore(self.workdir)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_sequence_and_stage_revision_are_monotonic(self) -> None:
        first = publish_demo_bandwidth(self.store, revision=0)
        second = publish_demo_bandwidth(self.store, revision=1)
        self.assertEqual((first["sequence"], first["stage_revision"]), (1, 1))
        self.assertEqual((second["sequence"], second["stage_revision"]), (2, 2))
        self.assertEqual(self.store.latest_sequence(), 2)

    def test_duplicate_or_stale_publication_returns_stale_checkpoint(self) -> None:
        publish_demo_bandwidth(self.store, revision=0)
        with self.assertRaises(WorkbenchError) as context:
            publish_demo_bandwidth(self.store, revision=0)
        self.assertEqual(context.exception.code, "E_STALE_CHECKPOINT")
        self.assertEqual(context.exception.details["current_revision"], 1)

    def test_missing_prerequisite_returns_invalid_stage(self) -> None:
        with self.assertRaises(WorkbenchError) as context:
            self.store.publish(
                stage="density",
                payload={"stage": "density", "columns": []},
                expected_previous_revision=0,
                config_version="config-v1",
                stage_input_fingerprint="input-density",
                dataset_fingerprint="dataset-v1",
                stage_config={},
                upstream={"bandwidth": "not-present"},
                accumulator={"kind": "demo"},
            )
        self.assertEqual(context.exception.code, "E_INVALID_STAGE")

    def test_interrupted_publication_leaves_old_record_and_no_half_record(self) -> None:
        original = publish_demo_bandwidth(self.store, revision=0)
        old_bytes = (self.workdir / "checkpoints" / "bandwidth.json").read_bytes()
        with mock.patch("kde_drift.canonical.os.replace", side_effect=OSError("simulated crash")):
            with self.assertRaises(OSError):
                publish_demo_bandwidth(self.store, revision=1)
        current_bytes = (self.workdir / "checkpoints" / "bandwidth.json").read_bytes()
        self.assertEqual(old_bytes, current_bytes)
        record = self.store.read_record("bandwidth")
        self.assertIsNotNone(record)
        self.assertEqual(record["sequence"], original["sequence"])
        self.assertEqual(record["stage_revision"], 1)

    def test_artifact_tampering_is_detected(self) -> None:
        record = publish_demo_bandwidth(self.store)
        artifact = self.workdir / record["artifact"]["path"]
        artifact.write_text('{"stage":"bandwidth","tampered":true}', encoding="utf-8")
        with self.assertRaises(WorkbenchError) as context:
            self.store.load_artifact(record)
        self.assertEqual(context.exception.code, "E_INVALID_STAGE")


if __name__ == "__main__":
    unittest.main()
