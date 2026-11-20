from __future__ import annotations

import multiprocessing
import tempfile
import unittest
from pathlib import Path

import numpy as np

from detdiff.artifacts import embedding_artifact, schedule_artifact
from detdiff.checkpoints import CheckpointStore
from detdiff.errors import BusinessError, ErrorCode
from detdiff.util import sha256_bytes


ARTIFACT = embedding_artifact("alpha", "prompt", np.asarray([1.0] + [0.0] * 15, dtype="<f8"))


class CheckpointConcurrencyTests(unittest.TestCase):
    def test_missing_prerequisite_returns_invalid_stage(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = CheckpointStore(Path(temp))
            weights = np.asarray([1.0], dtype="<f8")
            with self.assertRaises(BusinessError) as context:
                store.publish(
                    shard_id="alpha",
                    stage="schedule",
                    expected_sequence=1,
                    input_fingerprint="fp",
                    config_version="cv",
                    input_meta={},
                    artifact_bytes=schedule_artifact("alpha", weights),
                )
            self.assertEqual(context.exception.code, ErrorCode.E_INVALID_STAGE)
            self.assertEqual(context.exception.details["missing"], "encode")

    def test_duplicate_and_lagging_sequence_returns_stale_checkpoint(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = CheckpointStore(Path(temp))
            first = store.publish(
                shard_id="alpha",
                stage="encode",
                expected_sequence=1,
                input_fingerprint="fp",
                config_version="cv",
                input_meta={},
                artifact_bytes=ARTIFACT,
            )
            self.assertEqual(first.sequence, 1)
            with self.assertRaises(BusinessError) as context:
                store.publish(
                    shard_id="alpha",
                    stage="encode",
                    expected_sequence=1,
                    input_fingerprint="fp2",
                    config_version="cv",
                    input_meta={},
                    artifact_bytes=ARTIFACT,
                )
            self.assertEqual(context.exception.code, ErrorCode.E_STALE_CHECKPOINT)
            self.assertEqual(context.exception.details["expected"], 2)

    def test_two_processes_with_same_expected_sequence_one_wins_one_is_stale(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            queue = multiprocessing.Queue()
            processes = [
                multiprocessing.Process(target=_publish_worker, args=(root, queue))
                for _ in range(2)
            ]
            for process in processes:
                process.start()
            for process in processes:
                process.join(timeout=20)
                self.assertEqual(process.exitcode, 0)
            outcomes = sorted(queue.get(timeout=5) for _ in processes)
            self.assertEqual(outcomes, ["E_STALE_CHECKPOINT", "OK"])
            store = CheckpointStore(root)
            self.assertEqual(store.read_record("alpha", "encode").sequence, 1)


def _publish_worker(root: Path, queue: multiprocessing.Queue) -> None:
    store = CheckpointStore(root)
    try:
        store.publish(
            shard_id="alpha",
            stage="encode",
            expected_sequence=1,
            input_fingerprint="fp",
            config_version="cv",
            input_meta={},
            artifact_bytes=ARTIFACT,
        )
    except BusinessError as exc:
        queue.put(exc.code.value)
    else:
        queue.put("OK")


if __name__ == "__main__":
    unittest.main()

