from __future__ import annotations

import json
import multiprocessing
import tempfile
import unittest
from pathlib import Path
from typing import Any

from sparse_index.checkpoints import CheckpointStore, _record_digest
from sparse_index.errors import PipelineError
from sparse_index.utils import atomic_write_json


def _competing_publish(artifacts: str, result_queue: Any) -> None:
    from sparse_index.checkpoints import CheckpointStore
    from sparse_index.errors import PipelineError

    store = CheckpointStore(Path(artifacts))
    try:
        record = store.publish(
            stage="statistics",
            input_digest="input-2",
            config_version="cfg",
            artifact_digest="artifact-2",
            stage_fingerprint="fp-2",
            expected_seq=2,
        )
        result_queue.put({"result": "published", "seq": record["seq"]})
    except PipelineError as exc:
        result_queue.put({"result": "error", "code": exc.code} )


class CheckpointTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.artifacts = Path(self.temp.name) / "artifacts"
        self.store = CheckpointStore(self.artifacts)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _publish_stage1(self) -> dict[str, Any]:
        return self.store.publish(
            stage="tokenize",
            input_digest="input-1",
            config_version="cfg",
            artifact_digest="artifact-1",
            stage_fingerprint="fp-1",
            expected_seq=1,
        )

    def test_sequence_and_stage_guards(self) -> None:
        self._publish_stage1()
        with self.assertRaises(PipelineError) as context:
            self.store.publish(
                stage="statistics",
                input_digest="input-2",
                config_version="cfg",
                artifact_digest="artifact-2",
                stage_fingerprint="fp-2",
                expected_seq=1,
            )
        self.assertEqual(context.exception.code, "E_STALE_CHECKPOINT")
        with self.assertRaises(PipelineError) as context:
            self.store.publish(
                stage="query_summary",
                input_digest="input-4",
                config_version="cfg",
                artifact_digest="artifact-4",
                stage_fingerprint="fp-4",
                expected_seq=2,
            )
        self.assertEqual(context.exception.code, "E_INVALID_STAGE")

    def test_repeated_publish_is_stale(self) -> None:
        self._publish_stage1()
        self.store.publish(
            stage="statistics",
            input_digest="input-2",
            config_version="cfg",
            artifact_digest="artifact-2",
            stage_fingerprint="fp-2",
            expected_seq=2,
        )
        with self.assertRaises(PipelineError) as context:
            self.store.publish(
                stage="statistics",
                input_digest="input-2b",
                config_version="cfg",
                artifact_digest="artifact-2b",
                stage_fingerprint="fp-2b",
                expected_seq=3,
            )
        self.assertEqual(context.exception.code, "E_STALE_CHECKPOINT")

    def test_corrupt_and_duplicate_records_are_ignored(self) -> None:
        self._publish_stage1()
        checkpoint_dir = self.artifacts / "checkpoints"
        (checkpoint_dir / "00000002-statistics.json").write_text("{broken", encoding="utf-8")
        active, ignored = self.store.active()
        self.assertEqual([record["stage"] for record in active], ["tokenize"])
        self.assertTrue(any(item["reason"] == "unparseable_or_incomplete" for item in ignored))

        first = self.store.publish(
            stage="statistics",
            input_digest="input-2",
            config_version="cfg",
            artifact_digest="artifact-2",
            stage_fingerprint="fp-2",
            expected_seq=3,
        )
        duplicate = dict(first)
        duplicate["seq"] = 3
        duplicate["stage"] = "postings"
        duplicate["stage_number"] = 3
        duplicate["record_digest"] = _record_digest(duplicate)
        atomic_write_json(checkpoint_dir / "00000003-postings.json", duplicate)
        active, ignored = self.store.active()
        self.assertEqual([record["stage"] for record in active], ["tokenize"])
        self.assertTrue(any(item["reason"] == "duplicate_sequence" for item in ignored))

    def test_atomic_publish_has_no_partial_temp_record(self) -> None:
        self._publish_stage1()
        leftovers = list((self.artifacts / "checkpoints").glob("*.tmp")) + list(
            (self.artifacts / "checkpoints").glob(".*.tmp")
        )
        self.assertEqual(leftovers, [])

    def test_real_multiprocess_publish_race(self) -> None:
        self._publish_stage1()
        context = multiprocessing.get_context("spawn")
        queue = context.Queue()
        processes = [
            context.Process(target=_competing_publish, args=(str(self.artifacts), queue))
            for _ in range(2)
        ]
        for process in processes:
            process.start()
        for process in processes:
            process.join(timeout=20)
            self.assertEqual(process.exitcode, 0)
        outcomes = [queue.get(timeout=5), queue.get(timeout=5)]
        published = [outcome for outcome in outcomes if outcome["result"] == "published"]
        stale = [outcome for outcome in outcomes if outcome.get("code") == "E_STALE_CHECKPOINT"]
        self.assertEqual(len(published), 1)
        self.assertEqual(len(stale), 1)


if __name__ == "__main__":
    unittest.main()
