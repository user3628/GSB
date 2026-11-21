from __future__ import annotations

import multiprocessing
import tempfile
import unittest
from pathlib import Path
from typing import Any

from pose_pipeline.constants import ERROR_STALE_CHECKPOINT
from pose_pipeline.errors import PipelineError
from pose_pipeline.store import CheckpointStore


def _publish_racer(workspace: str, queue: Any) -> None:
    store = CheckpointStore(workspace)
    try:
        record = store.publish(
            shard_id="shard_01",
            stage="preprocess",
            input_fingerprint="a" * 64,
            config_version="b" * 64,
            artifact_digest="c" * 64,
            upstream_digest=None,
            expected_sequence=1,
            expected_shard_id="shard_01",
            expected_stage="preprocess",
        )
        queue.put(("ok", record["sequence"]))
    except PipelineError as exc:
        queue.put(("error", exc.code))


class ConcurrencyTests(unittest.TestCase):
    def test_cross_process_publish_race_rejects_stale_worker(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            workspace = str(Path(temporary) / "workspace")
            CheckpointStore(workspace).ensure()
            try:
                context = multiprocessing.get_context("fork")
            except ValueError:  # pragma: no cover - Windows
                context = multiprocessing.get_context("spawn")
            queue = context.Queue()
            workers = [
                context.Process(target=_publish_racer, args=(workspace, queue))
                for _ in range(2)
            ]
            for worker in workers:
                worker.start()
            outcomes = [queue.get(timeout=20) for _ in workers]
            for worker in workers:
                worker.join(timeout=20)
                self.assertEqual(worker.exitcode, 0)
            self.assertEqual(sum(1 for status, _ in outcomes if status == "ok"), 1)
            errors = [value for status, value in outcomes if status == "error"]
            self.assertEqual(errors, [ERROR_STALE_CHECKPOINT])
            scan = CheckpointStore(workspace).scan()
            self.assertEqual([record["sequence"] for record in scan.records], [1])
            self.assertEqual(scan.diagnostics["next_sequence"], 2)


if __name__ == "__main__":
    unittest.main()
