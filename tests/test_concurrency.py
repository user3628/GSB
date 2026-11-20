from __future__ import annotations

import multiprocessing as mp
import os
import tempfile
import unittest

from crwbench.checkpoints import CheckpointStore, STAGES
from crwbench.errors import E_STALE_CHECKPOINT


def _publisher(workdir: str, stage: str, expected_seq: int, queue):
    store = CheckpointStore(workdir)
    try:
        record = store.publish(
            stage=stage,
            input_digest="input",
            config_fingerprint="cfg",
            config_version="v1",
            artifact_path=f"{stage}.json",
            artifact={"stage": stage, "payload": list(range(100))},
            expected_seq=expected_seq,
        )
        queue.put(("ok", record["seq"], stage))
    except Exception as exc:  # noqa: BLE001
        queue.put(("error", getattr(exc, "code", "E_UNKNOWN"), stage))


class TestConcurrency(unittest.TestCase):
    def test_parallel_publish_only_contiguous_sequence_wins(self):
        with tempfile.TemporaryDirectory() as workdir:
            ctx = mp.get_context("fork")
            queue = ctx.Queue()
            # Four processes race to publish stage "features" at expected seq 1,
            # while only a stage-1 "normalize" record exists first.
            store = CheckpointStore(workdir)
            store.publish(
                stage="normalize",
                input_digest="input",
                config_fingerprint="cfg",
                config_version="v1",
                artifact_path="normalize.json",
                artifact={"k": "v"},
                expected_seq=0,
            )
            processes = [
                ctx.Process(target=_publisher, args=(workdir, "features", 1, queue))
                for _ in range(4)
            ]
            for proc in processes:
                proc.start()
            for proc in processes:
                proc.join()
            outcomes = [queue.get() for _ in processes]
            ok = [o for o in outcomes if o[0] == "ok"]
            errors = [o for o in outcomes if o[0] == "error"]
            self.assertEqual(len(ok), 1)
            self.assertEqual(ok[0][1], 2)
            self.assertTrue(all(o[1] == E_STALE_CHECKPOINT for o in errors))
            records = store.load_records()
            self.assertEqual([r["seq"] for r in records], [1, 2])
            self.assertEqual([r["stage"] for r in records], ["normalize", "features"])

    def test_backward_and_cross_stage_publish_rejected(self):
        with tempfile.TemporaryDirectory() as workdir:
            store = CheckpointStore(workdir)
            store.publish(
                stage="normalize",
                input_digest="input",
                config_fingerprint="cfg",
                config_version="v1",
                artifact_path="normalize.json",
                artifact={},
                expected_seq=0,
            )
            with self.assertRaises(Exception) as ctx:
                store.publish(
                    stage="normalize",
                    input_digest="input",
                    config_fingerprint="cfg",
                    config_version="v1",
                    artifact_path="again.json",
                    artifact={},
                    expected_seq=1,
                )
            self.assertEqual(ctx.exception.code, E_STALE_CHECKPOINT)


if __name__ == "__main__":
    unittest.main(verbosity=2)
