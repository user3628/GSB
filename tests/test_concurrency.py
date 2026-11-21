"""Concurrent stage publishing: real locks, sequence enforcement (req. 7)."""

from __future__ import annotations

import tempfile
import threading
import unittest
from pathlib import Path

from voxel_engine.checkpoints import CheckpointStore
from voxel_engine.errors import E_STALE_CHECKPOINT, EngineError

from tests.helpers import make_dataset, make_engine


def _publish(store, instance, seq):
    return store.publish(
        stage="projection",
        instance=instance,
        view_id=None,
        fingerprint=f"fp-{instance}",
        input_fingerprint="in",
        config_version="cfg",
        config={},
        upstream_fingerprints={},
        artifact_bytes=b"payload-" + instance.encode(),
        artifact_rel_path=f"artifacts/{instance}.bin",
        expected_prev_seq=seq,
        lock_timeout_ms=5000,
    )


class ConcurrencyTest(unittest.TestCase):
    def test_concurrent_same_seq_publish_single_winner(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = CheckpointStore(Path(tmp) / "ws")
            results = []
            errors = []
            barrier = threading.Barrier(6)

            def worker(idx):
                barrier.wait()
                try:
                    results.append(_publish(store, f"projection:v{idx}", 0))
                except EngineError as exc:
                    errors.append(exc)

            threads = [threading.Thread(target=worker, args=(i,)) for i in range(6)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
            self.assertEqual(len(results), 1)
            self.assertEqual(len(errors), 5)
            for error in errors:
                self.assertEqual(error.code, E_STALE_CHECKPOINT)
            chain, ignored, truncation = store.scan()
            self.assertEqual(len(chain), 1)
            self.assertEqual(ignored, [])

    def test_concurrent_pipeline_builds_one_stale(self):
        with tempfile.TemporaryDirectory() as tmp:
            data = make_dataset(Path(tmp) / "data")
            workspace = Path(tmp) / "ws"
            engine_a = make_engine(workspace, data)
            engine_b = make_engine(workspace, data)
            outcomes = []
            barrier = threading.Barrier(2)

            def build(engine):
                barrier.wait()
                try:
                    report = engine.build(mode="incremental")
                    outcomes.append(("ok", report))
                except EngineError as exc:
                    outcomes.append(("error", exc))

            threads = [
                threading.Thread(target=build, args=(engine_a,)),
                threading.Thread(target=build, args=(engine_b,)),
            ]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
            kinds = sorted(kind for kind, _ in outcomes)
            self.assertEqual(kinds, ["error", "ok"])
            error = next(payload for kind, payload in outcomes if kind == "error")
            self.assertEqual(error.code, E_STALE_CHECKPOINT)
            ok_report = next(payload for kind, payload in outcomes if kind == "ok")
            self.assertEqual(ok_report["status"], "completed")
            status = engine_a.status()
            for stage in status["stages"]:
                self.assertEqual(stage["status"], "published")


if __name__ == "__main__":
    unittest.main()
