import multiprocessing as mp
import tempfile
import unittest
from pathlib import Path

from detpipe.config import PipelineConfig
from detpipe.generate import generate_dataset
from detpipe.pipeline import Pipeline
from detpipe.validator import validate_run


def _publish_worker(dataset, root: str, index: int) -> None:
    pipe = Pipeline(dataset, PipelineConfig(workers=1, shard_size=2), root)
    # Every process races to publish the same shard 0; only one may win.
    plan = pipe.plans[index]
    record = pipe.build_record(
        plan, pipe.process_shard(plan), "processed", worker_id=0
    )
    try:
        pipe.store.publish(record)
    except Exception:
        # Losing racers are rejected with E_STALE_CHECKPOINT; the invariant
        # is verified by the parent process afterwards.
        pass


class TestConcurrentPublish(unittest.TestCase):
    def test_duplicate_publish_across_processes(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        root = str(Path(tmp.name) / "ws")
        dataset = generate_dataset()
        # Reinitialize an empty workspace manifest for a controlled race.
        pipe = Pipeline(dataset, PipelineConfig(workers=1, shard_size=2), root)
        pipe.store.write_manifest(pipe.build_manifest())

        ctx = mp.get_context("spawn")
        procs = [
            ctx.Process(target=_publish_worker, args=(dataset, root, 0))
            for _ in range(3)
        ]
        for proc in procs:
            proc.start()
        for proc in procs:
            proc.join()
            self.assertEqual(proc.exitcode, 0)

        store = pipe.store
        self.assertEqual(store.existing_indices(), [0])
        report = validate_run(root)
        self.assertTrue(report["chain_contiguous"])
        self.assertEqual(report["invalid_shards"], [])


if __name__ == "__main__":
    unittest.main()
