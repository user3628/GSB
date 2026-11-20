from __future__ import annotations

import multiprocessing
import tempfile
import unittest
from pathlib import Path
from typing import Any

from eval_engine.engine import Engine
from eval_engine.errors import EngineError
from eval_engine.generator import generate_workspace
from eval_engine.ranking import run_stage4
from tests.helpers import read_json, write_config


def _publish_worker(
    workspace: str,
    artifact: dict[str, Any],
    barrier: Any,
    result_queue: Any,
) -> None:
    try:
        engine = Engine(workspace)
        context = engine.store.load_context()
        barrier.wait(timeout=15)
        checkpoint = engine.store.publish_stage(4, 5, artifact, context)
        result_queue.put(("ok", checkpoint["checkpoint_seq"]))
    except EngineError as exc:
        result_queue.put(("error", exc.code))
    except Exception as exc:  # pragma: no cover - diagnostic for process failures
        result_queue.put(("exception", repr(exc)))


class ConcurrencyTests(unittest.TestCase):
    def test_concurrent_publishers_only_one_advances_sequence(self) -> None:
        with tempfile.TemporaryDirectory() as temp_name:
            root = Path(temp_name)
            generate_workspace(root, force=True)
            engine = Engine(root)
            engine.build()
            config = read_json(root / "configs" / "engine.json")
            config["ranking"]["top_k"] = 1
            write_config(root, config)
            context = engine.store.load_context()
            stage3 = engine.store.load_artifact(3)
            stage4 = run_stage4(
                stage3, context.config, context.stage_dependencies[4]
            )

            process_context = multiprocessing.get_context("spawn")
            barrier = process_context.Barrier(2)
            queue = process_context.Queue()
            processes = [
                process_context.Process(
                    target=_publish_worker,
                    args=(str(root), stage4, barrier, queue),
                )
                for _ in range(2)
            ]
            for process in processes:
                process.start()
            for process in processes:
                process.join(timeout=30)
                self.assertFalse(process.is_alive())
            results = [queue.get(timeout=5) for _ in range(2)]
            queue.close()
            self.assertEqual([result[0] for result in results].count("ok"), 1)
            stale = [result for result in results if result[0] == "error"]
            self.assertEqual(len(stale), 1)
            self.assertEqual(stale[0][1], "E_STALE_CHECKPOINT")
            self.assertEqual(engine.store.inspect()["last_checkpoint_seq"], 5)


if __name__ == "__main__":
    unittest.main()
