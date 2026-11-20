from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from eval_engine import store as store_module
from eval_engine.config import load_config
from eval_engine.engine import Engine, StopController
from eval_engine.errors import EngineError
from eval_engine.generator import generate_workspace
from eval_engine.ranking import run_stage4
from eval_engine.util import canonical_bytes, sha256_bytes
from tests.helpers import read_json, write_config


class RecoveryAndErrorTests(unittest.TestCase):
    def test_stop_at_stage_boundary_resume_matches_full_build(self) -> None:
        with tempfile.TemporaryDirectory() as temp_name:
            base = Path(temp_name)
            incremental_root = base / "incremental"
            full_root = base / "full"
            generate_workspace(incremental_root, force=True)
            generate_workspace(full_root, force=True)
            engine = Engine(incremental_root)
            stopped = engine.build(stop_controller=StopController(after_stage=2))
            self.assertEqual(stopped.status, "stopped")
            self.assertEqual(stopped.stopped_stage, 3)
            self.assertEqual(
                [item["checkpoint_seq"] for item in engine.store.checkpoints()], [1, 2]
            )
            self.assertEqual(engine.store.inspect()["earliest_invalid_stage"], 3)

            resumed = engine.build()
            self.assertEqual(resumed.status, "completed")
            self.assertEqual(
                [(item["checkpoint_seq"], item["stage"]) for item in resumed.checkpoints_published],
                [(3, 3), (4, 4)],
            )
            full_engine = Engine(full_root)
            full_engine.build()
            self.assertEqual(
                engine.store.artifact_path(4).read_bytes(),
                full_engine.store.artifact_path(4).read_bytes(),
            )
            self.assertEqual(
                [item["checkpoint_seq"] for item in engine.store.checkpoints()],
                [1, 2, 3, 4],
            )
            self.assertEqual(engine.verify_consistency()["status"], "consistent")

    def test_interruption_before_checkpoint_leaves_no_half_record(self) -> None:
        with tempfile.TemporaryDirectory() as temp_name:
            base = Path(temp_name)
            incremental_root = base / "interrupted"
            full_root = base / "full"
            generate_workspace(incremental_root, force=True)
            generate_workspace(full_root, force=True)
            engine = Engine(incremental_root)
            original_write = store_module.atomic_write_json
            failed = {"value": False}

            def fail_first_checkpoint(path, value, *args, **kwargs):
                path = Path(path)
                if path.parent == engine.store.checkpoint_dir and not failed["value"]:
                    failed["value"] = True
                    raise OSError("simulated interruption")
                return original_write(path, value, *args, **kwargs)

            with mock.patch.object(store_module, "atomic_write_json", side_effect=fail_first_checkpoint):
                with self.assertRaises(OSError):
                    engine.build()
            self.assertEqual(engine.store.checkpoints(), [])
            self.assertEqual(list(engine.store.checkpoint_dir.glob("*.json")), [])
            self.assertFalse(list(engine.store.checkpoint_dir.glob("*.tmp")))
            self.assertEqual(engine.store.inspect()["earliest_invalid_stage"], 1)

            resumed = engine.build()
            self.assertEqual(resumed.status, "completed")
            self.assertEqual(
                [item["checkpoint_seq"] for item in engine.store.checkpoints()],
                [1, 2, 3, 4],
            )
            full_engine = Engine(full_root)
            full_engine.build()
            self.assertEqual(
                engine.store.artifact_path(4).read_bytes(),
                full_engine.store.artifact_path(4).read_bytes(),
            )

    def test_stale_duplicate_and_cross_stage_publication(self) -> None:
        with tempfile.TemporaryDirectory() as temp_name:
            root = Path(temp_name)
            generate_workspace(root, force=True)
            engine = Engine(root)
            engine.build()
            context = engine.store.load_context()
            stage4 = engine.store.load_artifact(4)
            with self.assertRaises(EngineError) as duplicate:
                engine.store.publish_stage(4, 4, stage4, context)
            self.assertEqual(duplicate.exception.code, "E_STALE_CHECKPOINT")

            config = read_json(root / "configs" / "engine.json")
            config["ranking"]["top_k"] = 1
            write_config(root, config)
            new_context = engine.store.load_context()
            stage3 = engine.store.load_artifact(3)
            with self.assertRaises(EngineError) as crossed:
                engine.store.publish_stage(3, 5, stage3, new_context)
            self.assertEqual(crossed.exception.code, "E_STALE_CHECKPOINT")
            self.assertEqual(crossed.exception.details["expected_stage"], 4)

            new_stage4 = run_stage4(
                stage3,
                new_context.config,
                new_context.stage_dependencies[4],
            )
            checkpoint = engine.store.publish_stage(4, 5, new_stage4, new_context)
            self.assertEqual(checkpoint["checkpoint_seq"], 5)
            with self.assertRaises(EngineError) as repeated:
                engine.store.publish_stage(4, 5, new_stage4, new_context)
            self.assertEqual(repeated.exception.code, "E_STALE_CHECKPOINT")

    def test_invalid_stage_missing_prerequisite_and_unknown_benchmark(self) -> None:
        with tempfile.TemporaryDirectory() as temp_name:
            root = Path(temp_name)
            generate_workspace(root, force=True)
            engine = Engine(root)
            with self.assertRaises(EngineError) as missing:
                engine.query_stage(2)
            self.assertEqual(missing.exception.code, "E_INVALID_STAGE")
            with self.assertRaises(EngineError) as invalid:
                engine.query_stage("not-a-stage")
            self.assertEqual(invalid.exception.code, "E_INVALID_STAGE")
            with self.assertRaises(EngineError) as benchmark:
                engine.update_answers("missing-benchmark", {})
            self.assertEqual(benchmark.exception.code, "E_INVALID_STAGE")

            command = [
                sys.executable,
                "-m",
                "eval_engine",
                "query",
                "--workspace",
                str(root),
                "--stage",
                "2",
            ]
            completed = subprocess.run(
                command,
                cwd=Path(__file__).resolve().parents[1],
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(completed.returncode, 64)
            payload = json.loads(completed.stdout)
            self.assertEqual(payload["code"], "E_INVALID_STAGE")
            self.assertEqual(payload["code"], missing.exception.code)


if __name__ == "__main__":
    unittest.main()
