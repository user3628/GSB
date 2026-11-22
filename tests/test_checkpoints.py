from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from pipeline.checkpoints import CheckpointManager
from pipeline.errors import (
    E_INVALID_STAGE,
    E_STALE_CHECKPOINT,
    PipelineError,
)
from pipeline.util import sha256_file


def _artifact(workspace: Path, relative: str, batch_id: str) -> None:
    path = workspace / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({"batch_id": batch_id, "payload": "ok"}, sort_keys=True) + "\n",
        encoding="utf-8",
    )


class CheckpointTests(unittest.TestCase):
    def test_stage_order_duplicate_and_cross_batch_errors(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            workspace = Path(temporary)
            manager = CheckpointManager(workspace)
            with self.assertRaises(PipelineError) as jump:
                _artifact(workspace, "calibration/b1.json", "b1")
                manager.publish(
                    "b1",
                    "calibration",
                    "fp-cal",
                    "v1",
                    [{"path": "calibration/b1.json", "digest": sha256_file(workspace / "calibration/b1.json")}],
                )
            self.assertEqual(jump.exception.code, E_INVALID_STAGE)
            _artifact(workspace, "features/b1.jsonl", "b1")
            feature_path = workspace / "features/b1.jsonl"
            feature_digest = sha256_file(feature_path)
            manager.publish(
                "b1",
                "features",
                "fp-feature",
                "v1",
                [{"path": "features/b1.jsonl", "digest": feature_digest}],
            )
            with self.assertRaises(PipelineError) as duplicate:
                manager.publish(
                    "b1",
                    "features",
                    "fp-feature-2",
                    "v1",
                    [{"path": "features/b1.jsonl", "digest": feature_digest}],
                )
            self.assertEqual(duplicate.exception.code, E_STALE_CHECKPOINT)
            _artifact(workspace, "models/b2.json", "b2")
            model_digest = sha256_file(workspace / "models/b2.json")
            with self.assertRaises(PipelineError) as cross_batch:
                manager.publish(
                    "b1",
                    "train_score",
                    "fp-train",
                    "v1",
                    [{"path": "models/b2.json", "digest": model_digest}],
                )
            self.assertEqual(cross_batch.exception.code, E_STALE_CHECKPOINT)
            self.assertEqual(manager.verify_integrity()["last_sequence"], 1)
            with self.assertRaises(PipelineError) as stale_sequence:
                manager.publish(
                    "b1",
                    "train_score",
                    "fp-train",
                    "v1",
                    [{"path": "features/b1.jsonl", "digest": feature_digest}],
                    expected_sequence=99,
                )
            self.assertEqual(stale_sequence.exception.code, E_STALE_CHECKPOINT)

    def test_missing_prerequisite_artifact_is_invalid_stage(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            workspace = Path(temporary)
            manager = CheckpointManager(workspace)
            _artifact(workspace, "features/b1.jsonl", "b1")
            feature_digest = sha256_file(workspace / "features/b1.jsonl")
            manager.publish(
                "b1",
                "features",
                "fp-feature",
                "v1",
                [{"path": "features/b1.jsonl", "digest": feature_digest}],
            )
            (workspace / "features/b1.jsonl").unlink()
            _artifact(workspace, "scores/b1.jsonl", "b1")
            score_digest = sha256_file(workspace / "scores/b1.jsonl")
            with self.assertRaises(PipelineError) as missing:
                manager.publish(
                    "b1",
                    "train_score",
                    "fp-train",
                    "v1",
                    [{"path": "scores/b1.jsonl", "digest": score_digest}],
                )
            self.assertEqual(missing.exception.code, E_INVALID_STAGE)

    def test_atomic_failure_leaves_no_half_checkpoint(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            workspace = Path(temporary)
            manager = CheckpointManager(workspace)
            _artifact(workspace, "features/b1.jsonl", "b1")
            digest = sha256_file(workspace / "features/b1.jsonl")
            with patch("pipeline.util.os.fsync", side_effect=OSError("injected fsync failure")):
                with self.assertRaises(OSError):
                    manager.publish(
                        "b1",
                        "features",
                        "fp-feature",
                        "v1",
                        [{"path": "features/b1.jsonl", "digest": digest}],
                    )
            self.assertEqual(manager.state()["sequence"], 0)
            self.assertEqual(list((workspace / "checkpoints" / "records").glob("[0-9]*.json")), [])

    def test_two_processes_cannot_publish_same_stage(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            workspace = Path(temporary)
            _artifact(workspace, "features/b1.jsonl", "b1")
            command = [
                sys.executable,
                "-m",
                "pipeline.cli",
                "checkpoint-publish",
                "--workspace",
                str(workspace),
                "--batch-id",
                "b1",
                "--stage",
                "features",
                "--artifact",
                "features/b1.jsonl",
                "--input-fingerprint",
                "fp-feature",
                "--config-version",
                "v1",
            ]
            second = subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                cwd=str(Path(__file__).resolve().parents[1]),
            )
            first = subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                cwd=str(Path(__file__).resolve().parents[1]),
            )
            first_output, first_error = first.communicate(timeout=20)
            second_output, second_error = second.communicate(timeout=20)
            outputs = [first_output, second_output]
            statuses = sorted([first.returncode, second.returncode])
            self.assertEqual(statuses, [0, 2], msg=f"{outputs} {first_error} {second_error}")
            failure = json.loads(outputs[1] if first.returncode == 0 else outputs[0])
            self.assertEqual(failure["error_code"], E_STALE_CHECKPOINT)
            manager = CheckpointManager(workspace)
            self.assertEqual(manager.verify_integrity()["record_count"], 1)


if __name__ == "__main__":
    unittest.main()
