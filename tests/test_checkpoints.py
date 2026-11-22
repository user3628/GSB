from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from gspipe.artifacts import ArtifactStore
from gspipe.canonical import sha256_json
from gspipe.checkpoints import CheckpointStore, build_checkpoint_record
from gspipe.errors import (
    E_INVALID_STAGE,
    E_IO,
    E_STALE_CHECKPOINT,
    PipelineError,
)
from gspipe.fsutil import atomic_write_bytes


class CheckpointTests(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path(tempfile.mkdtemp(prefix="gspipe-checkpoint-"))
        self.artifacts = ArtifactStore(self.root)
        self.store = CheckpointStore(self.root, self.artifacts)

    def record(
        self,
        *,
        task_id: str = "task_a",
        stage: str = "parse",
        seq: int = 1,
        generation: int = 0,
        dependencies: dict[str, str] | None = None,
        label: str = "artifact",
    ) -> dict:
        ref = self.artifacts.put_json({"task_id": task_id, "stage": stage, "seq": seq, "label": label})
        components = {"task_id": task_id, "stage": stage, "seq": seq, "label": label}
        return build_checkpoint_record(
            task_id=task_id,
            stage=stage,
            seq=seq,
            generation=generation,
            input_fingerprint=sha256_json(components),
            config_version="cfg-v1",
            fingerprint_components=components,
            artifact_digest=ref.digest,
            artifact_type=ref.kind,
            artifact_path=ref.relative_path,
            dependencies=dependencies or {},
        )

    def test_publish_continuous_sequence(self) -> None:
        parse = self.store.publish("task_a", self.record(stage="parse", seq=1))
        execute = self.store.publish(
            "task_a",
            self.record(
                stage="execute",
                seq=2,
                dependencies={"parse": parse["artifact_digest"]},
            ),
        )
        verify = self.store.publish(
            "task_a",
            self.record(
                stage="verify",
                seq=3,
                dependencies={"execute": execute["artifact_digest"]},
            ),
        )
        state = self.store.state("task_a")
        self.assertEqual(state.last_seq, 3)
        self.assertTrue(state.is_valid("verify"))
        self.assertEqual(state.latest("verify")["artifact_digest"], verify["artifact_digest"])

    def test_duplicate_and_cross_task_are_stale(self) -> None:
        record = self.record()
        self.store.publish("task_a", record)
        with self.assertRaises(PipelineError) as ctx:
            self.store.publish("task_a", record)
        self.assertEqual(ctx.exception.code, E_STALE_CHECKPOINT)
        self.assertEqual(ctx.exception.details["reason"], "stale_sequence")

        other = self.record(task_id="task_b", seq=1)
        with self.assertRaises(PipelineError) as ctx:
            self.store.publish("task_a", other)
        self.assertEqual(ctx.exception.code, E_STALE_CHECKPOINT)
        self.assertEqual(ctx.exception.details["reason"], "cross_task_publish")

    def test_illegal_stage_jump_and_missing_predecessor(self) -> None:
        with self.assertRaises(PipelineError) as ctx:
            self.store.publish("task_new", self.record(task_id="task_new", stage="verify", seq=1))
        self.assertEqual(ctx.exception.code, E_INVALID_STAGE)

        parse = self.store.publish("task_a", self.record(stage="parse", seq=1))
        with self.assertRaises(PipelineError) as ctx:
            self.store.publish(
                "task_a",
                self.record(stage="verify", seq=2, dependencies={"parse": parse["artifact_digest"]}),
            )
        self.assertEqual(ctx.exception.code, E_INVALID_STAGE)
        self.assertEqual(ctx.exception.details["reason"], "stage_order")

    def test_sequence_gap_is_stale(self) -> None:
        parse = self.store.publish("task_a", self.record(stage="parse", seq=1))
        with self.assertRaises(PipelineError) as ctx:
            self.store.publish(
                "task_a",
                self.record(stage="execute", seq=3, dependencies={"parse": parse["artifact_digest"]}),
            )
        self.assertEqual(ctx.exception.code, E_STALE_CHECKPOINT)
        self.assertEqual(ctx.exception.details["reason"], "sequence_gap")

    def test_missing_predecessor_artifact(self) -> None:
        parse = self.store.publish("task_a", self.record(stage="parse", seq=1))
        parse_path = self.root / parse["artifact_path"]
        parse_path.unlink()
        with self.assertRaises(PipelineError) as ctx:
            self.store.publish(
                "task_a",
                self.record(stage="execute", seq=2, dependencies={"parse": parse["artifact_digest"]}),
            )
        self.assertEqual(ctx.exception.code, E_INVALID_STAGE)
        self.assertEqual(ctx.exception.details["reason"], "missing_predecessor_artifact")

    def test_rebuild_generation_can_reuse_valid_upstream(self) -> None:
        parse = self.store.publish("task_a", self.record(stage="parse", seq=1))
        execute = self.store.publish(
            "task_a",
            self.record(stage="execute", seq=2, dependencies={"parse": parse["artifact_digest"]}),
        )
        verify = self.store.publish(
            "task_a",
            self.record(stage="verify", seq=3, dependencies={"execute": execute["artifact_digest"]}),
        )
        self.store.publish(
            "task_a",
            self.record(stage="export", seq=4, dependencies={"verify": verify["artifact_digest"]}),
        )
        rebuilt_execute = self.store.publish(
            "task_a",
            self.record(
                stage="execute",
                seq=5,
                generation=1,
                dependencies={"parse": parse["artifact_digest"]},
                label="rebuilt",
            ),
        )
        state = self.store.state("task_a")
        self.assertEqual(state.generation, 1)
        self.assertFalse(state.is_valid("verify"))
        self.assertEqual(state.latest("execute")["artifact_digest"], rebuilt_execute["artifact_digest"])

    def test_atomic_write_preserves_old_file_on_replace_failure(self) -> None:
        target = self.root / "record.json"
        atomic_write_bytes(target, b'{"version":1}\n')
        with mock.patch("gspipe.fsutil._replace_with_retry", side_effect=PermissionError("busy")):
            with self.assertRaises(PipelineError) as ctx:
                atomic_write_bytes(target, b'{"version":2}\n')
        self.assertEqual(ctx.exception.code, E_IO)
        self.assertEqual(target.read_bytes(), b'{"version":1}\n')
        self.assertEqual(list(target.parent.glob(f".{target.name}.*.tmp")), [])

    def test_os_file_lock_across_processes(self) -> None:
        script = r"""
import json, sys
from gspipe.artifacts import ArtifactStore
from gspipe.canonical import sha256_json
from gspipe.checkpoints import CheckpointStore, build_checkpoint_record
from gspipe.errors import PipelineError
root, worker = sys.argv[1], sys.argv[2]
artifacts = ArtifactStore(root)
store = CheckpointStore(root, artifacts)
components = {"worker": worker}
ref = artifacts.put_json({"worker": worker})
record = build_checkpoint_record(
    task_id="task_lock", stage="parse", seq=1, generation=0,
    input_fingerprint=sha256_json(components), config_version="cfg",
    fingerprint_components=components, artifact_digest=ref.digest,
    artifact_type=ref.kind, artifact_path=ref.relative_path, dependencies={})
try:
    store.publish("task_lock", record)
    print(json.dumps({"ok": True, "worker": worker}))
except PipelineError as exc:
    print(json.dumps({"ok": False, "worker": worker, "code": exc.code, "details": exc.details}))
"""
        env = dict(os.environ)
        repo_root = str(Path(__file__).resolve().parents[1])
        env["PYTHONPATH"] = repo_root + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
        processes = [
            subprocess.Popen(
                [sys.executable, "-c", script, str(self.root), f"w{index}"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                env=env,
                cwd=repo_root,
            )
            for index in range(4)
        ]
        results = []
        for process in processes:
            stdout, stderr = process.communicate(timeout=30)
            self.assertEqual(process.returncode, 0, msg=stderr)
            results.append(json.loads(stdout.strip().splitlines()[-1]))
        successful = [result for result in results if result["ok"]]
        stale = [result for result in results if not result["ok"]]
        self.assertEqual(len(successful), 1)
        self.assertEqual(len(stale), 3)
        self.assertTrue(all(result["code"] == E_STALE_CHECKPOINT for result in stale))


if __name__ == "__main__":
    unittest.main()
