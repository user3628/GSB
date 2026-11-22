from __future__ import annotations

import json
import multiprocessing as mp
import tempfile
import unittest
from pathlib import Path
from typing import Any

from image_workbench.checkpoint import CheckpointStore
from image_workbench.errors import (
    E_INVALID_STAGE,
    E_STALE_CHECKPOINT,
    WorkbenchError,
)
from image_workbench.util import atomic_write_bytes, sha256_bytes


def _payload(workspace: Path, stage: str, fingerprint: str, upstream: str | None) -> dict[str, Any]:
    suffix = ".png" if stage in {"executed", "exported"} else ".json"
    artifact = workspace / "artifacts" / stage / f"t1{suffix}"
    data = f"artifact-{stage}".encode("utf-8")
    atomic_write_bytes(artifact, data)
    return {
        "input_digest": "input-digest",
        "config_version": "config-version",
        "stage_fingerprint": fingerprint,
        "upstream_fingerprint": upstream,
        "fingerprint_components": {"stage": stage},
        "artifact_path": artifact.relative_to(workspace).as_posix(),
        "artifact_sha256": sha256_bytes(data),
        "output_digest": f"output-{stage}",
        "output_size": [10, 10],
        "ok": True,
        "error": None,
    }


def _concurrent_publish(workspace: str, barrier, queue) -> None:
    store = CheckpointStore(Path(workspace), "t1")
    payload = _payload(Path(workspace), "parsed", "fp-1", None)
    barrier.wait()
    try:
        record = store.publish("parsed", payload, expected_seq=1)
        queue.put(("ok", record["seq"]))
    except WorkbenchError as exc:
        queue.put((exc.code, None))


class CheckpointTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.workspace = Path(self.temp.name)
        self.store = CheckpointStore(self.workspace, "t1")

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_sequence_stale_and_invalid_stage(self) -> None:
        first = self.store.publish(
            "parsed", _payload(self.workspace, "parsed", "fp-1", None), expected_seq=1
        )
        self.assertEqual(first["seq"], 1)
        with self.assertRaises(WorkbenchError) as stale:
            self.store.publish(
                "parsed",
                _payload(self.workspace, "parsed", "fp-1", None),
                expected_seq=1,
            )
        self.assertEqual(stale.exception.code, E_STALE_CHECKPOINT)

        other = CheckpointStore(self.workspace, "t1-other")
        with self.assertRaises(WorkbenchError) as invalid:
            other.publish(
                "executed",
                _payload(self.workspace, "executed", "fp-exec", "fp-1"),
                expected_seq=1,
            )
        self.assertEqual(invalid.exception.code, E_INVALID_STAGE)

    def test_predecessor_artifact_and_cross_task_artifact(self) -> None:
        parsed = self.store.publish(
            "parsed", _payload(self.workspace, "parsed", "fp-1", None), expected_seq=1
        )
        artifact = self.workspace / parsed["artifact_path"]
        artifact.unlink()
        with self.assertRaises(WorkbenchError) as missing:
            self.store.publish(
                "executed",
                _payload(self.workspace, "executed", "fp-exec", "fp-1"),
                expected_seq=2,
            )
        self.assertEqual(missing.exception.code, E_INVALID_STAGE)

        wrong_workspace = Path(self.temp.name) / "other"
        other_store = CheckpointStore(wrong_workspace, "t2")
        wrong_artifact = wrong_workspace / "artifacts" / "parsed" / "t1.json"
        atomic_write_bytes(wrong_artifact, b"wrong-task")
        payload = {
            "input_digest": "i",
            "config_version": "c",
            "stage_fingerprint": "fp",
            "upstream_fingerprint": None,
            "fingerprint_components": {},
            "artifact_path": wrong_artifact.relative_to(wrong_workspace).as_posix(),
            "artifact_sha256": sha256_bytes(b"wrong-task"),
            "output_digest": None,
            "output_size": None,
            "ok": True,
            "error": None,
        }
        with self.assertRaises(WorkbenchError) as cross:
            other_store.publish("parsed", payload, expected_seq=1)
        self.assertEqual(cross.exception.code, E_STALE_CHECKPOINT)

    def test_corrupt_and_duplicate_records_are_ignored_and_repaired(self) -> None:
        self.store.publish(
            "parsed", _payload(self.workspace, "parsed", "fp-1", None), expected_seq=1
        )
        (self.store.directory / "seq-00000002-executed.json").write_text(
            "{not valid json", encoding="utf-8"
        )
        (self.store.directory / "seq-00000003-size_checked.json").write_text(
            json.dumps({"seq": 3}), encoding="utf-8"
        )
        self.assertEqual([record["seq"] for record in self.store.read_consistent()], [1])
        repaired = self.store.publish(
            "executed",
            _payload(self.workspace, "executed", "fp-exec", "fp-1"),
            expected_seq=2,
        )
        self.assertEqual(repaired["seq"], 2)
        self.assertEqual([record["seq"] for record in self.store.read_consistent()], [1, 2])
        self.assertTrue(
            any((self.workspace / "checkpoints" / "_quarantine" / "t1").glob("*.json"))
        )

        duplicate_workspace = Path(self.temp.name) / "duplicate"
        duplicate_store = CheckpointStore(duplicate_workspace, "t1")
        record = duplicate_store.publish(
            "parsed",
            _payload(duplicate_workspace, "parsed", "fp-dup", None),
            expected_seq=1,
        )
        source = duplicate_store._filename(1, "parsed")
        duplicate = duplicate_store._filename(1, "executed")
        duplicate.write_bytes(source.read_bytes())
        self.assertEqual(duplicate_store.read_consistent(), [])
        repaired_duplicate = duplicate_store.publish(
            "parsed",
            _payload(duplicate_workspace, "parsed", "fp-dup", None),
            expected_seq=1,
        )
        self.assertEqual(repaired_duplicate["seq"], 1)
        self.assertEqual(len(duplicate_store.read_consistent()), 1)
        self.assertIsNotNone(record)

    def test_real_file_lock_allows_one_concurrent_sequence_publisher(self) -> None:
        context = mp.get_context("spawn")
        barrier = context.Barrier(4)
        queue = context.Queue()
        processes = [
            context.Process(target=_concurrent_publish, args=(str(self.workspace), barrier, queue))
            for _ in range(4)
        ]
        for process in processes:
            process.start()
        for process in processes:
            process.join(20)
        outcomes = [queue.get(timeout=5) for _ in range(4)]
        successes = [outcome for outcome in outcomes if outcome[0] == "ok"]
        stale = [outcome for outcome in outcomes if outcome[0] == E_STALE_CHECKPOINT]
        self.assertEqual(len(successes), 1)
        self.assertEqual(successes[0][1], 1)
        self.assertEqual(len(stale), 3)


if __name__ == "__main__":
    unittest.main()
