from __future__ import annotations

import multiprocessing
import unittest
from pathlib import Path

from dimcluster.errors import WorkbenchError
from dimcluster.state import StateStore, _record_digest
from dimcluster.util import atomic_write_json, read_json

from .helpers import workspace


def _publish_worker(state_dir: str, barrier: object, queue: object) -> None:
    store = StateStore(Path(state_dir))
    barrier.wait(timeout=10)
    try:
        store.publish(
            stage="preprocess",
            tier="small",
            input_fingerprint="same-input",
            config_fingerprint="same-config",
            config_version="same-version",
            files={"data.json": b"same-data\n"},
        )
        queue.put("ok")
    except WorkbenchError as exc:
        queue.put(exc.code)
    except Exception as exc:
        queue.put(f"unexpected:{type(exc).__name__}:{exc}")


class StateStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.root = workspace()
        self.state = StateStore(self.root / "state", root=self.root)

    def _publish(
        self,
        input_fingerprint: str,
        config_fingerprint: str,
        content: bytes,
        *,
        expected_digest: str | None = None,
        expected_sequence: int | None = None,
    ) -> dict:
        return self.state.publish(
            stage="preprocess",
            tier="small",
            input_fingerprint=input_fingerprint,
            config_fingerprint=config_fingerprint,
            config_version="v1",
            files={"data.json": content},
            expected_current_digest=expected_digest,
            expected_current_sequence=expected_sequence,
        )

    def test_duplicate_publish_is_stale_and_no_tmp_files_remain(self) -> None:
        first = self._publish("in-1", "cfg-1", b"one\n")
        with self.assertRaises(WorkbenchError) as raised:
            self._publish(
                "in-1",
                "cfg-1",
                b"one\n",
                expected_digest=first["artifact_digest"],
                expected_sequence=first["sequence"],
            )
        self.assertEqual(raised.exception.code, "E_STALE_CHECKPOINT")
        self.assertFalse(list((self.root / "state" / "checkpoints").glob("*.tmp")))
        self.assertFalse(list((self.root / "state" / "ledger").glob("*.tmp")))

    def test_stale_writer_with_old_sequence_gets_stale_code(self) -> None:
        first = self._publish("in-1", "cfg-1", b"one\n")
        second = self._publish(
            "in-2",
            "cfg-2",
            b"two\n",
            expected_digest=first["artifact_digest"],
            expected_sequence=first["sequence"],
        )
        with self.assertRaises(WorkbenchError) as raised:
            self._publish(
                "in-3",
                "cfg-3",
                b"three\n",
                expected_digest=first["artifact_digest"],
                expected_sequence=first["sequence"],
            )
        self.assertEqual(raised.exception.code, "E_STALE_CHECKPOINT")
        self.assertEqual(raised.exception.details["actual_sequence"], second["sequence"])

    def test_missing_prerequisite_is_invalid_stage(self) -> None:
        with self.assertRaises(WorkbenchError) as raised:
            self.state.publish(
                stage="reduce",
                tier="small",
                input_fingerprint="in",
                config_fingerprint="cfg",
                config_version="v1",
                files={"data.json": b"x"},
                predecessors=[
                    {
                        "stage": "preprocess",
                        "tier": "small",
                        "artifact_digest": "missing",
                    }
                ],
            )
        self.assertEqual(raised.exception.code, "E_INVALID_STAGE")

    def test_sequence_regression_detected_after_restore(self) -> None:
        first = self._publish("in-1", "cfg-1", b"one\n")
        checkpoint_path = self.root / "state" / "checkpoints" / "small__preprocess.json"
        old_bytes = checkpoint_path.read_bytes()
        second = self._publish(
            "in-2",
            "cfg-2",
            b"two\n",
            expected_digest=first["artifact_digest"],
            expected_sequence=first["sequence"],
        )
        self.assertGreater(second["sequence"], first["sequence"])
        checkpoint_path.write_bytes(old_bytes)
        result = self.state.verify_integrity()
        self.assertFalse(result["ok"])
        self.assertIn("E_SEQUENCE_REGRESSION", [issue["code"] for issue in result["issues"]])

    def test_replaced_record_detected(self) -> None:
        self._publish("in-1", "cfg-1", b"one\n")
        checkpoint_path = self.root / "state" / "checkpoints" / "small__preprocess.json"
        record = read_json(checkpoint_path)
        record["input_fingerprint"] = "replaced"
        base = {key: value for key, value in record.items() if key != "record_digest"}
        record["record_digest"] = _record_digest(base)
        atomic_write_json(checkpoint_path, record)
        result = self.state.verify_integrity()
        self.assertIn("E_VERSION_REPLACED", [issue["code"] for issue in result["issues"]])

    def test_artifact_digest_mismatch_detected(self) -> None:
        record = self._publish("in-1", "cfg-1", b"one\n")
        artifact_path = self.root / "state" / record["artifacts"]["data.json"]["path"]
        artifact_path.write_bytes(b"tampered\n")
        result = self.state.verify_integrity()
        self.assertIn("E_DIGEST_MISMATCH", [issue["code"] for issue in result["issues"]])

    def test_two_process_publication_has_one_winner(self) -> None:
        context = multiprocessing.get_context("spawn")
        barrier = context.Barrier(2)
        queue = context.Queue()
        args = (str(self.root / "state"), barrier, queue)
        processes = [context.Process(target=_publish_worker, args=args) for _ in range(2)]
        for process in processes:
            process.start()
        outcomes = [queue.get(timeout=15) for _ in range(2)]
        for process in processes:
            process.join(timeout=15)
            self.assertEqual(process.exitcode, 0)
        self.assertEqual(sorted(outcomes), ["E_STALE_CHECKPOINT", "ok"])
        self.assertTrue(self.state.verify_integrity()["ok"])


if __name__ == "__main__":
    unittest.main()
