from __future__ import annotations

import copy
import json
import multiprocessing
import tempfile
import unittest
from pathlib import Path
from typing import Any

from telemetry_align.canonical import (
    atomic_write_json,
    canonical_json_bytes,
    read_json,
    sha256_bytes,
)
from telemetry_align.checkpoint import CheckpointStore, record_hash
from telemetry_align.errors import TelemetryError
from telemetry_align.workspace import AlignmentWorkspace


def _publish_worker(root: str, record: dict[str, Any], barrier: Any, queue: Any) -> None:
    try:
        barrier.wait(timeout=10)
        result = CheckpointStore(root).publish(copy.deepcopy(record))
        queue.put({"ok": True, "result": result})
    except TelemetryError as exc:
        queue.put({"ok": False, "code": exc.code, "details": exc.details})
    except Exception as exc:  # pragma: no cover - diagnostic path
        queue.put({"ok": False, "code": "E_INTERNAL", "message": str(exc)})


class CheckpointTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.workspace = AlignmentWorkspace(Path(self.temp.name) / "workspace")
        self.workspace.init_demo(reset=True)
        self.workspace.align()

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _clone_head(self) -> dict[str, Any]:
        record = copy.deepcopy(self.workspace.store.head())
        record.pop("record_hash", None)
        record.pop("prev_record_hash", None)
        return record

    def test_duplicate_gap_cross_stage_and_config_change(self) -> None:
        head = self.workspace.store.head()
        duplicate = self._clone_head()
        duplicate["seq"] = head["seq"]
        with self.assertRaises(TelemetryError) as raised:
            self.workspace.store.publish(duplicate)
        self.assertEqual(raised.exception.code, "E_STALE_CHECKPOINT")
        self.assertEqual(raised.exception.details["kind"], "duplicate")

        gap = self._clone_head()
        gap["seq"] = head["seq"] + 2
        with self.assertRaises(TelemetryError) as raised:
            self.workspace.store.publish(gap)
        self.assertEqual(raised.exception.details["kind"], "sequence_gap")

        cross_stage = self._clone_head()
        cross_stage["seq"] = head["seq"] + 1
        cross_stage["stage"] = "fusion"
        with self.assertRaises(TelemetryError) as raised:
            self.workspace.store.publish(cross_stage)
        self.assertEqual(raised.exception.details["kind"], "cross_stage")

        config_changed = self._clone_head()
        config_changed["seq"] = head["seq"] + 1
        config_changed["config_fingerprint"] = "f" * 64
        with self.assertRaises(TelemetryError) as raised:
            self.workspace.store.publish(config_changed)
        self.assertEqual(raised.exception.details["kind"], "config_changed")

    def test_missing_prerequisite_returns_e_invalid_stage(self) -> None:
        head = self.workspace.store.head()
        record = copy.deepcopy(head)
        record.pop("record_hash", None)
        record.pop("prev_record_hash", None)
        record["seq"] = head["seq"] + 1
        record["run_id"] = "new-run-without-prerequisites"
        record["stage"] = "fusion"
        record["catalog"] = {"fusion": head["catalog"]["fusion"]}
        record["prerequisites"] = {}
        with self.assertRaises(TelemetryError) as raised:
            self.workspace.store.publish(record)
        self.assertEqual(raised.exception.code, "E_INVALID_STAGE")
        self.assertEqual(raised.exception.details["missing_prerequisite"], "normalize")

    def test_artifact_digest_tamper_is_detected(self) -> None:
        head = self.workspace.store.head()
        path = self.workspace.root / head["catalog"]["fusion"]["artifact_path"]
        original = path.read_bytes()
        path.write_bytes(original.replace(b'"schema_version": 1', b'"schema_version": 2', 1))
        with self.assertRaises(TelemetryError) as raised:
            self.workspace.store.verify()
        self.assertEqual(raised.exception.code, "E_STALE_CHECKPOINT")
        self.assertEqual(raised.exception.details["kind"], "tampered")

    def test_rolled_back_ledger_is_classified_as_rollback(self) -> None:
        old_ledger = (self.workspace.root / "state/checkpoints.jsonl").read_bytes()
        self.workspace.mutate_source_and_rebuild()
        self.assertEqual(self.workspace.store.summary()["head_seq"], 8)
        (self.workspace.root / "state/checkpoints.jsonl").write_bytes(old_ledger)
        with self.assertRaises(TelemetryError) as raised:
            self.workspace.store.records()
        self.assertEqual(raised.exception.code, "E_STALE_CHECKPOINT")
        self.assertEqual(raised.exception.details["kind"], "rollback")

    def test_rewritten_ledger_is_classified_as_tamper(self) -> None:
        ledger_path = self.workspace.root / "state/checkpoints.jsonl"
        data = ledger_path.read_bytes()
        ledger_path.write_bytes(data.replace(b'"schema_version":1', b'"schema_version":2', 1))
        with self.assertRaises(TelemetryError) as raised:
            self.workspace.store.records()
        self.assertEqual(raised.exception.code, "E_STALE_CHECKPOINT")
        self.assertEqual(raised.exception.details["kind"], "tampered")

    def test_concurrent_process_publish_accepts_only_one_contiguous_record(self) -> None:
        head = self.workspace.store.head()
        record = copy.deepcopy(head)
        record.pop("record_hash", None)
        record.pop("prev_record_hash", None)
        record["seq"] = head["seq"] + 1
        record["run_id"] = "concurrent-run"
        record["stage"] = "fusion"
        record["fingerprint"] = head["catalog"]["fusion"]["stage_fingerprint"]
        record["artifact_digest"] = head["catalog"]["fusion"]["artifact_digest"]
        record["catalog"] = {
            stage: head["catalog"][stage]
            for stage in ("normalize", "resample", "fusion")
        }
        record["prerequisites"] = {
            "normalize": head["catalog"]["normalize"]["stage_fingerprint"],
            "resample": head["catalog"]["resample"]["stage_fingerprint"],
        }

        context = multiprocessing.get_context("fork")
        barrier = context.Barrier(2)
        queue = context.Queue()
        processes = [
            context.Process(
                target=_publish_worker,
                args=(str(self.workspace.root), record, barrier, queue),
            )
            for _ in range(2)
        ]
        for process in processes:
            process.start()
        for process in processes:
            process.join(timeout=20)
        results = [queue.get(timeout=5) for _ in processes]
        accepted = [item for item in results if item.get("ok")]
        rejected = [item for item in results if not item.get("ok")]
        self.assertEqual(len(accepted), 1, results)
        self.assertEqual(len(rejected), 1, results)
        self.assertEqual(rejected[0]["code"], "E_STALE_CHECKPOINT")
        self.assertIn(
            rejected[0]["details"]["kind"], {"duplicate", "rollback", "cross_stage"}
        )
        self.assertEqual(self.workspace.store.summary()["sequence"], [1, 2, 3, 4, 5])

    def test_crash_between_ledger_and_anchor_is_recovered(self) -> None:
        store = self.workspace.store
        records = store.records()
        head = records[-1]
        new_record = copy.deepcopy(head)
        new_record.pop("record_hash", None)
        new_record.pop("prev_record_hash", None)
        new_record["seq"] = head["seq"] + 1
        new_record["run_id"] = "crash-recovery-run"
        new_record["stage"] = "fusion"
        new_record["fingerprint"] = head["catalog"]["fusion"]["stage_fingerprint"]
        new_record["artifact_digest"] = head["catalog"]["fusion"]["artifact_digest"]
        new_record["catalog"] = {
            stage: head["catalog"][stage]
            for stage in ("normalize", "resample", "fusion")
        }
        new_record["prerequisites"] = {
            "normalize": head["catalog"]["normalize"]["stage_fingerprint"],
            "resample": head["catalog"]["resample"]["stage_fingerprint"],
        }
        new_record["prev_record_hash"] = head["record_hash"]
        new_record["record_hash"] = record_hash(new_record)
        new_records = records + [new_record]
        new_ledger = b"".join(canonical_json_bytes(record) for record in new_records)
        new_anchor = store._build_anchor(new_records, new_ledger)

        old_ledger = (self.workspace.root / "state/checkpoints.jsonl").read_bytes()
        old_anchor = (self.workspace.root / "state/checkpoints.anchor.json").read_bytes()
        pending = {
            "schema_version": 1,
            "old": {
                "ledger_digest": sha256_bytes(old_ledger),
                "anchor_digest": sha256_bytes(old_anchor),
            },
            "new": {
                "ledger_digest": sha256_bytes(new_ledger),
                "anchor_digest": sha256_bytes(canonical_json_bytes(new_anchor)),
                "anchor_content": new_anchor,
            },
        }
        atomic_write_json(self.workspace.root / "state/checkpoint.pending.json", pending)
        (self.workspace.root / "state/checkpoints.jsonl").write_bytes(new_ledger)

        recovered = store.records()
        self.assertEqual([record["seq"] for record in recovered], [1, 2, 3, 4, 5])
        self.assertFalse((self.workspace.root / "state/checkpoint.pending.json").exists())
        anchor = read_json(self.workspace.root / "state/checkpoints.anchor.json")
        self.assertEqual(anchor["head_seq"], 5)


if __name__ == "__main__":
    unittest.main()

