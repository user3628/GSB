from __future__ import annotations

import dataclasses
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from slo_budget import codec
from slo_budget.errors import SloError
from slo_budget.storage import Storage

from tests.support import (
    AS_OF_MS,
    CURRENT_BIN,
    OVERWRITE,
    REJECT,
    ROOT,
)


def replace_snapshot(storage: Storage, path: Path, batch_id: str) -> dict:
    parsed = codec.decode(path)
    intent = storage.begin_write_intent(
        "snapshot_replace",
        batch_id=batch_id,
        payload={"source_path": str(path), "format": parsed.manifest.file_format},
    )
    return storage.apply_snapshot_replace(intent, parsed, source_path=str(path))


def apply_import(storage: Storage, path: Path, batch_id: str) -> dict:
    parsed = codec.decode(path)
    intent = storage.begin_write_intent(
        "import",
        batch_id=batch_id,
        payload={"source_path": str(path), "format": parsed.manifest.file_format},
    )
    return storage.apply_import(intent, parsed, source_path=str(path))


class StorageTests(unittest.TestCase):
    def test_conflict_tie_breaker_precedence(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with Storage(Path(tmp) / "tie.db", clock_ms=lambda: AS_OF_MS) as storage:
                existing = codec.decode(CURRENT_BIN).records[0]
                higher_version = dataclasses.replace(
                    existing,
                    format_version=existing.format_version + 1,
                    late_count=0,
                    last_write_ms=existing.last_write_ms - 1,
                )
                lower_version = dataclasses.replace(
                    existing,
                    format_version=existing.format_version - 1,
                    late_count=existing.late_count + 1,
                    last_write_ms=existing.last_write_ms + 1,
                )
                higher_late = dataclasses.replace(
                    existing,
                    late_count=existing.late_count + 1,
                    last_write_ms=existing.last_write_ms - 1,
                )
                later_write = dataclasses.replace(
                    existing,
                    last_write_ms=existing.last_write_ms + 1,
                )
                earlier_write = dataclasses.replace(
                    existing,
                    last_write_ms=existing.last_write_ms - 1,
                )
                ambiguous = dataclasses.replace(
                    existing,
                    good_events=existing.good_events - 1,
                )
                self.assertEqual(storage.resolve_conflict(higher_version, existing)[0], "OVERWRITE")
                self.assertEqual(storage.resolve_conflict(lower_version, existing)[0], "SKIP")
                self.assertEqual(storage.resolve_conflict(higher_late, existing)[0], "OVERWRITE")
                self.assertEqual(storage.resolve_conflict(later_write, existing)[0], "OVERWRITE")
                self.assertEqual(storage.resolve_conflict(earlier_write, existing)[0], "SKIP")
                self.assertEqual(storage.resolve_conflict(existing, existing)[0], "SKIP")
                self.assertEqual(storage.resolve_conflict(ambiguous, existing)[0], "REJECT")

    def test_snapshot_sequence_is_strictly_monotonic_and_state_survives(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with Storage(Path(tmp) / "state.db", clock_ms=lambda: AS_OF_MS + 7) as storage:
                first = replace_snapshot(storage, CURRENT_BIN, "first")
                before_state = storage.active_snapshot()
                before_records = storage.window_records()
                with self.assertRaises(SloError) as caught:
                    replace_snapshot(storage, CURRENT_BIN, "regression")
                self.assertEqual(caught.exception.code, "E_SNAPSHOT_INVALID")
                self.assertEqual(storage.active_snapshot(), before_state)
                self.assertEqual(storage.window_records(), before_records)
                self.assertEqual(first["result_state"]["record_count"], 48)
                report = storage.get_import_report("regression")
                self.assertEqual(report["status"], "ABORTED")
                self.assertEqual(report["business_code"], "E_SNAPSHOT_INVALID")

    def test_conflict_overwrite_and_skip_report(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with Storage(Path(tmp) / "conflict.db", clock_ms=lambda: AS_OF_MS + 8) as storage:
                replace_snapshot(storage, CURRENT_BIN, "base")
                report = apply_import(storage, OVERWRITE, "merge-overwrite")
                self.assertEqual(report["status"], "APPLIED")
                self.assertEqual(report["counts"]["overwritten"], 1)
                self.assertEqual(report["counts"]["skipped"], 1)
                actions = [decision["action"] for decision in report["decisions"]]
                self.assertIn("OVERWRITE", actions)
                self.assertIn("SKIP", actions)
                intents = storage.list_intents()
                merge_intent = next(
                    item for item in intents if item["batch_id"] == "merge-overwrite"
                )
                self.assertEqual(merge_intent["status"], "committed")

    def test_conflict_reject_rolls_back_entire_batch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with Storage(Path(tmp) / "reject.db", clock_ms=lambda: AS_OF_MS + 9) as storage:
                replace_snapshot(storage, CURRENT_BIN, "base")
                before_state = storage.active_snapshot()
                before_records = storage.window_records()
                report = apply_import(storage, REJECT, "merge-reject")
                self.assertEqual(report["status"], "ROLLED_BACK")
                self.assertEqual(report["counts"]["rejected"], 1)
                self.assertEqual(report["counts"]["skipped"], 1)
                self.assertEqual(storage.active_snapshot(), before_state)
                self.assertEqual(storage.window_records(), before_records)
                intents = storage.list_intents()
                merge_intent = next(
                    item for item in intents if item["batch_id"] == "merge-reject"
                )
                self.assertEqual(merge_intent["status"], "rolled_back")
                persisted = storage.get_import_report("merge-reject")
                self.assertEqual(persisted["status"], "ROLLED_BACK")
                self.assertEqual(persisted["business_code"], "E_IMPORT_CONFLICT")

    def test_import_into_empty_database_seeds_snapshot_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with Storage(Path(tmp) / "seed.db", clock_ms=lambda: AS_OF_MS + 10) as storage:
                parsed = codec.decode(CURRENT_BIN)
                intent = storage.begin_write_intent(
                    "import",
                    batch_id="seed",
                    payload={"source_path": str(CURRENT_BIN)},
                )
                report = storage.apply_import(intent, parsed, source_path=str(CURRENT_BIN))
                self.assertEqual(report["status"], "APPLIED")
                state = storage.active_snapshot()
                self.assertEqual(state["snapshot_sequence"], parsed.manifest.snapshot_sequence)
                self.assertEqual(state["record_count"], 48)
                self.assertEqual(len(storage.window_records()), 48)

    def test_memory_index_matches_sqlite_after_atomic_switch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with Storage(Path(tmp) / "atomic.db", clock_ms=lambda: AS_OF_MS + 11) as storage:
                replace_snapshot(storage, CURRENT_BIN, "base")
                db_count = storage.connection.execute("SELECT COUNT(*) AS n FROM windows").fetchone()["n"]
                self.assertEqual(db_count, len(storage.window_records()))
                self.assertEqual(
                    storage.connection.execute(
                        "SELECT record_count FROM snapshot_state WHERE id=1"
                    ).fetchone()["record_count"],
                    len(storage.window_records()),
                )

    def test_forced_exit_recovery_preserves_last_complete_version(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "crash.db"
            with Storage(db_path, clock_ms=lambda: AS_OF_MS + 12) as storage:
                replace_snapshot(storage, CURRENT_BIN, "stable-base")
                before_state = storage.active_snapshot()
                before_records = [record.persisted_dict() for record in storage.window_records()]

            env = {**os.environ, "PYTHONPATH": str(ROOT)}
            completed = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "slo_budget.cli",
                    "inject-crash",
                    "--db",
                    str(db_path),
                    "--kind",
                    "test_forced_exit",
                    "--batch-id",
                    "crash-batch",
                    "--fixed-clock-ms",
                    str(AS_OF_MS + 13),
                ],
                cwd=ROOT,
                env=env,
                capture_output=True,
                text=True,
                timeout=30,
            )
            self.assertEqual(completed.returncode, 17, completed.stderr)

            with Storage(db_path, clock_ms=lambda: AS_OF_MS + 14) as recovered:
                self.assertEqual(recovered.active_snapshot(), before_state)
                self.assertEqual(
                    [record.persisted_dict() for record in recovered.window_records()],
                    before_records,
                )
                reports = recovered.list_recovery_reports()
                self.assertTrue(
                    any(
                        report["batch_id"] == "crash-batch"
                        and report["action"] == "ROLLED_BACK_TO_LAST_COMPLETE_VERSION"
                        for report in reports
                    )
                )
                aborted = [
                    intent
                    for intent in recovered.list_intents()
                    if intent["batch_id"] == "crash-batch"
                ]
                self.assertEqual(aborted[0]["status"], "aborted")
                self.assertEqual(
                    recovered.connection.execute(
                        "SELECT record_count FROM snapshot_state WHERE id=1"
                    ).fetchone()["record_count"],
                    len(before_records),
                )

    def test_alert_write_has_intent_and_is_queryable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with Storage(Path(tmp) / "alerts.db", clock_ms=lambda: AS_OF_MS + 15) as storage:
                replace_snapshot(storage, CURRENT_BIN, "base")
                intent = storage.begin_write_intent(
                    "alert",
                    payload={"service": "checkout-api", "sli_type": "availability"},
                )
                stored = storage.persist_alert(
                    intent,
                    snapshot_sequence=storage.active_snapshot()["snapshot_sequence"],
                    service="checkout-api",
                    sli_type="availability",
                    as_of_ms=AS_OF_MS,
                    grade="CRITICAL",
                    result={"alert": {"grade": "CRITICAL"}},
                )
                self.assertEqual(stored["grade"], "CRITICAL")
                alerts = storage.list_alerts()
                self.assertEqual(len(alerts), 1)
                self.assertEqual(alerts[0]["grade"], "CRITICAL")
                alert_intent = next(
                    item for item in storage.list_intents() if item["kind"] == "alert"
                )
                self.assertEqual(alert_intent["status"], "committed")


if __name__ == "__main__":
    unittest.main()
