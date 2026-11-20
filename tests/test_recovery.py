from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path
from typing import Any

from rateguard.config import GuardConfig
from rateguard.engine import Engine
from rateguard.errors import BusinessError


class SimulatedCrash(BaseException):
    pass


def make_config() -> GuardConfig:
    return GuardConfig(
        window_length_ms=1_000,
        step_ms=1_000,
        min_samples_p50=1,
        min_samples_p95=2,
        min_samples_p99=2,
        max_quota=100,
        min_breaker_samples=2,
        consecutive_failure_threshold=2,
        open_duration_ms=1_000,
    )


def event(timestamp_ms: int, success: bool = True, latency_us: int = 30_000) -> dict[str, Any]:
    return {
        "timestamp_ms": timestamp_ms,
        "route": "/api/recovery",
        "success": success,
        "latency_us": latency_us,
    }


def batch(batch_id: str, events: list[dict[str, Any]]) -> dict[str, Any]:
    return {"batch_id": batch_id, "sequence": 1, "events": events}


class CrashRecoveryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.db = Path(self.temp.name) / "recovery.sqlite3"

    def tearDown(self) -> None:
        self.temp.cleanup()

    @staticmethod
    def crash_hook(stage_to_crash: str):
        def hook(stage: str, context: dict[str, Any]) -> None:
            if stage == stage_to_crash:
                raise SimulatedCrash(stage)

        return hook

    def test_ingest_crash_after_intent_marks_batch_aborted_and_preserves_versions(self) -> None:
        baseline = Engine(self.db, make_config())
        baseline.ingest_batch(batch("baseline", [event(100), event(200), event(1_100)]))
        before = baseline.get_circuit_state("/api/recovery")["composite_version"]

        crashing = Engine(
            self.db,
            make_config(),
            fault_hook=self.crash_hook("after_intent"),
        )
        with self.assertRaises(SimulatedCrash):
            crashing.ingest_batch(batch("crash-after-intent", [event(1_200), event(1_300)]))

        recovered = Engine(self.db, make_config())
        report = recovered.recovery_report()["report"]
        self.assertEqual(report["incomplete_intents_found"], 1)
        self.assertEqual(report["aborted_batches"][0]["batch_id"], "crash-after-intent")
        self.assertEqual(report["integrity"]["pending_intents_after_recovery"], 0)
        self.assertTrue(report["integrity"]["previous_complete_versions_preserved"])
        self.assertEqual(recovered.get_batch("crash-after-intent", 1)["batch"]["status"], "ABORTED")
        self.assertEqual(recovered.get_circuit_state("/api/recovery")["composite_version"], before)

    def test_window_recompute_crash_rolls_back_to_last_complete_version(self) -> None:
        baseline = Engine(self.db, make_config())
        baseline.ingest_batch(batch("baseline-window", [event(100), event(1_000)]))
        with sqlite3.connect(self.db) as connection:
            before_window_rows = connection.execute("SELECT COUNT(*) FROM window_versions").fetchone()[0]
            before_head = connection.execute(
                "SELECT current_version_id FROM window_heads WHERE start_ms=0"
            ).fetchone()[0]

        crashing = Engine(
            self.db,
            make_config(),
            fault_hook=self.crash_hook("after_window_version"),
        )
        with self.assertRaises(SimulatedCrash):
            crashing.ingest_batch(batch("crash-window", [event(300), event(2_000)]))

        recovered = Engine(self.db, make_config())
        report = recovered.recovery_report()["report"]
        self.assertEqual(report["incomplete_intents_found"], 1)
        self.assertEqual(recovered.get_batch("crash-window", 1)["batch"]["status"], "ABORTED")
        with sqlite3.connect(self.db) as connection:
            after_window_rows = connection.execute("SELECT COUNT(*) FROM window_versions").fetchone()[0]
            after_head = connection.execute(
                "SELECT current_version_id FROM window_heads WHERE start_ms=0"
            ).fetchone()[0]
        self.assertEqual(after_window_rows, before_window_rows)
        self.assertEqual(after_head, before_head)
        self.assertEqual(report["integrity"]["head_repairs"], [])

    def test_transition_crash_does_not_persist_new_state(self) -> None:
        baseline = Engine(self.db, make_config())
        baseline.ingest_batch(batch("baseline-state", [event(100), event(200), event(1_100)]))
        state_before = baseline.get_circuit_state("/api/recovery")
        transitions_before = baseline.list_transitions("/api/recovery")["count"]

        crashing = Engine(
            self.db,
            make_config(),
            fault_hook=self.crash_hook("transition_after_intent"),
        )
        with self.assertRaises(SimulatedCrash):
            crashing.manual_breaker_transition(
                {
                    "route": "/api/recovery",
                    "to_state": "OPEN",
                    "occurred_at_ms": 1_100,
                    "reason": "CRASH_TEST",
                }
            )

        recovered = Engine(self.db, make_config())
        state_after = recovered.get_circuit_state("/api/recovery")
        self.assertEqual(state_after["state"], "CLOSED")
        self.assertEqual(state_after["breaker_version"], state_before["breaker_version"])
        self.assertEqual(recovered.list_transitions("/api/recovery")["count"], transitions_before)
        report = recovered.recovery_report()["report"]
        self.assertEqual(report["incomplete_intents_found"], 1)
        self.assertEqual(report["aborted_intents"][0]["kind"], "TRANSITION")

    def test_illegal_transition_does_not_leave_prepared_intent(self) -> None:
        engine = Engine(self.db, make_config())
        engine.ingest_batch(batch("legal-state", [event(100), event(200), event(1_100)]))
        before = engine.get_circuit_state("/api/recovery")
        with self.assertRaises(BusinessError) as context:
            engine.manual_breaker_transition(
                {
                    "route": "/api/recovery",
                    "to_state": "HALF_OPEN",
                    "occurred_at_ms": 1_100,
                    "reason": "ILLEGAL",
                }
            )
        self.assertEqual(context.exception.code, "E_INVALID_TRANSITION")
        after = engine.get_circuit_state("/api/recovery")
        self.assertEqual(after["breaker_version"], before["breaker_version"])
        with sqlite3.connect(self.db) as connection:
            prepared = connection.execute(
                "SELECT COUNT(*) FROM intents WHERE state='PREPARED'"
            ).fetchone()[0]
            aborted = connection.execute(
                "SELECT COUNT(*) FROM intents WHERE state='ABORTED_REJECTED'"
            ).fetchone()[0]
        self.assertEqual(prepared, 0)
        self.assertEqual(aborted, 1)


if __name__ == "__main__":
    unittest.main()

