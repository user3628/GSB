from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from sqlplan_engine.demo import (
    _free_port,
    _request_json,
    _start_server,
    _store_counts,
    _wait_process,
    _wait_until_healthy,
)
from sqlplan_engine.sample import generate_sample_database
from sqlplan_engine.service import PlanService
from sqlplan_engine.store import PlanStore, statement_key_for


class RecoveryTests(unittest.TestCase):
    def test_pending_intent_without_version_is_aborted(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            sample = root / "sample.sqlite3"
            store_path = root / "plans.sqlite3"
            generate_sample_database(sample)
            service = PlanService(sample_db=sample, store_db=store_path)
            store = service.store
            request_id = "simulated-crash"
            sql = "SELECT id FROM employees LIMIT 1"
            intent_id = store.begin_intent(
                request_id, sql, statement_key_for(sql), {"kind": "simulation"}
            )
            self.assertEqual(store.require_intent(intent_id)["status"], "pending")
            restarted = PlanService(sample_db=sample, store_db=store_path)
            report = restarted.recovery()["recovery_report"]
            fact = next(item for item in report["facts"] if item["request_id"] == request_id)
            self.assertEqual(fact["before_status"], "pending")
            self.assertEqual(fact["after_status"], "aborted")
            self.assertEqual(fact["action"], "aborted_without_committed_version")
            self.assertEqual(report["pending_after_recovery"], 0)
            self.assertFalse(report["complete_exit_report_available"])

    def test_pending_intent_retains_last_complete_version(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            sample = root / "sample.sqlite3"
            store_path = root / "plans.sqlite3"
            generate_sample_database(sample)
            service = PlanService(sample_db=sample, store_db=store_path)
            sql = "SELECT id FROM employees ORDER BY id LIMIT 1"
            committed = service.submit(sql, request_id="already-committed")
            service.store.begin_intent(
                "already-committed",
                sql,
                statement_key_for(sql),
                {"kind": "simulated-second-intent"},
            )
            restarted = PlanService(sample_db=sample, store_db=store_path)
            report = restarted.recovery()["recovery_report"]
            fact = next(
                item for item in report["facts"] if item["request_id"] == "already-committed"
            )
            self.assertEqual(fact["action"], "aborted_and_retained_last_complete_version")
            self.assertEqual(fact["last_complete_version"], committed["version_id"])
            fetched = restarted.get_request("already-committed")
            self.assertEqual(fetched["version_id"], committed["version_id"])

    def test_actual_subprocess_forced_exit_then_recovery(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            sample = root / "sample.sqlite3"
            store_path = root / "plans.sqlite3"
            exit_summary = root / "exit-summary.json"
            generate_sample_database(sample)
            port = _free_port()
            process = _start_server(port, sample, store_path, exit_summary)
            try:
                base_url = f"http://127.0.0.1:{port}"
                _wait_until_healthy(process, base_url)
                status, payload = _request_json(
                    "POST",
                    base_url + "/v1/plans",
                    {
                        "sql": "SELECT COUNT(*) FROM employees",
                        "request_id": "subprocess-crash",
                        "inject": {"kind": "crash_after_intent"},
                    },
                    timeout=5,
                )
                self.assertEqual(status, 0)
                self.assertEqual(payload["business_code"], "E_TRANSPORT")
                self.assertEqual(_wait_process(process, timeout=10), 70)
                counts = _store_counts(store_path)
                self.assertGreaterEqual(counts["intents"], 1)
                self.assertFalse(exit_summary.exists())
                recovered = PlanService(sample_db=sample, store_db=store_path)
                report = recovered.recovery()["recovery_report"]
                fact = next(
                    item for item in report["facts"] if item["request_id"] == "subprocess-crash"
                )
                self.assertEqual(fact["before_status"], "pending")
                self.assertEqual(fact["after_status"], "aborted")
                self.assertFalse(report["complete_exit_report_available"])
                process = None
            finally:
                if process is not None and process.poll() is None:
                    process.kill()
                    process.wait(timeout=5)


if __name__ == "__main__":
    unittest.main()

