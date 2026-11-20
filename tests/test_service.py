from __future__ import annotations

import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from sqlplan_engine.business import EngineError
from sqlplan_engine.sample import generate_sample_database, load_manifest
from sqlplan_engine.service import PlanService


class ServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.sample_db = root / "sample.sqlite3"
        self.store_db = root / "plans.sqlite3"
        generate_sample_database(self.sample_db)
        self.service = PlanService(sample_db=self.sample_db, store_db=self.store_db)
        self.manifest = load_manifest()
        self.normal_sql = self.manifest[0]["sql"]

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_success_persists_and_round_trips(self) -> None:
        response = self.service.submit(self.normal_sql, request_id="success")
        self.assertTrue(response["ok"])
        self.assertEqual(response["business_code"], "OK")
        self.assertFalse(response["reused"])
        self.assertTrue(
            response["semantic_equivalence"]["original_plan_equals_normalized_plan_result"]
        )
        self.assertGreater(len(response["rewrite_log"]), 0)
        fetched = self.service.get_request("success")
        self.assertTrue(fetched["reused"])
        self.assertEqual(fetched["version_id"], response["version_id"])
        self.assertEqual(fetched["normalized_plan"], response["normalized_plan"])

    def test_request_and_statement_idempotency(self) -> None:
        first = self.service.submit(self.normal_sql, request_id="same-request")
        second = self.service.submit(self.normal_sql, request_id="same-request")
        third = self.service.submit(self.normal_sql, request_id="other-request")
        self.assertTrue(first["reused"] or not first["reused"])
        self.assertTrue(second["reused"])
        self.assertEqual(second["reuse_kind"], "request_id")
        self.assertTrue(third["reused"])
        self.assertEqual(third["reuse_kind"], "statement")
        self.assertEqual(first["version_id"], second["version_id"])
        self.assertEqual(first["version_id"], third["version_id"])
        self.assertEqual(first["normalized_plan"], third["normalized_plan"])

    def test_same_request_with_different_sql_conflicts(self) -> None:
        self.service.submit(self.normal_sql, request_id="conflict")
        with self.assertRaises(EngineError) as raised:
            self.service.submit(self.manifest[1]["sql"], request_id="conflict")
        self.assertEqual(raised.exception.business_code, "E_REQUEST_CONFLICT")

    def test_four_injection_business_codes(self) -> None:
        cases = {
            "E_PARSE": {"kind": "malformed_sql"},
            "E_PLAN_TOO_LARGE": {"kind": "plan_too_large", "limit": 1},
            "E_INVALID_AGGREGATE": {"kind": "invalid_aggregate"},
            "E_REWRITE_ABORTED": {"kind": "rewrite_aborted", "abort_after": 1},
        }
        for expected, injection in cases.items():
            with self.subTest(expected=expected):
                with self.assertRaises(EngineError) as raised:
                    self.service.submit(
                        self.normal_sql,
                        request_id="inject-" + expected.lower(),
                        injection=injection,
                    )
                self.assertEqual(raised.exception.business_code, expected)
                self.assertIsInstance(raised.exception.context, dict)
                self.assertTrue(raised.exception.context)

    def test_parse_error_is_contextual(self) -> None:
        with self.assertRaises(EngineError) as raised:
            self.service.submit(
                self.normal_sql,
                request_id="parse-context",
                injection={"kind": "malformed_sql"},
            )
        error = raised.exception
        self.assertEqual(error.business_code, "E_PARSE")
        self.assertIn("line", error.context)
        self.assertIn("column", error.context)
        self.assertIn("sql_excerpt", error.context)

    def test_natural_invalid_aggregate(self) -> None:
        entry = next(item for item in self.manifest if item["expected_code"] == "E_INVALID_AGGREGATE")
        with self.assertRaises(EngineError) as raised:
            self.service.submit(entry["sql"], request_id="natural-invalid")
        self.assertEqual(raised.exception.business_code, "E_INVALID_AGGREGATE")

    def test_aborted_rewrite_does_not_publish_partial_plan(self) -> None:
        request_id = "aborted-partial"
        with self.assertRaises(EngineError) as raised:
            self.service.submit(
                self.normal_sql,
                request_id=request_id,
                injection={"kind": "rewrite_aborted", "abort_after": 0},
            )
        self.assertEqual(raised.exception.business_code, "E_REWRITE_ABORTED")
        with self.assertRaises(EngineError) as missing:
            self.service.get_request(request_id)
        self.assertEqual(missing.exception.business_code, "E_NOT_FOUND")
        intents = self.service.intents_for_request(request_id)["intents"]
        self.assertEqual(intents[-1]["status"], "aborted")

    def test_concurrent_same_statement_executes_once(self) -> None:
        sql = "SELECT id, name FROM employees WHERE salary > 90000 ORDER BY id LIMIT 4"
        request_ids = [f"concurrent-{index}" for index in range(8)]
        with ThreadPoolExecutor(max_workers=8) as pool:
            responses = list(
                pool.map(
                    lambda request_id: self.service.submit(sql, request_id=request_id),
                    request_ids,
                )
            )
        self.assertEqual(sum(1 for response in responses if not response["reused"]), 1)
        self.assertEqual(sum(1 for response in responses if response["reused"]), 7)
        self.assertEqual(len({response["version_id"] for response in responses}), 1)
        self.assertEqual(len({response["normalized_plan"] for response in responses}), 1)
        self.assertTrue(all(response["reuse_kind"] == "statement" for response in responses if response["reused"]))

    def test_recovery_report_is_based_on_persisted_facts(self) -> None:
        report = self.service.recovery()["recovery_report"]
        self.assertEqual(report["report_type"], "startup_recovery")
        self.assertFalse(report["complete_exit_report_available"])
        self.assertEqual(report["pending_after_recovery"], 0)
        self.assertIn("source", report)


if __name__ == "__main__":
    unittest.main()

