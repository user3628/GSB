from __future__ import annotations

import json
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from pathlib import Path

from sqlplan_engine.sample import generate_sample_database, load_manifest
from sqlplan_engine.server import create_server
from sqlplan_engine.service import PlanService


class HTTPTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        sample = root / "sample.sqlite3"
        generate_sample_database(sample)
        self.service = PlanService(sample_db=sample, store_db=root / "plans.sqlite3")
        self.server = create_server(self.service, port=0)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        host, port = self.server.server_address[:2]
        self.base_url = f"http://{host}:{port}"

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=3)
        self.temp_dir.cleanup()

    def request(self, method: str, path: str, body: dict | None = None):
        data = None
        headers = {"Accept": "application/json"}
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(
            self.base_url + path, data=data, headers=headers, method=method
        )
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status, json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            try:
                return exc.code, json.loads(exc.read().decode("utf-8"))
            finally:
                exc.close()

    def test_health_samples_plan_and_lookup(self) -> None:
        status, health = self.request("GET", "/health")
        self.assertEqual(status, 200)
        self.assertEqual(health["business_code"], "OK")
        status, samples = self.request("GET", "/v1/samples")
        self.assertEqual(status, 200)
        self.assertEqual(len(samples["statements"]), 6)
        sql = load_manifest()[0]["sql"]
        status, created = self.request(
            "POST", "/v1/plans", {"sql": sql, "request_id": "http-plan"}
        )
        self.assertEqual(status, 200)
        self.assertEqual(created["business_code"], "OK")
        status, fetched = self.request("GET", "/v1/plans/http-plan")
        self.assertEqual(status, 200)
        self.assertEqual(fetched["normalized_plan"], created["normalized_plan"])
        self.assertTrue(fetched["reused"])

    def test_http_errors_are_json_with_codes(self) -> None:
        sql = load_manifest()[0]["sql"]
        base_request = {"sql": sql}
        cases = {
            "E_PARSE": (400, {"kind": "malformed_sql"}),
            "E_PLAN_TOO_LARGE": (413, {"kind": "plan_too_large", "limit": 1}),
            "E_INVALID_AGGREGATE": (422, {"kind": "invalid_aggregate"}),
            "E_REWRITE_ABORTED": (409, {"kind": "rewrite_aborted", "abort_after": 0}),
        }
        for expected, (status_code, injection) in cases.items():
            with self.subTest(expected=expected):
                status, payload = self.request(
                    "POST",
                    "/v1/plans",
                    {**base_request, "request_id": "http-" + expected.lower(), "inject": injection},
                )
                self.assertEqual(status, status_code)
                self.assertFalse(payload["ok"])
                self.assertEqual(payload["business_code"], expected)
                self.assertIsInstance(payload["context"], dict)
                self.assertEqual(payload["request_id"], "http-" + expected.lower())

    def test_query_parameter_injection_and_recovery_endpoint(self) -> None:
        sql = load_manifest()[0]["sql"]
        status, payload = self.request(
            "POST",
            "/v1/plans?inject=malformed_sql&request_id=query-inject",
            {"sql": sql},
        )
        self.assertEqual(status, 400)
        self.assertEqual(payload["business_code"], "E_PARSE")
        status, recovery = self.request("GET", "/v1/recovery-report")
        self.assertEqual(status, 200)
        self.assertIn("recovery_report", recovery)


if __name__ == "__main__":
    unittest.main()

