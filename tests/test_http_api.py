from __future__ import annotations

import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from capacity_lab.deterministic_service import start_server
from capacity_lab.server import CapacityApplication, CapacityHttpServer

from tests.support import http_call, load_fixture


class HttpApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.service, _thread = start_server(port=0)
        self.tempdir = tempfile.TemporaryDirectory()
        db_path = Path(self.tempdir.name) / "api.db"
        self.application = CapacityApplication(db_path)
        self.server = CapacityHttpServer(("127.0.0.1", 0), self.application)
        self.server_thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.server_thread.start()
        self.base_url = f"http://127.0.0.1:{self.server.server_port}"

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.service.shutdown()
        self.service.server_close()
        self.tempdir.cleanup()

    def _plan(self, fixture: str = "sample_plan.json") -> dict:
        plan = load_fixture(fixture)
        plan["target"] = f"http://127.0.0.1:{self.service.server_port}/load"
        return plan

    def test_plan_failure_is_json_business_error_and_keeps_version(self) -> None:
        plan = self._plan()
        status, payload = http_call(
            self.base_url, "PUT", "/api/plans/capacity-demo", plan
        )
        self.assertEqual(status, 200)
        self.assertEqual(payload["data"]["version"], 1)

        invalid = self._plan()
        invalid["stages"][2]["concurrency"] = invalid["stages"][1]["concurrency"]
        status, payload = http_call(
            self.base_url, "PUT", "/api/plans/capacity-demo", invalid
        )
        self.assertEqual(status, 422)
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["error"]["code"], "E_PLAN_INVALID")
        status, versions = http_call(
            self.base_url, "GET", "/api/plans/capacity-demo/versions"
        )
        self.assertEqual(status, 200)
        self.assertEqual(len(versions["data"]), 1)

        status, payload = http_call(self.base_url, "GET", "/api/not-a-route")
        self.assertEqual(status, 404)
        self.assertEqual(payload["error"]["code"], "E_NOT_FOUND")

    def test_full_run_evidence_is_consistent_for_concurrent_readers(self) -> None:
        status, _payload = http_call(
            self.base_url, "PUT", "/api/plans/capacity-demo", self._plan()
        )
        self.assertEqual(status, 200)
        status, payload = http_call(
            self.base_url,
            "POST",
            "/api/plans/capacity-demo/runs",
            {"async": False},
            timeout=30,
        )
        self.assertEqual(status, 200)
        run = payload["data"]
        self.assertEqual(run["status"], "CONVERGED")
        self.assertEqual(run["inflection"]["concurrency"], 12)
        self.assertEqual(run["conclusion_version"], 5)
        target_round = run["rounds"][3]
        self.assertEqual(target_round["conclusion"]["verdict"], "REJECT")

        def reader() -> tuple[int, dict]:
            return http_call(
                self.base_url,
                "GET",
                f"/api/rounds/{target_round['round_id']}?include_samples=false",
            )

        with ThreadPoolExecutor(max_workers=8) as executor:
            results = list(executor.map(lambda _: reader(), range(16)))
        for status, response in results:
            self.assertEqual(status, 200)
            self.assertEqual(response["data"]["status"], "CONVERGED")
            self.assertEqual(
                response["data"]["conclusion"]["conclusion_version"],
                target_round["conclusion"]["conclusion_version"],
            )
            self.assertEqual(
                response["data"]["conclusion"]["computation_digest"],
                target_round["conclusion"]["computation_digest"],
            )
            self.assertEqual(
                response["data"]["sample_aggregate"]["sample_digest"],
                target_round["conclusion"]["metrics"]["sample_digest"],
            )

        status, report = http_call(
            self.base_url, "GET", "/api/plans/capacity-demo/report"
        )
        self.assertEqual(status, 200)
        self.assertEqual(
            report["data"]["decision_basis"]["outcome"], "DOWNSHIFT_RETEST_PASSED"
        )

    def test_active_run_rejects_duplicate_start_and_hides_uncommitted_samples(self) -> None:
        plan = self._plan()
        plan["stages"] = [plan["stages"][0]]
        plan["stages"][0]["service_sleep_ms"] = 30
        status, _payload = http_call(
            self.base_url, "PUT", "/api/plans/capacity-demo", plan
        )
        self.assertEqual(status, 200)
        status, payload = http_call(
            self.base_url,
            "POST",
            "/api/plans/capacity-demo/runs",
            {"async": True},
        )
        self.assertEqual(status, 200)
        run_id = payload["data"]["run_id"]

        deadline = time.time() + 10
        sampling_round = None
        while time.time() < deadline:
            status, current = http_call(self.base_url, "GET", f"/api/runs/{run_id}")
            self.assertEqual(status, 200)
            rounds = current["data"]["rounds"]
            if rounds and rounds[0]["status"] == "SAMPLING":
                sampling_round = rounds[0]
                break
            time.sleep(0.02)
        self.assertIsNotNone(sampling_round)

        status, payload = http_call(
            self.base_url,
            "POST",
            "/api/plans/capacity-demo/runs",
            {"async": True},
        )
        self.assertEqual(status, 409)
        self.assertEqual(payload["error"]["code"], "E_ROUND_ACTIVE")

        status, observed = http_call(
            self.base_url, "GET", f"/api/rounds/{sampling_round['round_id']}"
        )
        self.assertEqual(status, 200)
        self.assertEqual(observed["data"]["status"], "SAMPLING")
        self.assertNotIn("samples", observed["data"])
        self.assertNotIn("sample_aggregate", observed["data"])
        self.assertNotIn("conclusion", observed["data"])

        deadline = time.time() + 20
        terminal = None
        while time.time() < deadline:
            status, current = http_call(self.base_url, "GET", f"/api/rounds/{sampling_round['round_id']}")
            self.assertEqual(status, 200)
            if current["data"]["status"] == "CONVERGED":
                terminal = current["data"]
                break
            time.sleep(0.05)
        self.assertIsNotNone(terminal)
        self.assertIn("samples", terminal)
        self.assertIn("conclusion", terminal)
        self.assertEqual(
            terminal["sample_aggregate"]["sample_digest"],
            terminal["conclusion"]["metrics"]["sample_digest"],
        )


if __name__ == "__main__":
    unittest.main()

