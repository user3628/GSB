from __future__ import annotations

import json
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from pathlib import Path

from image_workbench.demo import generate_demo
from image_workbench.server import make_server


class HTTPTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.workspace = Path(self.temp.name) / "demo"
        generate_demo(self.workspace)
        self.server = make_server(self.workspace, port=0)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base = f"http://127.0.0.1:{self.server.server_port}"

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(10)
        self.temp.cleanup()

    def _request(self, method: str, path: str, body: dict | None = None) -> dict:
        data = None if body is None else json.dumps(body).encode("utf-8")
        request = urllib.request.Request(
            self.base + path,
            data=data,
            method=method,
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))

    def test_http_status_budget_staged_run_stop_and_resume(self) -> None:
        health = self._request("GET", "/api/health")
        self.assertTrue(health["ok"])
        budget = self._request("GET", "/api/budget")["result"]
        self.assertEqual(len(budget["accepted"]), 3)
        self.assertEqual(len(budget["rejected"]), 2)

        staged = self._request("POST", "/api/run", {"until_stage": "parsed"})["result"]
        self.assertEqual(staged["status"], "partial")
        self.assertIn("schedule", staged["reports"])
        status = self._request("GET", "/api/status")["result"]
        self.assertEqual(status["tasks"]["t010"]["checkpoints"]["next_seq"], 2)

        stop = self._request("POST", "/api/stop", {})["result"]
        self.assertTrue(stop["stop_requested"])
        resumed = self._request("POST", "/api/resume", {"until_stage": "exported"})["result"]
        self.assertEqual(resumed["status"], "completed")
        self.assertEqual(resumed["task_counts"]["verified"], 3)
        report = self._request("GET", "/api/reports?name=verification.json")["result"]
        self.assertEqual(report["schema_version"], 1)

        try:
            self._request("POST", "/api/run", {"until_stage": "bogus"})
        except urllib.error.HTTPError as exc:
            try:
                self.assertEqual(exc.code, 400)
                error = json.loads(exc.read().decode("utf-8"))
                self.assertEqual(error["error"]["code"], "E_INVALID_STAGE")
            finally:
                exc.close()
        else:
            self.fail("invalid stage request should fail")


if __name__ == "__main__":
    unittest.main()
