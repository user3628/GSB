import json
import os
import shutil
import tempfile
import threading
import time
import unittest
import urllib.request
import urllib.error

from src.http_app import create_server


REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
MANIFEST = os.path.join(REPO_ROOT, "samples", "manifest.json")
CONFIG = os.path.join(REPO_ROOT, "config", "backtest.yaml")


class HttpLoopbackTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not os.path.exists(MANIFEST):
            from scripts.generate_samples import generate
            generate(os.path.join(REPO_ROOT, "samples"))

    def setUp(self):
        self.workspace = tempfile.mkdtemp(prefix="wb-http-")
        self.server, _ = create_server(
            self.workspace, MANIFEST, CONFIG, host="127.0.0.1", port=0)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever,
                                       daemon=True)
        self.thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        shutil.rmtree(self.workspace, ignore_errors=True)

    def _request(self, path, payload=None):
        url = f"http://127.0.0.1:{self.port}{path}"
        data = None
        headers = {}
        if payload is not None:
            data = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(url, data=data, headers=headers,
                                         method="POST" if data else "GET")
        try:
            with urllib.request.urlopen(request, timeout=20) as response:
                return response.status, json.loads(response.read().decode())
        except urllib.error.HTTPError as exc:
            with exc:
                return exc.code, json.loads(exc.read().decode())

    def test_health_and_status(self):
        status, body = self._request("/health")
        self.assertEqual(status, 200)
        self.assertEqual(body["bind"], "127.0.0.1")
        status, body = self._request("/api/status")
        self.assertEqual(body["run_status"], "never_run")

    def test_run_stop_resume_and_export(self):
        status, body = self._request("/api/run",
                                     {"parallel": True,
                                      "stop_after_folds": 5})
        self.assertEqual(status, 202)
        self._wait(lambda: self._request("/api/status")[1]
                   .get("run_status") == "stopped")
        status, _ = self._request("/api/stop", {})
        self.assertEqual(status, 200)
        status, _ = self._request("/api/resume", {"parallel": True})
        self.assertEqual(status, 202)
        self._wait(lambda: self._request("/api/status")[1]
                   .get("run_status") == "completed")
        status, body = self._request("/api/export", {})
        self.assertEqual(status, 200)
        self.assertIn("dashboard", body["exports"])

        status, body = self._request("/api/fingerprints")
        self.assertTrue(body["aggregate_fingerprint"])
        status, body = self._request("/api/budget")
        self.assertIn("scheduler", body)
        status, body = self._request("/api/unprocessed")
        self.assertIn("series_errors", body)

    def test_stage_gate_on_early_export(self):
        status, _ = self._request("/api/export", {})
        self.assertEqual(status, 409)

    def test_unknown_path_returns_not_found(self):
        status, body = self._request("/api/nope")
        self.assertEqual(status, 404)
        self.assertEqual(body["code"], "E_NOT_FOUND")

    def test_non_loopback_bind_rejected(self):
        from src.errors import WorkbenchError
        with self.assertRaises(WorkbenchError):
            create_server(self.workspace, MANIFEST, CONFIG,
                          host="0.0.0.0", port=0)

    def _wait(self, predicate, timeout=30):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if predicate():
                return
            time.sleep(0.1)
        self.fail("condition not reached before timeout")


if __name__ == "__main__":
    unittest.main()
