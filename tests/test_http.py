from __future__ import annotations

import json
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
from pathlib import Path

from kde_drift.http_server import create_server
from kde_drift.service import WorkbenchService

from tests.common import fast_config, generate_small_data


class HttpWorkstationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.temp = tempfile.TemporaryDirectory()
        cls.data_dir = generate_small_data(Path(cls.temp.name) / "data")
        cls.service = WorkbenchService(
            cls.data_dir,
            Path(cls.temp.name) / "runs",
            dataset="small",
            config=fast_config(),
        )
        cls.server = create_server(cls.service, port=0)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.base = f"http://127.0.0.1:{cls.server.server_address[1]}"

    @classmethod
    def tearDownClass(cls) -> None:
        cls.service.stop()
        cls.service.wait(timeout=5)
        cls.server.shutdown()
        cls.server.server_close()
        cls.temp.cleanup()

    def request(self, path: str, *, method: str = "GET", body=None):
        data = None if body is None else json.dumps(body).encode("utf-8")
        request = urllib.request.Request(
            self.base + path,
            data=data,
            method=method,
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(request, timeout=10) as response:
            return response.status, response.read(), response.headers

    def test_health_static_and_run_lifecycle(self) -> None:
        status, payload, _ = self.request("/api/health")
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(payload)["bind"], "127.0.0.1")
        status, payload, headers = self.request("/")
        self.assertEqual(status, 200)
        self.assertIn(b"KDE Drift Lab", payload)
        self.assertIn("text/html", headers["Content-Type"])

        status, payload, _ = self.request(
            "/api/run",
            method="POST",
            body={"dataset": "small", "config": fast_config(), "batch_size": 181, "batch_order": "reverse"},
        )
        self.assertEqual(status, 202)
        self.assertEqual(json.loads(payload)["run"]["state"], "running")
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            _, payload, _ = self.request("/api/status")
            status_value = json.loads(payload)
            if status_value["state"] in {"completed", "error", "stopped"}:
                break
            time.sleep(0.05)
        self.assertEqual(status_value["state"], "completed")
        self.assertTrue(status_value["analysis"]["valid"])

        _, payload, _ = self.request("/api/results")
        results = json.loads(payload)
        self.assertEqual(len(results["payloads"]["density"]["columns"]), 8)
        self.assertIn("drift", results["payloads"])

        status, payload, headers = self.request("/api/export/density")
        self.assertEqual(status, 200)
        self.assertIn("attachment", headers["Content-Disposition"])
        export = json.loads(payload)
        self.assertEqual(export["kind"], "density_curves")
        self.assertEqual(len(export["columns"]), 8)

    def test_invalid_batch_http_error_is_structured(self) -> None:
        request = urllib.request.Request(
            self.base + "/api/run",
            data=json.dumps({"dataset": "small", "batches": [[]]}).encode("utf-8"),
            method="POST",
            headers={"Content-Type": "application/json"},
        )
        with self.assertRaises(urllib.error.HTTPError) as context:
            urllib.request.urlopen(request, timeout=10)
        self.assertEqual(context.exception.code, 422)
        error = json.loads(context.exception.read())
        self.assertEqual(error["error"]["code"], "E_INVALID_BATCH")


if __name__ == "__main__":
    unittest.main()
