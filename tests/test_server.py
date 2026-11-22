import json
import tempfile
import threading
import unittest
import urllib.request
from pathlib import Path

from shotcheck.generator import generate_tier
from shotcheck.server import serve


class ServerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.workspace = Path(self.temp.name) / "small"
        generate_tier(self.workspace, "small", 20261123)
        self.server = serve(self.workspace, port=0)
        self.host, self.port = self.server.server_address[:2]
        self.thread = threading.Thread(
            target=self.server.serve_forever, daemon=True
        )
        self.thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        self.temp.cleanup()

    def request(self, path, method="GET", body=None):
        payload = None if body is None else json.dumps(body).encode("utf-8")
        request = urllib.request.Request(
            f"http://{self.host}:{self.port}{path}",
            method=method,
            data=payload,
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(request, timeout=20) as response:
            return response.status, json.loads(response.read())

    def test_health_run_result_stop(self):
        status, health = self.request("/health")
        self.assertEqual(status, 200)
        self.assertTrue(health["ok"])
        status, run = self.request("/run", method="POST", body={})
        self.assertEqual(status, 200)
        self.assertEqual(run["status"], "complete")
        status, result = self.request("/result")
        self.assertEqual(status, 200)
        self.assertEqual(result["summary"]["shot_count"], 14)
        status, stopped = self.request("/stop", method="POST", body={})
        self.assertEqual(status, 202)
        self.assertFalse(stopped["stop_requested"])


if __name__ == "__main__":
    unittest.main()
