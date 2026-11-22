from __future__ import annotations

import json
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from pathlib import Path

from segval.http_server import WorkbenchServer


class HttpTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.server = WorkbenchServer(("127.0.0.1", 0), Path(self.temp.name))
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        host, port = self.server.server_address
        self.base = f"http://{host}:{port}"

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        self.temp.cleanup()

    def test_dashboard_and_status_are_available(self):
        with urllib.request.urlopen(self.base + "/") as response:
            self.assertEqual(response.status, 200)
            self.assertIn("SegVal", response.read().decode("utf-8"))
        with urllib.request.urlopen(self.base + "/api/status") as response:
            payload = json.loads(response.read())
            self.assertEqual(payload["status"], "new")
            self.assertEqual(payload["shards"], [])

    def test_invalid_threshold_returns_structured_error(self):
        request = urllib.request.Request(
            self.base + "/api/config",
            data=json.dumps({"matching": {"iou_threshold": 2.0}}).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with self.assertRaises(urllib.error.HTTPError) as caught:
            urllib.request.urlopen(request)
        self.assertEqual(caught.exception.code, 400)
        payload = json.loads(caught.exception.read())
        caught.exception.close()
        self.assertEqual(payload["error"]["code"], "E_INVALID_IOU_THRESHOLD")

    def test_init_demo_imports_four_local_shards(self):
        request = urllib.request.Request(
            self.base + "/api/init-demo",
            data=json.dumps({"shard_count": 4}).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(request) as response:
            payload = json.loads(response.read())
        self.assertTrue(payload["ok"])
        self.assertEqual(len(payload["imported"]), 4)
        with urllib.request.urlopen(self.base + "/api/status") as response:
            status = json.loads(response.read())
        self.assertEqual(len(status["shards"]), 4)


if __name__ == "__main__":
    unittest.main()

