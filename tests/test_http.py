"""HTTP interface end-to-end smoke test (stdlib server + urllib client)."""

from __future__ import annotations

import json
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from pathlib import Path

from apps.http_server import create_server

from tests.helpers import make_dataset


class HttpInterfaceTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._tmp = tempfile.TemporaryDirectory()
        root = Path(cls._tmp.name)
        cls.data = make_dataset(root / "data")
        cls.server = create_server(root / "ws", host="127.0.0.1", port=0)
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=5)
        cls._tmp.cleanup()

    def _call(self, method, path, body=None):
        url = f"http://127.0.0.1:{self.port}{path}"
        data = None
        headers = {}
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                return response.status, json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read().decode("utf-8"))

    def test_full_http_flow(self):
        status, body = self._call("GET", "/api/health")
        self.assertEqual(status, 200)
        self.assertTrue(body["ok"])

        status, body = self._call(
            "POST", "/api/import", {"manifest_path": str(self.data / "manifest.json")}
        )
        self.assertEqual(status, 200)
        self.assertEqual(len(body["imported_views"]), 4)

        status, body = self._call("POST", "/api/build", {"mode": "full"})
        self.assertEqual(status, 200)
        self.assertEqual(body["status"], "completed")
        self.assertEqual(len(body["rebuilt"]), 10)

        status, body = self._call("GET", "/api/status")
        self.assertEqual(status, 200)
        self.assertEqual(len(body["stages"]), 10)
        for stage in body["stages"]:
            self.assertEqual(stage["status"], "published")
            self.assertIsNotNone(stage["desired_fingerprint"])
        self.assertIsNotNone(body["sparsity_stats"])

        status, body = self._call("GET", "/api/invalidations")
        self.assertEqual(status, 200)
        self.assertIsNotNone(body["invalidation"])
        self.assertIsNotNone(body["sparsity_stats"])

        alt = self.data / "shards_alt" / "view_02.png"
        status, body = self._call(
            "POST", "/api/view/replace", {"view_id": "view_02", "image_path": str(alt)}
        )
        self.assertEqual(status, 200)
        status, body = self._call("POST", "/api/build", {"mode": "incremental"})
        self.assertEqual(status, 200)
        self.assertEqual(
            set(body["rebuilt"]),
            {"projection:view_02", "voxelize:view_02", "fusion", "sparsify"},
        )

        status, body = self._call(
            "POST", "/api/config", {"updates": {"voxel_resolution": 0.4}}
        )
        self.assertEqual(status, 200)
        status, body = self._call("POST", "/api/build", {"mode": "incremental"})
        self.assertEqual(status, 200)
        self.assertEqual(len(body["rebuilt"]), 6)

        status, body = self._call("POST", "/api/verify")
        self.assertEqual(status, 200)
        self.assertTrue(body["match"])

        status, body = self._call("GET", "/api/export/summary")
        self.assertEqual(status, 200)
        self.assertGreater(body["voxel_count"], 0)
        self.assertIn("digests", body)

        status, body = self._call("POST", "/api/stop")
        self.assertEqual(status, 200)
        status, body = self._call("POST", "/api/build", {"mode": "incremental"})
        self.assertEqual(status, 409)
        self.assertEqual(body["error"]["code"], "E_STOPPED")
        status, body = self._call("POST", "/api/recover")
        self.assertEqual(status, 200)
        self.assertIn("resumed_seq", body)
        status, body = self._call("POST", "/api/build", {"mode": "resume"})
        self.assertEqual(status, 200)
        self.assertEqual(body["status"], "completed")

    def test_unknown_route_404(self):
        status, body = self._call("GET", "/api/nope")
        self.assertEqual(status, 404)
        self.assertEqual(body["error"]["code"], "E_NOT_FOUND")

    def test_business_error_is_structured_409(self):
        status, body = self._call(
            "POST", "/api/import", {"manifest_path": str(self.data / "manifest.json")}
        )
        self.assertEqual(status, 200)
        status, body = self._call(
            "POST", "/api/config", {"updates": {"no_such_key": 1}}
        )
        self.assertEqual(status, 409)
        self.assertEqual(body["error"]["code"], "E_INVALID_STAGE")


if __name__ == "__main__":
    unittest.main()
