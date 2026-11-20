from __future__ import annotations

import json
import sys
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from join_shuffle_engine.http_api import LocalApiServer  # noqa: E402
from join_shuffle_engine.service import EngineService  # noqa: E402
from scripts.generate_samples import generate_samples  # noqa: E402


DATA_ROOT = PROJECT_ROOT / "data" / "snapshots"


class ApiTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        generate_samples(DATA_ROOT, force=True)

    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(prefix="join-api-tests-")
        self.addCleanup(self.temp.cleanup)
        self.service = EngineService(Path(self.temp.name) / "api.db")
        self.server = LocalApiServer(("127.0.0.1", 0), self.service)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base = f"http://127.0.0.1:{self.server.server_address[1]}"

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5.0)
        self.service.close()

    def request(self, method: str, path: str, body: dict | None = None):
        data = None
        headers = {}
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(
            self.base + path, data=data, headers=headers, method=method
        )
        try:
            with urllib.request.urlopen(req, timeout=20) as response:
                return response.status, json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as error:
            try:
                return error.code, json.loads(error.read().decode("utf-8"))
            finally:
                error.close()

    def test_http_api_end_to_end_and_error_codes(self) -> None:
        status, body = self.request("GET", "/v1/health")
        self.assertEqual(status, 200)
        self.assertTrue(body["data"]["loopback_only"])

        status, body = self.request(
            "POST",
            "/v1/snapshots/load",
            {
                "dataset_id": "demo",
                "left_path": str(DATA_ROOT / "demo-left-v1"),
                "right_path": str(DATA_ROOT / "demo-right-v1"),
            },
        )
        self.assertEqual(status, 200)
        self.assertEqual(body["data"]["dataset"]["pair_version"], "L1:R1")

        status, body = self.request(
            "POST",
            "/v1/join",
            {
                "request_id": "api-join",
                "dataset_id": "demo",
                "partitions": 7,
                "join_type": "inner",
                "measure": "left",
                "precision": {"mode": "high"},
            },
        )
        self.assertEqual(status, 200)
        self.assertEqual(body["data"]["request"]["status"], "committed")
        self.assertGreater(len(body["data"]["result"]["results"]), 0)

        status, body = self.request("GET", "/v1/requests/api-join")
        self.assertEqual(status, 200)
        self.assertEqual(body["data"]["status"], "committed")

        status, body = self.request("GET", "/v1/requests/api-join/partitions")
        self.assertEqual(status, 200)
        self.assertEqual(len(body["data"]["partitions"]), 7)

        status, body = self.request("GET", "/v1/requests/api-join/result")
        self.assertEqual(status, 200)
        self.assertEqual(
            body["data"]["results"][0]["group_key"],
            min(item["group_key"] for item in body["data"]["results"]),
        )

        status, body = self.request("GET", "/v1/requests/api-join/precision")
        self.assertEqual(status, 200)
        self.assertEqual(body["data"]["contract"]["selected_path"], "high")

        status, body = self.request("GET", "/v1/requests/not-found/result")
        self.assertEqual(status, 404)
        self.assertEqual(body["error"]["code"], "E_REQUEST_NOT_FOUND")

        status, body = self.request(
            "POST",
            "/v1/join",
            {
                "request_id": "api-fast-reject",
                "dataset_id": "demo",
                "partitions": 7,
                "precision": {
                    "mode": "fast",
                    "target_abs_error": 1.0e-20,
                },
            },
        )
        self.assertEqual(status, 422)
        self.assertEqual(body["error"]["code"], "E_PRECISION_BUDGET")

        status, body = self.request("GET", "/v1/recovery/report")
        self.assertEqual(status, 200)
        self.assertEqual(body["data"]["report_count"], 0)


if __name__ == "__main__":
    unittest.main()
