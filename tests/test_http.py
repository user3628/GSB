from __future__ import annotations

import json
import tempfile
import threading
import unittest
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from cmir_alignment.datagen import generate_dataset
from cmir_alignment.http_app import create_server


class HttpTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(prefix="cmir-http-")
        self.root = Path(self.temp.name)
        self.data = self.root / "data"
        generate_dataset(self.data, shards=3, items_per_shard=3)
        self.server = create_server(self.root / "ws", host="127.0.0.1", port=0)
        self.port = int(self.server.server_address[1])
        self.base = f"http://127.0.0.1:{self.port}"
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        self.temp.cleanup()

    def request(self, method: str, path: str, body: dict | None = None) -> tuple[int, dict]:
        data = None if body is None else json.dumps(body).encode("utf-8")
        request = Request(
            self.base + path,
            data=data,
            method=method,
            headers={"Content-Type": "application/json"},
        )
        try:
            with urlopen(request, timeout=30) as response:
                return response.status, json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            payload = json.loads(exc.read().decode("utf-8"))
            exc.close()
            return exc.code, payload

    def test_full_http_workflow_and_error_codes(self) -> None:
        status, health = self.request("GET", "/health")
        self.assertEqual(status, 200)
        self.assertTrue(health["ok"])
        status, imported = self.request("POST", "/import", {"dataset_dir": str(self.data)})
        self.assertEqual(status, 200)
        status, configured = self.request(
            "POST", "/config", {"bits": 8, "granularity": "per_dim"}
        )
        self.assertEqual(status, 200)
        status, stopped = self.request("POST", "/build", {"stop_after_units": 3})
        self.assertEqual(status, 200)
        self.assertEqual(stopped["status"], "stopped")
        status, stop_request = self.request("POST", "/stop", {})
        self.assertEqual(status, 200)
        status, status_payload = self.request("GET", "/status")
        self.assertEqual(status, 200)
        self.assertTrue(status_payload["stop_requested"])
        status, resumed = self.request("POST", "/resume", {})
        self.assertEqual(status, 200)
        self.assertEqual(resumed["status"], "complete")
        status, search = self.request(
            "POST",
            "/search",
            {"mode": "text2image", "query": "红色 red 方块 square", "k": 3},
        )
        self.assertEqual(status, 200)
        self.assertEqual(len(search["top_k"]), 3)
        self.assertIn("error_propagation", search["top_k"][0])
        status, invalid = self.request(
            "POST", "/search", {"mode": "text2image", "query": "", "k": 3}
        )
        self.assertEqual(status, 400)
        self.assertEqual(invalid["error"]["code"], "E_INVALID_QUERY")
        status, bad_bits = self.request("POST", "/config", {"bits": 6})
        self.assertEqual(status, 422)
        self.assertEqual(bad_bits["error"]["code"], "E_INVALID_BITWIDTH")
        status, verify = self.request("POST", "/verify", {})
        self.assertEqual(status, 200)
        self.assertTrue(verify["consistent"])
        status, audit = self.request("GET", "/checkpoints/audit")
        self.assertEqual(status, 200)
        self.assertEqual(audit["status"], "advance")
        status, exported = self.request(
            "POST", "/export", {"output_dir": str(self.root / "export")}
        )
        self.assertEqual(status, 200)
        self.assertTrue((self.root / "export" / "report.json").is_file())


if __name__ == "__main__":
    unittest.main()
