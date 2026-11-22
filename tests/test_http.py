from __future__ import annotations

import json
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

from telemetry_align.http_api import create_server
from telemetry_align.workspace import AlignmentWorkspace


class HttpTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.temp = tempfile.TemporaryDirectory()
        cls.root = Path(cls.temp.name) / "workspace"
        AlignmentWorkspace(cls.root).init_demo(reset=True)
        cls.server = create_server(cls.root, port=0, quiet=True)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        host, port = cls.server.server_address[:2]
        cls.base = f"http://{host}:{port}"

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=5)
        cls.temp.cleanup()

    def request(
        self, path: str, *, method: str = "GET", body: dict[str, Any] | None = None
    ) -> tuple[int, Any]:
        data = None
        headers = {}
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(
            self.base + path, data=data, headers=headers, method=method
        )
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                raw = response.read()
                content_type = response.headers.get("Content-Type", "")
                payload = (
                    json.loads(raw.decode("utf-8"))
                    if "json" in content_type
                    else raw.decode("utf-8")
                )
                return response.status, payload
        except urllib.error.HTTPError as exc:
            payload = json.loads(exc.read().decode("utf-8"))
            exc.close()
            return exc.code, payload

    def test_static_and_health(self) -> None:
        status, html = self.request("/")
        self.assertEqual(status, 200)
        self.assertIn("多源时序增量对齐台", html)
        status, health = self.request("/api/health")
        self.assertEqual(status, 200)
        self.assertTrue(health["ok"])

    def test_summary_stale_then_run_then_available(self) -> None:
        status, payload = self.request("/api/summary")
        self.assertEqual(status, 409)
        self.assertEqual(payload["error"]["code"], "E_STALE_RESULT")
        status, run = self.request("/api/run", method="POST", body={"config": "v2"})
        self.assertEqual(status, 200)
        self.assertEqual(run["status"], "completed")
        status, summary = self.request("/api/summary")
        self.assertEqual(status, 200)
        self.assertEqual(summary["counts"]["grid_points"], 31)
        status, status_payload = self.request("/api/status")
        self.assertTrue(status_payload["complete"])

    def test_errors_are_stable_json_codes(self) -> None:
        status, payload = self.request(
            "/api/stop", method="POST", body={"after_stage": "invalid"}
        )
        self.assertEqual(status, 409)
        self.assertEqual(payload["error"]["code"], "E_INVALID_STAGE")
        status, payload = self.request("/api/does-not-exist")
        self.assertEqual(status, 404)
        self.assertEqual(payload["error"]["code"], "E_NOT_FOUND")

    def test_inline_import_materialises_three_sources_and_two_configs(self) -> None:
        manifest = AlignmentWorkspace(self.root).load_manifest()
        sources = {
            source_id: {
                "content": (self.root / relative_path).read_text(encoding="utf-8")
            }
            for source_id, relative_path in manifest["sources"].items()
        }
        configs = {
            name: {
                "content": (self.root / relative_path).read_text(encoding="utf-8")
            }
            for name, relative_path in manifest["configs"].items()
        }
        status, payload = self.request(
            "/api/import",
            method="POST",
            body={
                "sources": sources,
                "configs": configs,
                "active_config": "v2",
            },
        )
        self.assertEqual(status, 200)
        self.assertEqual(set(payload["manifest"]["sources"]), {"sensor_a", "sensor_b", "sensor_c"})
        self.assertEqual(set(payload["manifest"]["configs"]), {"v1", "v2"})
        self.assertIn("imports/sources/", payload["manifest"]["sources"]["sensor_a"])


if __name__ == "__main__":
    unittest.main()

