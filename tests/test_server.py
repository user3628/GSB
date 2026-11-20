from __future__ import annotations

import json
import threading
import unittest
import urllib.error
import urllib.request

from dimcluster.samples import generate_samples
from dimcluster.server import create_server

from .helpers import small_config, workspace


class ServerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.root = workspace()
        generate_samples(cls.root / "samples")
        cls.manifest = cls.root / "samples" / "manifest.json"
        cls.config = small_config(cls.manifest, ["small"])
        cls.server, cls.context = create_server(cls.root, cls.config, "127.0.0.1", 0)
        cls.port = int(cls.server.server_address[1])
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=5)

    def _request(
        self,
        method: str,
        path: str,
        body: dict | None = None,
    ) -> tuple[int, dict | str]:
        data = None if body is None else json.dumps(body).encode("utf-8")
        request = urllib.request.Request(
            f"http://127.0.0.1:{self.port}{path}",
            data=data,
            method=method,
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                raw = response.read().decode("utf-8")
                status = response.status
        except urllib.error.HTTPError as exc:
            raw = exc.read().decode("utf-8")
            status = exc.code
            exc.close()
        try:
            return status, json.loads(raw)
        except json.JSONDecodeError:
            return status, raw

    def test_health_and_html(self) -> None:
        status, payload = self._request("GET", "/api/health")
        self.assertEqual(status, 200)
        self.assertTrue(payload["ok"])
        status, html = self._request("GET", "/")
        self.assertEqual(status, 200)
        self.assertIn("DimCluster Workbench", html)

    def test_invalid_config_http_error(self) -> None:
        invalid = json.loads(json.dumps(self.config))
        invalid["reduction"]["target_dimension"] = 1
        status, payload = self._request(
            "POST", "/api/config/validate", {"config": invalid}
        )
        self.assertEqual(status, 400)
        self.assertEqual(payload["error"]["code"], "E_INVALID_CONFIG")

    def test_background_stop_and_resume_preserves_config(self) -> None:
        import time

        root = workspace()
        generate_samples(root / "samples")
        manifest = root / "samples" / "manifest.json"
        config = small_config(manifest, ["small", "medium"])
        server, _ = create_server(root, config, "127.0.0.1", 0)
        port = int(server.server_address[1])
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()

        def request(method: str, path: str, body: dict | None = None) -> tuple[int, dict]:
            data = None if body is None else json.dumps(body).encode("utf-8")
            req = urllib.request.Request(
                f"http://127.0.0.1:{port}{path}",
                data=data,
                method=method,
                headers={"Content-Type": "application/json"},
            )
            try:
                with urllib.request.urlopen(req, timeout=30) as response:
                    return response.status, json.loads(response.read().decode("utf-8"))
            except urllib.error.HTTPError as exc:
                payload = json.loads(exc.read().decode("utf-8"))
                exc.close()
                return exc.code, payload

        try:
            status, started = request(
                "POST",
                "/api/run",
                {
                    "config": config,
                    "mode": "full",
                    "background": True,
                    "stop_after_tier": "small",
                },
            )
            self.assertEqual(status, 202)
            self.assertTrue(started["running"])
            latest: dict = {}
            for _ in range(200):
                time.sleep(0.05)
                _, latest = request("GET", "/api/runs/latest")
                if not latest.get("running"):
                    break
            result = latest["last_result"]
            self.assertTrue(result["stopped"])
            self.assertFalse(result["report_written"])
            self.assertEqual(result["completed_tiers"], ["small"])

            status, resumed = request("POST", "/api/resume", {"verify_full": False})
            self.assertEqual(status, 200)
            self.assertTrue(resumed["report_written"])
            self.assertEqual(resumed["completed_tiers"], ["small", "medium"])
            _, report = request("GET", "/api/report")
            self.assertEqual(report["overall"]["tier_count"], 2)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

    def test_run_status_stop_and_report(self) -> None:
        status, imported = self._request(
            "POST", "/api/import", {"manifest_path": str(self.manifest)}
        )
        self.assertEqual(status, 200)
        self.assertTrue(imported["verification"]["ok"])
        status, payload = self._request(
            "POST",
            "/api/run",
            {
                "config": self.config,
                "mode": "full",
                "background": False,
            },
        )
        self.assertEqual(status, 200)
        self.assertTrue(payload["ok"])
        self.assertTrue(payload["report_written"])
        status, status_payload = self._request("GET", "/api/status")
        self.assertEqual(status, 200)
        self.assertGreaterEqual(status_payload["sequence_head"], 5)
        status, fingerprints = self._request("GET", "/api/fingerprints")
        self.assertEqual(status, 200)
        self.assertIn("report", fingerprints)
        status, report = self._request("GET", "/api/report")
        self.assertEqual(status, 200)
        self.assertEqual(report["tiers"]["small"]["rows"], 400)
        status, rationale = self._request("GET", "/api/rationale?tier=small")
        self.assertEqual(status, 200)
        self.assertIn("selection", rationale["reduction"])
        status, stop_payload = self._request(
            "POST", "/api/stop", {"reason": "unit-test"}
        )
        self.assertEqual(status, 200)
        self.assertTrue(stop_payload["stop_requested"])


if __name__ == "__main__":
    unittest.main()
