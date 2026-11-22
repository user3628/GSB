from __future__ import annotations

import base64
import json
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from pathlib import Path

from support import ROOT, predicates, sample_bytes

from joincost.http_server import create_server
from joincost.service import Service
from joincost.store import Store


class HTTPTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        db = Path(self.temp.name) / "http.db"
        self.service = Service(Store(db), workspace_root=ROOT, recover_on_start=False)
        self.server = create_server(self.service, port=0)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base = f"http://127.0.0.1:{self.server.server_address[1]}"

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(3)
        self.temp.cleanup()

    def request(self, path: str, body=None, expected_status=200):
        data = None if body is None else json.dumps(body).encode("utf-8")
        request = urllib.request.Request(
            self.base + path,
            data=data,
            headers={"Content-Type": "application/json"},
            method="GET" if body is None else "POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                status = response.status
                payload = json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            status = exc.code
            try:
                payload = json.loads(exc.read().decode("utf-8"))
            finally:
                exc.close()
        self.assertEqual(status, expected_status, payload)
        return payload

    def test_loopback_binding_dashboard_and_health(self):
        self.assertEqual(self.server.server_address[0], "127.0.0.1")
        with urllib.request.urlopen(self.base + "/", timeout=5) as response:
            html = response.read().decode("utf-8")
        self.assertIn("JoinCost Diagnostic Workbench", html)
        health = self.request("/health")
        self.assertTrue(health["loopback_only"])

    def test_import_plan_get_and_replay(self):
        imported = self.request(
            "/v1/snapshots/import", {"path": "samples/stats_v2.bin"}
        )
        self.assertEqual(imported["import_report"]["decision"], "ACCEPT")
        payload = {
            "request_id": "http-request",
            "snapshot_version": 1001,
            "predicates": predicates(),
        }
        first = self.request("/v1/plans", payload)
        second = self.request("/v1/plans", payload)
        self.assertEqual(first, second)
        fetched = self.request("/v1/plans/http-request")
        self.assertEqual(fetched["cost"], first["cost"])
        snapshot = self.request("/v1/snapshot")
        self.assertEqual(snapshot["current_snapshot"]["snapshot_version"], 1001)

    def test_inline_base64_corrupt_returns_unified_json_error(self):
        payload = {
            "content_base64": base64.b64encode(b"not-a-snapshot").decode("ascii"),
            "source": "bad",
        }
        result = self.request("/v1/snapshots/import", payload, expected_status=422)
        self.assertFalse(result["ok"])
        self.assertEqual(result["error"]["code"], "E_FORMAT_UNSUPPORTED")
        snapshot = self.request("/v1/snapshot")
        self.assertIsNone(snapshot["current_snapshot"])

    def test_conflicting_request_returns_409(self):
        self.request("/v1/snapshots/import", {"path": "samples/stats_v2.bin"})
        base_payload = {
            "request_id": "conflict",
            "snapshot_version": 1001,
            "predicates": predicates(),
        }
        self.request("/v1/plans", base_payload)
        changed = dict(base_payload)
        changed["predicates"] = predicates()[:-1]
        result = self.request("/v1/plans", changed, expected_status=409)
        self.assertEqual(result["error"]["code"], "E_CONFLICTING_REQUEST")

    def test_unknown_endpoint_is_json_404(self):
        result = self.request("/v1/nope", expected_status=404)
        self.assertEqual(result["error"]["code"], "E_NOT_FOUND")


if __name__ == "__main__":
    unittest.main()

