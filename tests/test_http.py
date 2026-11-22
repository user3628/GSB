from __future__ import annotations

import json
import tempfile
import threading
import unittest
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

from slo_budget.server import SloHttpServer
from slo_budget.service import SloService

from tests.support import AS_OF_MS, CURRENT_BIN, EVENTS, FUTURE_BIN


class HttpApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.service = SloService(
            Path(self.tmp.name) / "http.db",
            clock_ms=lambda: AS_OF_MS + 20,
        )
        self.server = SloHttpServer(("127.0.0.1", 0), self.service)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        host, port = self.server.server_address[:2]
        self.base = f"http://{host}:{port}"

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.service.close()
        self.tmp.cleanup()

    def request(
        self,
        method: str,
        path: str,
        body: dict | None = None,
    ) -> tuple[int, dict]:
        data = None
        headers = {"Accept": "application/json"}
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(
            self.base + path,
            data=data,
            headers=headers,
            method=method,
        )
        try:
            with urllib.request.urlopen(request, timeout=20) as response:
                return response.status, json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as error:
            return error.code, json.loads(error.read().decode("utf-8"))

    def test_http_health_preflight_load_query_export_and_failure_envelope(self) -> None:
        status, health = self.request("GET", "/health")
        self.assertEqual(status, 200)
        self.assertTrue(health["ok"])

        status, preflight = self.request(
            "POST", "/v1/preflight", {"path": str(FUTURE_BIN)}
        )
        self.assertEqual(status, 200)
        self.assertEqual(preflight["verdict"], "REJECT")
        self.assertEqual(preflight["business_code"], "E_FORMAT_UNSUPPORTED")

        status, loaded = self.request(
            "POST",
            "/v1/snapshots/replace",
            {"path": str(CURRENT_BIN), "batch_id": "http-load"},
        )
        self.assertEqual(status, 200)
        self.assertTrue(loaded["ok"])

        query = urllib.parse.urlencode(
            {
                "service": "checkout-api",
                "sli_type": "availability",
                "as_of_ms": AS_OF_MS,
                "events_path": str(EVENTS),
                "persist_alert": "true",
            }
        )
        status, result = self.request("GET", "/v1/query?" + query)
        self.assertEqual(status, 200)
        self.assertEqual(result["alert"]["grade"], "CRITICAL")
        self.assertIn("alert_persistence", result)
        self.assertEqual(len(self.service.storage.list_alerts()), 1)

        with tempfile.TemporaryDirectory() as tmp:
            export_path = Path(tmp) / "http-export.jsonl"
            status, exported = self.request(
                "POST",
                "/v1/export",
                {"path": str(export_path), "format": "jsonl"},
            )
            self.assertEqual(status, 200)
            self.assertTrue(export_path.exists())
            self.assertEqual(exported["record_count"], 48)

        before = self.request("GET", "/v1/state")[1]["active"]
        status, rejection = self.request(
            "POST",
            "/v1/snapshots/replace",
            {"path": str(FUTURE_BIN), "batch_id": "http-future"},
        )
        self.assertEqual(status, 415)
        self.assertEqual(rejection["error"]["code"], "E_FORMAT_UNSUPPORTED")
        after = self.request("GET", "/v1/state")[1]["active"]
        self.assertEqual(before, after)

        missing = urllib.parse.urlencode(
            {
                "service": "does-not-exist",
                "sli_type": "availability",
                "as_of_ms": AS_OF_MS,
            }
        )
        status, insufficient = self.request("GET", "/v1/query?" + missing)
        self.assertEqual(status, 422)
        self.assertEqual(insufficient["error"]["code"], "E_INSUFFICIENT_SAMPLES")
        evaluation = insufficient["error"]["details"]["evaluation"]
        self.assertEqual(evaluation["windows"][0]["sli"], "1.000000")
        self.assertEqual(evaluation["windows"][0]["error_budget"]["burn_rate"], "0.000000")

        status, not_found = self.request("GET", "/v1/unknown")
        self.assertEqual(status, 404)
        self.assertEqual(not_found["error"]["code"], "E_NOT_FOUND")

        status, recovery = self.request("GET", "/v1/recovery")
        self.assertEqual(status, 200)
        self.assertTrue(recovery["ok"])
        self.assertIn("recovery_reports", recovery)


if __name__ == "__main__":
    unittest.main()
