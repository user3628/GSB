from __future__ import annotations

import json
import tempfile
import threading
import unittest
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

from rateguard.config import GuardConfig
from rateguard.engine import Engine
from rateguard.http_api import RateGuardHTTPServer


class HTTPAPITests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        db = Path(self.temp.name) / "http.sqlite3"
        config = GuardConfig(
            window_length_ms=1_000,
            step_ms=1_000,
            min_samples_p50=1,
            min_samples_p95=2,
            min_samples_p99=100,
            max_quota=100,
            min_breaker_samples=2,
            consecutive_failure_threshold=2,
            open_duration_ms=1_000,
        )
        self.engine = Engine(db, config)
        self.server = RateGuardHTTPServer(("127.0.0.1", 0), self.engine)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base_url = f"http://127.0.0.1:{self.server.server_address[1]}"

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        self.temp.cleanup()

    def request(
        self, method: str, path: str, body: dict[str, Any] | None = None
    ) -> tuple[int, dict[str, Any]]:
        data = None if body is None else json.dumps(body).encode("utf-8")
        request = urllib.request.Request(
            self.base_url + path,
            data=data,
            method=method,
            headers={"Content-Type": "application/json"} if data is not None else {},
        )
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status, json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            with exc:
                return exc.code, json.loads(exc.read().decode("utf-8"))

    @property
    def route_path(self) -> str:
        return "/v1/circuit/state?route=" + urllib.parse.quote("/api/http", safe="")

    def test_end_to_end_contract_and_required_error_codes(self) -> None:
        status, health = self.request("GET", "/health")
        self.assertEqual(status, 200)
        self.assertEqual(health["numeric_contract"]["window_boundary"], "[start_ms,end_ms)")

        batch = {
            "batch_id": "http-b1",
            "sequence": 1,
            "events": [
                {"timestamp_ms": 100, "route": "/api/http", "success": True, "latency_us": 30_000},
                {"timestamp_ms": 200, "route": "/api/http", "success": True, "latency_us": 31_000},
                {"timestamp_ms": 1_100, "route": "/api/http", "success": True, "latency_us": 32_000},
            ],
        }
        status, ingested = self.request("POST", "/v1/events/batches", batch)
        self.assertEqual(status, 200)
        self.assertTrue(ingested["ok"])

        status, duplicate = self.request("POST", "/v1/events/batches", batch)
        self.assertEqual(status, 200)
        self.assertTrue(duplicate["duplicate"])

        conflict = json.loads(json.dumps(batch))
        conflict["events"][0]["latency_us"] = 99_000
        status, conflict_payload = self.request("POST", "/v1/events/batches", conflict)
        self.assertEqual(status, 409)
        self.assertEqual(conflict_payload["error"]["code"], "E_CONFLICTING_REPORT")

        status, state = self.request("GET", self.route_path)
        self.assertEqual(status, 200)
        self.assertIn(state["state"], ("CLOSED", "OPEN", "HALF_OPEN"))

        status, decision = self.request(
            "POST",
            "/v1/limit/decision",
            {
                "route": "/api/http",
                "current_concurrency": 0,
                "budget_watermark": "1.0",
                "as_of_ms": 1_100,
            },
        )
        self.assertEqual(status, 200)
        self.assertTrue(decision["consistency"]["same_window_version_in_limiter_and_breaker"])

        status, insufficient = self.request(
            "GET",
            "/v1/windows/stats?route="
            + urllib.parse.quote("/api/http", safe="")
            + "&strict=P99",
        )
        self.assertEqual(status, 422)
        self.assertEqual(insufficient["error"]["code"], "E_INSUFFICIENT_SAMPLES")

        status, stale = self.request(
            "POST",
            "/v1/limit/decision",
            {"route": "/api/http", "current_concurrency": 0, "window_version": 999},
        )
        self.assertEqual(status, 409)
        self.assertEqual(stale["error"]["code"], "E_STALE_WINDOW")

        status, missing = self.request("GET", "/v1/events/batches/missing/1")
        self.assertEqual(status, 404)
        self.assertEqual(missing["error"]["code"], "E_BATCH_NOT_FOUND")

        status, invalid = self.request(
            "POST",
            "/v1/circuit/transition",
            {
                "route": "/api/http",
                "to_state": "HALF_OPEN",
                "occurred_at_ms": 1_100,
                "reason": "ILLEGAL",
            },
        )
        self.assertEqual(status, 409)
        self.assertEqual(invalid["error"]["code"], "E_INVALID_TRANSITION")

        status, recovery = self.request("GET", "/v1/recovery/report")
        self.assertEqual(status, 200)
        self.assertFalse(recovery["report"]["policy"]["exit_report_guarantee"])
        self.assertIn("sqlite_quick_check", recovery["report"]["integrity"])

        status, quota = self.request(
            "GET", "/v1/quota/derivation?route=" + urllib.parse.quote("/api/http", safe="")
        )
        self.assertEqual(status, 200)
        self.assertIn("steps", quota["derivation"])


if __name__ == "__main__":
    unittest.main()

