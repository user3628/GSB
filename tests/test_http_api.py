from __future__ import annotations

import json
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from pathlib import Path

from lease_lock.engine import LeaseEngine
from lease_lock.server import LeaseHTTPServer, LeaseRequestHandler


ROOT = Path(__file__).resolve().parents[1]
SAMPLE = json.loads((ROOT / "samples" / "deterministic_leases.json").read_text(encoding="utf-8"))


class LeaseHTTPTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.engine = LeaseEngine(Path(self.tempdir.name) / "http.db")
        self.server = LeaseHTTPServer(("127.0.0.1", 0), LeaseRequestHandler, self.engine)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base_url = f"http://127.0.0.1:{self.server.server_address[1]}"

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        self.engine.close()
        self.tempdir.cleanup()

    def test_http_contract_and_error_body(self) -> None:
        health = self.call("GET", "/healthz")
        self.assertTrue(health["ok"])
        loaded = self.call("PUT", "/v1/snapshot/replace", {
            "base_version": 0,
            "leases": SAMPLE["leases"],
        })
        self.assertEqual(loaded["snapshot_version"], 1)

        acquired = self.call("POST", "/v1/locks/acquire", {
            "lock_key": "http-lock",
            "holder": "http-holder",
            "ttl_ms": 5000,
        })
        self.assertEqual(acquired["snapshot_version"], 2)
        held = self.call("POST", "/v1/locks/acquire", {
            "lock_key": "http-lock",
            "holder": "other",
            "ttl_ms": 5000,
        })
        self.assertEqual(held["code"], "E_LOCK_HELD")
        self.assertEqual(held["snapshot_version"], 2)
        self.assertIn("remaining_ttl_ms", held["details"])

        current = self.call("GET", "/v1/snapshot")
        self.assertEqual(current["snapshot_version"], 2)
        historical = self.call("GET", "/v1/snapshot?version=1")
        self.assertEqual(historical["snapshot_version"], 1)
        recovery = self.call("GET", "/v1/recovery/report")
        self.assertFalse(recovery["report"]["complete_exit_report"])

    def call(self, method: str, path: str, body: dict | None = None) -> dict:
        data = None
        headers = {"Accept": "application/json"}
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(
            self.base_url + path, data=data, headers=headers, method=method
        )
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            try:
                return json.loads(exc.read().decode("utf-8"))
            finally:
                exc.close()


if __name__ == "__main__":
    unittest.main()

