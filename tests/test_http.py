from __future__ import annotations

import json
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
from pathlib import Path

from sumengine.http_api import create_server

from .common import workspace_with_samples


class HttpApiTests(unittest.TestCase):
    def test_http_run_edit_export_and_errors(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            engine = workspace_with_samples(Path(temp))
            server = create_server(engine, host="127.0.0.1", port=0)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            base = f"http://127.0.0.1:{server.server_address[1]}"

            def request(method: str, path: str, body=None):
                data = None if body is None else json.dumps(body).encode("utf-8")
                req = urllib.request.Request(
                    base + path,
                    data=data,
                    method=method,
                    headers={"Content-Type": "application/json"},
                )
                with urllib.request.urlopen(req, timeout=15) as response:
                    return response.status, json.loads(response.read().decode("utf-8"))

            try:
                status, health = request("GET", "/health")
                self.assertEqual(status, 200)
                self.assertTrue(health["ok"])

                status, started = request("POST", "/run", {"mode": "parallel"})
                self.assertEqual(status, 202)
                run_id = started["run_id"]
                job = None
                deadline = time.monotonic() + 20
                while time.monotonic() < deadline:
                    _, job = request("GET", f"/runs/{run_id}")
                    if job["status"] != "running":
                        break
                    time.sleep(0.05)
                self.assertIsNotNone(job)
                self.assertEqual(job["status"], "complete")
                self.assertEqual(job["result"]["status"], "complete")
                self.assertEqual(len(job["result"]["documents"]), 4)

                _, candidates = request("GET", "/candidates?doc_id=news-zh-002")
                self.assertEqual(len(candidates["candidates"]), 3)
                self.assertEqual(len(candidates["fusion"]["candidates"]), 3)

                status, edit = request(
                    "POST",
                    "/edit",
                    {"doc_id": "news-zh-002", "text": "HTTP 人工编辑摘要：保留试点范围与后续公开数据安排。"},
                )
                self.assertEqual(status, 200)
                self.assertEqual(edit["edit_version"], 1)

                _, exported = request("GET", "/export")
                edited = next(doc for doc in exported["documents"] if doc["doc_id"] == "news-zh-002")
                self.assertTrue(edited["summary"].startswith("HTTP 人工编辑摘要"))

                _, stopped = request("POST", "/stop", {"reason": "http-test"})
                self.assertTrue(stopped["stop_requested"])

                try:
                    request("GET", "/candidates?doc_id=missing")
                except urllib.error.HTTPError as exc:
                    self.assertEqual(exc.code, 404)
                    exc.close()
                else:
                    self.fail("expected HTTP 404 for an unknown doc_id")
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)


if __name__ == "__main__":
    unittest.main()
