from __future__ import annotations

import json
import tempfile
import threading
import unittest
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from shardseek.demo import run_demo
from shardseek.http_server import ShardSeekHTTPServer
from shardseek.workspace import Workspace

from tests.helpers import PROJECT_ROOT


def _request_json(base: str, path: str, *, method: str = "GET", body: dict | None = None):
    payload = None if body is None else json.dumps(body).encode("utf-8")
    request = Request(
        base + path,
        data=payload,
        method=method,
        headers={"Content-Type": "application/json"} if payload else {},
    )
    try:
        with urlopen(request, timeout=30) as response:
            return response.status, json.loads(response.read())
    except HTTPError as exc:
        return exc.code, json.loads(exc.read())


class HTTPTests(unittest.TestCase):
    def test_http_import_build_search_and_error_routes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            workspace = Workspace(Path(directory) / "runtime", project_root=PROJECT_ROOT).init()
            server = ShardSeekHTTPServer(("127.0.0.1", 0), workspace, PROJECT_ROOT)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            base = f"http://127.0.0.1:{server.server_address[1]}"
            try:
                status, health = _request_json(base, "/api/health")
                self.assertEqual(status, 200)
                self.assertTrue(health["ok"])

                status, payload = _request_json(
                    base,
                    "/api/reset",
                    method="POST",
                    body={"manifest_path": "samples/manifest.jsonl", "config": "baseline_8bit"},
                )
                self.assertEqual(status, 200)
                self.assertEqual(payload["import"]["enabled_count"], 7)

                status, built = _request_json(
                    base,
                    "/api/build",
                    method="POST",
                    body={"mode": "full"},
                )
                self.assertEqual(status, 200)
                self.assertFalse(built["stopped"])

                status, report = _request_json(
                    base,
                    "/api/search",
                    method="POST",
                    body={"direction": "text_to_image", "query_text": "红色方块 blue gradient", "top_k": 3},
                )
                self.assertEqual(status, 200)
                self.assertEqual(report["results"][0]["item_id"], "item-06")

                status, diagnostic = _request_json(base, "/api/diagnostics")
                self.assertEqual(status, 200)
                self.assertEqual(len(diagnostic["series"]), 7)

                status, error_payload = _request_json(
                    base,
                    "/api/search",
                    method="POST",
                    body={"direction": "text_to_image", "query_text": "red", "top_k": 999},
                )
                self.assertEqual(status, 400)
                self.assertEqual(error_payload["error"]["code"], "E_INVALID_K")

                with urlopen(base + "/api/image/item-01", timeout=10) as response:
                    self.assertEqual(response.status, 200)
                    self.assertTrue(response.read().startswith(b"\x89PNG"))
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)


class DemoTests(unittest.TestCase):
    def test_demo_generates_fixed_passing_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            report = run_demo(
                project_root=PROJECT_ROOT,
                root=root / "runtime",
                artifacts_dir=root / "artifacts",
            )
            self.assertEqual(report["demo_status"], "passed")
            self.assertTrue(report["stop_then_resume"]["all_equal_to_full_build"])
            self.assertTrue(report["incremental_vs_full"]["all_equal"])
            self.assertEqual(report["incremental_vs_full"]["earliest_stage"], "project_quantize")
            self.assertGreater(report["compact_4bit"]["max_error_bound_ratio_vs_8bit"], 1.0)
            codes = {entry["code"] for entry in report["business_error_demonstrations"].values()}
            self.assertIn("E_EMPTY_CAPTION", codes)
            self.assertIn("E_INVALID_K", codes)
            self.assertIn("E_STALE_CHECKPOINT", codes)
            self.assertIn("E_DIM_MISMATCH", codes)
            saved = json.loads((root / "artifacts" / "demo_report.json").read_text(encoding="utf-8"))
            self.assertEqual(saved["demo_sha256"], report["demo_sha256"])


if __name__ == "__main__":
    unittest.main()

