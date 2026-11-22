from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

from gspipe.sample_data import sample_import_payload
from gspipe.server import create_server
from gspipe.workspace import Workspace


class HTTPTests(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path(tempfile.mkdtemp(prefix="gspipe-http-test-"))
        Workspace(self.root).ensure_layout()
        self.server = create_server(self.root, "127.0.0.1", 0)
        host, port = self.server.server_address[:2]
        self.base = f"http://{host}:{port}"
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)

    def request(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
        *,
        raw: bool = False,
    ) -> tuple[int, Any, dict[str, str]]:
        data = None if payload is None else json.dumps(payload).encode("utf-8")
        request = urllib.request.Request(
            self.base + path,
            data=data,
            headers={"Content-Type": "application/json"} if data is not None else {},
            method=method,
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                body = response.read()
                headers = {key.lower(): value for key, value in response.headers.items()}
                return response.status, body if raw else json.loads(body), headers
        except urllib.error.HTTPError as exc:
            body = exc.read()
            headers = {key.lower(): value for key, value in exc.headers.items()}
            exc.close()
            return exc.code, body if raw else json.loads(body), headers

    def test_import_run_query_stop_resume_export(self) -> None:
        payload = sample_import_payload()
        status, imported, _ = self.request("POST", "/v1/import", payload)
        self.assertEqual(status, 200)
        self.assertEqual(imported["task_count"], 6)

        status, run_report, _ = self.request("POST", "/v1/run", {"stages": "parse,execute,verify,export"})
        self.assertEqual(status, 200)
        self.assertEqual(run_report["status"], "COMPLETED")
        self.assertEqual(run_report["summary_digest"], "886d754242dc2decbbd13dc64640b44c35ba4d9e9a0ebd8a213f009c3bb81a58")

        status, task_status, _ = self.request("GET", "/v1/status")
        self.assertEqual(status, 200)
        statuses = {item["task_id"]: item["status"] for item in task_status["tasks"]}
        self.assertEqual(statuses["task_006"], "UNPROCESSED")

        status, first_query, headers_first = self.request("POST", "/v1/query", {"task_id": "task_001"})
        self.assertEqual(status, 200)
        self.assertIn(headers_first["x-gspipe-cache"], {"HIT", "MISS"})
        status, second_query, headers_second = self.request("GET", "/v1/query/task_001")
        self.assertEqual(status, 200)
        self.assertEqual(headers_second["x-gspipe-cache"], "HIT")
        self.assertEqual(first_query, second_query)

        status, png_bytes, headers = self.request("GET", "/v1/results/task_001.png", raw=True)
        self.assertEqual(status, 200)
        self.assertEqual(headers["content-type"], "image/png")
        self.assertTrue(png_bytes.startswith(b"\x89PNG\r\n\x1a\n"))

        status, summary, _ = self.request("GET", "/v1/export/summary")
        self.assertEqual(status, 200)
        self.assertEqual(summary["schema"], "gspipe-edit-summary-v1")

        status, stopped, _ = self.request("POST", "/v1/stop", {"reason": "http_test"})
        self.assertEqual(status, 200)
        self.assertTrue(stopped["stop_requested"])
        status, stopped_run, _ = self.request("POST", "/v1/run", {})
        self.assertEqual(status, 200)
        self.assertEqual(stopped_run["status"], "STOPPED")
        status, resumed, _ = self.request("POST", "/v1/resume", {})
        self.assertEqual(status, 200)
        self.assertEqual(resumed["status"], "COMPLETED")
        self.assertEqual(resumed["summary_digest"], run_report["summary_digest"])

    def test_invalid_stage_http_error_is_structured(self) -> None:
        payload = sample_import_payload()
        self.assertEqual(self.request("POST", "/v1/import", payload)[0], 200)
        status, error, _ = self.request("POST", "/v1/run", {"stages": "verify"})
        self.assertEqual(status, 409)
        self.assertEqual(error["error"]["code"], "E_INVALID_STAGE")
        self.assertIn("reason", error["error"]["details"])

    def test_invalid_chain_parse_endpoint_has_business_code_and_context(self) -> None:
        payload = sample_import_payload()
        task = next(item for item in payload["tasks"] if item["task_id"] == "task_001")
        task["instruction"] = "sharpen strength=2"
        self.assertEqual(self.request("POST", "/v1/import", payload)[0], 200)
        status, error, _ = self.request("GET", "/v1/parse/task_001")
        self.assertEqual(status, 422)
        self.assertEqual(error["error"]["code"], "E_INVALID_CHAIN")
        self.assertEqual(error["error"]["task_id"], "task_001")
        self.assertEqual(error["error"]["operator"], "sharpen")


class CLITests(unittest.TestCase):
    def test_cli_end_to_end(self) -> None:
        root = Path(tempfile.mkdtemp(prefix="gspipe-cli-test-"))
        repo_root = str(Path(__file__).resolve().parents[1])

        def run(*args: str) -> dict[str, Any]:
            completed = subprocess.run(
                [sys.executable, "-m", "gspipe", *args],
                cwd=repo_root,
                capture_output=True,
                text=True,
                timeout=30,
            )
            if completed.returncode != 0:
                raise AssertionError(completed.stdout + "\n" + completed.stderr)
            return json.loads(completed.stdout)

        initialized = run("init-demo", "--workspace", str(root))
        self.assertEqual(initialized["task_count"], 6)
        parse_only = run("run", "--workspace", str(root), "--stages", "parse")
        self.assertEqual(parse_only["checkpoints_written"], 6)
        completed = run("run", "--workspace", str(root))
        self.assertEqual(completed["status"], "COMPLETED")
        self.assertEqual(completed["summary_digest"], "886d754242dc2decbbd13dc64640b44c35ba4d9e9a0ebd8a213f009c3bb81a58")
        status = run("status", "--workspace", str(root))
        self.assertEqual(status["task_count"], 6)
        query = run("query", "task_001", "--workspace", str(root), "--json")
        self.assertEqual(query["task_id"], "task_001")
        exported = run("export", "--workspace", str(root))
        self.assertEqual(exported["export"]["summary_digest"], completed["summary_digest"])


if __name__ == "__main__":
    unittest.main()
