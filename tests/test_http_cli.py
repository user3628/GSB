from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

from motion_engine.server import create_server
from motion_engine.workspace import Workspace

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"


def _request(
    base: str,
    method: str,
    path: str,
    payload: dict[str, Any] | None = None,
    timeout: float = 10.0,
) -> tuple[int, dict[str, Any]]:
    body = None
    headers: dict[str, str] = {}
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(
        base + path, data=body, headers=headers, method=method
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as error:
        try:
            return error.code, json.loads(error.read().decode("utf-8"))
        finally:
            error.close()


class HttpApplicationTests(unittest.TestCase):
    def test_http_import_step_stop_resume_and_error_codes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "data"
            shutil.copytree(DATA, source)
            http_workspace = root / "http-workspace"
            full_workspace = Workspace(root / "full-workspace")
            full_workspace.import_manifest(source / "manifest.json")
            full = full_workspace.run("alpha_translate")

            server = create_server(http_workspace, "127.0.0.1", 0)
            host, port = server.server_address[:2]
            base = f"http://{host}:{port}"
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                status, health = _request(base, "GET", "/api/health")
                self.assertEqual(status, 200)
                self.assertTrue(health["ok"])
                status, imported = _request(
                    base,
                    "POST",
                    "/api/import",
                    {"manifest_path": str(source / "manifest.json")},
                )
                self.assertEqual(status, 200)
                self.assertEqual(len(imported["series"]), 3)

                status, stepped = _request(
                    base, "POST", "/api/series/alpha_translate/step", {}
                )
                self.assertEqual(status, 200)
                self.assertEqual(stepped["report"]["checkpoint"]["last_sequence"], 1)

                status, listed = _request(base, "GET", "/api/series")
                self.assertEqual(status, 200)
                alpha_status = next(
                    item
                    for item in listed["series"]
                    if item["series_id"] == "alpha_translate"
                )
                self.assertIn("stages", alpha_status["version_fingerprints"])
                self.assertEqual(alpha_status["checkpoint"]["last_sequence"], 1)

                run_response: list[tuple[int, dict[str, Any]]] = []

                def start_run() -> None:
                    run_response.append(
                        _request(
                            base,
                            "POST",
                            "/api/series/alpha_translate/run",
                            {"step_delay": 0.04},
                            timeout=20.0,
                        )
                    )

                runner_thread = threading.Thread(target=start_run)
                runner_thread.start()
                deadline = time.time() + 5.0
                while time.time() < deadline:
                    _, health = _request(base, "GET", "/api/health")
                    if "alpha_translate" in health["active_series"]:
                        break
                    time.sleep(0.01)
                else:
                    self.fail("HTTP run did not become active")
                time.sleep(0.07)
                status, stopped = _request(
                    base, "POST", "/api/series/alpha_translate/stop", {}
                )
                self.assertEqual(status, 202)
                self.assertTrue(stopped["stop_requested"])
                runner_thread.join(timeout=10)
                self.assertFalse(runner_thread.is_alive())
                self.assertEqual(run_response[0][0], 200)
                partial_report = run_response[0][1]["report"]
                self.assertFalse(partial_report["complete"])
                self.assertLess(partial_report["checkpoint"]["last_sequence"], 16)
                self.assertTrue(run_response[0][1]["run"]["stopped"])

                status, resumed = _request(
                    base, "POST", "/api/series/alpha_translate/resume", {}
                )
                self.assertEqual(status, 200)
                resumed_report = resumed["report"]
                self.assertTrue(resumed_report["complete"])
                self.assertEqual(
                    resumed_report["report_fingerprint"], full["report_fingerprint"]
                )

                status, report_response = _request(
                    base, "GET", "/api/series/alpha_translate/report"
                )
                self.assertEqual(status, 200)
                self.assertEqual(
                    report_response["report"]["report_fingerprint"],
                    full["report_fingerprint"],
                )

                status, steps_response = _request(
                    base, "GET", "/api/series/alpha_translate/steps"
                )
                self.assertEqual(status, 200)
                self.assertEqual(len(steps_response["steps"]), 16)
                self.assertEqual(len(steps_response["error_curve"]), 16)
                self.assertEqual(steps_response["steps"][0]["step"], 1)
                self.assertEqual(steps_response["steps"][-1]["step"], 16)

                status, error = _request(base, "GET", "/api/series/does-not-exist")
                self.assertEqual(status, 422)
                self.assertEqual(error["error"]["code"], "E_INVALID_STATE")
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)


class CliTests(unittest.TestCase):
    def test_cli_generate_import_step_status_and_report(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            data_dir = root / "data"
            workspace = root / "runtime"
            environment = dict(os.environ)
            environment["PYTHONPATH"] = str(ROOT)
            commands = [
                [sys.executable, "-m", "motion_engine", "generate", "--out", str(data_dir)],
                [
                    sys.executable,
                    "-m",
                    "motion_engine",
                    "import",
                    "--workspace",
                    str(workspace),
                    "--manifest",
                    str(data_dir / "manifest.json"),
                ],
                [
                    sys.executable,
                    "-m",
                    "motion_engine",
                    "step",
                    "--workspace",
                    str(workspace),
                    "--series",
                    "alpha_translate",
                ],
                [
                    sys.executable,
                    "-m",
                    "motion_engine",
                    "status",
                    "--workspace",
                    str(workspace),
                    "--series",
                    "alpha_translate",
                ],
                [
                    sys.executable,
                    "-m",
                    "motion_engine",
                    "report",
                    "--workspace",
                    str(workspace),
                    "--series",
                    "alpha_translate",
                    "--output",
                    str(root / "report.json"),
                ],
            ]
            outputs: list[subprocess.CompletedProcess[str]] = []
            for command in commands:
                completed = subprocess.run(
                    command,
                    cwd=ROOT,
                    env=environment,
                    text=True,
                    capture_output=True,
                    timeout=30,
                )
                self.assertEqual(
                    completed.returncode,
                    0,
                    msg=(
                        f"command failed: {command}\n"
                        f"stdout={completed.stdout}\nstderr={completed.stderr}"
                    ),
                )
                outputs.append(completed)
            status = json.loads(outputs[3].stdout)
            self.assertEqual(status["checkpoint"]["last_sequence"], 1)
            report = json.loads((root / "report.json").read_text(encoding="utf-8"))
            self.assertEqual(report["checkpoint"]["last_sequence"], 1)
            self.assertEqual(report["steps"][0]["step"], 1)


if __name__ == "__main__":
    unittest.main()
