from __future__ import annotations

import json
import os
import queue
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


ROOT = Path(__file__).resolve().parents[1]


class ProcessRecoveryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tempdir.name, "recovery.db")
        self.seed_path = os.path.join(self.tempdir.name, "seed.json")
        self.processes: list[subprocess.Popen[str]] = []
        seed_route = {
            "id": "seed",
            "kind": "reverse",
            "match": {"methods": ["GET"], "path_prefix": "/seed"},
            "upstream": "http://127.0.0.1:9",
            "timeout": {"connect": 0.2, "read": 0.2},
            "retries": {"attempts": 1},
            "headers": {},
            "health": {"enabled": False},
            "failure_policy": {"mode": "fail", "group": "seed-group"},
            "in_flight_policy": "continue",
        }
        with open(self.seed_path, "w", encoding="utf-8") as handle:
            json.dump({"kind": "snapshot", "version": 1, "routes": [seed_route]}, handle)

    def tearDown(self) -> None:
        for process in self.processes:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=5)
            if process.stdout is not None:
                process.stdout.close()
            if process.stderr is not None:
                process.stderr.close()
        self.tempdir.cleanup()

    def _start(self, *, failpoint: bool = False) -> tuple[subprocess.Popen[str], dict[str, Any]]:
        env = os.environ.copy()
        env["PYTHONPATH"] = str(ROOT)
        env["PYTHONUNBUFFERED"] = "1"
        if failpoint:
            env["PROXY_FAILPOINT"] = "after_intent"
        else:
            env.pop("PROXY_FAILPOINT", None)
        process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "proxy_workbench",
                "serve",
                "--db",
                self.db,
                "--seed",
                self.seed_path,
                "--port",
                "0",
                "--graceful-timeout",
                "2",
            ],
            cwd=str(ROOT),
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        self.processes.append(process)
        lines: queue.Queue[str] = queue.Queue()

        def read_line() -> None:
            assert process.stdout is not None
            lines.put(process.stdout.readline())

        threading.Thread(target=read_line, daemon=True).start()
        try:
            line = lines.get(timeout=8)
        except queue.Empty:
            process.kill()
            stderr = process.stderr.read() if process.stderr is not None else ""
            self.fail(f"server did not become ready; stderr={stderr}")
        if not line:
            stderr = process.stderr.read() if process.stderr is not None else ""
            self.fail(f"server exited before ready; stderr={stderr}")
        ready = json.loads(line)
        self.assertEqual(ready["event"], "ready", ready)
        return process, ready

    def _post(self, port: int, payload: dict[str, Any]) -> dict[str, Any] | None:
        request = urllib.request.Request(
            f"http://127.0.0.1:{port}/__workbench/config",
            data=json.dumps(payload).encode("utf-8"),
            method="POST",
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(request, timeout=3) as response:
                return json.loads(response.read().decode("utf-8"))
        except (urllib.error.URLError, ConnectionError, TimeoutError, OSError):
            return None

    def _get(self, port: int, path: str) -> dict[str, Any]:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=3) as response:
            return json.loads(response.read().decode("utf-8"))

    def _graceful_shutdown(self, port: int) -> None:
        request = urllib.request.Request(
            f"http://127.0.0.1:{port}/__workbench/shutdown",
            data=b"{}",
            method="POST",
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(request, timeout=3) as response:
            self.assertEqual(response.status, 200)

    def test_force_kill_rolls_back_unfinished_intent_and_graceful_exit_is_clean(self) -> None:
        process, ready = self._start(failpoint=True)
        self.assertEqual(ready["version"], 1)
        port = ready["port"]
        increment_payload = {
            "kind": "incremental",
            "base_version": 1,
            "version": 2,
            "routes": [
                {
                    "id": "seed",
                    "kind": "reverse",
                    "match": {"methods": ["GET"], "path_prefix": "/seed"},
                    "upstream": "http://127.0.0.1:10",
                    "timeout": {"connect": 0.2, "read": 0.2},
                    "retries": {"attempts": 1},
                    "headers": {},
                    "health": {"enabled": False},
                    "failure_policy": {"mode": "fail", "group": "seed-group"},
                    "in_flight_policy": "continue",
                }
            ],
        }
        self.assertIsNone(self._post(port, increment_payload))
        process.wait(timeout=8)
        self.assertNotEqual(process.returncode, 0)

        restarted, ready_after_crash = self._start(failpoint=False)
        self.assertEqual(ready_after_crash["version"], 1)
        recovery = ready_after_crash["recovery"]
        self.assertEqual(recovery["last_complete_version"], 1)
        self.assertFalse(recovery["previous_exit_clean"])
        self.assertTrue(
            any(action["action"] == "rolled_back_to_last_complete_snapshot" for action in recovery["actions"]),
            recovery,
        )
        state = self._get(ready_after_crash["port"], "/__workbench/state")
        self.assertEqual(state["version"], 1)
        self.assertEqual(state["config"]["routes"][0]["upstream"], "http://127.0.0.1:9")

        self._graceful_shutdown(ready_after_crash["port"])
        restarted.wait(timeout=8)
        self.assertEqual(restarted.returncode, 0)

        clean_process, ready_clean = self._start(failpoint=False)
        recovery = ready_clean["recovery"]
        self.assertTrue(recovery["previous_exit_clean"], recovery)
        self.assertEqual(recovery["last_complete_version"], 1)
        self.assertEqual(recovery["previous_shutdown"]["mode"], "graceful")
        state = self._get(ready_clean["port"], "/__workbench/state")
        self.assertEqual(state["version"], 1)
        self.assertIsNotNone(state["shutdown"])
        self.assertEqual(state["shutdown"]["mode"], "graceful")


if __name__ == "__main__":
    unittest.main()

