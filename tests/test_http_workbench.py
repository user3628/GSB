from __future__ import annotations

import json
import tempfile
import threading
import time
import types
import unittest
import urllib.request
from pathlib import Path

from ctr_cvr_workbench.server import Workbench, WorkbenchHTTPServer

from .helpers import make_workspace


class HTTPWorkbenchTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        make_workspace(self.root, shards=2, rows_per_shard=45, epochs=2)
        self.workbench = Workbench(self.root, "configs/test.yaml")
        self.server = WorkbenchHTTPServer(("127.0.0.1", 0), self.workbench)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base = f"http://127.0.0.1:{self.server.server_address[1]}"

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        self.temporary.cleanup()

    def _request(self, path: str, body: dict | None = None) -> dict:
        data = None if body is None else json.dumps(body).encode("utf-8")
        request = urllib.request.Request(
            self.base + path,
            data=data,
            headers={"Content-Type": "application/json"} if body is not None else {},
            method="POST" if body is not None else "GET",
        )
        with urllib.request.urlopen(request, timeout=10) as response:
            return json.loads(response.read().decode("utf-8"))

    def _poll_job(self, job_id: int, desired: set[str], timeout: float = 15.0) -> dict:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            payload = self._request(f"/api/jobs/{job_id}")["job"]
            if payload["status"] in desired:
                return payload
            time.sleep(0.05)
        self.fail(f"job {job_id} did not reach {desired}")

    def test_import_run_stop_resume_and_queries(self) -> None:
        health = self._request("/health")
        self.assertTrue(health["ok"])
        imported = self._request("/api/import", {"config_path": "configs/test.yaml"})
        self.assertEqual(imported["status"], "imported")
        config_path = imported["config_path"]
        started = self._request(
            "/api/run",
            {
                "config_path": config_path,
                "execution_mode": "sequential",
                "stop_after_epochs": 1,
            },
        )
        job_id = started["job"]["id"]
        stopped = self._poll_job(job_id, {"stopped"})
        self.assertEqual(stopped["result"]["status"], "stopped")
        resumed = self._request(f"/api/jobs/{job_id}/resume", {})
        completed = self._poll_job(resumed["job"]["id"], {"completed"})
        self.assertEqual(completed["status"], "completed")
        status = self._request(f"/api/status?config={config_path}")
        self.assertEqual(status["stages"]["infer"]["status"], "valid")
        losses = self._request(f"/api/losses?config={config_path}")
        self.assertEqual(losses["epochs"], 2)
        self.assertIn("traversal_digest", losses["losses"][0])
        funnel = self._request(f"/api/funnel?config={config_path}")
        self.assertEqual(funnel["diagnostics"]["violation_count"], 0)
        summary = self._request(f"/api/summary?config={config_path}")
        self.assertEqual(summary["summary"]["format"], "ctr-cvr-experiment-summary-v1")
        model = self._request(f"/api/model?config={config_path}")
        self.assertEqual(model["model"]["format"], "ctr-cvr-fixed-point-v1")
        scores = self._request(f"/api/scores?config={config_path}&limit=5")
        self.assertEqual(len(scores["records"]), 5)

    def test_explicit_stop_endpoint_is_cooperative(self) -> None:
        def fake_run_job(workbench: Workbench, job) -> None:
            try:
                job.status = "running"
                job.stop_event.wait(timeout=5.0)
                job.status = "stopped" if job.stop_requested else "completed"
                job.result = {"status": job.status}
            finally:
                with workbench.lock:
                    if workbench.active_job_id == job.id:
                        workbench.active_job_id = None

        self.workbench._run_job = types.MethodType(fake_run_job, self.workbench)  # type: ignore[method-assign]
        started = self._request(
            "/api/run",
            {"config_path": "configs/test.yaml", "execution_mode": "sequential"},
        )
        job_id = started["job"]["id"]
        accepted = self._request(f"/api/jobs/{job_id}/stop", {})
        self.assertEqual(accepted["status"], "stopping")
        self.assertIn("cooperative", accepted["note"])
        stopped = self._poll_job(job_id, {"stopped"})
        self.assertTrue(stopped["stop_requested"])
        self.assertEqual(stopped["result"]["status"], "stopped")


if __name__ == "__main__":
    unittest.main()
