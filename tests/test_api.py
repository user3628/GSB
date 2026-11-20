from __future__ import annotations

import io
import shutil
import tempfile
import threading
import unittest
from pathlib import Path

import numpy as np
import requests

from vecengine.api import create_server
from vecengine.engine import ExecutionEngine
from vecengine.samples import generate_samples
from vecengine.selection import EngineConfig

try:
    from .test_engine import PROJECTION_PLAN
except ImportError:  # unittest discover -s tests imports modules as top-level
    from test_engine import PROJECTION_PLAN


class ApiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.root = Path(tempfile.mkdtemp(prefix="vecengine-api-"))
        cls.data_dir = cls.root / "samples"
        generate_samples(cls.data_dir, sizes={"small": 200}, force=True)
        cls.engine = ExecutionEngine(
            db_path=cls.root / "api.db",
            data_dir=cls.data_dir,
            results_dir=cls.root / "results",
            config=EngineConfig(batch_min=16, batch_max=64),
        )
        cls.server = create_server(cls.engine, "127.0.0.1", 0)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.base_url = f"http://127.0.0.1:{cls.server.server_address[1]}"

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=5)
        shutil.rmtree(cls.root, ignore_errors=True)

    def test_full_api_flow_and_error_codes(self) -> None:
        response = requests.post(
            f"{self.base_url}/sessions",
            json={
                "request_id": "api-request",
                "plan": PROJECTION_PLAN,
                "execution": {"path": "vector", "batch_size": 16},
            },
            timeout=30,
        )
        self.assertEqual(response.status_code, 201, response.text)
        data = response.json()["data"]
        self.assertEqual(data["status"], "ready")
        session_id = data["session_id"]

        repeated = requests.post(
            f"{self.base_url}/sessions",
            json={
                "request_id": "api-request",
                "plan": PROJECTION_PLAN,
                "execution": {"path": "vector", "batch_size": 16},
            },
            timeout=30,
        )
        self.assertEqual(repeated.status_code, 200)
        self.assertTrue(repeated.json()["data"]["reused"])
        self.assertEqual(repeated.json()["data"]["session_id"], session_id)

        result = requests.get(
            f"{self.base_url}/sessions/{session_id}/result?limit=3", timeout=30
        )
        self.assertEqual(result.status_code, 200, result.text)
        page = result.json()["data"]
        self.assertEqual(page["row_count"], 199)
        self.assertEqual(page["limit"], 3)
        self.assertIsInstance(page["columns"][0]["values"][0], int)

        download = requests.get(
            f"{self.base_url}/sessions/{session_id}/result?format=npz", timeout=30
        )
        self.assertEqual(download.status_code, 200)
        with np.load(io.BytesIO(download.content), allow_pickle=False) as archive:
            self.assertIn("add_i__values", archive)

        batches = requests.get(f"{self.base_url}/sessions/{session_id}/batches", timeout=30)
        self.assertEqual(batches.status_code, 200)
        self.assertEqual(batches.json()["data"]["session_id"], session_id)

        conflict = requests.post(
            f"{self.base_url}/sessions",
            json={
                "request_id": "api-request",
                "plan": PROJECTION_PLAN,
                "execution": {"path": "scalar", "batch_size": 16},
            },
            timeout=30,
        )
        self.assertEqual(conflict.status_code, 409)
        self.assertEqual(conflict.json()["error"]["code"], "E_DUPLICATE_REQUEST")

        invalid = requests.post(
            f"{self.base_url}/sessions",
            json={
                "request_id": "api-invalid",
                "plan": {
                    "dataset": "small",
                    "operators": [
                        {"op": "project", "name": "x", "expr": {"op": "column", "name": "nope"}}
                    ],
                },
            },
            timeout=30,
        )
        self.assertEqual(invalid.status_code, 400)
        self.assertEqual(invalid.json()["error"]["code"], "E_PLAN_INVALID")

        missing = requests.get(f"{self.base_url}/sessions/not-a-session", timeout=30)
        self.assertEqual(missing.status_code, 404)
        self.assertEqual(missing.json()["error"]["code"], "E_SESSION_NOT_FOUND")

        memory = requests.post(
            f"{self.base_url}/sessions",
            json={
                "request_id": "api-memory",
                "plan": PROJECTION_PLAN,
                "execution": {"memory_budget_bytes": 128 * 1024, "batch_size": 64},
            },
            timeout=30,
        )
        self.assertEqual(memory.status_code, 400)
        self.assertEqual(memory.json()["error"]["code"], "E_MEMORY_BUDGET")

        recovery = requests.get(f"{self.base_url}/recovery/report", timeout=30)
        self.assertEqual(recovery.status_code, 200)
        self.assertIn("reports", recovery.json()["data"])

    def test_invalid_json_and_unknown_route_use_unified_error_shape(self) -> None:
        response = requests.post(
            f"{self.base_url}/sessions",
            data=b"{not-json",
            headers={"Content-Type": "application/json"},
            timeout=30,
        )
        self.assertEqual(response.status_code, 400)
        payload = response.json()
        self.assertFalse(payload["ok"])
        self.assertIn("code", payload["error"])
        route = requests.get(f"{self.base_url}/missing", timeout=30)
        self.assertEqual(route.status_code, 404)
        self.assertEqual(route.json()["error"]["code"], "E_ROUTE_NOT_FOUND")

