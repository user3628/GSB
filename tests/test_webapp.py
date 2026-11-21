import json
import os
import shutil
import tempfile
import unittest

from tls_workbench.samplegen import generate_all
from tls_workbench.webapp import create_app


class WebAppTests(unittest.TestCase):
    def setUp(self):
        self.base_dir = tempfile.mkdtemp(prefix="tls-wb-web-")
        generate_all(self.base_dir)
        self.db_path = os.path.join(self.base_dir, "data", "web.sqlite3")
        app = create_app(base_dir=self.base_dir, db_path=self.db_path)
        app.testing = True
        self.client = app.test_client()
        with open(os.path.join(self.base_dir, "samples", "handshake_samples.json"),
                  encoding="utf-8") as handle:
            self.params = json.load(handle)["samples"][0]

    def tearDown(self):
        shutil.rmtree(self.base_dir, ignore_errors=True)

    def test_health_and_chain_metadata(self):
        self.assertTrue(self.client.get("/api/health").get_json()["ok"])
        response = self.client.get("/api/chains/rsa2048_chain")
        self.assertEqual(response.status_code, 200)
        self.assertIn("leaf", response.get_json()["chain"]["certificates"])

    def test_full_handshake_flow_and_snapshot_reads(self):
        create = self.client.post(
            "/api/sessions",
            json={
                "session_id": "http-1",
                "chain_id": self.params["chain_id"],
                "params": self.params,
            },
        )
        self.assertEqual(create.status_code, 201)
        advance = self.client.post(
            "/api/sessions/http-1/advance", json={"params": self.params}
        )
        self.assertEqual(advance.status_code, 200)
        diagnostic = advance.get_json()["diagnostic"]
        self.assertEqual(diagnostic["session_status"], "COMPLETED")
        snapshot = self.client.get("/api/sessions/http-1").get_json()
        self.assertEqual(snapshot["session"]["version"], 5)
        second_advance = self.client.post(
            "/api/sessions/http-1/advance", json={"params": self.params}
        )
        self.assertEqual(
            second_advance.get_json()["diagnostic"]["version"], 5
        )

    def test_conflicting_params_return_conflict_code(self):
        self.client.post(
            "/api/sessions",
            json={"session_id": "http-2", "chain_id": self.params["chain_id"],
                  "params": self.params},
        )
        changed = dict(self.params)
        changed["sni"] = "other.local"
        response = self.client.post(
            "/api/sessions/http-2/advance", json={"params": changed}
        )
        self.assertEqual(response.status_code, 409)
        self.assertEqual(
            response.get_json()["error_code"], "E_CONFLICTING_SESSION"
        )

    def test_budget_exceeded_endpoint(self):
        with open(os.path.join(self.base_dir, "samples", "handshake_samples.json"),
                  encoding="utf-8") as handle:
            over = next(
                item for item in json.load(handle)["samples"]
                if item["sample_id"] == "over_budget_handshake"
            )
        self.client.post(
            "/api/sessions",
            json={"session_id": "http-3", "chain_id": over["chain_id"],
                  "params": over},
        )
        response = self.client.post(
            "/api/sessions/http-3/advance", json={"params": over}
        )
        self.assertEqual(response.status_code, 409)
        self.assertEqual(
            response.get_json()["error_code"], "E_BUDGET_EXCEEDED"
        )

    def test_numeric_audit_endpoint(self):
        response = self.client.get("/api/numeric/audit")
        self.assertEqual(response.status_code, 200)
        audit = response.get_json()["audit"]
        self.assertTrue(audit["bound_respected"])
        self.assertIn("naive_error_upper_bound", audit)

    def test_recover_endpoint(self):
        self.client.post(
            "/api/sessions",
            json={"session_id": "http-4", "chain_id": self.params["chain_id"],
                  "params": self.params},
        )
        response = self.client.post("/api/recover")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["recovery_report"]["actions"], [])


if __name__ == "__main__":
    unittest.main()
