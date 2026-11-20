"""End-to-end HTTP API behaviour on a loopback ephemeral port."""

import json
import os
import shutil
import tempfile
import threading
import unittest
import urllib.error
import urllib.request

from qopt.server import create_server


def doc(version="v1"):
    return {
        "format": "qopt-snapshot/1",
        "version": version,
        "generated_at": "2026-11-20T00:00:00Z",
        "tables": {
            "t": {"row_count": 1000, "pages": 20,
                  "columns": {
                      "a": {"ndv": 100, "null_frac": 0.0},
                      "b": {"ndv": 50, "null_frac": 0.2},
                  }},
        },
        "indexes": [
            {"name": "idx_a", "table": "t", "columns": ["a"], "pages": 5},
        ],
        "correlations": [],
    }


class ApiTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dir = tempfile.mkdtemp()
        cls.snap_path = os.path.join(cls.dir, "snap.json")
        with open(cls.snap_path, "w") as fh:
            json.dump(doc(), fh)
        cls.broken_path = os.path.join(cls.dir, "broken.json")
        with open(cls.broken_path, "w") as fh:
            fh.write("{not json")
        cls.server, cls.registry = create_server(port=0)
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        shutil.rmtree(cls.dir)

    def http(self, method, path, body=None):
        data = None if body is None else json.dumps(body).encode("utf-8")
        req = urllib.request.Request(
            "http://127.0.0.1:%d%s" % (self.port, path),
            data=data, method=method,
            headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                return resp.status, json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read().decode("utf-8"))

    def test_health(self):
        status, body = self.http("GET", "/v1/health")
        self.assertEqual(status, 200)
        self.assertTrue(body["ok"])

    def test_unknown_route(self):
        status, body = self.http("GET", "/nope")
        self.assertEqual(status, 404)
        self.assertEqual(body["error"]["code"], "E_NOT_FOUND")

    def test_load_and_plan_and_versioning(self):
        status, body = self.http("POST", "/v1/snapshot/load",
                                 {"path": self.snap_path})
        self.assertEqual(status, 200)
        self.assertEqual(body["snapshot"]["version"], "v1")

        query = {"tables": ["t"],
                 "predicates": [{"type": "eq", "table": "t", "column": "a",
                                 "value": 5}]}
        status, body = self.http("POST", "/v1/plan", {"query": query})
        self.assertEqual(status, 200)
        plan = body["plan"]
        self.assertEqual(plan["snapshot_version"], "v1")
        self.assertIn("total_cost", plan)
        rounding = plan["total_cost"]["rounding"]
        self.assertEqual(rounding["decimals"], 6)

        status, body = self.http("POST", "/v1/plan",
                                 {"query": query, "snapshot_version": "v2"})
        self.assertEqual(status, 400)
        self.assertEqual(body["error"]["code"], "E_SNAPSHOT_VERSION_MISMATCH")

    def test_broken_snapshot_rejected_and_old_kept(self):
        status, body = self.http("POST", "/v1/snapshot/load",
                                 {"path": self.snap_path})
        self.assertEqual(status, 200)
        status, body = self.http("POST", "/v1/snapshot/load",
                                 {"path": self.broken_path})
        self.assertEqual(status, 400)
        self.assertEqual(body["error"]["code"], "E_SNAPSHOT_INVALID")
        status, body = self.http("GET", "/v1/snapshot")
        self.assertEqual(body["snapshot"]["version"], "v1")

    def test_stats_missing(self):
        self.http("POST", "/v1/snapshot/load", {"path": self.snap_path})
        status, body = self.http("POST", "/v1/plan",
                                 {"query": {"tables": ["ghost"]}})
        self.assertEqual(status, 400)
        self.assertEqual(body["error"]["code"], "E_STATS_MISSING")

    def test_bad_json(self):
        req = urllib.request.Request(
            "http://127.0.0.1:%d/v1/plan" % self.port,
            data=b"{oops", method="POST",
            headers={"Content-Type": "application/json"})
        try:
            urllib.request.urlopen(req, timeout=10)
            self.fail("expected HTTPError")
        except urllib.error.HTTPError as exc:
            body = json.loads(exc.read().decode("utf-8"))
            self.assertEqual(body["error"]["code"], "E_BAD_JSON")

    def test_plan_deterministic_over_http(self):
        self.http("POST", "/v1/snapshot/load", {"path": self.snap_path})
        query = {"tables": ["t"],
                 "predicates": [{"type": "range", "table": "t", "column": "b",
                                 "low": 1, "high": 9}]}
        payload = json.dumps({"query": query}).encode("utf-8")
        raws = []
        for _ in range(3):
            req = urllib.request.Request(
                "http://127.0.0.1:%d/v1/plan" % self.port,
                data=payload, method="POST",
                headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(req, timeout=10) as resp:
                raws.append(resp.read())
        self.assertEqual(raws[0], raws[1])
        self.assertEqual(raws[1], raws[2])


if __name__ == "__main__":
    unittest.main()
