"""Local HTTP interface: import, config, run, status, selection, stop."""

import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.error
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from workbench.config import default_config_store
from workbench.sample import generate_csv
from workbench.store import Store

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class TestHttp(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.mkdtemp(prefix="wb-http-")
        cls.state = os.path.join(cls.tmp, "state")
        cls.sample = os.path.join(cls.tmp, "sample.csv")
        generate_csv(cls.sample, seed=20261120, rows=400)
        store = Store(cls.state)
        store.ensure()
        store.write_config_store(default_config_store())
        cls.port = _free_port()
        cls.base = "http://127.0.0.1:%d" % cls.port
        cls.proc = subprocess.Popen(
            [sys.executable, "-m", "workbench", "--state", cls.state,
             "serve", "--host", "127.0.0.1", "--port", str(cls.port)],
            cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        for _ in range(100):
            try:
                cls._get("/health")
                break
            except OSError:
                time.sleep(0.05)
        else:
            raise RuntimeError("server did not start")

    @classmethod
    def tearDownClass(cls):
        cls.proc.send_signal(signal.SIGTERM)
        cls.proc.wait(timeout=15)
        shutil.rmtree(cls.tmp, True)

    @classmethod
    def _get(cls, path):
        with urllib.request.urlopen(cls.base + path, timeout=10) as r:
            return json.loads(r.read().decode("utf-8"))

    @classmethod
    def _post(cls, path, body=None, method="POST"):
        req = urllib.request.Request(
            cls.base + path,
            data=json.dumps(body or {}).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method=method,
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                return r.status, json.loads(r.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read().decode("utf-8"))

    def test_01_health(self):
        self.assertTrue(self._get("/health")["ok"])

    def test_02_import_sample(self):
        status, body = self._post("/sample", {"path": self.sample})
        self.assertEqual(status, 200)
        self.assertEqual(body["imported"]["rows"], 400)

    def test_03_run_and_query(self):
        status, body = self._post("/run", {})
        self.assertEqual(status, 200)
        self.assertEqual(body["status"], "completed")
        self.assertIn("summary_digest", body)
        status = self._get("/status")
        self.assertEqual(status["journal"]["head_seq"], 4)
        self.assertEqual(len(status["plan"]), 4)
        sel = self._get("/selection")
        self.assertEqual(sel["method"], "mutual_info")
        self.assertGreater(sel["kept_count"], 0)
        self.assertGreater(sel["dropped_count"], 0)
        encode = self._get("/stages/encode")
        self.assertIn("target_encoding", encode)
        self.assertIn("loyalty", encode["target_encoding"])

    def test_04_latency_budget_error(self):
        status, body = self._post("/run", {"budget_ms": 0})
        self.assertEqual(status, 400)
        self.assertEqual(body["error"]["code"], "E_LATENCY_BUDGET")

    def test_05_patch_config_incremental(self):
        status, body = self._post("/config", {"set": {"selection.method": "variance"}},
                                  method="PATCH")
        self.assertEqual(status, 200)
        self.assertEqual(body["config_version"], 2)
        status, body = self._post("/run", {})
        self.assertEqual(status, 200)
        sources = {s["stage"]: s["source"] for s in body["stages"]}
        self.assertEqual(sources["infer"], "journal")
        self.assertEqual(sources["select"], "computed")
        sel = self._get("/selection")
        self.assertEqual(sel["method"], "variance")

    def test_06_verify_consistent(self):
        status, body = self._post("/verify")
        self.assertEqual(status, 200)
        self.assertTrue(body["consistent"])

    def test_07_unknown_route(self):
        try:
            self._get("/nope")
            self.fail("expected HTTPError")
        except urllib.error.HTTPError as e:
            self.assertEqual(e.code, 400)


if __name__ == "__main__":
    unittest.main()
