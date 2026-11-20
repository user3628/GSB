"""CLI exit codes and business error surfacing (E_xxx in JSON/stderr)."""

import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from workbench.config import default_config_store
from workbench.sample import generate_csv
from workbench.store import Store

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class TestCli(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="wb-cli-")
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.state = os.path.join(self.tmp, "state")
        self.sample = os.path.join(self.tmp, "sample.csv")
        generate_csv(self.sample, seed=20261120, rows=400)

    def cli(self, *args):
        return subprocess.run(
            [sys.executable, "-m", "workbench", "--state", self.state] + list(args),
            cwd=ROOT, capture_output=True, text=True,
        )

    def _bootstrap(self):
        store = Store(self.state)
        store.ensure()
        store.write_config_store(default_config_store())
        store.write_sample_registry(
            {"path": self.sample, "sha256": "", "rows": 400, "columns": []}
        )

    def test_success_exit_zero(self):
        self._bootstrap()
        p = self.cli("run")
        self.assertEqual(p.returncode, 0, p.stderr)
        report = json.loads(p.stdout)
        self.assertEqual(report["status"], "completed")

    def test_business_error_exit_2_with_code(self):
        p = self.cli("run")
        self.assertEqual(p.returncode, 2)
        err = json.loads(p.stderr)
        self.assertEqual(err["error"]["code"], "E_CONFIG")

    def test_latency_budget_exit_2(self):
        self._bootstrap()
        p = self.cli("run", "--budget-ms", "0")
        self.assertEqual(p.returncode, 2)
        err = json.loads(p.stderr)
        self.assertEqual(err["error"]["code"], "E_LATENCY_BUDGET")

    def test_inconsistent_exit_2(self):
        self._bootstrap()
        p = self.cli("run")
        self.assertEqual(p.returncode, 0)
        store = Store(self.state)
        record = store.recover()[1]
        path = store.artifact_matrix_path(record["cache_key"])
        with open(path, "r+b") as f:
            data = bytearray(f.read())
            data[0] ^= 0xFF
            f.seek(0)
            f.write(data)
        p = self.cli("verify")
        self.assertEqual(p.returncode, 2)
        err = json.loads(p.stderr)
        self.assertEqual(err["error"]["code"], "E_INCONSISTENT")
        diffs = err["error"]["details"]["diffs"]
        self.assertTrue(any(d["stage"] == "encode" for d in diffs))
        self.assertTrue(all("offset" in d for d in diffs))

    def test_consistent_verify_exit_0(self):
        self._bootstrap()
        self.assertEqual(self.cli("run").returncode, 0)
        p = self.cli("verify")
        self.assertEqual(p.returncode, 0, p.stderr)
        self.assertTrue(json.loads(p.stdout)["consistent"])

    def test_config_set_and_status(self):
        self._bootstrap()
        p = self.cli("config-set", "--set", "selection.method=variance")
        self.assertEqual(p.returncode, 0)
        out = json.loads(p.stdout)
        self.assertEqual(out["config_version"], 2)
        self.assertEqual(out["changed_sections"], ["selection"])
        p = self.cli("status")
        self.assertEqual(p.returncode, 0)
        status = json.loads(p.stdout)
        self.assertEqual(status["config"]["version"], 2)
        self.assertEqual([s["stage"] for s in status["plan"]],
                         ["infer", "encode", "select", "summarize"])

    def test_invalid_config_rejected(self):
        self._bootstrap()
        p = self.cli("config-set", "--set", "selection.method=bogus")
        self.assertEqual(p.returncode, 2)
        self.assertEqual(json.loads(p.stderr)["error"]["code"], "E_CONFIG")


if __name__ == "__main__":
    unittest.main()
