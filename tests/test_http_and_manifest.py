import json
import os
import shutil
import tempfile
import threading
import unittest
import urllib.request

from tests._helpers import make_workspace
from qrwb.config import load_config
from qrwb.server import serve
from qrwb.workspace import load_workspace


class HttpAndManifestTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.ws = make_workspace(os.path.join(self.tmp, "http"))
        self.config_path = os.path.join(self.ws, "config.yaml")
        self.server, self.url = serve(self.ws, self.config_path,
                                      port=0)
        self.thread = threading.Thread(target=self.server.serve_forever,
                                       daemon=True)
        self.thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _post(self, path, payload=None):
        data = json.dumps(payload or {}).encode("utf-8")
        req = urllib.request.Request(
            self.url + path, data=data,
            headers={"Content-Type": "application/json"}, method="POST")
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def _get(self, path):
        with urllib.request.urlopen(self.url + path, timeout=10) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def test_health_run_status_budget_stop_resume(self):
        self.assertEqual(self._get("/health")["status"], "ok")
        report = self._post("/run")
        self.assertIn(report["status"], ("completed", "stopped"))
        budget = self._get("/budget")
        self.assertIn("global_used", budget)
        query_id = report["waves"][0][0] if report["waves"] else None
        if query_id:
            status = self._get(f"/status?query_id={query_id}")
            self.assertTrue(status["found"])
            self.assertIn("checkpoints", status)
            trace = self._get(f"/trace?query_id={query_id}")
            self.assertIn("adoptions", trace)
        self._post("/stop")
        resumed = self._post("/resume")
        self.assertEqual(resumed["status"], "completed")

    def test_manifest_detects_corruption(self):
        cfg = load_config(self.config_path)
        resources, records, manifest = load_workspace(self.ws, cfg)
        self.assertEqual(len(records), manifest["total_rows"])
        # 破坏一个分片文件 → 装载必须报错
        shard_path = os.path.join(self.ws, "inputs",
                                   manifest["shards"][0]["path"])
        rows = [json.loads(line) for line in open(shard_path, encoding="utf-8")]
        rows.append({"query_id": "rogue", "raw_query": "x"})
        with open(shard_path, "w", encoding="utf-8", newline="\n") as handle:
            handle.write("\n".join(json.dumps(r, ensure_ascii=False)
                                   for r in rows) + "\n")
        with self.assertRaises(ValueError):
            load_workspace(self.ws, cfg)

    def test_manifest_declares_cross_shard_duplicates(self):
        manifest_path = os.path.join(self.ws, "inputs", "manifest.json")
        manifest = json.load(open(manifest_path, encoding="utf-8"))
        self.assertTrue(manifest["duplicate_queries"])
        dup = manifest["duplicate_queries"][0]
        self.assertIn("query_id", dup)
        self.assertIn("same_as", dup)


if __name__ == "__main__":
    unittest.main()
