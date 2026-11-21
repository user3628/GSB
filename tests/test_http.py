import json
import os
import tempfile
import threading
import unittest
import urllib.error
import urllib.request

from exp_tracker.models import MetricPoint, Run
from exp_tracker.samples import generate_sample_files, tamper_binary_crc
from exp_tracker.server import build_server


def call(base_url, method, path, payload=None):
    data = json.dumps(payload).encode() if payload is not None else None
    headers = {"Content-Type": "application/json"}
    req = urllib.request.Request(base_url + path, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode())


class HttpApiTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.db = os.path.join(self.tmp, "s.db")
        self.server = build_server(self.db, "127.0.0.1", 0)
        self.thread = self.server.serve_in_thread()
        self.base = f"http://127.0.0.1:{self.server.server_address[1]}"
        self.paths = generate_sample_files(os.path.join(self.tmp, "samples"))

    def tearDown(self):
        self.server.shutdown()
        self.thread.join(timeout=5)
        self.server.store.close()

    def test_health_and_import_and_summary(self):
        status, body = call(self.base, "GET", "/health")
        self.assertEqual(status, 200)
        status, report = call(self.base, "POST", "/imports",
                              {"path": self.paths["binary_v2"], "report_id": "r1"})
        self.assertEqual(status, 200)
        self.assertTrue(report["committed"])
        status, summaries = call(self.base, "GET", "/summaries")
        self.assertEqual(status, 200)
        self.assertTrue(summaries["summaries"])

    def test_corrupt_error_body_shape(self):
        bad = os.path.join(self.tmp, "bad.etrk")
        tamper_binary_crc(self.paths["binary_v2"], 0, bad)
        status, body = call(self.base, "POST", "/imports", {"path": bad, "report_id": "x"})
        self.assertEqual(status, 400)
        self.assertEqual(body["error"]["code"], "E_RECORD_CORRUPT")
        self.assertIn("context", body["error"])
        self.assertEqual(body["error"]["context"]["record_index"], 0)

    def test_precheck_does_not_mutate(self):
        before = self.server.store.revision()
        status, body = call(self.base, "POST", "/precheck",
                            {"path": self.paths["jsonl_v1_omit_params"]})
        self.assertEqual(status, 200)
        self.assertEqual(body["verdict"], "downgrade_accept")
        self.assertEqual(self.server.store.revision(), before)

    def test_concurrent_readers(self):
        call(self.base, "POST", "/imports",
             {"path": self.paths["binary_v2"], "report_id": "r2"})
        errors = []

        def reader():
            try:
                status, _ = call(self.base, "GET", "/runs")
                if status != 200:
                    errors.append(status)
            except Exception as exc:  # pragma: no cover
                errors.append(str(exc))

        threads = [threading.Thread(target=reader) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertFalse(errors)

    def test_unknown_endpoint_error_code(self):
        status, body = call(self.base, "GET", "/nope")
        self.assertEqual(status, 404)
        self.assertEqual(body["error"]["code"], "E_NOT_FOUND")

    def test_loopback_only_guard(self):
        from exp_tracker.server import TrackerHttpServer
        from exp_tracker.store import ExperimentStore
        guard_store = ExperimentStore(self.db + ".guard")
        try:
            with self.assertRaises(ValueError):
                TrackerHttpServer("0.0.0.0", 0, guard_store)
        finally:
            guard_store.close()


if __name__ == "__main__":
    unittest.main()
