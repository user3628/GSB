import json
import os
import tempfile
import unittest
import urllib.request

from exp_tracker.server import build_server


def post(base_url, path, payload):
    req = urllib.request.Request(
        base_url + path, data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode())


class NormalizeApiTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.server = build_server(os.path.join(self.tmp, "n.db"), "127.0.0.1", 0)
        self.thread = self.server.serve_in_thread()
        self.base = f"http://127.0.0.1:{self.server.server_address[1]}"

    def tearDown(self):
        self.server.shutdown()
        self.thread.join(timeout=5)
        self.server.store.close()

    def test_ms_and_us_normalize_to_same_internal_grid(self):
        body = post(self.base, "/normalize", {
            "rule": "truncate",
            "records": [
                {"run_id": "bin", "unit": "ms", "time_ms": 1234},
                {"run_id": "json", "unit": "us", "time_us": 1234700},
            ],
        })
        records = {item["run_id"]: item for item in body["records"]}
        self.assertEqual(records["bin"]["projected_us"], 1234000)
        self.assertEqual(records["json"]["projected_us"], 1234000)
        self.assertEqual(records["json"]["rounded_digits"], 3)
        self.assertEqual(records["json"]["discarded_us"], -700)
        self.assertEqual(records["json"]["cumulative_error_us"], -700)


if __name__ == "__main__":
    unittest.main()
