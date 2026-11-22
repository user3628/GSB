import json
import threading
import unittest
import urllib.request
from http.server import ThreadingHTTPServer

from ragwb.server import STATE, WorkbenchHandler
from tests.helpers import fresh_run_dir, make_workspace


class HttpInterfaceTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.data_dir = make_workspace()
        cls.run_dir = fresh_run_dir(cls.data_dir, "http-run")
        cls.httpd = ThreadingHTTPServer(
            ("127.0.0.1", 0), WorkbenchHandler)
        cls.port = cls.httpd.server_address[1]
        STATE.bind(cls.data_dir, cls.run_dir)
        cls.thread = threading.Thread(target=cls.httpd.serve_forever,
                                      daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.httpd.shutdown()
        cls.httpd.server_close()
        cls.thread.join(timeout=2)

    def _request(self, method: str, path: str, payload=None):
        data = None
        headers = {}
        if payload is not None:
            data = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(
            f"http://127.0.0.1:{self.port}{path}", data=data,
            headers=headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status, json.loads(
                    response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read().decode("utf-8"))

    def test_full_lifecycle_over_http(self):
        status, body = self._request(
            "POST", "/run", {"session_ids": ["sess-s1-aurora"]})
        self.assertEqual(status, 200)
        self.assertTrue(body["ok"])

        status, body = self._request(
            "GET", "/sessions/sess-s1-aurora/fingerprints")
        self.assertEqual(status, 200)
        self.assertEqual(len(body["fingerprints"]["turns"]), 3)

        status, body = self._request(
            "GET", "/sessions/sess-s1-aurora/evidence?turn=0")
        self.assertEqual(status, 200)
        self.assertTrue(body["evidence"]["evidence_chain"])

        status, body = self._request(
            "GET", "/sessions/sess-s1-aurora/plan?turn=0")
        self.assertEqual(status, 200)
        self.assertEqual(body["plan"]["strategy"], "exact_full")

        status, body = self._request("POST", "/stop", {})
        self.assertEqual(status, 200)
        self.assertTrue(body["stop_requested"])

        status, body = self._request(
            "GET", "/export/sess-s1-aurora")
        self.assertEqual(status, 200)
        self.assertTrue(body["export"]["answers"])


if __name__ == "__main__":
    unittest.main()
