"""Real socket roundtrip against the HTTP server on 127.0.0.1."""

from __future__ import annotations

import json
import threading
import urllib.request
from http.server import ThreadingHTTPServer

from _base import REPO_ROOT, WorkspaceTestCase

from rerank_bench.server import Api, make_handler


class TestHttpServer(WorkspaceTestCase):
    def setUp(self):
        super().setUp()
        api = Api(self.ws_root, repo_root=REPO_ROOT)
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(api))
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever,
                                       daemon=True)
        self.thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        super().tearDown()

    def _post(self, path, body):
        req = urllib.request.Request(
            f"http://127.0.0.1:{self.port}{path}",
            data=json.dumps(body).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST")
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def _get(self, path):
        with urllib.request.urlopen(
                f"http://127.0.0.1:{self.port}{path}", timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def test_roundtrip(self):
        out = self._post("/import", {"data_dir": self.data_dir})
        self.assertTrue(out["ok"])
        out = self._post("/run", {"serial": True})
        self.assertTrue(out["result"]["aggregated"])
        out = self._get("/export")
        self.assertEqual(out["result"]["schema"], "rerank-bench.summary.v1")
        out = self._get("/budget")
        self.assertGreater(out["result"]["consumed_evaluations"], 0)

    def test_error_response_shape(self):
        self._post("/import", {"data_dir": self.data_dir})
        req = urllib.request.Request(
            f"http://127.0.0.1:{self.port}/export", method="GET")
        try:
            urllib.request.urlopen(req, timeout=30)
            self.fail("expected HTTPError")
        except urllib.error.HTTPError as exc:
            self.assertEqual(exc.code, 409)
            body = json.loads(exc.read().decode("utf-8"))
            self.assertFalse(body["ok"])
            self.assertEqual(body["error"]["code"], "E_INVALID_STAGE")
