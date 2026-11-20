"""HTTP API tests: loopback server, endpoints, error codes."""

from __future__ import annotations

import json
import shutil
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from pathlib import Path

from incseg.engine import Engine
from incseg.generator import generate_workspace
from incseg.server import ThreadingHTTPServer, _Handler


class ServerCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = Path(tempfile.mkdtemp(prefix="incseg-http-test-"))
        generate_workspace(cls.tmp / "ws", tiles=2, seed=11)
        handler = type("TestHandler", (_Handler,), {})
        cls.httpd = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        cls.httpd.engine = Engine(cls.tmp / "ws")
        cls.thread = threading.Thread(target=cls.httpd.serve_forever, daemon=True)
        cls.thread.start()
        cls.base = "http://127.0.0.1:%d" % cls.httpd.server_address[1]

    @classmethod
    def tearDownClass(cls):
        cls.httpd.shutdown()
        cls.httpd.server_close()
        cls.thread.join(timeout=5)
        shutil.rmtree(cls.tmp, ignore_errors=True)

    def get(self, path, expect_error=False):
        try:
            with urllib.request.urlopen(self.base + path) as resp:
                return resp.status, resp.read()
        except urllib.error.HTTPError as exc:
            if not expect_error:
                raise
            return exc.code, exc.read()

    def post(self, path, payload=None, expect_error=False):
        data = json.dumps(payload or {}).encode()
        req = urllib.request.Request(self.base + path, data=data,
                                     headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req) as resp:
                return resp.status, json.loads(resp.read())
        except urllib.error.HTTPError as exc:
            if not expect_error:
                raise
            return exc.code, json.loads(exc.read())

    def test_health_and_tiles(self):
        status, body = self.get("/health")
        self.assertEqual(status, 200)
        status, body = self.get("/tiles")
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body)["tiles"], ["tile_000", "tile_001"])

    def test_run_status_export_verify(self):
        status, report = self.post("/run")
        self.assertEqual(status, 200)
        self.assertEqual(report["units_completed"], 8)
        status, body = self.get("/tiles/tile_000")
        self.assertEqual(status, 200)
        plan = json.loads(body)
        for stage in ("classify", "instances", "postprocess", "quantize"):
            self.assertEqual(plan["stages"][stage]["status"], "valid")
            self.assertIsNotNone(plan["stages"][stage]["checkpoint_fingerprint"])
        status, body = self.get("/tiles/tile_000/export?kind=summary")
        self.assertEqual(status, 200)
        summary = json.loads(body)
        self.assertEqual(summary["tile_id"], "tile_000")
        status, body = self.get("/tiles/tile_000/export?kind=semantic")
        self.assertEqual(status, 200)
        self.assertTrue(body.startswith(b"\x89PNG"))
        status, report = self.post("/verify")
        self.assertEqual(status, 200)
        self.assertEqual(report["status"], "ok")

    def test_stop_resume_and_invalidations(self):
        status, body = self.post("/stop")
        self.assertEqual(status, 200)
        self.assertTrue(body["stop_requested"])
        status, _ = self.post("/resume")
        self.assertEqual(status, 200)
        status, body = self.get("/invalidations")
        self.assertEqual(status, 200)

    def test_import_tile(self):
        image = self.tmp / "ws" / "tiles" / "tile_000.png"
        status, body = self.post("/tiles/import", {
            "tile_id": "tile_009",
            "image_path": str(image),
            "like": "tile_000",
        })
        self.assertEqual(status, 200)
        status, body = self.post("/tiles/tile_009/rebuild")
        self.assertEqual(status, 200)
        self.assertEqual(body["units_completed"], 4)

    def test_error_responses(self):
        status, body = self.post("/tiles/tile_404/rebuild", expect_error=True)
        self.assertEqual(status, 400)
        self.assertEqual(body["error"]["code"], "E_WORKSPACE")
        status, body = self.get("/nope", expect_error=True)
        self.assertEqual(status, 404)


if __name__ == "__main__":
    unittest.main()
