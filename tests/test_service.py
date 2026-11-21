from __future__ import annotations

import json
import threading
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.request import Request, urlopen

from contract_elements.service import ContractHTTPServer

from .common import ROOT, config, paths


class ServiceTests(unittest.TestCase):
    def test_http_parse_candidates_version_experiment_stop_and_resume(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            server = ContractHTTPServer(("127.0.0.1", 0), config(), paths(root / "output", root / "runtime"))
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            base = f"http://127.0.0.1:{server.server_address[1]}"

            def get(path: str) -> dict:
                with urlopen(base + path, timeout=10) as response:
                    return json.loads(response.read().decode("utf-8"))

            def post(path: str, body: dict) -> dict:
                request = Request(
                    base + path,
                    data=(json.dumps(body, separators=(",", ":")).encode("utf-8")),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with urlopen(request, timeout=30) as response:
                    return json.loads(response.read().decode("utf-8"))

            try:
                self.assertEqual(get("/health")["status"], "ok")
                contract = get("/v1/contracts/C-ALPHA-01")
                self.assertEqual(contract["contract_id"], "C-ALPHA-01")
                self.assertTrue(contract["conflicts"])
                candidates = get("/v1/contracts/C-ALPHA-01/candidates")
                self.assertTrue(candidates["candidates"])
                experiment = post("/v1/experiments", {})
                self.assertEqual(experiment["status"], "completed")
                version = get("/v1/version")
                self.assertIsNotNone(version["version_state"])
                self.assertTrue(version["version_state"]["version_fingerprint"])
                stop = post("/v1/stop", {})
                self.assertEqual(stop["status"], "stop_requested")
                resumed = post("/v1/resume", {})
                self.assertEqual(resumed["status"], "completed")
                interop = post("/v1/interop/import", {"source": str(ROOT / "data/annotations_v1.jsonl")})
                self.assertTrue(interop["compatibility"]["success"])
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)


if __name__ == "__main__":
    unittest.main()

