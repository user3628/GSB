from __future__ import annotations

import json
import threading
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from sampling_engine.config import EngineConfig
from sampling_engine.engine import SamplingEngine
from sampling_engine.service import create_server


def _call(base: str, method: str, path: str, payload=None):
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    request = Request(
        base + path,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urlopen(request, timeout=20) as response:
            return response.status, json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        return exc.code, json.loads(exc.read().decode("utf-8"))


def test_http_full_demo_endpoints(tmp_path):
    config = EngineConfig(host="127.0.0.1", port=0, max_temperature=5.0)
    engine = SamplingEngine(tmp_path / "workspace", config)
    server = create_server(engine, config, port=0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address[:2]
    base = f"http://{host}:{port}"
    try:
        status, health = _call(base, "GET", "/health")
        assert status == 200 and health["ok"] is True
        assert health["binding"].startswith("127.0.0.1:")

        manifest_path = str(Path("data/manifest_v2.json").resolve())
        status, imported = _call(base, "POST", "/api/import", {"path": manifest_path})
        assert status == 200 and imported["ok"] is True
        manifest_id = imported["manifest_id"]
        assert imported["request_ids"] == ["r2_small", "r2_tie", "r2_nan", "r2_inf"]

        status, run = _call(
            base, "POST", "/api/run", {"stop_after_requests": 1}
        )
        assert status == 200 and run["stopped"] is True
        assert run["processed"][0]["request_id"] == "r2_small"

        status, request_status = _call(
            base, "GET", "/api/requests/r2_small/status"
        )
        assert status == 200 and request_status["status"] == "complete"
        assert request_status["sequence"]["contiguous"] is True

        status, fingerprints = _call(
            base, "GET", "/api/requests/r2_small/fingerprints"
        )
        assert status == 200
        assert set(fingerprints["stages"]) == {
            "postprocess",
            "normalize",
            "decode",
            "summarize",
        }

        status, tokens = _call(base, "GET", "/api/requests/r2_small/tokens")
        assert status == 200 and len(tokens["tokens"]) == 7
        status, temperature = _call(
            base, "GET", "/api/requests/r2_small/temperature"
        )
        assert status == 200 and temperature["effective_temperature"] == 0.7

        status, resumed = _call(base, "POST", "/api/resume", {})
        assert status == 200 and resumed["result"]["corpus"]["token_count"] > 0
        _, final_result = _call(base, "GET", "/api/result")
        assert final_result["result"]["corpus"]["failed_request_count"] == 2
        assert any(
            item["status"] == "error"
            and item["error"]["code"] == "E_NUMERIC"
            for item in final_result["result"]["requests"]
        )

        status, exported = _call(
            base, "GET", f"/api/manifests/{manifest_id}/export?target_version=1"
        )
        assert status == 200 and exported["manifest"]["manifest_version"] == 1
        assert "top_p" not in exported["manifest"]["requests"][0]

        status, stopped = _call(base, "POST", "/api/stop", {})
        assert status == 200 and stopped["stop_requested"] is True

        status, error = _call(
            base,
            "POST",
            "/api/import",
            {
                "manifest": {
                    "manifest_version": 2,
                    "vocab": ["a"],
                    "requests": [
                        {
                            "logits_path": "x.npy",
                            "temperature": 1.0,
                            "top_k": 1,
                            "top_p": 0.9,
                            "repetition_penalty": 1.0,
                            "seed": 1,
                            "steps": 1,
                        }
                    ],
                }
            },
        )
        assert status == 400 and error["code"] == "E_INVALID_MANIFEST"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
