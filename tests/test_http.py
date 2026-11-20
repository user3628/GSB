from __future__ import annotations

import json
import threading
import urllib.error
import urllib.request
from typing import Any

from qiu.engine import QueryEngine
from qiu.server import create_server


def _request(url: str, method: str = "GET", body: dict[str, Any] | None = None) -> tuple[int, dict[str, Any]]:
    data = json.dumps(body, ensure_ascii=False).encode("utf-8") if body is not None else None
    request = urllib.request.Request(url, data=data, method=method, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read())


def test_http_parse_version_and_error(engine: QueryEngine, data_paths: dict) -> None:
    server = create_server(engine, eval_path=data_paths["eval"], port=0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address
    base = f"http://{host}:{port}"
    try:
        status, health = _request(base + "/health")
        assert status == 200 and health["ok"] is True

        payload = {
            "query": {
                "channel": "map",
                "history_categories": [],
                "locale": "zh-CN",
                "query_id": "http-1",
                "raw_query": "北京咖啡店",
            },
            "faults": {},
        }
        status, result = _request(base + "/v1/parse", method="POST", body=payload)
        assert status == 200
        assert result["intent"]["label"] == "local_service"
        assert result["cache"]["status"] == "miss"
        status, result = _request(base + "/v1/parse", method="POST", body=payload)
        assert status == 200 and result["cache"]["status"] == "hit"

        bad_payload = {**payload, "faults": {"invalid_locale": True}}
        status, error = _request(base + "/v1/parse", method="POST", body=bad_payload)
        assert status == 400
        assert error["error"]["code"] == "E_INVALID_LOCALE"

        status, version = _request(base + "/v1/version")
        assert status == 200
        assert set(version["stage_fingerprints"]) == {"normalization", "intent", "slots", "decision"}
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
