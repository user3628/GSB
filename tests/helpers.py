from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from gateway.domain import compute_route_checksum


ROOT = Path(__file__).resolve().parents[1]


def sample_route(backend_url: str, version: int = 1) -> dict[str, Any]:
    path = ROOT / "samples" / f"route_v{version}.json"
    with open(path, "r", encoding="utf-8") as handle:
        document = json.load(handle)
    document.pop("checksum", None)
    for backend in document["backends"].values():
        backend["endpoint"] = backend_url
    document["checksum"] = compute_route_checksum(document)
    return document


def sample_batch() -> dict[str, Any]:
    with open(ROOT / "samples" / "requests_batch.json", "r", encoding="utf-8") as handle:
        return json.load(handle)


def wait_terminal(client, request_id: str, timeout: float = 5.0) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        response = client.get(f"/requests/{request_id}")
        assert response.status_code == 200, response.text
        last = response.json()["request"]
        if last["status"] in {"COMPLETED", "FAILED", "SHED", "ABORTED"}:
            return last
        time.sleep(0.02)
    assert last is not None
    return last


def wait_status(client, request_id: str, status: str, timeout: float = 3.0) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = client.get(f"/requests/{request_id}").json()["request"]
        if last["status"] == status:
            return last
        time.sleep(0.01)
    raise AssertionError(f"request {request_id} did not reach {status}: {last}")

