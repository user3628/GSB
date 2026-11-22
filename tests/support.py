from __future__ import annotations

import json
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]


def load_fixture(name: str) -> dict[str, Any]:
    return json.loads((ROOT / "fixtures" / name).read_text(encoding="utf-8"))


def http_call(
    base_url: str,
    method: str,
    path: str,
    body: dict[str, Any] | None = None,
    *,
    timeout: float = 30.0,
) -> tuple[int, dict[str, Any]]:
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body, ensure_ascii=False).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(base_url + path, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        try:
            return exc.code, json.loads(exc.read().decode("utf-8"))
        finally:
            exc.close()

