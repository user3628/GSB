"""Exercise the local HTTP service without external HTTP dependencies."""

from __future__ import annotations

import json
import shutil
import threading
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen

from .canonical import digest_obj
from .configs import default_config
from .datagen import generate_dataset
from .http_app import create_server


def _request(base: str, method: str, path: str, body: dict[str, Any] | None = None) -> dict[str, Any]:
    data = None if body is None else json.dumps(body, ensure_ascii=False).encode("utf-8")
    request = Request(
        base + path,
        data=data,
        method=method,
        headers={"Content-Type": "application/json; charset=utf-8"},
    )
    with urlopen(request, timeout=60) as response:
        return json.loads(response.read().decode("utf-8"))


def run_http_demo(
    *,
    workspace: str | Path = "runtime/http-demo-workspace",
    data_dir: str | Path = "data/sample",
    port: int = 0,
) -> dict[str, Any]:
    workspace_path = Path(workspace).resolve()
    data_path = Path(data_dir).resolve()
    if workspace_path.exists():
        shutil.rmtree(workspace_path)
    generated = generate_dataset(data_path, shards=3, items_per_shard=4, seed=20261122)
    server = create_server(workspace_path, host="127.0.0.1", port=port)
    actual_port = int(server.server_address[1])
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{actual_port}"
    try:
        health = _request(base, "GET", "/health")
        imported = _request(base, "POST", "/import", {"dataset_dir": str(data_path)})
        configured = _request(
            base,
            "POST",
            "/config",
            {"bits": 8, "granularity": "per_dim"},
        )
        first_build = _request(base, "POST", "/build", {"stop_after_units": 4})
        _request(base, "POST", "/stop", {})
        stopped_status = _request(base, "GET", "/status")
        stop_observable = stopped_status["stop_requested"]
        resumed = _request(base, "POST", "/resume", {})
        q8 = _request(
            base,
            "POST",
            "/search",
            {"mode": "text2image", "query": "红色 red 方块 square", "k": 5},
        )
        i8 = _request(
            base,
            "POST",
            "/search",
            {
                "mode": "image2text",
                "query_item_id": "shard-00-item-000",
                "k": 5,
            },
        )
        changed = _request(base, "POST", "/config", {"bits": 4})
        rebuilt = _request(base, "POST", "/build", {})
        verified = _request(base, "POST", "/verify", {})
        exported = _request(
            base,
            "POST",
            "/export",
            {"output_dir": str(workspace_path / "reports" / "http")},
        )
        audit = _request(base, "GET", "/checkpoints/audit")
        summary = {
            "ok": True,
            "port": actual_port,
            "health": health,
            "dataset_digest": imported["dataset_digest"],
            "config_digest_8": configured["config_digest"],
            "first_build_status": first_build["status"],
            "stop_observable": stop_observable,
            "resume_status": resumed["status"],
            "quant8_top1": q8["top_k"][0]["item_id"],
            "quant8_bound": q8["top_k"][0]["error_propagation"]["similarity_error_bound"],
            "image2text_top1": i8["top_k"][0]["item_id"],
            "quant4_config_digest": changed["config_digest"],
            "quant4_status": rebuilt["status"],
            "earliest_stage": rebuilt["invalidations"][-1]["earliest_stage"],
            "verify": verified,
            "export": exported,
            "checkpoint_audit": audit["status"],
            "http_demo_digest": digest_obj(
                {
                    "dataset": generated["digest"],
                    "run_digest": rebuilt["run_digest"],
                    "q8": q8["top_k"],
                    "i8": i8["top_k"],
                    "verify": verified,
                }
            ),
        }
        return summary
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
