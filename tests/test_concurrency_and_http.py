from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from fastapi.testclient import TestClient

from mmbackfill.api import create_app
from tests.conftest import make_engine


def test_two_processes_with_same_expected_sequence_have_one_stale(tmp_path: Path, demo_dataset: Path) -> None:
    workspace = tmp_path / "concurrent"
    make_engine(workspace, demo_dataset)
    commands = [
        [
            sys.executable,
            "-m",
            "mmbackfill",
            "--workspace",
            str(workspace),
            "backfill",
            "--batch-id",
            batch_id,
            "--text-ids",
            text_id,
            "--expected-seq",
            "0",
            "--order",
            "id",
        ]
        for batch_id, text_id in (("proc-a", "txt-a"), ("proc-b", "txt-b"))
    ]
    env = dict(os.environ)
    env["PYTHONPATH"] = str(Path(__file__).resolve().parents[1])
    processes = [subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env, cwd=Path(__file__).resolve().parents[1]) for command in commands]
    outputs = [process.communicate(timeout=30) for process in processes]
    return_codes = [process.returncode for process in processes]
    assert sorted(return_codes) == [0, 1]
    combined = "\n".join(stdout + stderr for stdout, stderr in outputs)
    assert "E_STALE_CHECKPOINT" in combined
    assert len(list((workspace / "runtime/records").glob("batch-*.json"))) == 1
    from mmbackfill.engine import Engine

    engine = Engine(workspace)
    assert engine.status()["last_batch_seq"] == 1
    assert engine.batches()["latest_sequence"] == 1
    index = engine.index()
    seen = set()
    for shard in index["shards"]:
        for entry in shard["entries"]:
            assert entry["vector_digest"] not in seen
            seen.add(entry["vector_digest"])


def test_http_status_backfill_stop_recover(tmp_path: Path, demo_dataset: Path) -> None:
    workspace = tmp_path / "http"
    engine = make_engine(workspace, demo_dataset)
    client = TestClient(create_app(workspace))
    assert client.get("/health").json()["ok"] is True
    status = client.get("/status").json()
    assert status["last_batch_seq"] == 0
    response = client.post(
        "/backfill",
        json={"batch_id": "http-1", "text_ids": ["txt-a", "txt-b"], "expected_sequence": 0, "order": "id"},
    )
    assert response.status_code == 200
    assert response.json()["batch_seq"] == 1
    assert client.get("/index").json()["entry_count"] == 2
    assert client.get("/batches").json()["latest_sequence"] == 1
    stop_response = client.post("/stop", json={"reason": "http test"})
    assert stop_response.status_code == 200
    stale = client.post(
        "/backfill",
        json={"batch_id": "http-2", "text_ids": ["txt-c"], "expected_sequence": 1, "order": "id"},
    )
    assert stale.status_code == 409
    assert stale.json()["error"]["code"] == "E_STOPPED"
    recovered = client.post("/recover", json={"clear_stop": True})
    assert recovered.status_code == 200
    assert recovered.json()["reused_existing_index"] is True
    assert client.get("/health").json()["ok"] is True
