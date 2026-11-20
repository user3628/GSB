import os

from fastapi.testclient import TestClient

from lora_bench.generator import DEFAULT_MODEL_CONFIG, generate_all, write_config
from lora_bench.config import parse_config
from lora_bench.http_app import app
from lora_bench.state import request_stop

client = TestClient(app)


def _prep(tmp_path):
    data = str(tmp_path / "data")
    ws = str(tmp_path / "ws")
    manifest = generate_all(data)
    return ws, manifest["config_path"]


def test_health_and_full_flow(tmp_path):
    ws, config_path = _prep(tmp_path)
    assert client.get("/health").json() == {"status": "ok"}

    resp = client.post("/import", json={"workspace": ws, "config_path": config_path})
    assert resp.status_code == 200
    assert resp.json()["experiment"] == "lora-deterministic-demo"

    summary = client.post("/run", json={"workspace": ws}).json()
    assert summary["run_phase"] == "completed"

    state = client.get("/state", params={"workspace": ws}).json()
    assert state["global_consumed"] <= state["global_cap"]

    budget = client.get("/budget", params={"workspace": ws}).json()
    reasons = {t["task_id"]: t["rejection_reason"] for t in budget["tasks"]}
    assert reasons["task-gamma-overbudget"] == "per_task_quota_exceeded"

    detail = client.get(
        "/tasks/task-gamma-overbudget", params={"workspace": ws}
    ).json()
    assert detail["status"] == "not_started"
    assert detail["config_fingerprint"]

    loss_csv = client.get("/export/loss", params={"workspace": ws})
    assert loss_csv.text.startswith("task_id,step,loss")


def test_stop_phase_error_when_idle(tmp_path):
    ws, _ = _prep(tmp_path)
    resp = client.post("/stop", json={"workspace": ws})
    assert resp.status_code == 409
    assert resp.json()["error"]["code"] == "E_INVALID_PHASE"


def test_stop_and_resume_over_http(tmp_path):
    ws, config_path = _prep(tmp_path)
    payload = parse_config(config_path).to_dict()
    payload["checkpoint_every"] = 1
    frequent = os.path.join(os.path.dirname(config_path), "f.yaml")
    write_config(frequent, payload)
    client.post("/import", json={"workspace": ws, "config_path": frequent})
    request_stop(ws)
    partial = client.post("/run", json={"workspace": ws})
    assert partial.status_code == 200
    resumed = client.post("/resume", json={"workspace": ws}).json()
    fresh_ws = os.path.join(os.path.dirname(ws), "fresh")
    client.post("/import", json={"workspace": fresh_ws, "config_path": frequent})
    fresh = client.post("/run", json={"workspace": fresh_ws}).json()
    assert resumed["summary_fingerprint"] == fresh["summary_fingerprint"]


def test_resume_unknown_task_returns_invalid_task(tmp_path):
    ws, config_path = _prep(tmp_path)
    client.post("/import", json={"workspace": ws, "config_path": config_path})
    resp = client.post(
        "/resume", json={"workspace": ws, "task_id": "ghost"}
    )
    assert resp.status_code in (404, 409)
    assert resp.json()["error"]["code"] == "E_INVALID_TASK"


def test_state_unknown_workspace_404(tmp_path):
    resp = client.get("/state", params={"workspace": str(tmp_path / "missing")})
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "E_NOT_FOUND"
