import json

import pytest
from fastapi.testclient import TestClient

from evalbench.api import create_app


@pytest.fixture()
def client():
    return TestClient(create_app())


def run_body(workspace, config="data/configs/config_v2.json", data="data/shards"):
    return {"workspace": workspace, "data_dir": data, "config_path": config}


def test_run_status_export_roundtrip(client, workspace, shards_dir, config_v2):
    r = client.post("/run", json=run_body(workspace, config_v2, shards_dir))
    assert r.status_code == 200 and r.json()["status"] == "completed"
    r = client.get("/status", params={"workspace": workspace, "data_dir": shards_dir,
                                      "config_path": config_v2})
    assert r.json()["shards"]["shard_v1_a"]["parse"]["state"] == "valid"
    r = client.get("/exports/reliability_report", params={"workspace": workspace})
    assert r.status_code == 200
    assert r.json()["summary"]["overall"]["n_samples"] == 208
    r = client.post("/validate/roundtrip",
                    json={"data_dir": shards_dir, "config_path": config_v2})
    assert all(v["ok"] for v in r.json()["shards"].values())


def test_version_mismatch_error_parity(client, workspace):
    r = client.post("/run", json=run_body(workspace, data="data/inject"))
    body = r.json()
    assert r.status_code == 400
    assert body["error"]["code"] == "E_VERSION_MISMATCH"
    assert body["error"]["details"]["shard_id"] == "shard_bad_version"
    assert body["error"]["details"]["record_position"] == 1


def test_config_invalid_error(client, workspace, shards_dir):
    r = client.post("/run", json=run_body(
        workspace, config="data/inject/config_out_of_range.json", data=shards_dir))
    assert r.json()["error"]["code"] == "E_CONFIG_INVALID"


def test_stale_and_invalid_stage_codes(client, workspace, shards_dir, config_v2):
    client.post("/run", json=run_body(workspace, config_v2, shards_dir))
    r = client.post("/publish", json={"workspace": workspace, "shard_id": "shard_v1_a",
        "record": {"stage": "parse", "seq": 1, "record_fingerprint": "x",
                   "config_fingerprint": "y", "artifact": {}}})
    assert r.status_code == 409 and r.json()["error"]["code"] == "E_STALE_CHECKPOINT"
    r = client.post("/publish", json={"workspace": workspace, "shard_id": "fresh",
        "record": {"stage": "diagnose", "seq": 3, "record_fingerprint": "x",
                   "config_fingerprint": "y", "artifact": {}}})
    assert r.status_code == 409 and r.json()["error"]["code"] == "E_INVALID_STAGE"
    r = client.post("/publish", json={"workspace": workspace, "shard_id": "fresh",
        "record": {"shard_id": "other", "stage": "parse", "seq": 1,
                   "record_fingerprint": "x", "config_fingerprint": "y", "artifact": {}}})
    assert r.json()["error"]["code"] == "E_STALE_CHECKPOINT"


def test_upgrade_endpoint_notes(client, config_v1):
    r = client.post("/configs/upgrade", json={"config_path": config_v1})
    body = r.json()
    assert body["upgraded_config"]["config_version"] == 2
    fields = {n["field"] for n in body["upgrade_notes"]}
    assert {"method", "n_bins", "expected_positive_rate"} <= fields


def test_cli_error_parity(tmp_path):
    import subprocess
    import sys
    proc = subprocess.run(
        [sys.executable, "-m", "evalbench", "run", "--data", "data/inject",
         "--config", "data/configs/config_v2.json", "--workspace", str(tmp_path / "w")],
        capture_output=True, text=True)
    assert proc.returncode == 2
    body = json.loads(proc.stderr)
    assert body["error"]["code"] == "E_VERSION_MISMATCH"
