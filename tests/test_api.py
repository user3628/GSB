"""HTTP API tests through the FastAPI test client."""
import pytest
from fastapi.testclient import TestClient

from sft_workbench.generator import generate_shards
from sft_workbench.server import create_app


@pytest.fixture(scope="module")
def shards(tmp_path_factory):
    return generate_shards(tmp_path_factory.mktemp("data"))


@pytest.fixture()
def client(tmp_path):
    return TestClient(create_app(tmp_path / "ws"))


def test_full_flow(client, shards):
    resp = client.post("/samples/import", json={"shards": [str(p) for p in shards]})
    assert resp.status_code == 200
    resp = client.post("/stages/run", json={"stage": "all"})
    assert resp.status_code == 200
    assert resp.json()["status"] == "completed"

    fp = client.get("/fingerprint").json()
    assert fp["version_fingerprint"]

    quality = client.get("/quality").json()
    assert len(quality["scores"]) > 0

    mixture = client.get("/mixture").json()
    assert len(mixture["mixture"]) > 0

    exported = client.get("/export/mixture")
    assert exported.status_code == 200
    lines = [l for l in exported.text.strip().splitlines() if l]
    assert len(lines) == len(mixture["mixture"])

    checks = client.get("/checkpoints").json()
    assert checks["verification"]["kind"] == "normal"
    assert [r["seq"] for r in checks["records"]] == [1, 2, 3, 4, 5]

    errors = client.get("/errors").json()
    codes = {e["code"] for e in errors["import_errors"]}
    assert "E_DECODE_SAMPLE" in codes
    assert "E_UNSUPPORTED_VERSION" in codes


def test_invalid_stage_error_shape(client):
    resp = client.post("/stages/run", json={"stage": "score"})
    assert resp.status_code == 409
    body = resp.json()
    assert set(body["error"].keys()) == {"code", "message", "details"}
    assert body["error"]["code"] == "E_INVALID_STAGE"


def test_unknown_stage_name(client):
    resp = client.post("/stages/run", json={"stage": "frobnicate"})
    assert resp.status_code == 409
    assert resp.json()["error"]["code"] == "E_INVALID_STAGE"


def test_quality_before_score_stage(client, shards):
    resp = client.get("/quality")
    assert resp.status_code == 409
    assert resp.json()["error"]["code"] == "E_INVALID_STAGE"


def test_stop_and_resume(client, shards):
    assert client.post("/stop").json()["status"] == "stop_requested"
    resp = client.post("/samples/import", json={"shards": [str(p) for p in shards]})
    assert resp.json()["status"] == "stopped"
    resp = client.post("/resume")
    assert resp.json()["status"] == "completed"
    assert client.get("/checkpoints").json()["verification"]["kind"] == "normal"


def test_import_requires_shards(client):
    resp = client.post("/samples/import", json={})
    assert resp.status_code == 409
    assert resp.json()["error"]["code"] == "E_INVALID_INPUT"
