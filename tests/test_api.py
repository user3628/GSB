from __future__ import annotations

import base64
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from qos_workbench.api import create_app


@pytest.fixture
def client(tmp_path) -> TestClient:
    return TestClient(create_app(tmp_path / "api.db"))


@pytest.fixture
def sample_paths() -> Path:
    return Path(__file__).resolve().parent.parent / "samples"


def b64(content: bytes) -> str:
    return base64.b64encode(content).decode("ascii")


def test_api_import_export_binary_and_jsonl_roundtrip(client, sample_paths):
    content_v3 = (sample_paths / "policies_v3.jsonl").read_bytes()
    imported = client.post(
        "/api/policies/import",
        json={"content_base64": b64(content_v3), "apply_snapshot": True},
    )
    assert imported.status_code == 200, imported.text
    imported_json = imported.json()
    assert imported_json["commit"]["snapshot_no"] == 1
    assert imported_json["import_report"]["format"] == "jsonl"
    original_policies = imported_json["snapshot"]["policies"]

    binary_export = client.get(
        "/api/policies/export", params={"format": "binary", "endianness": "big"}
    )
    assert binary_export.status_code == 200
    assert binary_export.content[:4] == b"QOSB"
    assert binary_export.headers["x-qos-snapshot-seq"] == "1"
    binary_imported = client.post(
        "/api/policies/import",
        json={
            "format": "binary",
            "content_base64": b64(binary_export.content),
            "snapshot_no_override": 2,
            "apply_snapshot": True,
        },
    )
    assert binary_imported.status_code == 200, binary_imported.text
    assert binary_imported.json()["import_report"]["endianness"] == "big"
    assert binary_imported.json()["snapshot"]["policies"] == original_policies

    json_export = client.post(
        "/api/policies/export",
        json={"format": "jsonl", "version": 3, "response_encoding": "text"},
    )
    assert json_export.status_code == 200
    assert "content" in json_export.json()
    json_imported = client.post(
        "/api/policies/import",
        json={
            "format": "jsonl",
            "content": json_export.json()["content"],
            "snapshot_no_override": 3,
            "apply_snapshot": True,
        },
    )
    assert json_imported.status_code == 200, json_imported.text
    assert json_imported.json()["snapshot"]["policies"] == original_policies

    current = client.get("/api/snapshots/current").json()
    assert current["snapshot"]["snapshot_no"] == 3
    assert len(current["snapshot"]["policies"]) == 4


def test_api_rejects_newer_version_without_partial_import(client, sample_paths):
    content = (sample_paths / "policies_v3.jsonl").read_bytes()
    client.post("/api/policies/import", json={"content_base64": b64(content)})
    current_before = client.get("/api/snapshots/current").json()["snapshot"]

    binary = bytearray((sample_paths / "policies_v3_little_endian.bin").read_bytes())
    binary[4] = 4
    rejected = client.post(
        "/api/policies/import",
        json={"content_base64": b64(bytes(binary)), "snapshot_no_override": 2},
    )
    assert rejected.status_code == 422
    error = rejected.json()["error"]
    assert error["code"] == "E_FORMAT_UNSUPPORTED"
    assert error["details"]["partial_import"] is False
    current_after = client.get("/api/snapshots/current").json()["snapshot"]
    assert current_after == current_before

    corrupt = bytearray((sample_paths / "policies_v3_little_endian.bin").read_bytes())
    corrupt[-6] ^= 1
    rejected = client.post(
        "/api/policies/import",
        json={"content_base64": b64(bytes(corrupt)), "snapshot_no_override": 2},
    )
    assert rejected.status_code == 400
    assert rejected.json()["error"]["code"] == "E_CRC_MISMATCH"
    assert client.get("/api/snapshots/current").json()["snapshot"] == current_before


def test_api_dependency_cycle_is_rejected_and_old_snapshot_stays_active(client, sample_paths):
    content = (sample_paths / "policies_v3.jsonl").read_bytes()
    client.post("/api/policies/import", json={"content_base64": b64(content)})
    before = client.get("/api/snapshots/current").json()

    cycle = {
        "format_version": 3,
        "snapshot_no": 10,
        "generated_at_us": 10,
        "policies": [
            {
                "queue_id": "a",
                "cir_bps": 1000,
                "pir_bps": 2000,
                "cbs_bytes": 1000,
                "pbs_bytes": 2000,
                "priority": 1,
                "match_rule": "all",
                "parent_queue_id": "b",
                "timestamp_us": 10,
            },
            {
                "queue_id": "b",
                "cir_bps": 1000,
                "pir_bps": 2000,
                "cbs_bytes": 1000,
                "pbs_bytes": 2000,
                "priority": 1,
                "match_rule": "all",
                "parent_queue_id": "a",
                "timestamp_us": 10,
            },
        ],
    }
    loaded = client.post("/api/snapshots/load", json=cycle)
    assert loaded.status_code == 409
    error = loaded.json()["error"]
    assert error["code"] == "E_SNAPSHOT_INVALID"
    assert error["details"]["cause_code"] == "E_DEPENDENCY_CYCLE"
    assert client.get("/api/snapshots/current").json() == before

    newer = dict(cycle)
    newer["format_version"] = 4
    rejected_newer = client.post("/api/snapshots/load", json=newer)
    assert rejected_newer.status_code == 409
    assert rejected_newer.json()["error"]["code"] == "E_SNAPSHOT_INVALID"
    assert rejected_newer.json()["error"]["details"]["cause_code"] == "E_FORMAT_UNSUPPORTED"
    assert client.get("/api/snapshots/current").json() == before


def test_api_import_reports_cycle_and_duplicate_without_commit(client):
    rows = [
        '{"format_version":3,"snapshot_no":7,"generated_at_ms":1,"queue_id":"dup",'
        '"cir_kbps":1000,"pir_kbps":1000,"cbs_kb":1,"pbs_kb":1,"priority":1,'
        '"match_rule":"first","parent_queue_id":null,"timestamp_ms":1}',
        '{"format_version":3,"snapshot_no":7,"generated_at_ms":1,"queue_id":"dup",'
        '"cir_kbps":1000,"pir_kbps":1000,"cbs_kb":1,"pbs_kb":1,"priority":5,'
        '"match_rule":"last","parent_queue_id":null,"timestamp_ms":1}',
    ]
    validated = client.post(
        "/api/policies/validate",
        json={"content": "\n".join(rows) + "\n", "apply_snapshot": False},
    )
    assert validated.status_code == 200, validated.text
    duplicate = validated.json()["import_report"]["duplicate_resolution"]
    assert duplicate["duplicate_count"] == 1
    assert validated.json()["snapshot"]["policies"][0]["priority"] == 5
    assert client.get("/api/health").json()["active_snapshot"] is None


def test_api_timestamp_truncation_and_v1_degrade(client, sample_paths):
    content = (sample_paths / "policies_v3.jsonl").read_bytes()
    validated = client.post(
        "/api/policies/validate",
        json={
            "content_base64": b64(content),
            "timestamp_truncation": "floor_ms",
            "apply_snapshot": False,
        },
    )
    assert validated.status_code == 200, validated.text
    report = validated.json()["import_report"]["timestamp_normalization"]
    assert report["truncation_rule"] == "floor_ms"
    assert report["cumulative_rounding_error_us"] == "-1824"

    v1 = (sample_paths / "policies_v1.jsonl").read_bytes()
    degraded = client.post(
        "/api/policies/import",
        json={"content_base64": b64(v1), "snapshot_no_override": 2, "apply_snapshot": True},
    )
    assert degraded.status_code == 200, degraded.text
    assert degraded.json()["import_report"]["negotiation"]["decision"] == "degraded_accept"
    assert len(degraded.json()["import_report"]["degrade_details"]) == 4
    for policy in degraded.json()["snapshot"]["policies"]:
        assert policy["pir_bps"] == policy["cir_bps"]
        assert policy["pbs_bytes"] == policy["cbs_bytes"]


def test_api_simulation_is_deterministic_and_diagnostics_are_queryable(client, sample_paths):
    content = (sample_paths / "policies_v3.jsonl").read_bytes()
    assert client.post("/api/policies/import", json={"content_base64": b64(content)}).status_code == 200

    first = client.post("/api/simulations/from-sample/traffic.jsonl")
    second = client.post("/api/simulations/from-sample/traffic.jsonl")
    assert first.status_code == 200, first.text
    assert second.status_code == 200, second.text
    first_json = first.json()
    second_json = second.json()
    assert first_json["batch_id"] != second_json["batch_id"]
    assert first_json["simulation"]["packets"] == second_json["simulation"]["packets"]
    assert first_json["simulation"]["per_queue"] == second_json["simulation"]["per_queue"]
    assert first_json["diagnostics"] == second_json["diagnostics"]
    assert first_json["diagnostics"]["precision_contract"]["decimal_places"] == 6
    assert first_json["diagnostics"]["no_traffic_queue_ids"] == ["wan"]
    assert first_json["diagnostics"]["under_shaped_queue_ids"] == ["bulk", "video", "voice"]

    latest = client.get("/api/diagnostics/latest")
    assert latest.status_code == 200
    assert latest.json()["batch_id"] == second_json["batch_id"]

    stored = client.get(f"/api/simulations/{first_json['batch_id']}")
    assert stored.status_code == 200
    assert stored.json()["batch"]["result_json"]["packets"] == first_json["simulation"]["packets"]

    recovery = client.get("/api/recovery/report").json()
    assert recovery["facts_only"] is True
    assert recovery["report_limitations"]


def test_api_negotiate_and_health(client, sample_paths):
    content = (sample_paths / "policies_v1_little_endian.bin").read_bytes()
    response = client.post(
        "/api/formats/negotiate",
        json={"content_base64": b64(content), "format": "auto"},
    )
    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["format"] == "binary"
    assert payload["detected_version"] == 1
    assert payload["negotiation"]["decision"] == "degraded_accept"

    matrix = client.get("/api/formats/compatibility").json()
    assert matrix["current_version"] == 3
    health = client.get("/api/health").json()
    assert health["ok"] is True
    assert health["active_snapshot"] is None


def test_api_rejects_invalid_request_model(client):
    response = client.post("/api/policies/import", json={"apply_snapshot": True})
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "E_REQUEST_INVALID"

    traversal = client.post(
        "/api/simulations", json={"sample_name": "../policies_v3.jsonl"}
    )
    assert traversal.status_code == 400
    assert traversal.json()["error"]["code"] == "E_REQUEST_INVALID"

