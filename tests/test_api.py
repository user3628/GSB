from __future__ import annotations

import copy
from pathlib import Path

from fastapi.testclient import TestClient

from app.main import create_app
from app.models import PolicySnapshot, TraceSample
from app.service import WorkbenchService
from app.storage import Storage


def submit_policy(client: TestClient, payload: dict) -> dict:
    response = client.post("/api/policies", json=payload)
    assert response.status_code == 201, response.text
    return response.json()


def run_simulation(client: TestClient, trace: dict, version: int) -> dict:
    response = client.post(
        "/api/simulations",
        json={"snapshot_version": version, "trace": trace},
    )
    assert response.status_code == 201, response.text
    return response.json()


def test_snapshot_staleness_and_checksum_are_atomic(client, v1_payload, v2_payload):
    submit_policy(client, v1_payload)

    stale = copy.deepcopy(v1_payload)
    stale["checksum"] = "sha256:" + "0" * 64
    response = client.post("/api/policies", json=stale)
    assert response.status_code == 409
    assert response.json()["error"]["code"] == "E_STALE_SNAPSHOT"
    assert response.json()["error"]["details"]["current_version"] == 1
    assert client.get("/api/policies/current").json()["version"] == 1

    invalid = copy.deepcopy(v2_payload)
    invalid["checksum"] = "sha256:" + "1" * 64
    response = client.post("/api/policies", json=invalid)
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "E_SNAPSHOT_INVALID"
    assert client.get("/api/policies/current").json()["version"] == 1

    submit_policy(client, v2_payload)
    current = client.get("/api/policies/current").json()
    assert current["version"] == 2
    assert current["checksum"] == v2_payload["checksum"]
    version_one = client.get("/api/policies/1").json()
    assert version_one["checksum"] == v1_payload["checksum"]
    assert version_one["snapshot"]["target_rules"]["checkout"]["timeout_ms"] == 300


def test_unified_errors_and_route_ambiguity(client, v2_payload):
    missing_snapshot = client.get("/api/policies/current")
    assert missing_snapshot.status_code == 404
    assert missing_snapshot.json()["error"]["code"] == "E_SNAPSHOT_NOT_FOUND"

    batch_missing = client.get("/api/simulations/not-a-batch")
    assert batch_missing.status_code == 404
    assert batch_missing.json()["error"]["code"] == "E_BATCH_NOT_FOUND"

    order_missing = client.get("/api/orders/not-an-order")
    assert order_missing.status_code == 404
    assert order_missing.json()["error"]["code"] == "E_ORDER_NOT_FOUND"

    invalid_body = client.post("/api/policies", json={"version": "one"})
    assert invalid_body.status_code == 422
    assert invalid_body.json()["error"]["code"] == "E_INVALID_ARGUMENT"

    submit_policy(client, v2_payload)
    ambiguity_request = {
        "snapshot_version": 2,
        "path": "/api/checkout/order",
        "headers": {"x-tenant": "premium", "x-retry": "1", "x-campaign": "launch"},
        "strict": True,
    }
    ambiguous = client.post("/api/routes/resolve", json=ambiguity_request)
    assert ambiguous.status_code == 409
    assert ambiguous.json()["error"]["code"] == "E_ROUTE_AMBIGUOUS"

    ambiguity_request["strict"] = False
    resolved = client.post("/api/routes/resolve", json=ambiguity_request)
    assert resolved.status_code == 200
    assert resolved.json()["route_id"] == "checkout_campaign"
    assert resolved.json()["tied_at_top_precedence"] is True


def test_float_timestamp_is_rejected_as_invalid_argument(client, v1_payload, trace_payload):
    submit_policy(client, v1_payload)
    floating_trace = copy.deepcopy(trace_payload)
    floating_trace["requests"][0]["timestamp_ms"] = 1763596800000.5
    response = client.post(
        "/api/simulations",
        json={"snapshot_version": 1, "trace": floating_trace},
    )
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "E_INVALID_ARGUMENT"


def test_repeat_simulation_and_order_traceability(client, v1_payload, trace_payload):
    submit_policy(client, v1_payload)
    first = run_simulation(client, trace_payload, 1)
    second = run_simulation(client, trace_payload, 1)
    assert first["decisions"] == second["decisions"]
    assert first["batch"]["snapshot_version"] == second["batch"]["snapshot_version"] == 1
    assert first["batch"]["snapshot_checksum"] == v1_payload["checksum"]

    batch_id = second["batch"]["batch_id"]
    decisions = client.get(f"/api/simulations/{batch_id}/decisions")
    assert decisions.status_code == 200
    assert decisions.json()["decisions"] == first["decisions"]
    assert decisions.json()["decision_count"] == 18

    orders = client.get(f"/api/simulations/{batch_id}/orders").json()["orders"]
    assert orders
    assert len({(item["route_id"], item["difference_type"]) for item in orders}) == len(orders)
    for order in orders:
        one = client.get(f"/api/orders/{order['order_id']}")
        assert one.status_code == 200
        assert one.json()["route_id"] == order["route_id"]
        assert one.json()["trigger_condition"] == order["trigger_condition"]

    conflicts = client.get(f"/api/simulations/{batch_id}/conflicts")
    assert conflicts.status_code == 200
    assert conflicts.json()["conflicts"] == []


def test_version_comparison_reports_route_level_conflicts(client, v1_payload, v2_payload, trace_payload):
    submit_policy(client, v1_payload)
    left = run_simulation(client, trace_payload, 1)
    submit_policy(client, v2_payload)
    right = run_simulation(client, trace_payload, 2)
    comparison = client.get(
        "/api/simulations/compare",
        params={"left": left["batch"]["batch_id"], "right": right["batch"]["batch_id"]},
    )
    assert comparison.status_code == 200
    body = comparison.json()
    assert body["decision_change_count"] > 0
    assert body["trace_checksum"] == left["batch"]["trace_checksum"]
    conflicts = {item["route_id"]: item for item in body["route_conflict_comparison"]}
    assert conflicts["checkout"]["status"] == "ADDED"
    assert conflicts["checkout"]["right"]["timeout_ms"] == 6000
    assert conflicts["checkout"]["right"]["breaker_window_ms"] == 3000
    assert conflicts["checkout_campaign"]["status"] == "ADDED"
    assert all(item["status"] == "ADDED" for item in conflicts.values())


def test_illegal_transition_does_not_change_state(client, v1_payload, trace_payload):
    submit_policy(client, v1_payload)
    prepared = client.post(
        "/api/simulations",
        json={"snapshot_version": 1, "trace": trace_payload, "execution": "prepare"},
    )
    assert prepared.status_code == 201
    batch_id = prepared.json()["batch"]["batch_id"]
    assert prepared.json()["batch"]["status"] == "PENDING"

    illegal = client.post(
        f"/api/simulations/{batch_id}/transition",
        json={"status": "COMPLETED"},
    )
    assert illegal.status_code == 409
    assert illegal.json()["error"]["code"] == "E_INVALID_ARGUMENT"
    assert client.get(f"/api/simulations/{batch_id}").json()["batch"]["status"] == "PENDING"

    legal = client.post(
        f"/api/simulations/{batch_id}/transition",
        json={"status": "ABORTED"},
    )
    assert legal.status_code == 200
    assert legal.json()["batch"]["status"] == "ABORTED"


def test_recovery_report_preserves_staged_facts_but_hides_conclusions(
    client, v2_payload, trace_payload
):
    submit_policy(client, v2_payload)
    prepared = client.post(
        "/api/simulations",
        json={"snapshot_version": 2, "trace": trace_payload, "execution": "prepare"},
    )
    batch_id = prepared.json()["batch"]["batch_id"]
    service: WorkbenchService = client.app.state.service
    staged = service.execute_prepared_batch(batch_id, stage_limit=5, finalize=False)
    assert staged["batch"]["status"] == "RUNNING"
    assert staged["staged_count"] == 5

    running_summary = client.get(f"/api/simulations/{batch_id}").json()
    assert running_summary["batch"]["status"] == "RUNNING"
    assert running_summary["batch"]["conclusion_visible"] is False
    assert "decisions" not in running_summary
    hidden_before = client.get(f"/api/simulations/{batch_id}/decisions")
    assert hidden_before.status_code == 409
    assert hidden_before.json()["error"]["code"] == "E_BATCH_INCOMPLETE"

    scan = client.post("/api/recovery/scan")
    assert scan.status_code == 200
    matching = [item for item in scan.json()["reports"] if item["batch_id"] == batch_id]
    assert len(matching) == 1
    report = matching[0]
    assert report["outcome"] == "ABORTED"
    assert report["facts"]["persisted_staged_decisions"] == 5
    assert report["facts"]["persisted_final_decisions"] == 0
    assert report["facts"]["complete_conclusion_available"] is False

    aborted = client.get(f"/api/simulations/{batch_id}").json()["batch"]
    assert aborted["status"] == "ABORTED"
    assert aborted["recovery_report_id"] == report["report_id"]
    hidden_after = client.get(f"/api/simulations/{batch_id}/decisions")
    assert hidden_after.status_code == 409
    assert hidden_after.json()["error"]["code"] == "E_BATCH_INCOMPLETE"
    fetched_report = client.get(f"/api/recovery/reports/{batch_id}")
    assert fetched_report.status_code == 200
    assert fetched_report.json()["facts"]["persisted_staged_decisions"] == 5
    assert client.post("/api/recovery/scan").json()["recovered_count"] == 0


def test_startup_scan_recovers_an_abandoned_pending_batch(tmp_path: Path, v1_payload, trace_payload):
    db_path = tmp_path / "startup-recovery.db"
    storage = Storage(db_path)
    service = WorkbenchService(storage)
    service.submit_snapshot(PolicySnapshot.model_validate(v1_payload))
    prepared = service.prepare_batch(1, TraceSample.model_validate(trace_payload))
    batch_id = prepared["batch_id"]

    application = create_app(db_path)
    with TestClient(application) as restarted:
        health = restarted.get("/api/health").json()
        assert health["startup_recovery_count"] == 1
        summary = restarted.get(f"/api/simulations/{batch_id}").json()["batch"]
        assert summary["status"] == "ABORTED"
        report = restarted.get(f"/api/recovery/reports/{batch_id}")
        assert report.status_code == 200
        assert report.json()["facts"]["previous_status"] == "PENDING"
