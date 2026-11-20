from __future__ import annotations

import copy
import json
from pathlib import Path

from fastapi.testclient import TestClient

from app.main import create_app

from .conftest import ROOT, SAMPLE_CONFIG


def client_for(tmp_path: Path, *, overrides=None):
    tmp_path.mkdir(parents=True, exist_ok=True)
    if overrides:
        raw = json.loads(SAMPLE_CONFIG.read_text(encoding="utf-8"))
        raw.update(overrides)
        config_path = tmp_path / "config.json"
        config_path.write_text(json.dumps(raw), encoding="utf-8")
    else:
        config_path = SAMPLE_CONFIG
    app = create_app(config_path=config_path, runtime_dir=tmp_path / "runtime")
    return TestClient(app)


def test_invalid_event_and_conflicting_batch_error_contract(tmp_path):
    with client_for(tmp_path) as client:
        invalid = client.post(
            "/v1/batches",
            json={
                "batch_id": "invalid",
                "events": [
                    {"timestamp_ms": 1000, "group": "g", "value": 1.0},
                    {"timestamp_ms": 1001, "value": 2.0},
                ],
            },
        )
        assert invalid.status_code == 400
        assert invalid.json()["error"]["code"] == "E_EVENT_INVALID"
        assert client.get("/v1/state/summary").json()["state"]["state_version"] == 0

        good = {"batch_id": "dup", "events": [{"timestamp_ms": 1000, "group": "g", "value": 1.0}]}
        assert client.post("/v1/batches", json=good).status_code == 200
        replay = client.post("/v1/batches", json=good)
        assert replay.status_code == 200
        assert replay.json()["replayed"] is True
        conflict = client.post(
            "/v1/batches",
            json={"batch_id": "dup", "events": [{"timestamp_ms": 1000, "group": "g", "value": 2.0}]},
        )
        assert conflict.status_code == 409
        assert conflict.json()["error"]["code"] == "E_CONFLICTING_BATCH"

        illegal = client.post(
            "/v1/state/transitions/validate",
            json={"current": "OPEN", "target": "EMITTED"},
        )
        assert illegal.status_code == 409
        assert illegal.json()["error"]["code"] == "E_INVALID_TRANSITION"


def test_reject_late_policy_and_capacity_codes_via_http(tmp_path):
    with client_for(tmp_path / "late", overrides={"late_policy": "reject"}) as client:
        client.post(
            "/v1/batches",
            json={"batch_id": "b1", "events": [{"timestamp_ms": 100, "group": "g", "value": 1.0}]},
        )
        client.post(
            "/v1/batches",
            json={"batch_id": "b2", "events": [{"timestamp_ms": 2000, "group": "g", "value": 2.0}]},
        )
        late = client.post(
            "/v1/batches",
            json={"batch_id": "late", "events": [{"timestamp_ms": 500, "group": "g", "value": 3.0}]},
        )
        assert late.status_code == 409
        assert late.json()["error"]["code"] == "E_LATE_EVENT"

    with client_for(
        tmp_path / "capacity",
        overrides={"active_window_soft_limit": 2, "active_window_hard_limit": 5},
    ) as client:
        response = client.post(
            "/v1/batches",
            json={"batch_id": "big", "events": [{"timestamp_ms": 1000, "group": "g", "value": 1.0}]},
        )
        assert response.status_code == 429
        assert response.json()["error"]["code"] == "E_CAPACITY_EXCEEDED"
        assert client.get("/v1/state/summary").json()["state"]["state_version"] == 0


def test_snapshot_monotonic_checksum_and_whole_state_replacement(tmp_path):
    with client_for(tmp_path) as client:
        client.post(
            "/v1/batches",
            json={"batch_id": "b1", "events": [{"timestamp_ms": 100, "group": "g1", "value": 1.0}]},
        )
        snapshot_one = client.post("/v1/snapshots", json={"label": "one"}).json()["snapshot"]
        client.post(
            "/v1/batches",
            json={"batch_id": "b2", "events": [{"timestamp_ms": 5000, "group": "g2", "value": 2.0}]},
        )
        before = client.get("/v1/state/summary").json()["state"]
        nonmonotonic = client.post("/v1/snapshots/load", json={"version": snapshot_one["version"]})
        assert nonmonotonic.status_code == 409
        assert nonmonotonic.json()["error"]["code"] == "E_SNAPSHOT_INVALID"
        assert client.get("/v1/state/summary").json()["state"] == before

        runtime = tmp_path / "runtime"
        engine = client.app.state.engine
        original = engine.store.load_version(snapshot_one["version"])
        external_body = copy.deepcopy(original.body)
        external_body["version"] = 50
        envelope = engine.store.build_envelope(external_body)
        envelope["intent_id"] = "external-fixture"
        envelope["batch_id"] = None
        external_path = tmp_path / "external-snapshot.json"
        external_path.write_text(json.dumps(envelope), encoding="utf-8")

        loaded = client.post("/v1/snapshots/load", json={"path": str(external_path)})
        assert loaded.status_code == 200, loaded.text
        payload = loaded.json()
        assert payload["whole_state_replaced"] is True
        assert payload["loaded_from_version"] == 50
        assert payload["window_count"] == len(original.body["windows"])
        windows = client.post(
            "/v1/windows/query",
            json={"start_ms": -10000, "end_ms": 100000, "limit": 1000},
        ).json()["items"]
        assert windows
        assert all(item["group"] != "g2" for item in windows)

        corrupted = copy.deepcopy(envelope)
        corrupted["checksum"] = "0" * 64
        corrupted_path = tmp_path / "corrupt-snapshot.json"
        corrupted_path.write_text(json.dumps(corrupted), encoding="utf-8")
        version_before = client.get("/v1/state/summary").json()["state"]["state_version"]
        bad = client.post("/v1/snapshots/load", json={"path": str(corrupted_path)})
        assert bad.status_code == 409
        assert bad.json()["error"]["code"] == "E_SNAPSHOT_INVALID"
        assert client.get("/v1/state/summary").json()["state"]["state_version"] == version_before


def test_full_sample_http_flow_and_late_queue(tmp_path):
    with client_for(tmp_path) as client:
        for fixture in ("phase1.json", "crash_batch.json", "phase2.json"):
            payload = json.loads((ROOT / "samples" / fixture).read_text(encoding="utf-8"))
            for batch in payload["batches"]:
                response = client.post(
                    "/v1/batches",
                    json={"batch_id": batch["batch_id"], "events": batch["events"]},
                )
                assert response.status_code == 200, response.text
                assert client.post("/v1/snapshots", json={"label": batch["batch_id"]}).status_code == 200

        query_body = {
            "start_ms": -10_000_000_000,
            "end_ms": 10_000_000_000_000,
            "limit": 5,
        }
        first = client.post("/v1/windows/query", json=query_body).json()
        assert first["total"] > 0
        assert first["next_cursor"]
        late = client.get("/v1/late-events", params={"limit": 1000}).json()
        assert late["queue_length"] > 0
        recovery = client.get("/v1/recovery/report").json()["report"]
        assert recovery["fact_source"].startswith("complete snapshot")


def test_fixed_snapshot_sample_loads_as_whole_state(tmp_path):
    sample_snapshot = ROOT / "samples" / "snapshot_sample.json"
    with client_for(tmp_path) as client:
        response = client.post(
            "/v1/snapshots/load",
            json={"path": str(sample_snapshot)},
        )
        assert response.status_code == 200, response.text
        payload = response.json()
        assert payload["loaded_from_version"] == 100
        assert payload["whole_state_replaced"] is True
        summary = client.get("/v1/state/summary").json()["state"]
        assert summary["window_count"] == payload["window_count"]
        assert summary["late_queue_length"] == payload["late_queue_length"]
        windows = client.post(
            "/v1/windows/query",
            json={
                "start_ms": -10_000,
                "end_ms": 1700000010000,
                "limit": 1000,
            },
        ).json()
        assert windows["total"] > 0
