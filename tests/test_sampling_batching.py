from __future__ import annotations

from copy import deepcopy

from conftest import bootstrap


def _selected_business(selection: dict) -> list[tuple]:
    return [
        (
            record["timestamp_ms"],
            record["connection_id"],
            record["source_service"],
            record["destination_service"],
            record["method"],
            record["path_template"],
            record["status_code"],
            record["response_bytes"],
            record["duration_us"],
        )
        for record in selection["selected_records"]
    ]


def test_sampling_is_identical_across_commit_split(client_factory, records, policy):
    one_shot = client_factory()
    one_state = bootstrap(one_shot, records, policy, batch_id="all-records")

    split = client_factory()
    first = bootstrap(split, records[:101], policy, batch_id="split-a")
    response = split.post(
        "/api/v1/samples/batches",
        json={"base_version": first["commit"]["version"], "batch_id": "split-b", "records": records[101:]},
    )
    assert response.status_code == 201, response.text
    split_state = response.json()

    one_selection = one_shot.get("/api/v1/selection").json()["selection"]
    split_selection = split.get("/api/v1/selection").json()["selection"]
    assert _selected_business(one_selection) == _selected_business(split_selection)
    assert one_selection["rejection_counts"] == split_selection["rejection_counts"]
    assert one_state["selection_summary"]["coverage"] == split_state["selection_summary"]["coverage"]


def test_path_rules_precede_sampling_and_dedup_window(client_factory, records, policy):
    adjusted = deepcopy(policy)
    adjusted["sampling_rate"] = 1.0
    adjusted["dedup_window_ms"] = 90
    adjusted["include_path_templates"] = ["/api/*"]
    adjusted["exclude_path_templates"] = ["/api/payment/*", "/internal/*"]
    client = client_factory()
    state = bootstrap(client, records, adjusted)
    selected = client.get("/api/v1/selection").json()["selection"]
    assert selected["rejection_counts"]["path_excluded"] == 52
    assert selected["rejection_counts"]["path_not_included"] == 0
    assert selected["rejection_counts"]["dedup_window"] > 0
    assert all("/api/payment/" not in record["path_template"] for record in selected["selected_records"])
    assert all("/internal/" not in record["path_template"] for record in selected["selected_records"])
    assert state["selection_summary"]["selected_count"] == len(selected["selected_records"])


def test_batch_budget_causes_deterministic_downshift(client_factory, records, policy):
    adjusted = deepcopy(policy)
    adjusted["replay_multiplier"] = 3
    adjusted["max_batch_samples"] = 2
    adjusted["sampling_rate"] = 1.0
    adjusted["dedup_window_ms"] = 0
    adjusted["batch_max_bytes"] = 1000000
    adjusted["batch_max_duration_us"] = 1000000
    client = client_factory()
    state = bootstrap(client, records, adjusted)
    batches = client.get("/api/v1/batches").json()["batches"]
    assert batches["downshift"]["applied"] is True
    assert batches["downshift"]["reason"] == "batch_limits_exceeded"
    assert batches["downshift"]["affected_samples"] > 0
    assert batches["effective_multiplier"] == 1
    assert batches["split_connections"]
    for batch in batches["batches"]:
        assert batch["sample_count"] <= adjusted["max_batch_samples"]
        assert batch["estimated_bytes"] <= adjusted["batch_max_bytes"]
        assert batch["estimated_duration_us"] <= adjusted["batch_max_duration_us"]
    assert state["batch_summary"]["operation_count"] == sum(batch["operation_count"] for batch in batches["batches"])


def test_impossible_single_sample_budget_is_atomic_error(client_factory, records, policy):
    adjusted = deepcopy(policy)
    adjusted["batch_max_bytes"] = 10
    client = client_factory()
    response = client.post(
        "/api/v1/snapshots",
        json={
            "base_version": None,
            "snapshot_version": "snap-1",
            "batch_id": "too-large",
            "records": records,
            "policy": adjusted,
        },
    )
    assert response.status_code == 422
    body = response.json()
    assert body["code"] == "E_BATCH_BUDGET_EXCEEDED"
    assert body["details"]["batch_max_bytes"] == 10
    commits = client.get("/api/v1/commits").json()
    assert commits["current_version"] is None
    assert commits["commits"] == []


def test_replay_timeout_is_a_separate_business_code_on_live_server(live_server, records, policy):
    base_url, client = live_server
    response = client.post(
        f"{base_url}/api/v1/snapshots",
        json={
            "base_version": None,
            "snapshot_version": "snap-1",
            "batch_id": "base",
            "records": records,
            "policy": policy,
        },
    )
    assert response.status_code == 201, response.text
    version = response.json()["commit"]["version"]
    first_batch = response.json()["batch_summary"]
    assert first_batch["batch_count"] > 0
    batches = client.get(f"{base_url}/api/v1/batches").json()["batches"]["batches"]
    batch_id = batches[0]["batch_id"]
    timed = client.post(
        f"{base_url}/api/v1/replays",
        json={
            "source_version": version,
            "batch_id": batch_id,
            "timeout_ms": 80,
            "fault_mode": "hang",
            "hang_ms": 1500,
            "raise_on_timeout": True,
        },
    )
    assert timed.status_code == 504, timed.text
    body = timed.json()
    assert body["code"] == "E_REPLAY_TIMEOUT"
    assert body["details"]["timeout_count"] > 0
    assert body["details"]["failure_count"] >= body["details"]["timeout_count"]
    result_version = body["details"]["result_version"]
    bias = client.get(f"{base_url}/api/v1/bias?version={result_version}").json()["bias"]
    assert bias["attribution"]["separated"] is True
    assert bias["attribution"]["timeout_component"]["count"] == body["details"]["timeout_count"]
    assert bias["attribution"]["replay_failure_component"]["classification"] == "replay_failure"
    assert bias["attribution"]["sampling_component"]["classification"] == "sampling"


def test_normal_replay_materializes_immutable_verdict_version(live_server, records, policy):
    base_url, client = live_server
    response = client.post(
        f"{base_url}/api/v1/snapshots",
        json={
            "base_version": None,
            "snapshot_version": "snap-1",
            "batch_id": "base",
            "records": records,
            "policy": policy,
        },
    )
    assert response.status_code == 201
    source = response.json()
    replay = client.post(
        f"{base_url}/api/v1/replays",
        json={"source_version": source["commit"]["version"], "fault_mode": "mirror", "timeout_ms": 1000},
    )
    assert replay.status_code == 201, replay.text
    result = replay.json()
    assert result["result_version"] == source["commit"]["version"] + 1
    assert result["metrics"]["timeout_count"] == 0
    assert result["metrics"]["attempted"] == source["batch_summary"]["operation_count"]
    assert result["bias"]["attribution"]["replay_failure_component"]["failure_count"] == result["metrics"]["failure_count"]
    assert result["bias"]["sampling_bias"]["classification"] == "sampling"
    old = client.get(f"{base_url}/api/v1/bias?version={source['commit']['version']}").json()["bias"]
    assert old["replay"] is None

    batches = client.get(f"{base_url}/api/v1/batches?version={result['result_version']}").json()["batches"]["batches"]
    forced_error = client.post(
        f"{base_url}/api/v1/replays",
        json={
            "source_version": result["result_version"],
            "batch_id": batches[0]["batch_id"],
            "fault_mode": "status",
            "fault_status": 503,
            "timeout_ms": 1000,
        },
    )
    assert forced_error.status_code == 201, forced_error.text
    forced_metrics = forced_error.json()["metrics"]
    assert forced_metrics["status_distribution"].get("503") == forced_metrics["attempted"]
    assert forced_metrics["success_count"] == 0
