from __future__ import annotations

import copy

from app.domain import SEVERITY_RANK

from conftest import submit_increment, submit_v1, submit_v2


def seed_to_v3(store):
    submit_v1(store)
    submit_v2(store)
    return submit_increment(store)


def test_increment_cascade_rebuilds_downstream_and_references(store):
    submit_v1(store)
    submit_v2(store)
    old_bundle = store.environment_bundle("prod", version=2)
    result = submit_increment(store)

    assert result["snapshot_version"] == 3
    assert result["affected"]["changed_services"] == ["inventory"]
    assert result["affected"]["dependent_services"] == ["gateway", "orders"]
    assert result["affected"]["affected_services"] == ["gateway", "inventory", "orders"]

    event_pairs = {
        (event["subject_key"], event["artifact_key"], event["reason"])
        for event in result["cascade_trace"]
        if event["artifact_type"] == "deployment_plan"
    }
    assert ("inventory", "service:inventory", "direct_change:image_digest") in event_pairs
    assert ("orders", "service:orders", "downstream_dependency") in event_pairs
    assert ("gateway", "service:gateway", "downstream_dependency") in event_pairs
    assert any(
        event["environment"] == "staging"
        and event["artifact_type"] == "consistency_score"
        for event in result["cascade_trace"]
    )

    new_bundle = store.environment_bundle("prod", version=3)
    old_gateway = next(item for item in old_bundle["plan"] if item["service"] == "gateway")
    new_gateway = next(item for item in new_bundle["plan"] if item["service"] == "gateway")
    old_inventory = next(item for item in old_bundle["plan"] if item["service"] == "inventory")
    new_inventory = next(item for item in new_bundle["plan"] if item["service"] == "inventory")
    assert old_inventory["image_digest"] != new_inventory["image_digest"]
    assert (
        old_gateway["dependency_fingerprints"]["inventory"]
        != new_gateway["dependency_fingerprints"]["inventory"]
    )
    assert (
        old_bundle["artifact_hashes"]["deployment_plan:service:gateway"]
        != new_bundle["artifact_hashes"]["deployment_plan:service:gateway"]
    )
    assert new_bundle["topology"]["complete"] is True
    assert new_bundle["expected_state"]["service_count"] == 7

    inventory_refs = store.references("prod", version=3, subject="inventory")["references"]
    config_refs = [
        reference
        for reference in inventory_refs
        if reference["subject_type"] == "config_key"
        and reference["subject_key"] == "inventory:CACHE_TTL_SECONDS"
    ]
    assert {reference["artifact_type"] for reference in config_refs} == {
        "deployment_plan",
        "topology",
        "expected_state",
        "consistency_score",
    }
    unit_refs = [
        reference
        for reference in inventory_refs
        if reference["subject_type"] == "unit" and reference["subject_key"] == "inventory"
    ]
    assert len(unit_refs) == 4


def test_diagnostics_are_stable_sorted_and_do_not_change_plan(client, sample):
    for name, endpoint in (
        ("snapshot_v1", "/api/commits/full"),
        ("snapshot_v2", "/api/commits/full"),
        ("increment_image", "/api/commits/incremental"),
    ):
        assert client.post(endpoint, json=sample(name)).status_code == 201

    before = client.get("/api/environments/prod/bundle", params={"version": 3}).json()["data"]
    diagnostics = client.post("/api/diagnostics/run", params={"version": 3}).json()["data"]
    after = client.get("/api/environments/prod/bundle", params={"version": 3}).json()["data"]

    assert diagnostics["read_only"] is True
    assert before["artifact_hashes"] == after["artifact_hashes"]
    assert before["plan"] == after["plan"]
    assert before["topology"] == after["topology"]
    assert diagnostics["counts"]["critical"] >= 1
    assert diagnostics["environment_lag_markers"][0]["environment"] == "staging"
    assert diagnostics["environment_lag_markers"][0]["behind_by"] == 1
    assert diagnostics["environment_lag_markers"][0]["tolerance"] == 0

    order = [
        (
            SEVERITY_RANK[item["severity"]],
            item["environment"],
            item["service"] or "",
            item["diff_type"],
            item["config_key"] or "",
        )
        for item in diagnostics["items"]
    ]
    assert order == sorted(order)
    for item in diagnostics["items"]:
        assert {"environment", "service", "diff_type", "left", "right"} <= set(item)
        assert item["left"]["environment"] == "prod"
        assert item["right"]["environment"] == item["environment"]

    assert client.get("/api/environments").json()["data"]["snapshot_version"] == 3


def test_cycle_is_rejected_with_partial_topology_and_no_state_change(client, sample):
    for name, endpoint in (
        ("snapshot_v1", "/api/commits/full"),
        ("snapshot_v2", "/api/commits/full"),
        ("increment_image", "/api/commits/incremental"),
    ):
        assert client.post(endpoint, json=sample(name)).status_code == 201
    before = client.get("/api/environments/prod/bundle").json()["data"]

    response = client.post("/api/commits/incremental", json=sample("cycle_increment"))
    assert response.status_code == 409
    error = response.json()["error"]
    assert error["code"] == "E_DEPENDENCY_CYCLE"
    assert error["details"]["cycle"] == ["auth", "identity-db"]
    assert error["details"]["partial_layers"] == [
        ["ledger"],
        ["inventory", "payments"],
        ["orders"],
    ]
    assert error["details"]["blocked_units"] == ["auth", "gateway", "identity-db"]

    after = client.get("/api/environments/prod/bundle").json()["data"]
    assert after == before
    assert client.get("/api/environments").json()["data"]["snapshot_version"] == 3
    assert client.get("/api/commits/demo-inc-dev-cycle").status_code == 404


def test_cycle_topology_isolated_from_unaffected_prefix(store):
    from app.domain import compute_topology

    units = {
        "a": {"dependencies": ["b"]},
        "b": {"dependencies": ["a"]},
        "c": {"dependencies": []},
        "d": {"dependencies": ["a"]},
        "e": {"dependencies": ["c"]},
    }
    topology = compute_topology(units)
    assert topology.layers == [["c"], ["e"]]
    assert topology.blocked_units == ["a", "b", "d"]
    assert topology.cycles == [["a", "b"]]


def test_config_key_change_cascades_to_plan_topology_and_scores(store, sample):
    submit_v1(store)
    submit_v2(store)
    old_bundle = store.environment_bundle("prod", version=2)
    old_gateway = next(item for item in old_bundle["plan"] if item["service"] == "gateway")
    payload = copy.deepcopy(sample("increment_image"))
    payload["commit_id"] = "config-change"
    payload["units"] = [
        {
            "name": "gateway",
            "image_digest": old_gateway["image_digest"],
            "config": {"LOG_LEVEL": "debug", "UPSTREAM_TIMEOUT_MS": 2500},
            "dependencies": ["auth", "orders"],
            "expected_version": "1.8.0",
        }
    ]
    result = submit_increment(store, payload)
    assert result["affected"]["changed_services"] == ["gateway"]
    assert result["affected"]["dependent_services"] == []
    assert result["affected"]["field_changes"]["gateway"] == ["config"]
    assert any(
        event["subject_type"] == "config_key"
        and event["subject_key"] == "gateway:LOG_LEVEL"
        and event["reason"] == "config_key_changed"
        for event in result["cascade_trace"]
    )
    bundle = store.environment_bundle("prod", version=3)
    gateway = next(item for item in bundle["plan"] if item["service"] == "gateway")
    assert gateway["config"]["LOG_LEVEL"] == "debug"
    assert bundle["artifact_hashes"]["topology:environment"]
    assert {
        event["environment"]
        for event in result["cascade_trace"]
        if event["artifact_type"] == "consistency_score"
    } == {"dev", "prod", "staging"}
    assert store.references("prod", version=3, subject="gateway")["references"]


def test_dependency_change_cascades_to_old_and_new_reverse_closure(store, sample):
    submit_v1(store)
    submit_v2(store)
    old_bundle = store.environment_bundle("prod", version=2)
    payload = copy.deepcopy(sample("increment_image"))
    payload["commit_id"] = "dependency-change"
    payload["units"] = [
        {
            "name": "orders",
            "image_digest": "sha256:" + "d" * 64,
            "config": {"BATCH_SIZE": 100, "LOG_LEVEL": "info"},
            "dependencies": ["auth", "inventory", "payments"],
            "expected_version": "2.3.0",
        }
    ]
    result = submit_increment(store, payload)
    assert result["affected"]["changed_services"] == ["orders"]
    assert result["affected"]["dependent_services"] == ["gateway"]
    assert result["affected"]["field_changes"]["orders"] == ["image_digest", "dependencies"]

    new_bundle = store.environment_bundle("prod", version=3)
    old_gateway = next(item for item in old_bundle["plan"] if item["service"] == "gateway")
    new_gateway = next(item for item in new_bundle["plan"] if item["service"] == "gateway")
    assert old_gateway["dependency_fingerprints"]["orders"] != new_gateway[
        "dependency_fingerprints"
    ]["orders"]
    assert new_gateway["dependency_fingerprints"]["auth"]
    assert any(
        event["subject_key"] == "gateway"
        and event["reason"] == "downstream_dependency"
        for event in result["cascade_trace"]
    )
