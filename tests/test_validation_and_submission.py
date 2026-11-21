from __future__ import annotations

import copy

from app.models import FullSnapshotInput


def test_invalid_spec_rejects_entire_snapshot(client, sample):
    duplicate = sample("snapshot_v1")
    duplicate["environments"][0]["units"].append(
        copy.deepcopy(duplicate["environments"][0]["units"][0])
    )
    response = client.post("/api/commits/full", json=duplicate)
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "E_SPEC_INVALID"

    bad_digest = sample("snapshot_v1")
    bad_digest["commit_id"] = "bad-digest"
    bad_digest["environments"][0]["units"][0]["image_digest"] = "sha256:not-a-digest"
    response = client.post("/api/commits/full", json=bad_digest)
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "E_SPEC_INVALID"

    missing_dependency = sample("snapshot_v1")
    missing_dependency["commit_id"] = "missing-dependency"
    missing_dependency["environments"][0]["units"][0]["dependencies"] = ["does-not-exist"]
    response = client.post("/api/commits/full", json=missing_dependency)
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "E_SPEC_INVALID"

    health = client.get("/api/health").json()["data"]
    assert health["snapshot_version"] == 0
    empty = client.get("/api/environments").json()["data"]
    assert empty == {"snapshot_version": 0, "environments": []}
    empty_diagnostics = client.get("/api/diagnostics").json()["data"]
    assert empty_diagnostics["snapshot_version"] == 0
    assert empty_diagnostics["items"] == []


def test_full_snapshot_version_replacement_and_idempotency(client, sample):
    first = client.post("/api/commits/full", json=sample("snapshot_v1"))
    assert first.status_code == 201
    assert first.json()["data"]["snapshot_version"] == 1

    replay = client.post("/api/commits/full", json=sample("snapshot_v1"))
    assert replay.status_code == 201
    assert replay.json()["data"]["snapshot_version"] == 1
    assert replay.json()["data"]["idempotent_replay"] is True

    conflicting = sample("snapshot_v1")
    conflicting["environments"][0]["version_lag_tolerance"] = 9
    response = client.post("/api/commits/full", json=conflicting)
    assert response.status_code == 409
    assert response.json()["error"]["code"] == "E_CONFLICTING_COMMIT"
    assert client.get("/api/environments").json()["data"]["snapshot_version"] == 1

    stale_version = sample("snapshot_v1")
    stale_version["commit_id"] = "new-id-same-version"
    response = client.post("/api/commits/full", json=stale_version)
    assert response.status_code == 409
    assert response.json()["error"]["code"] == "E_STALE_BASE"

    second = client.post("/api/commits/full", json=sample("snapshot_v2"))
    assert second.status_code == 201
    assert second.json()["data"]["snapshot_version"] == 2
    assert all(
        environment["env_version"] == 2
        for environment in client.get("/api/environments").json()["data"]["environments"]
    )


def test_bound_versions_and_retired_error(client, sample):
    client.post("/api/commits/full", json=sample("snapshot_v1"))
    client.post("/api/commits/full", json=sample("snapshot_v2"))
    client.post("/api/commits/incremental", json=sample("increment_image"))

    v1 = client.get("/api/environments/prod/bundle", params={"version": 1}).json()["data"]
    v2 = client.get("/api/environments/prod/bundle", params={"version": 2}).json()["data"]
    v3 = client.get("/api/environments/prod/bundle", params={"version": 3}).json()["data"]
    assert (v1["snapshot_version"], v2["snapshot_version"], v3["snapshot_version"]) == (1, 2, 3)
    assert v1["state_hash"] != v2["state_hash"]
    assert v2["plan"] != v3["plan"]

    retired = client.get("/api/environments/prod/bundle", params={"version": 99})
    assert retired.status_code == 410
    assert retired.json()["error"]["code"] == "E_VERSION_RETIRED"

    missing_env = client.get("/api/environments/qa/bundle", params={"version": 3})
    assert missing_env.status_code == 404
    assert missing_env.json()["error"]["code"] == "E_ENV_NOT_FOUND"

    by_application = client.get(
        "/api/environments/prod/applications/inventory", params={"version": 3}
    ).json()["data"]
    assert by_application["application"] == "inventory"
    assert by_application["plan"]["service"] == "inventory"
    assert by_application["dependents"] == ["gateway", "orders"]
    assert by_application["topology"]["complete"] is True


def test_model_rejects_second_baseline(sample):
    payload = sample("snapshot_v1")
    payload["environments"][0]["is_baseline"] = True
    try:
        FullSnapshotInput.model_validate(payload)
    except Exception as exc:  # pydantic ValidationError
        assert "baseline" in str(exc)
    else:
        raise AssertionError("two baselines must be rejected")
