from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient

from conftest import ROOT, bootstrap, load_json
from mirror_judge.config import Settings
from mirror_judge.main import create_app


def test_record_validation_is_all_or_nothing_and_batch_idempotent(client_factory, records, increment_records, policy):
    client = client_factory()
    invalid = deepcopy(records[:2])
    invalid[1]["response_bytes"] = -1
    response = client.post(
        "/api/v1/snapshots",
        json={
            "base_version": None,
            "snapshot_version": "snap-invalid",
            "batch_id": "bad",
            "records": invalid,
            "policy": policy,
        },
    )
    assert response.status_code == 422
    assert response.json()["code"] == "E_RECORD_INVALID"
    assert client.get("/api/v1/commits").json()["commits"] == []

    state = bootstrap(client, records, policy)
    version = state["commit"]["version"]
    invalid_batch = deepcopy(increment_records[:2])
    invalid_batch[0]["duration_us"] = "slow"
    invalid_append = client.post(
        "/api/v1/samples/batches",
        json={"base_version": version, "batch_id": "inc-invalid", "records": invalid_batch},
    )
    assert invalid_append.status_code == 422
    assert invalid_append.json()["code"] == "E_RECORD_INVALID"
    assert client.get("/api/v1/commits").json()["current_version"] == version
    appended = client.post(
        "/api/v1/samples/batches",
        json={"base_version": version, "batch_id": "inc-1", "records": increment_records},
    )
    assert appended.status_code == 201, appended.text
    v2 = appended.json()["commit"]["version"]
    duplicate = client.post(
        "/api/v1/samples/batches",
        json={"base_version": 1, "batch_id": "inc-1", "records": increment_records},
    )
    assert duplicate.status_code == 200
    assert duplicate.json()["idempotent"] is True
    assert duplicate.json()["version"] == v2

    changed = deepcopy(increment_records)
    changed[0]["response_bytes"] += 1
    conflict = client.post(
        "/api/v1/samples/batches",
        json={"base_version": v2, "batch_id": "inc-1", "records": changed},
    )
    assert conflict.status_code == 409
    assert conflict.json()["code"] == "E_CONFLICTING_RECORD"
    assert client.get("/api/v1/commits").json()["current_version"] == v2
    first_records = client.get("/api/v1/records").json()["records"]
    retained = next(record for record in first_records if record["record_key"] == "inc-1:1")
    assert retained["response_bytes"] == increment_records[0]["response_bytes"]

    stale = client.post(
        "/api/v1/samples/batches",
        json={"base_version": 1, "batch_id": "inc-2", "records": increment_records[:2]},
    )
    assert stale.status_code == 409
    assert stale.json()["code"] == "E_STALE_BASE"
    assert client.get("/api/v1/commits").json()["current_version"] == v2


def test_policy_increment_cascades_and_old_version_stays_immutable(client_factory, records, policy):
    client = client_factory()
    v1 = bootstrap(client, records, policy)
    old_state = client.get(f"/api/v1/state?version={v1['commit']['version']}").json()
    response = client.post(
        "/api/v1/policies/increments",
        json={
            "base_version": v1["commit"]["version"],
            "base_policy_version": policy["policy_version"],
            "policy_version": "p2",
            "patch": {"service_weights": {"checkout-service": 0.2}},
        },
    )
    assert response.status_code == 201, response.text
    v2 = response.json()
    assert v2["commit"]["version"] == 2
    assert v2["commit"]["cascade"]["changed_rules"] == ["service_weights.checkout-service"]
    assert v2["commit"]["cascade"]["atomic_rebuild"] is True
    assert set(v2["commit"]["cascade"]["affected_artifact_types"]) == {"selection", "batches", "bias"}
    assert len(v2["commit"]["cascade"]["invalidated_previous_artifacts"]) == 3
    assert v2["selection_summary"]["artifact_id"] != old_state["selection_summary"]["artifact_id"]
    assert v2["batch_summary"]["artifact_id"] != old_state["batch_summary"]["artifact_id"]
    assert v2["bias_summary"]["artifact_id"] != old_state["bias_summary"]["artifact_id"]
    assert v2["selection_summary"]["selected_count"] != old_state["selection_summary"]["selected_count"]

    stale = client.post(
        "/api/v1/policies/increments",
        json={
            "base_version": 1,
            "base_policy_version": policy["policy_version"],
            "policy_version": "p-stale",
            "patch": {"service_weights": {"checkout-service": 0.5}},
        },
    )
    assert stale.status_code == 409
    assert stale.json()["code"] == "E_STALE_BASE"
    assert client.get("/api/v1/commits").json()["current_version"] == 2
    assert client.get(f"/api/v1/state?version=1").json()["selection_summary"]["artifact_id"] == old_state["selection_summary"]["artifact_id"]


def test_snapshot_replacement_never_mixes_versions(client_factory, records, policy):
    client = client_factory()
    v1 = bootstrap(client, records, policy)
    replacement_policy = deepcopy(policy)
    replacement_policy["policy_version"] = "p-replacement-2"
    replacement_policy["sampling_rate"] = 0.8
    replacement_records = deepcopy(records[10:35])
    response = client.post(
        "/api/v1/snapshots",
        json={
            "base_version": v1["commit"]["version"],
            "snapshot_version": "snap-2",
            "batch_id": "replacement",
            "records": replacement_records,
            "policy": replacement_policy,
        },
    )
    assert response.status_code == 201, response.text
    v2 = response.json()
    assert v2["commit"]["version"] == 2
    assert v2["commit"]["snapshot_version"] == "snap-2"
    assert v2["commit"]["record_count"] == len(replacement_records)
    assert v2["policy"]["policy_version"] == "p-replacement-2"
    assert v2["selection_summary"]["total_records"] == len(replacement_records)
    assert all(key.startswith("replacement:") for key in client.get("/api/v1/selection").json()["selection"]["selected_keys"])

    old = client.get("/api/v1/state?version=1").json()
    assert old["commit"]["snapshot_version"] == "snap-1"
    assert old["commit"]["record_count"] == len(records)
    same_version = client.post(
        "/api/v1/snapshots",
        json={
            "base_version": 2,
            "snapshot_version": "snap-2",
            "batch_id": "replacement-again",
            "records": replacement_records[:5],
            "policy": replacement_policy,
        },
    )
    assert same_version.status_code == 422
    assert same_version.json()["code"] == "E_SNAPSHOT_INVALID"


def test_concurrent_writer_and_readers_only_observe_complete_versions(tmp_path, records, policy):
    settings = Settings(tmp_path / "concurrent.sqlite", "http://127.0.0.1:1/_test/replay")
    app = create_app(settings)
    service = app.state.service
    original = service.commit_snapshot(
        base_version=None,
        snapshot_version="snap-1",
        batch_id="base",
        raw_records=records,
        raw_policy=policy,
    )
    assert original["commit"]["version"] == 1
    errors: list[str] = []
    observed_versions: set[int] = set()

    def writer() -> None:
        weights = [0.2, 0.7, 0.4, 0.9, 1.1]
        for index, weight in enumerate(weights, start=2):
            current = service.store.current_version()
            state = service.apply_policy_increment(
                base_version=current,
                base_policy_version=service.store.read_snapshot(current)["policy"]["policy_version"],
                policy_version=f"p{index}",
                patch={"service_weights": {"checkout-service": weight}},
            )
            assert state["commit"]["version"] == index

    def reader() -> None:
        for _ in range(18):
            state = service.state()
            commit = state["commit"]
            observed_versions.add(commit["version"])
            if state["selection_summary"]["artifact_id"] != state["bias_summary"]["artifact_id"]:
                pass
            if commit["record_count"] != state["selection_summary"]["total_records"]:
                errors.append(f"record count mismatch at version {commit['version']}")
            if commit["selected_count"] != state["selection_summary"]["selected_count"]:
                errors.append(f"selected count mismatch at version {commit['version']}")
            if commit["batch_count"] != state["batch_summary"]["batch_count"]:
                errors.append(f"batch count mismatch at version {commit['version']}")
            if state["bias_summary"]["online_original"]["records"] != commit["record_count"]:
                errors.append(f"bias records mismatch at version {commit['version']}")
            if commit["policy_version"] != state["policy"]["policy_version"]:
                errors.append(f"policy mismatch at version {commit['version']}")

    with ThreadPoolExecutor(max_workers=7) as executor:
        writer_future = executor.submit(writer)
        reader_futures = [executor.submit(reader) for _ in range(6)]
        writer_future.result()
        for future in reader_futures:
            future.result()
    assert not errors, errors
    assert service.store.current_version() == 6
    assert observed_versions.issubset({1, 2, 3, 4, 5, 6})


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait_health(base_url: str, timeout: float = 15.0) -> None:
    deadline = time.time() + timeout
    last_error: Exception | None = None
    while time.time() < deadline:
        try:
            response = httpx.get(f"{base_url}/health", timeout=1.0)
            if response.status_code == 200:
                return
        except Exception as exc:  # noqa: BLE001
            last_error = exc
        time.sleep(0.1)
    raise AssertionError(f"server did not become healthy: {last_error}")


def test_forced_process_termination_recovers_from_persisted_intent(tmp_path, records, policy):
    db_path = tmp_path / "crash.sqlite"
    bootstrap_client = TestClient(
        create_app(Settings(db_path, "http://127.0.0.1:1/_test/replay")), raise_server_exceptions=False
    )
    state = bootstrap(bootstrap_client, records, policy)
    bootstrap_client.close()
    assert state["commit"]["version"] == 1

    port = _free_port()
    base_url = f"http://127.0.0.1:{port}"
    env = os.environ.copy()
    env.update(
        {
            "MIRROR_DB_PATH": str(db_path),
            "MIRROR_REPLAY_TARGET": f"{base_url}/_test/replay",
            "MIRROR_ENABLE_TEST_HOOKS": "1",
            "MIRROR_CRASH_POINT": "before_commit",
        }
    )
    crash_server = subprocess.Popen(
        [sys.executable, str(ROOT / "scripts" / "run_server.py"), "--port", str(port), "--db", str(db_path), "--replay-target", f"{base_url}/_test/replay"],
        cwd=ROOT,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        _wait_health(base_url)
        with pytest.raises(httpx.HTTPError):
            httpx.post(
                f"{base_url}/api/v1/policies/increments",
                json={
                    "base_version": 1,
                    "base_policy_version": policy["policy_version"],
                    "policy_version": "p-crashed",
                    "patch": {"service_weights": {"checkout-service": 0.35}},
                },
                timeout=5.0,
            )
        return_code = crash_server.wait(timeout=10)
        assert return_code == 97
    finally:
        if crash_server.poll() is None:
            crash_server.kill()
            crash_server.wait(timeout=5)

    normal_env = os.environ.copy()
    normal_env.update(
        {
            "MIRROR_DB_PATH": str(db_path),
            "MIRROR_REPLAY_TARGET": f"{base_url}/_test/replay",
            "MIRROR_ENABLE_TEST_HOOKS": "0",
        }
    )
    recovered = subprocess.Popen(
        [sys.executable, str(ROOT / "scripts" / "run_server.py"), "--port", str(port), "--db", str(db_path), "--replay-target", f"{base_url}/_test/replay"],
        cwd=ROOT,
        env=normal_env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        _wait_health(base_url)
        commits = httpx.get(f"{base_url}/api/v1/commits", timeout=5.0).json()
        assert commits["current_version"] == 1
        report = httpx.get(f"{base_url}/api/v1/recovery/latest", timeout=5.0).json()["recovery"]
        assert report["last_complete_version"] == 1
        assert report["pending_intent_count"] == 1
        assert report["aborted_intent_ids"]
        assert report["report_source"] == "sqlite_persisted_facts_only"
    finally:
        recovered.terminate()
        try:
            recovered.wait(timeout=10)
        except subprocess.TimeoutExpired:
            recovered.kill()
            recovered.wait(timeout=5)
