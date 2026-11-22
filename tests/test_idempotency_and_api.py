from __future__ import annotations

import threading
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from gitops_convergence.api import create_app
from gitops_convergence.engine import Engine, EngineConfig
from gitops_convergence.errors import BusinessError

from conftest import app, increment, snapshot


def test_convergence_trigger_is_idempotent_and_conflicts_on_content(engine: Engine) -> None:
    engine.submit_snapshot(snapshot([app("a", desired="v2", effective="v1")]))
    first = engine.trigger_convergence(1, {"mode": "auto"})
    second = engine.trigger_convergence(1, {"mode": "auto"})
    assert first["execution_count"] == 1
    assert second["execution_count"] == 1
    assert second["reused"] is True
    assert second["batches"] == first["batches"]
    with pytest.raises(BusinessError) as captured:
        engine.trigger_convergence(1, {"mode": "full"})
    assert captured.value.code == "E_CONFLICTING_TRIGGER"


def test_concurrent_same_trigger_executes_once(tmp_path: Path) -> None:
    engine = Engine(tmp_path / "trigger-race.db")
    engine.submit_snapshot(snapshot([app("a", desired="v2", effective="v1")]))
    barrier = threading.Barrier(3)
    results: list[dict] = []
    errors: list[BaseException] = []

    def worker() -> None:
        barrier.wait()
        try:
            results.append(engine.trigger_convergence(1, {"mode": "auto"}))
        except BaseException as exc:  # pragma: no cover - failure is asserted below
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(2)]
    for thread in threads:
        thread.start()
    barrier.wait()
    for thread in threads:
        thread.join(timeout=20)
    assert not errors
    assert len(results) == 2
    assert sorted(result["reused"] for result in results) == [False, True]
    assert {result["execution_count"] for result in results} == {1}


def test_http_business_errors_and_end_to_end(tmp_path: Path) -> None:
    application = create_app(
        tmp_path / "http.db",
        EngineConfig(switch_threshold=100, max_batch_replicas=100),
    )
    with TestClient(application) as client:
        invalid = client.post(
            "/v1/snapshots",
            json=snapshot([app("a", dependencies=["missing"])]),
        )
        assert invalid.status_code == 422
        assert invalid.json()["code"] == "E_SPEC_INVALID"

        created = client.post(
            "/v1/snapshots",
            json=snapshot(
                [
                    app("a", desired="v2", effective="v1"),
                    app("b", desired="v1", effective="v1", dependencies=["a"]),
                ]
            ),
        )
        assert created.status_code == 201
        trigger = client.post("/v1/snapshots/1/convergence", json={})
        assert trigger.status_code == 200
        assert trigger.json()["final_state_counts"] == {"CONVERGED": 2}
        reused = client.post("/v1/snapshots/1/convergence", json={})
        assert reused.json()["reused"] is True
        conflict = client.post("/v1/snapshots/1/convergence", json={"mode": "full"})
        assert conflict.status_code == 409
        assert conflict.json()["code"] == "E_CONFLICTING_TRIGGER"

        stale = client.post(
            "/v1/snapshots/999/increments",
            json=increment(
                version=1000,
                base_version=999,
                changes=[{"app_id": "a", "desired_version": "v3"}],
            ),
        )
        assert stale.status_code == 409
        assert stale.json()["code"] == "E_STALE_BASE"
        plan = client.get("/v1/snapshots/1/plan").json()
        assert plan["status"] == "ACTIVE"
        assert plan["batches"][0]["apps"][0]["state"] == "CONVERGED"
