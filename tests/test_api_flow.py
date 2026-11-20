from __future__ import annotations

import copy
import asyncio
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient

from capacity_workbench.api import create_app
from capacity_workbench.db import Store
from capacity_workbench.service import WorkbenchService


def _create(client: TestClient, plan: dict) -> dict:
    response = client.post("/plans", json=plan)
    assert response.status_code == 201, response.text
    return response.json()


def test_invalid_plan_is_not_persisted(db_path: Path, base_plan: dict) -> None:
    invalid = copy.deepcopy(base_plan)
    invalid["stages"][1]["concurrency"] = 1
    with TestClient(create_app(db_path)) as client:
        response = client.post("/plans", json=invalid)
        assert response.status_code == 422
        assert response.json()["error"]["code"] == "E_PLAN_INVALID"
        assert client.get("/plans").json() == {"plans": []}


def test_complete_sequence_and_immutability(db_path: Path, base_plan: dict) -> None:
    with TestClient(create_app(db_path)) as client:
        created = _create(client, base_plan)
        plan_id = created["plan"]["id"]
        response = client.post(f"/plans/{plan_id}/run")
        assert response.status_code == 200, response.text
        report = response.json()
        assert report["plan"]["status"] == "COMPLETED"
        assert report["conclusion"]["outcome"] == "SEQUENCE_EXHAUSTED"
        assert [row["status"] for row in report["rounds"]] == [
            "CONVERGED",
            "CONVERGED",
        ]
        assert all(row["judgment"]["passed"] for row in report["rounds"])
        first_round = report["rounds"][0]
        assert first_round["aggregate"]["sample_size"] == 100
        assert first_round["aggregate"]["throughput_rps"] == 100.0
        assert first_round["judgment"]["percentile_method"] == "nearest_rank"

        # Re-triggering a converged stage reuses the same terminal round.
        repeated = client.post(f"/plans/{plan_id}/rounds/0/execute")
        assert repeated.status_code == 200
        assert repeated.json()["id"] == first_round["id"]

        # A completed plan cannot acquire a new conclusion.
        repeated_run = client.post(f"/plans/{plan_id}/run")
        assert repeated_run.status_code == 200
        assert repeated_run.json()["conclusion"] == report["conclusion"]

    import sqlite3

    with sqlite3.connect(db_path) as connection:
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "UPDATE judgments SET passed = 0 WHERE round_id = ?",
                (first_round["id"],),
            )


def test_fallback_conclusion_uses_last_passed_capacity(
    db_path: Path, base_plan: dict
) -> None:
    plan = copy.deepcopy(base_plan)
    plan["profile"] = "knee"
    plan["seed"] = 20261121
    plan["stages"] = [
        {"concurrency": 2, "duration_seconds": 1},
        {"concurrency": 4, "duration_seconds": 1},
        {"concurrency": 8, "duration_seconds": 1},
        {"concurrency": 16, "duration_seconds": 1},
    ]
    plan["thresholds"]["p99_ms"] = 65.0
    plan["decision_rule"]["on_reject"] = "fallback"
    with TestClient(create_app(db_path)) as client:
        created = _create(client, plan)
        plan_id = created["plan"]["id"]
        report = client.post(f"/plans/{plan_id}/run").json()
        assert report["conclusion"]["outcome"] == "KNEE_FOUND"
        assert report["conclusion"]["knee"]["rejected_concurrency"] == 8
        assert report["conclusion"]["capacity"]["concurrency"] == 4
        assert report["conclusion"]["capacity"]["stage_index"] == 1
        assert len(report["rounds"]) == 3
        assert report["rounds"][-1]["judgment"]["passed"] is False
        assert report["rounds"][-1]["judgment"]["triggered"][0]["threshold"] == "p99_ms"


def test_step_down_retry_has_a_distinct_round_and_finds_capacity(
    db_path: Path, base_plan: dict
) -> None:
    plan = copy.deepcopy(base_plan)
    plan["profile"] = "knee"
    plan["seed"] = 20261123
    plan["stages"] = [
        {"concurrency": 2, "duration_seconds": 1},
        {"concurrency": 8, "duration_seconds": 1},
    ]
    plan["thresholds"]["p99_ms"] = 65.0
    plan["decision_rule"] = {
        "on_reject": "step_down_retry",
        "max_retries": 1,
        "step_down_factor": 0.75,
    }
    with TestClient(create_app(db_path)) as client:
        created = _create(client, plan)
        plan_id = created["plan"]["id"]
        report = client.post(f"/plans/{plan_id}/run").json()
        conclusion = report["conclusion"]
        assert conclusion["outcome"] == "KNEE_FOUND"
        assert conclusion["knee"]["rejected_concurrency"] == 8
        assert conclusion["capacity"]["concurrency"] == 6
        assert conclusion["capacity"]["basis"] == "step_down_retry_pass"
        retry = [row for row in report["rounds"] if row["origin"] == "step_down_retry"]
        assert len(retry) == 1
        assert retry[0]["attempt"] == 2
        assert retry[0]["round_key"] == "stage-1-r2"
        assert retry[0]["retry_of_round_id"] == report["rounds"][1]["id"]


def test_duplicate_stage_is_idempotent_and_concurrency_is_bounded(
    db_path: Path, base_plan: dict
) -> None:
    plan = copy.deepcopy(base_plan)
    plan["stages"] = [{"concurrency": 4, "duration_seconds": 1}]
    with TestClient(create_app(db_path)) as client:
        created = _create(client, plan)
        plan_id = created["plan"]["id"]
        with ThreadPoolExecutor(max_workers=2) as executor:
            first = executor.submit(
                client.post, f"/plans/{plan_id}/rounds/0/execute"
            )
            second = executor.submit(
                client.post, f"/plans/{plan_id}/rounds/0/execute"
            )
            responses = [first.result(), second.result()]
        assert [response.status_code for response in responses] == [200, 200]
        assert responses[0].json()["id"] == responses[1].json()["id"]
        rounds = client.get(f"/plans/{plan_id}/rounds").json()["rounds"]
        assert len(rounds) == 1
        round_id = rounds[0]["id"]
        target_base = plan["target_url"].rsplit("/sample", 1)[0]
        stats = httpx.get(f"{target_base}/__stats/{round_id}").json()
        assert stats["requests"] == 400
        assert stats["max_active"] <= 4


def test_second_stage_start_is_rejected_while_sampling(
    db_path: Path, base_plan: dict
) -> None:
    with TestClient(create_app(db_path)) as client:
        created = _create(client, base_plan)
        plan_id = created["plan"]["id"]
        holder: dict = {}

        def execute_first() -> None:
            holder["response"] = client.post(
                f"/plans/{plan_id}/rounds/0/execute"
            )

        thread = threading.Thread(target=execute_first)
        thread.start()
        for _ in range(100):
            snapshot = client.get(f"/plans/{plan_id}/snapshot").json()
            if snapshot["rounds"] and snapshot["rounds"][0]["status"] == "SAMPLING":
                break
            time.sleep(0.01)
        assert snapshot["rounds"][0]["status"] == "SAMPLING"
        # During sampling no aggregate/judgment is visible.
        active_detail = client.get(
            f"/plans/{plan_id}/rounds/0"
        ).json()
        assert active_detail["aggregate"] is None
        assert active_detail["judgment"] is None
        conflict = client.post(f"/plans/{plan_id}/rounds/1/execute")
        assert conflict.status_code == 409
        assert conflict.json()["error"]["code"] == "E_ROUND_ACTIVE"
        thread.join(timeout=10)
        assert holder["response"].status_code == 200


def test_insufficient_samples_aborts_round(db_path: Path, base_plan: dict) -> None:
    plan = copy.deepcopy(base_plan)
    plan["stages"] = [{"concurrency": 1, "duration_seconds": 1}]
    plan["sampling"]["min_samples_for_percentiles"] = 200
    with TestClient(create_app(db_path)) as client:
        created = _create(client, plan)
        plan_id = created["plan"]["id"]
        response = client.post(f"/plans/{plan_id}/rounds/0/execute")
        assert response.status_code == 422
        assert response.json()["error"]["code"] == "E_INSUFFICIENT_SAMPLES"
        rounds = client.get(f"/plans/{plan_id}/rounds").json()["rounds"]
        assert rounds[0]["status"] == "ABORTED"
        assert rounds[0]["aggregate"] is None
        assert rounds[0]["judgment"] is None


def test_duplicate_stage_does_not_rerun_after_aborted_result(
    db_path: Path, base_plan: dict
) -> None:
    plan = copy.deepcopy(base_plan)
    plan["stages"] = [{"concurrency": 1, "duration_seconds": 1}]
    plan["sampling"]["min_samples_for_percentiles"] = 200
    with TestClient(create_app(db_path)) as client:
        created = _create(client, plan)
        plan_id = created["plan"]["id"]
        with ThreadPoolExecutor(max_workers=2) as executor:
            first = executor.submit(
                client.post, f"/plans/{plan_id}/rounds/0/execute"
            )
            second = executor.submit(
                client.post, f"/plans/{plan_id}/rounds/0/execute"
            )
            responses = [first.result(), second.result()]
        rounds = client.get(f"/plans/{plan_id}/rounds").json()["rounds"]
        assert len(rounds) == 1
        assert rounds[0]["status"] == "ABORTED"
        assert sorted(response.status_code for response in responses) == [200, 422]


def test_timeout_counts_as_error_and_closes_request(
    db_path: Path, base_plan: dict
) -> None:
    plan = copy.deepcopy(base_plan)
    plan["stages"] = [{"concurrency": 2, "duration_seconds": 1}]
    plan["profile"] = "timeout_mode"
    plan["seed"] = 777
    plan["request_timeout_ms"] = 30
    plan["thresholds"]["p99_ms"] = 1000
    plan["thresholds"]["error_rate"] = 1.0
    with TestClient(create_app(db_path)) as client:
        created = _create(client, plan)
        plan_id = created["plan"]["id"]
        response = client.post(f"/plans/{plan_id}/rounds/0/execute")
        assert response.status_code == 200, response.text
        detail = response.json()
        assert detail["aggregate"]["timeout_count"] > 0
        assert detail["aggregate"]["error_count"] >= detail["aggregate"]["timeout_count"]
        assert detail["aggregate"]["sample_size"] == 200
        target_base = plan["target_url"].rsplit("/sample", 1)[0]
        stats = httpx.get(f"{target_base}/__stats/{detail['id']}").json()
        assert stats["timeouts"] > 0
        assert stats["client_disconnects"] > 0


def test_resume_after_crash_between_retry_result_and_plan_finalization(
    db_path: Path, base_plan: dict, monkeypatch
) -> None:
    store = Store(db_path)
    store.init_schema()
    service = WorkbenchService(store)
    plan_payload = copy.deepcopy(base_plan)
    plan_payload["profile"] = "knee"
    plan_payload["seed"] = 20261124
    plan_payload["stages"] = [
        {"concurrency": 2, "duration_seconds": 1},
        {"concurrency": 8, "duration_seconds": 1},
    ]
    plan_payload["thresholds"]["p99_ms"] = 65.0
    plan_payload["decision_rule"] = {
        "on_reject": "step_down_retry",
        "max_retries": 1,
        "step_down_factor": 0.75,
    }
    plan = store.insert_plan(plan_payload)
    original_finalize = store.finalize_plan
    failure = {"raised": False}

    def flaky_finalize(*args, **kwargs):
        if kwargs.get("action") == "step_down_retry_passed" and not failure["raised"]:
            failure["raised"] = True
            raise RuntimeError("simulated kill before plan finalization")
        return original_finalize(*args, **kwargs)

    monkeypatch.setattr(store, "finalize_plan", flaky_finalize)

    async def scenario() -> dict:
        with pytest.raises(RuntimeError):
            await service.run_plan(plan.id)
        interrupted = store.get_plan(plan.id)
        assert interrupted.status == "RUNNING"
        assert interrupted.conclusion is None
        return await service.run_plan(plan.id)

    report = asyncio.run(scenario())
    assert report["conclusion"]["outcome"] == "KNEE_FOUND"
    assert report["conclusion"]["capacity"]["concurrency"] == 6
    retries = [row for row in report["rounds"] if row["origin"] == "step_down_retry"]
    assert len(retries) == 1
    assert retries[0]["attempt"] == 2


def test_resume_does_not_exceed_retry_budget_after_finalization_crash(
    db_path: Path, base_plan: dict, monkeypatch
) -> None:
    store = Store(db_path)
    store.init_schema()
    service = WorkbenchService(store)
    plan_payload = copy.deepcopy(base_plan)
    plan_payload["profile"] = "knee"
    plan_payload["seed"] = 20261125
    plan_payload["stages"] = [
        {"concurrency": 2, "duration_seconds": 1},
        {"concurrency": 8, "duration_seconds": 1},
    ]
    plan_payload["thresholds"]["p99_ms"] = 35.0
    plan_payload["decision_rule"] = {
        "on_reject": "step_down_retry",
        "max_retries": 1,
        "step_down_factor": 0.75,
    }
    plan = store.insert_plan(plan_payload)
    original_finalize = store.finalize_plan
    failure = {"raised": False}

    def flaky_finalize(*args, **kwargs):
        if kwargs.get("action") == "step_down_exhausted" and not failure["raised"]:
            failure["raised"] = True
            raise RuntimeError("simulated kill before plan finalization")
        return original_finalize(*args, **kwargs)

    monkeypatch.setattr(store, "finalize_plan", flaky_finalize)

    async def scenario() -> dict:
        with pytest.raises(RuntimeError):
            await service.run_plan(plan.id)
        return await service.run_plan(plan.id)

    report = asyncio.run(scenario())
    retries = [row for row in report["rounds"] if row["origin"] == "step_down_retry"]
    assert len(retries) == 1
    assert report["conclusion"]["capacity"]["concurrency"] == 2

