from __future__ import annotations

import json
from pathlib import Path

import pytest

from feature_workbench import Workbench
from feature_workbench.budget import BudgetLedger, plan_schedule
from feature_workbench.config import load_config
from feature_workbench.dataset import build_shard_specs, load_manifest
from feature_workbench.errors import WorkbenchError

from conftest import mutate_config


def test_budget_ledger_never_exceeds_global_limit() -> None:
    ledger = BudgetLedger(global_limit=10)
    ledger.charge(7, "a")
    assert ledger.remaining == 3
    with pytest.raises(WorkbenchError) as raised:
        ledger.charge(4, "b")
    assert raised.value.code == "E_BUDGET_EXCEEDED"
    assert ledger.used == 7


def test_scheduler_honours_all_three_limits(workspace: Path) -> None:
    manifest = load_manifest(workspace / "data" / "manifest.json")
    specs = build_shard_specs(manifest, workspace / "data")
    config = load_config(workspace / "data" / "config.yaml")
    config["budget"]["global_feature_evaluations"] = 400_000
    config["budget"]["per_worker_evaluations"] = 120_000
    config["budget"]["max_workers"] = 2
    plan = plan_schedule(specs, config)
    assert plan["global_used"] <= 400_000
    assert plan["concurrency"] <= 2
    for lane in plan["lanes"]:
        assert lane["used"] <= 120_000
    assert plan["unprocessed"]
    assert all(
        item["reason"] in {"GLOBAL_BUDGET", "PER_WORKER_QUOTA"}
        for item in plan["unprocessed"]
    )


def test_parallel_and_sequential_results_are_byte_identical(make_workspace) -> None:
    sequential_root = make_workspace()
    process_root = make_workspace()
    sequential = Workbench(
        root=sequential_root,
        state_dir=sequential_root / "state",
        export_dir=sequential_root / "exports",
    )
    process = Workbench(
        root=process_root,
        state_dir=process_root / "state",
        export_dir=process_root / "exports",
    )
    sequential_result = sequential.run(worker_mode="sequential")
    process_result = process.run(worker_mode="process")
    assert sequential_result["result_digest"] == process_result["result_digest"]
    assert sequential_result["tiers"].keys() == process_result["tiers"].keys()
    for tier in sequential_result["tiers"]:
        assert sequential_result["tiers"][tier]["selected"] == process_result["tiers"][tier]["selected"]
        assert (
            sequential_result["tiers"][tier]["selected_matrix_digest"]
            == process_result["tiers"][tier]["selected_matrix_digest"]
        )


def test_stop_between_shards_then_resume_matches_full_run(make_workspace) -> None:
    root = make_workspace()
    baseline = Workbench(
        root=root,
        state_dir=root / "baseline-state",
        export_dir=root / "baseline-exports",
    )
    expected = baseline.run(worker_mode="process")
    stopped = Workbench(
        root=root,
        state_dir=root / "resume-state",
        export_dir=root / "resume-exports",
    )
    first = stopped.run(worker_mode="sequential", stop_after_shards=1)
    assert first["status"] == "stopped"
    assert first["encodings"]["computed"] == 1
    assert first["checkpoints"]["last_sequence"] == 2
    resumed = stopped.run(worker_mode="process", resume=True)
    assert resumed["status"] == "complete"
    assert resumed["result_digest"] == expected["result_digest"]
    assert resumed["reused"]["encoding_records"] == 1
    assert resumed["checkpoints"]["continuous"] is True
    assert resumed["checkpoints"]["count"] == 11
    records = stopped.store.list_records()
    assert [record["sequence"] for record in records] == list(range(1, len(records) + 1))
    assert [(record["stage"], record["scope"]) for record in records] == [
        ("type_inference", "ALL"),
        ("encoding", "shard:0"),
        ("encoding", "shard:1"),
        ("encoding", "shard:2"),
        ("encoding", "shard:3"),
        ("encoding", "shard:4"),
        ("encoding", "STAGE"),
        ("selection", "tier:medium"),
        ("selection", "tier:small"),
        ("selection", "STAGE"),
        ("aggregate", "ALL"),
    ]
    assert resumed["tiers"] == expected["tiers"]


def test_worker_failure_is_structured_and_preserves_successes(
    make_workspace, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = make_workspace()
    workbench = Workbench(root=root)
    monkeypatch.setenv("FEATURE_WORKBENCH_FAIL_SHARD", "1")
    with pytest.raises(WorkbenchError) as raised:
        workbench.run(worker_mode="sequential")
    error = raised.value
    assert error.code == "E_WORKER_FAILED"
    assert error.details["successful_shards"] == [0]
    assert "successful_shards" in error.details
    assert "timeout" not in error.message.lower()
    records = workbench.store.list_records()
    encoding_records = [record for record in records if record["stage"] == "encoding"]
    assert [record["shard_id"] for record in encoding_records] == [0]
    status = workbench.status()
    assert status["stages"]["encoding"]["completed_shards"] == 1
    assert status["runtime"]["last_error"]["code"] == "E_WORKER_FAILED"


def test_reduced_budget_marks_shards_unprocessed_and_completes(
    workspace: Path,
) -> None:
    mutate_config(workspace, {"budget.global_feature_evaluations": 200_000})
    workbench = Workbench(root=workspace)
    result = workbench.run(worker_mode="sequential")
    assert result["status"] == "complete"
    assert result["budget"]["global_used"] <= 200_000
    unprocessed = result["budget"]["unprocessed"]
    assert unprocessed
    assert all(item["reason"] == "GLOBAL_BUDGET" for item in unprocessed)
    summary = workbench.read_selection_summary()
    assert summary is not None
    assert len(summary["unprocessed_shards"]) == len(unprocessed)
    medium_shards = summary["tiers"]["medium"]["shards"]
    assert len(medium_shards) == 1

