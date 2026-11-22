from __future__ import annotations

import json
from pathlib import Path

import pytest

from feature_workbench import Workbench
from feature_workbench.dataset import build_shard_specs, load_manifest
from feature_workbench.errors import WorkbenchError
from feature_workbench.generator import generate_demo
from feature_workbench.util import file_sha256

from conftest import read_manifest, write_manifest


def test_generated_sample_contract_and_determinism(tmp_path: Path, demo_source: Path) -> None:
    manifest = read_manifest(demo_source)
    assert set(manifest["tiers"]) == {"small", "medium"}
    assert manifest["tiers"]["small"]["rows"] == 600
    assert manifest["tiers"]["small"]["columns"] == 10
    assert manifest["tiers"]["medium"]["rows"] == 6000
    assert manifest["tiers"]["medium"]["columns"] == 40
    assert len(manifest["files"]) == 5
    for tier in ("small", "medium"):
        assert len(manifest["tiers"][tier]["categorical_columns"]) >= 4
    assert any(
        "copy" in name
        for entry in manifest["files"]
        for name in entry["column_names"]
    )

    second = tmp_path / "second" / "data"
    generate_demo(second)
    first_data = demo_source / "data"
    first_files = sorted(path.name for path in first_data.iterdir() if path.is_file())
    second_files = sorted(path.name for path in second.iterdir() if path.is_file())
    assert first_files == second_files
    for name in first_files:
        assert file_sha256(first_data / name) == file_sha256(second / name), name


def test_type_inference_outputs_domains_and_missing_statistics(workbench: Workbench) -> None:
    result = workbench.run(until_stage="type_inference", worker_mode="sequential")
    assert result["status"] == "partial"
    context = workbench._load_context()
    record = workbench.store.latest_record(
        stage="type_inference",
        stage_version=context["stage_versions"]["type_inference"],
        scope="ALL",
    )
    assert record is not None
    schema = workbench._load_type_artifact(record)
    small = schema["tiers"]["small"]
    assert small["rows"] == 600
    assert small["label_column"]["name"] == "label"
    city = small["source_columns"][3]
    assert city["dtype"] == "categorical"
    assert city["observed_cardinality"] == 8
    assert city["missing_count"] > 0
    income = small["source_columns"][7]
    assert income["dtype"] == "numeric"
    assert income["missing_count"] > 0
    assert "domain" in income
    status = workbench.status()
    assert status["stages"]["type_inference"]["complete"] is True
    assert status["stages"]["encoding"]["complete"] is False


def test_column_declaration_conflict_has_column_and_both_values(workspace: Path) -> None:
    manifest = read_manifest(workspace)
    target = next(entry for entry in manifest["files"] if entry["file"] == "small_00.csv")
    city = next(item for item in target["categorical_columns"] if item["name"] == "city")
    city["cardinality"] = 7
    write_manifest(workspace, manifest)
    workbench = Workbench(root=workspace, state_dir=workspace / "state", export_dir=workspace / "exports")
    with pytest.raises(WorkbenchError) as raised:
        workbench.run(until_stage="type_inference", worker_mode="sequential")
    error = raised.value
    assert error.code == "E_COLUMN_MISMATCH"
    assert error.details["column"] == "city"
    assert error.details["declared"] == 7
    assert error.details["observed_cardinality"] == 8


def test_numeric_declaration_conflict_reports_observed_value(workspace: Path) -> None:
    csv_path = workspace / "data" / "small_00.csv"
    lines = csv_path.read_text(encoding="utf-8").splitlines()
    fields = lines[1].split(",")
    fields[1] = "not-a-number"
    lines[1] = ",".join(fields)
    csv_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    workbench = Workbench(root=workspace, state_dir=workspace / "state", export_dir=workspace / "exports")
    with pytest.raises(WorkbenchError) as raised:
        workbench.run(until_stage="type_inference", worker_mode="sequential")
    assert raised.value.code == "E_COLUMN_MISMATCH"
    assert raised.value.details["column"] == "amount"
    assert raised.value.details["declared"] == "numeric"
    assert "not-a-number" in raised.value.details["observed"]


def test_budget_report_exposes_admission_and_unprocessed_reasons(workspace: Path) -> None:
    from conftest import mutate_config

    mutate_config(workspace, {"budget.global_feature_evaluations": 200_000})
    workbench = Workbench(root=workspace)
    report = workbench.budget_report()
    plan = report["budget"]
    assert plan["global_used"] <= plan["global_budget"] == 200_000
    assert plan["concurrency"] <= plan["max_workers"]
    assert plan["unprocessed"]
    assert any(item["reason"] == "GLOBAL_BUDGET" for item in plan["unprocessed"])

