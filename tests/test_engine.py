from __future__ import annotations

import copy
import json
import shutil
from pathlib import Path

import pytest

from conftest import DATA_DIR, fast_config
from linreg_workbench.canonical import atomic_write_bytes, digest, sha256_file
from linreg_workbench.checkpoints import CheckpointStore
from linreg_workbench.errors import InvalidCV, InvalidHyperparameter, NumericError
from linreg_workbench.workbench import Workbench


def test_full_pipeline_checkpoint_contract_and_result_bytes(tmp_path: Path) -> None:
    wb = Workbench(tmp_path / "one", data_dir=DATA_DIR)
    wb.create_session("s", fast_config("small"), overwrite=True)
    result = wb.start_training("s", background=False)["result"]
    status = wb.status("s")
    assert status["status"] == "complete"
    assert status["progress"] == {
        "preprocess": True,
        "folds": True,
        "solve_completed": 4,
        "solve_total": 4,
        "cv_completed": 3,
        "cv_total": 3,
    }
    records = wb.checkpoints("s")["records"]
    assert [record["sequence"] for record in records] == list(range(1, len(records) + 1))
    assert [record["stage"] for record in records] == [
        "preprocess",
        "folds",
        "solve",
        "solve",
        "solve",
        "solve",
        "aggregate",
    ]
    assert [record["role"] for record in records] == [
        "preprocess",
        "folds",
        "solve",
        "solve",
        "solve",
        "final",
        "aggregate",
    ]
    assert all(record["input_fingerprint"] for record in records)
    assert all(record["config_version"] == result["config_version"] for record in records)
    assert records[-1]["coefficient_summary"] == result["trajectory_summary"]["coefficient_summary"]
    assert status["folds"][-1]["role"] == "final"

    # A second clean workspace must emit byte-identical result and summary files.
    wb2 = Workbench(tmp_path / "two", data_dir=DATA_DIR)
    wb2.create_session("s", fast_config("small"), overwrite=True)
    result2 = wb2.start_training("s", background=False)["result"]
    run1 = wb._run_dir(wb.status("s")["active_run_id"])
    run2 = wb2._run_dir(wb2.status("s")["active_run_id"])
    assert (run1 / "artifacts" / "result.json").read_bytes() == (
        run2 / "artifacts" / "result.json"
    ).read_bytes()
    assert (run1 / "artifacts" / "result-summary.json").read_bytes() == (
        run2 / "artifacts" / "result-summary.json"
    ).read_bytes()
    assert result["result_digest"] == result2["result_digest"]


def test_invalidation_earliest_stage_for_each_input_class(tmp_path: Path) -> None:
    wb = Workbench(tmp_path / "inv", data_dir=DATA_DIR)
    wb.create_session("s", fast_config("small"), overwrite=True)
    wb.start_training("s", background=False)
    cases = [
        ({"preprocessing": {"numeric_normalization": "minmax"}}, "preprocess"),
        ({"preprocessing": {"categorical_encoding": "target"}}, "preprocess"),
        ({"cv": {"folds": 4}}, "folds"),
        ({"cv": {"seed": 999}}, "folds"),
        ({"regularization": {"strengths": [0.05]}}, "solve"),
        ({"regularization": {"l1_ratios": [0.8]}}, "solve"),
        ({"solver": {"tolerance": 1e-6}}, "solve"),
        ({"cv": {"metric": "accuracy"}}, "aggregate"),
    ]
    for index, (patch, expected) in enumerate(cases):
        if index:
            # Restore the original session config without starting another run;
            # invalidation always compares against the last completed run.
            wb.update_config("s", fast_config("small"))
        update = wb.update_config("s", patch)
        assert update["invalidation"]["earliest_stage"] == expected, (patch, update["invalidation"])
        assert update["invalidation"]["affected_stages"][0] == expected


def test_incremental_rebuild_reuses_upstream_and_never_stale_coefficients(tmp_path: Path) -> None:
    wb = Workbench(tmp_path / "incremental", data_dir=DATA_DIR)
    wb.create_session("s", fast_config("small"), overwrite=True)
    first = wb.start_training("s", background=False)["result"]
    wb.update_config("s", {"regularization": {"strengths": [0.07]}})
    second = wb.start_training("s", background=False)["result"]
    reuse = {item["stage"]: item["reused"] for item in wb.status("s")["reuse"]}
    assert reuse["preprocess"] is True
    assert reuse["folds"] is True
    assert first["result_digest"] != second["result_digest"]
    assert first["coefficients"] != second["coefficients"]


def test_stop_resume_matches_full_and_ignores_corrupt_tail(tmp_path: Path) -> None:
    config = fast_config("small")
    full_wb = Workbench(tmp_path / "full", data_dir=DATA_DIR)
    full_wb.create_session("s", config, overwrite=True)
    full = full_wb.start_training("s", background=False)["result"]

    wb = Workbench(tmp_path / "resumable", data_dir=DATA_DIR)
    wb.create_session("s", config, overwrite=True)
    stopped = wb.start_training("s", stop_after_tasks=2, background=False)
    assert stopped["status"] == "stopped"
    prefix_before = [
        {"sequence": record["sequence"], "checksum": record["checksum"]}
        for record in wb.checkpoints("s")["records"]
    ]
    # Deliberately leave a half-written/corrupt next checkpoint.
    next_sequence = wb.checkpoints("s")["next_sequence"]
    corrupt = wb._run_dir(wb.status("s")["active_run_id"]) / "checkpoints" / f"{next_sequence:08d}.json"
    atomic_write_bytes(corrupt, b'{"sequence":')

    resumed = wb.resume("s", background=False)
    assert resumed["status"] == "complete"
    assert resumed["result"]["result_digest"] == full["result_digest"]
    final_records = wb.checkpoints("s")["records"]
    assert [record["sequence"] for record in final_records] == list(range(1, len(final_records) + 1))
    assert all(
        before["checksum"] == after["checksum"]
        for before, after in zip(prefix_before, final_records)
    )
    assert len(final_records) == 7


def test_invalid_hyperparameters_and_numeric_failure(tmp_path: Path) -> None:
    wb = Workbench(tmp_path / "errors", data_dir=DATA_DIR)
    wb.create_session("s", fast_config("small"), overwrite=True)
    with pytest.raises(InvalidHyperparameter):
        wb.update_config("s", {"regularization": {"l1_ratios": [1.5]}})
    with pytest.raises(InvalidCV):
        wb.update_config("s", {"cv": {"folds": 1}})

    config = fast_config("small", folds=2)
    config["solver"]["objective_limit"] = 0.001
    wb.create_session("numeric", config, overwrite=True)
    with pytest.raises(NumericError) as caught:
        wb.start_training("numeric", background=False)
    assert caught.value.details["fold"] is not None
    assert caught.value.details["iteration"] is not None
    status = wb.status("numeric")
    assert status["status"] == "error"
    assert status["folds"] == []
    assert status["checkpoints"]["valid_count"] == 2


def test_zero_regularization_full_run(tmp_path: Path) -> None:
    wb = Workbench(tmp_path / "zero", data_dir=DATA_DIR)
    config = fast_config("small", folds=2)
    config["regularization"] = {
        "mode": "elasticnet",
        "strengths": [0.0],
        "l1_ratios": [0.5],
    }
    wb.create_session("s", config, overwrite=True)
    result = wb.start_training("s", background=False)["result"]
    assert all(value == value for value in result["coefficients"])
    assert result["selected_hyperparameters"]["strength"] == 0.0


def test_sample_content_change_invalidates_preprocess(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    shutil.copytree(DATA_DIR, data_dir)
    wb = Workbench(tmp_path / "data-change", data_dir=data_dir)
    wb.create_session("s", fast_config("small"), overwrite=True)
    wb.start_training("s", background=False)
    csv_path = data_dir / "small.csv"
    lines = csv_path.read_text("utf-8").splitlines()
    fields = lines[1].split(",")
    fields[0] = f"{float(fields[0]) + 1.0:.2f}"
    lines[1] = ",".join(fields)
    csv_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    declaration_path = data_dir / "dataset_declaration.json"
    declaration = json.loads(declaration_path.read_text("utf-8"))
    declaration["datasets"][0]["sha256"] = sha256_file(csv_path)
    declaration_path.write_text(
        json.dumps(declaration, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    update = wb.update_config("s", {})
    assert update["invalidation"]["earliest_stage"] == "preprocess"
    assert update["invalidation"]["data_changed"] is True


def test_corrupt_fold_artifact_is_rebuilt_from_consistent_prefix(tmp_path: Path) -> None:
    config = fast_config("small")
    full_wb = Workbench(tmp_path / "artifact-full", data_dir=DATA_DIR)
    full_wb.create_session("s", config, overwrite=True)
    expected = full_wb.start_training("s", background=False)["result"]

    wb = Workbench(tmp_path / "artifact-resume", data_dir=DATA_DIR)
    wb.create_session("s", config, overwrite=True)
    wb.start_training("s", stop_after_tasks=2, background=False)
    run_dir = wb._run_dir(wb.status("s")["active_run_id"])
    damaged = run_dir / "artifacts" / "solve-0002.json"
    atomic_write_bytes(damaged, b'{"artifact_type":')
    resumed = wb.resume("s", background=False)
    assert resumed["status"] == "complete"
    assert resumed["result"]["result_digest"] == expected["result_digest"]
    sequences = [record["sequence"] for record in wb.checkpoints("s")["records"]]
    assert sequences == list(range(1, len(sequences) + 1))


def test_named_session_is_created_on_first_training(tmp_path: Path) -> None:
    wb = Workbench(tmp_path / "auto-session", data_dir=DATA_DIR)
    outcome = wb.start_training(
        "cli-demo",
        config_patch=fast_config("small", folds=2),
        background=False,
    )
    assert outcome["status"] == "complete"
    assert wb.status("cli-demo")["status"] == "complete"
