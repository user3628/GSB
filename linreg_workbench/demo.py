"""Deterministic multi-scenario demonstration and fixed-summary verification."""

from __future__ import annotations

import copy
import multiprocessing
import shutil
import tempfile
from pathlib import Path
from typing import Any

from .canonical import atomic_write_json, canonical_bytes, digest, sha256_bytes
from .checkpoints import CheckpointStore
from .config import default_config
from .errors import InvalidCV, InvalidHyperparameter, InvalidStage, NumericError, StaleCheckpoint, WorkbenchError
from .workbench import Workbench

PROJECT_ROOT = Path(__file__).resolve().parent.parent
EXPECTED_DIR = PROJECT_ROOT / "expected"


def _full_config(dataset_id: str) -> dict[str, Any]:
    config = default_config(dataset_id)
    config["cv"] = {
        "folds": 4,
        "seed": 20261120,
        "stratify": True,
        "metric": "log_loss",
    }
    config["solver"] = {
        "name": "coordinate_descent",
        "tolerance": 1e-5,
        "max_iter": 250,
        "objective_limit": 1_000_000.0,
    }
    if dataset_id == "small":
        config["preprocessing"]["categorical_encoding"] = "onehot"
        config["regularization"] = {
            "mode": "elasticnet",
            "strengths": [0.005, 0.02, 0.08],
            "l1_ratios": [0.2, 0.7],
        }
    elif dataset_id == "medium":
        config["preprocessing"]["categorical_encoding"] = "target"
        config["regularization"] = {
            "mode": "elasticnet",
            "strengths": [0.005, 0.03],
            "l1_ratios": [0.35],
        }
        config["solver"]["max_iter"] = 220
    else:
        raise ValueError(f"Unknown demo dataset: {dataset_id}")
    return config


def _summary_bytes(summary: dict[str, Any]) -> bytes:
    return canonical_bytes(summary, trailing_newline=True)


def _concurrent_publish_child(
    run_dir: str,
    run_id: str,
    artifact_path: str,
    barrier: Any,
    queue: Any,
) -> None:
    try:
        store = CheckpointStore(run_dir, run_id)
        barrier.wait(timeout=10)
        store.publish(
            stage="folds",
            fold=None,
            task_index=None,
            role="folds",
            input_fingerprint="concurrent-fold-fingerprint",
            config_version="concurrent-config",
            coefficient_summary=None,
            artifact_path=artifact_path,
        )
        queue.put({"result": "OK"})
    except WorkbenchError as exc:
        queue.put({"result": exc.code, "details": exc.details})
    except Exception as exc:  # pragma: no cover - diagnostic path
        queue.put({"result": "E_CHILD", "details": {"reason": str(exc)}})


def _exercise_checkpoint_errors(workspace: Path) -> dict[str, Any]:
    base = workspace / "checkpoint-lab"
    if base.exists():
        shutil.rmtree(base)
    base.mkdir(parents=True, exist_ok=True)
    run_id = "checkpoint-lab-run"
    run_dir = base / run_id
    (run_dir / "artifacts").mkdir(parents=True, exist_ok=True)
    atomic_write_json(
        run_dir / "artifacts" / "preprocess.json",
        {"artifact_type": "preprocess", "fingerprint": "preprocess-demo"},
    )
    atomic_write_json(
        run_dir / "artifacts" / "folds.json",
        {"artifact_type": "folds", "fingerprint": "folds-demo"},
    )
    invalid_stage_code = None
    invalid_store = CheckpointStore(str(run_dir), run_id)
    try:
        invalid_store.publish(
            stage="folds",
            fold=None,
            task_index=None,
            role="folds",
            input_fingerprint="illegal-jump",
            config_version="demo",
            coefficient_summary=None,
            artifact_path="artifacts/folds.json",
        )
    except InvalidStage as exc:
        invalid_stage_code = exc.code

    store = CheckpointStore(str(run_dir), run_id)
    store.publish(
        stage="preprocess",
        fold=None,
        task_index=None,
        role="preprocess",
        input_fingerprint="preprocess-demo",
        config_version="demo",
        coefficient_summary=None,
        artifact_path="artifacts/preprocess.json",
    )
    context = multiprocessing.get_context("fork")
    barrier = context.Barrier(2)
    queue = context.Queue()
    processes = [
        context.Process(
            target=_concurrent_publish_child,
            args=(
                str(run_dir),
                run_id,
                "artifacts/folds.json",
                barrier,
                queue,
            ),
        )
        for _ in range(2)
    ]
    for process in processes:
        process.start()
    outcomes = [queue.get(timeout=15) for _ in processes]
    for process in processes:
        process.join(timeout=10)
    codes = sorted(item["result"] for item in outcomes)
    records = store.inspect()["records"]
    return {
        "invalid_stage_code": invalid_stage_code,
        "concurrent_codes": codes,
        "concurrent_one_success_one_stale": codes == ["E_STALE_CHECKPOINT", "OK"],
        "sequence": [record["sequence"] for record in records],
        "records_after_concurrency": len(records),
    }


def _write_empty_feature_csv(path: Path) -> None:
    lines = ["signal,empty_column,label"]
    for index in range(48):
        lines.append(f"{index % 9},,{index % 2}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _run_scenarios(workspace: Path, data_dir: Path) -> dict[str, Any]:
    wb = Workbench(workspace, data_dir=data_dir)

    # Dataset declarations and full two-tier runs.
    small_config = _full_config("small")
    wb.create_session("full-small", small_config, overwrite=True)
    small_outcome = wb.start_training("full-small", background=False)
    small_result = small_outcome["result"]
    small_summary = wb.summary("full-small")

    medium_config = _full_config("medium")
    wb.create_session("full-medium", medium_config, overwrite=True)
    medium_outcome = wb.start_training("full-medium", background=False)
    medium_result = medium_outcome["result"]
    medium_summary = wb.summary("full-medium")

    # Independent fresh workspace confirms byte equality from zero.
    fresh_root = Path(tempfile.mkdtemp(prefix="fresh-run-", dir=str(workspace)))
    try:
        fresh_wb = Workbench(fresh_root, data_dir=data_dir)
        fresh_wb.create_session("fresh-small", small_config, overwrite=True)
        fresh_result = fresh_wb.start_training("fresh-small", background=False)["result"]
        fresh_summary = fresh_wb.summary("fresh-small")
    finally:
        shutil.rmtree(fresh_root, ignore_errors=True)
    determinism_ok = canonical_bytes(small_summary) == canonical_bytes(fresh_summary)

    # Stop between solve tasks, retain checkpoint prefix, then resume.
    wb.create_session("stop-small", small_config, overwrite=True)
    stopped = wb.start_training(
        "stop-small", stop_after_tasks=7, background=False
    )
    stopped_status = wb.status("stop-small")
    stopped_records = wb.checkpoints("stop-small")["records"]
    stopped_prefix = [
        {"sequence": record["sequence"], "checksum": record["checksum"]}
        for record in stopped_records
    ]
    resumed = wb.resume("stop-small", background=False)
    resumed_status = wb.status("stop-small")
    final_records = wb.checkpoints("stop-small")["records"]
    final_sequences = [record["sequence"] for record in final_records]
    prefix_preserved = all(
        item["sequence"] == record["sequence"] and item["checksum"] == record["checksum"]
        for item, record in zip(stopped_prefix, final_records)
    )
    stop_resume_ok = (
        stopped["status"] == "stopped"
        and resumed["status"] == "complete"
        and resumed["result"]["result_digest"] == small_result["result_digest"]
        and prefix_preserved
        and final_sequences == list(range(1, len(final_sequences) + 1))
    )

    # Regularization change invalidates solve + aggregate but reuses first stages.
    wb.create_session("invalidation", small_config, overwrite=True)
    first_outcome = wb.start_training("invalidation", background=False)
    changed_patch = {
        "regularization": {
            "mode": "elasticnet",
            "strengths": [0.012, 0.04],
            "l1_ratios": [0.2, 0.7],
        }
    }
    update = wb.update_config("invalidation", changed_patch)
    invalidation = update["invalidation"]
    second_outcome = wb.start_training("invalidation", background=False)
    second_status = wb.status("invalidation")
    reuse_map = {item["stage"]: item["reused"] for item in second_status["reuse"]}
    invalidation_ok = (
        invalidation["earliest_stage"] == "solve"
        and invalidation["reusable_stages"] == ["preprocess", "folds"]
        and reuse_map.get("preprocess") is True
        and reuse_map.get("folds") is True
        and second_outcome["result"]["result_digest"] != first_outcome["result"]["result_digest"]
    )

    # Hyperparameter validation and CV validation.
    invalid_ratio_code = None
    try:
        wb.update_config(
            "invalidation",
            {"regularization": {"l1_ratios": [1.5]}},
        )
    except InvalidHyperparameter as exc:
        invalid_ratio_code = exc.code
    invalid_fold_code = None
    try:
        wb.update_config("invalidation", {"cv": {"folds": 1}})
    except InvalidCV as exc:
        invalid_fold_code = exc.code

    # Numerical divergence must not checkpoint a completed fold.
    numeric_config = copy.deepcopy(small_config)
    numeric_config["cv"]["folds"] = 2
    numeric_config["regularization"] = {
        "mode": "l2",
        "strengths": [0.01],
        "l1_ratios": [0.0],
    }
    numeric_config["solver"]["objective_limit"] = 0.01
    wb.create_session("numeric-error", numeric_config, overwrite=True)
    numeric_error = None
    try:
        wb.start_training("numeric-error", background=False)
    except NumericError as exc:
        numeric_error = exc.to_dict()
    numeric_status = wb.status("numeric-error")
    numeric_checkpoint_count = numeric_status["checkpoints"]["valid_count"]
    numeric_ok = (
        numeric_error is not None
        and numeric_error["code"] == "E_NUMERIC"
        and numeric_error["details"].get("fold") is not None
        and numeric_error["details"].get("iteration") is not None
        and numeric_checkpoint_count == 2
        and not numeric_status["folds"]
    )

    # Empty feature diagnostic on an imported local CSV.
    aux = workspace / "aux"
    aux.mkdir(parents=True, exist_ok=True)
    empty_csv = aux / "empty-feature.csv"
    _write_empty_feature_csv(empty_csv)
    wb.import_dataset(empty_csv, "empty-feature", target="label", override=True)
    empty_config = copy.deepcopy(small_config)
    empty_config["dataset_id"] = "empty-feature"
    empty_config["cv"]["folds"] = 2
    empty_config["regularization"] = {
        "mode": "l2",
        "strengths": [0.01],
        "l1_ratios": [0.0],
    }
    wb.create_session("empty-feature", empty_config, overwrite=True)
    empty_result = wb.start_training("empty-feature", background=False)["result"]
    empty_codes = [item["code"] for item in empty_result["empty_feature_errors"]]
    empty_feature_ok = empty_codes == ["E_EMPTY_FEATURE"]

    checkpoint_errors = _exercise_checkpoint_errors(workspace)

    return {
        "schema_version": 1,
        "datasets": {
            "small": {
                "rows": 600,
                "columns": 12,
                "summary": small_summary,
                "summary_digest": small_summary["result_digest"],
                "result_bytes_sha256": sha256_bytes(_summary_bytes(small_summary)),
            },
            "medium": {
                "rows": 6000,
                "columns": 40,
                "missing_columns": next(
                    item["missing_columns"]
                    for item in wb.datasets()
                    if item["dataset_id"] == "medium"
                ),
                "redundant_columns": next(
                    item["redundant_columns"]
                    for item in wb.datasets()
                    if item["dataset_id"] == "medium"
                ),
                "summary": medium_summary,
                "summary_digest": medium_summary["result_digest"],
                "result_bytes_sha256": sha256_bytes(_summary_bytes(medium_summary)),
            },
        },
        "determinism": {
            "ok": determinism_ok,
            "first_digest": small_result["result_digest"],
            "second_digest": fresh_result["result_digest"],
        },
        "stop_resume": {
            "ok": stop_resume_ok,
            "stopped_status": stopped_status["status"],
            "checkpoints_before_resume": len(stopped_records),
            "checkpoints_after_resume": len(final_records),
            "prefix_preserved": prefix_preserved,
            "sequences_contiguous": final_sequences == list(range(1, len(final_sequences) + 1)),
            "result_digest": resumed["result"]["result_digest"],
            "matches_full": resumed["result"]["result_digest"] == small_result["result_digest"],
        },
        "invalidation": {
            "ok": invalidation_ok,
            "earliest_stage": invalidation["earliest_stage"],
            "reusable_stages": invalidation["reusable_stages"],
            "affected_stages": invalidation["affected_stages"],
            "changed_config_paths": invalidation["changed_config_paths"],
            "reuse_map": {
                "preprocess": reuse_map.get("preprocess"),
                "folds": reuse_map.get("folds"),
            },
            "old_digest": first_outcome["result"]["result_digest"],
            "new_digest": second_outcome["result"]["result_digest"],
        },
        "errors": {
            "invalid_l1_ratio": invalid_ratio_code,
            "single_fold_cv": invalid_fold_code,
            "numeric": numeric_error,
            "empty_feature": {
                "code": "E_EMPTY_FEATURE",
                "ok": empty_feature_ok,
                "diagnostics": empty_result["empty_feature_errors"],
            },
            "checkpoint_lab": checkpoint_errors,
        },
    }


def _add_overall_status(report: dict[str, Any]) -> None:
    errors = report["errors"]
    report["ok"] = all(
        [
            report["determinism"]["ok"],
            report["stop_resume"]["ok"],
            report["invalidation"]["ok"],
            errors["invalid_l1_ratio"] == "E_INVALID_HYPERPARAM",
            errors["single_fold_cv"] == "E_INVALID_CV",
            errors["numeric"]["code"] == "E_NUMERIC",
            errors["empty_feature"]["ok"],
            errors["checkpoint_lab"]["invalid_stage_code"] == "E_INVALID_STAGE",
            errors["checkpoint_lab"]["concurrent_one_success_one_stale"],
        ]
    )


def run_demo(
    *,
    workspace: str | Path = "runtime/demo",
    data_dir: str | Path = "data",
    write_expected: bool = False,
) -> dict[str, Any]:
    workspace_path = Path(workspace).resolve()
    workspace_path.mkdir(parents=True, exist_ok=True)
    data_path = Path(data_dir).resolve()
    report = _run_scenarios(workspace_path, data_path)
    _add_overall_status(report)

    comparisons = {}
    expected = EXPECTED_DIR
    expected.mkdir(parents=True, exist_ok=True)
    for dataset_id in ("small", "medium"):
        summary = report["datasets"][dataset_id]["summary"]
        target = expected / f"{dataset_id}.summary.json"
        payload = _summary_bytes(summary)
        if write_expected:
            target.write_bytes(payload)
            comparisons[dataset_id] = {
                "baseline": f"{dataset_id}.summary.json",
                "written": True,
                "matches": True,
            }
        else:
            matches = target.exists() and target.read_bytes() == payload
            comparisons[dataset_id] = {
                "baseline": f"{dataset_id}.summary.json",
                "written": False,
                "matches": matches,
            }
    report["expected_summary_comparison"] = comparisons
    if not write_expected:
        report["ok"] = report["ok"] and all(
            item["matches"] for item in comparisons.values()
        )
    stable_report = copy.deepcopy(report)
    stable_report.pop("report_digest", None)
    for item in stable_report.get("expected_summary_comparison", {}).values():
        item.pop("written", None)
    report["report_digest"] = digest(stable_report)
    if write_expected:
        atomic_write_json(expected / "demo_report.json", report)
    return report


def verify_demo(
    *,
    workspace: str | Path | None = None,
    data_dir: str | Path = "data",
) -> dict[str, Any]:
    temporary = workspace is None
    if workspace is None:
        workspace = tempfile.mkdtemp(prefix="linear-workbench-verify-")
    else:
        workspace = str(Path(workspace).resolve())
    try:
        report = run_demo(
            workspace=workspace,
            data_dir=data_dir,
            write_expected=False,
        )
        report["verification_workspace"] = workspace
        return report
    finally:
        if temporary:
            shutil.rmtree(str(workspace), ignore_errors=True)
