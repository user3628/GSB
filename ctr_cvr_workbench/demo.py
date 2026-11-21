"""Reproducible acceptance demonstration for the local workbench."""

from __future__ import annotations

import copy
import hashlib
import json
import shutil
from pathlib import Path
from typing import Any

import yaml

from .config import Config
from .errors import PipelineError
from .generator import write_dataset
from .pipeline import Pipeline
from .util import atomic_write_bytes, atomic_write_json


def _digest_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _file_digest(path: Path) -> str | None:
    if not path.exists():
        return None
    return _digest_bytes(path.read_bytes())


def _copy_data(template_shards: Path, target_base: Path) -> None:
    target = target_base / "data" / "shards"
    if target.exists():
        shutil.rmtree(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(template_shards, target)


def _write_config(template: dict[str, Any], target_base: Path) -> None:
    path = target_base / "configs" / "demo.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    data = yaml.safe_dump(template, sort_keys=True, allow_unicode=True).encode("utf-8")
    atomic_write_bytes(path, data)


def _prepare_base(
    name: str,
    demo_root: Path,
    template_shards: Path,
    config_template: dict[str, Any],
) -> Path:
    base = demo_root / name
    if base.exists():
        shutil.rmtree(base)
    base.mkdir(parents=True)
    _copy_data(template_shards, base)
    _write_config(config_template, base)
    return base


def _run(path: Path, *, execution_mode: str | None = None, stop_after_epochs: int | None = None) -> dict[str, Any]:
    return Pipeline(
        "configs/demo.yaml",
        base_dir=path,
        execution_mode=execution_mode,
        stop_after_epochs=stop_after_epochs,
    ).run()


def _inject(path: Path, injection: str) -> dict[str, Any]:
    try:
        Pipeline("configs/demo.yaml", base_dir=path, injection=injection, execution_mode="sequential").run()
    except PipelineError as exc:
        return exc.to_dict()
    return {"code": "NO_ERROR", "message": "injection unexpectedly succeeded"}


def _artifact_digests(base: Path, config_path: str = "configs/demo.yaml") -> dict[str, str | None]:
    config = Config.load(config_path, base)
    return {
        "summary": _file_digest(config.outputs_root / "experiment_summary.json"),
        "model": _file_digest(config.outputs_root / "train" / "model.json"),
        "metrics": _file_digest(config.outputs_root / "train" / "metrics.json"),
        "scores": _file_digest(config.outputs_root / "infer" / "scores.jsonl"),
        "funnel": _file_digest(config.outputs_root / "funnel" / "diagnostics.json"),
    }


def run_demo(
    out_dir: str | Path,
    *,
    config_path: str = "configs/default.yaml",
    base_dir: str | Path = ".",
) -> dict[str, Any]:
    project_base = Path(base_dir).resolve()
    demo_root = Path(out_dir)
    if not demo_root.is_absolute():
        demo_root = project_base / demo_root
    demo_root.mkdir(parents=True, exist_ok=True)
    template_root = demo_root / "_template"
    template_shards = template_root / "data" / "shards"
    if template_root.exists():
        shutil.rmtree(template_root)
    generation = write_dataset(template_shards, seed=20261122, shards=4, rows_per_shard=160)

    base_config = Config.load(config_path, project_base)
    raw = copy.deepcopy(base_config.raw)
    raw["experiment"]["name"] = "local-ctr-cvr-demo"
    raw["training"]["epochs"] = 3
    raw["training"]["learning_rate"] = 0.15
    raw["training"]["combo_weight"] = 0.35
    raw["scheduler"]["max_global_sample_rounds"] = 1200
    raw["scheduler"]["per_shard_sample_quota"] = 160
    raw["scheduler"]["max_workers"] = 2
    raw["paths"]["manifest"] = "data/shards/manifest.json"
    raw["paths"]["checkpoints"] = "checkpoints"
    raw["paths"]["outputs"] = "outputs"
    raw["paths"]["state"] = "state"

    run_a = _prepare_base("run_full_a", demo_root, template_shards, raw)
    run_b = _prepare_base("run_full_b", demo_root, template_shards, raw)
    run_parallel = _prepare_base("run_parallel", demo_root, template_shards, raw)
    run_stop = _prepare_base("run_stop_resume", demo_root, template_shards, raw)
    run_invalidation = _prepare_base("run_invalidation", demo_root, template_shards, raw)

    result_a = _run(run_a, execution_mode="sequential")
    result_b = _run(run_b, execution_mode="sequential")
    result_parallel = _run(run_parallel, execution_mode="parallel")
    stopped = _run(run_stop, execution_mode="sequential", stop_after_epochs=2)
    resumed = _run(run_stop, execution_mode="sequential")
    _run(run_invalidation, execution_mode="sequential")
    changed_raw = copy.deepcopy(raw)
    changed_raw["training"]["learning_rate"] = 0.2
    changed_config_path = run_invalidation / "configs" / "demo_lr_changed.yaml"
    atomic_write_bytes(
        changed_config_path,
        yaml.safe_dump(changed_raw, sort_keys=True, allow_unicode=True).encode("utf-8"),
    )
    invalidated_status = Pipeline.status("configs/demo_lr_changed.yaml", base_dir=run_invalidation)
    feature_seq_before = invalidated_status["stages"]["features"]["seq"]
    _run_invalidation = Pipeline(
        "configs/demo_lr_changed.yaml", base_dir=run_invalidation, execution_mode="sequential"
    ).run()
    rebuilt_status = Pipeline.status("configs/demo_lr_changed.yaml", base_dir=run_invalidation)
    invalidation_records = rebuilt_status["invalidations"]
    earliest_invalidation = next(
        (item["earliest_stage"] for item in invalidation_records if item["earliest_stage"] == "train"),
        None,
    )
    invalidation_ok = (
        invalidated_status["stages"]["features"]["status"] == "valid"
        and invalidated_status["stages"]["train"]["status"] == "stale"
        and invalidated_status["stages"]["funnel"]["status"] == "blocked"
        and rebuilt_status["stages"]["features"]["status"] == "valid"
        and rebuilt_status["stages"]["train"]["status"] == "valid"
        and rebuilt_status["stages"]["features"]["seq"] == feature_seq_before
        and earliest_invalidation == "train"
    )

    digests_a = _artifact_digests(run_a)
    digests_b = _artifact_digests(run_b)
    digests_parallel = _artifact_digests(run_parallel)
    digests_stop = _artifact_digests(run_stop)

    injections = {
        name: _inject(run_a, name)
        for name in (
            "manifest_mismatch",
            "cvr_without_ctr",
            "invalid_ctr_label",
            "negative_lr",
            "zero_feature_dim",
        )
    }

    config = Config.load("configs/demo.yaml", run_a)
    feature_artifact = json.loads((config.outputs_root / "features" / "features.json").read_text(encoding="utf-8"))
    probe_row = next(
        row for shard in feature_artifact["shards"] for row in shard["rows"] if row["missing"]
    )
    checkpoint_files = sorted((config.checkpoints_root).glob("*.json"))
    checkpoint_seqs = [json.loads(path.read_text(encoding="utf-8"))["seq"] for path in checkpoint_files]
    temp_files = sorted(
        path.relative_to(demo_root).as_posix()
        for path in demo_root.rglob("*")
        if path.is_file() and (path.name.endswith(".tmp") or ".tmp." in path.name)
    )

    repeated_equal = digests_a["summary"] == digests_b["summary"]
    parallel_equal = all(digests_a[key] == digests_parallel[key] for key in ("summary", "model", "metrics", "scores"))
    resume_equal = all(digests_a[key] == digests_stop[key] for key in ("summary", "model", "metrics", "scores"))
    expected_errors = {
        "manifest_mismatch": "E_MANIFEST_MISMATCH",
        "cvr_without_ctr": "E_FUNNEL_VIOLATION",
        "invalid_ctr_label": "E_LABEL_INVALID",
        "negative_lr": "E_INVALID_PARAM",
        "zero_feature_dim": "E_INVALID_PARAM",
    }
    errors_ok = all(injections[name].get("code") == code for name, code in expected_errors.items())
    schedule = result_a["schedule"]
    atomic_ok = checkpoint_seqs == sorted(checkpoint_seqs) and len(set(checkpoint_seqs)) == len(checkpoint_seqs) and not temp_files
    funnel_ok = result_a["summary"]["funnel"]["violation_count"] == 0
    repro_ok = repeated_equal and parallel_equal and resume_equal
    ok = bool(
        repro_ok
        and errors_ok
        and atomic_ok
        and funnel_ok
        and invalidation_ok
        and stopped.get("status") == "stopped"
    )

    report = {
        "format": "ctr-cvr-demo-report-v1",
        "ok": ok,
        "environment": {
            "python": __import__("platform").python_version(),
            "operating_system": __import__("platform").system(),
            "numpy": __import__("numpy").__version__,
            "pyyaml": __import__("yaml").__version__,
            "gpu_required": False,
            "network_required": False,
            "container_required": False,
        },
        "generation": generation,
        "schedule": schedule,
        "unprocessed_shards": schedule["unprocessed"],
        "config_version": result_a["config_version"],
        "data_content_fingerprint": result_a["data_content_fingerprint"],
        "objective_metrics": result_a["summary"]["inference"],
        "funnel": result_a["summary"]["funnel"],
        "reproducibility": {
            "two_identical_runs_equal": repeated_equal,
            "parallel_vs_sequential_equal": parallel_equal,
            "stop_resume_vs_full_equal": resume_equal,
            "run_a_digests": digests_a,
            "run_b_digests": digests_b,
            "parallel_digests": digests_parallel,
            "stop_resume_digests": digests_stop,
        },
        "stop_resume": {
            "stopped_status": stopped.get("status"),
            "stopped_after_epochs": stopped.get("epochs_completed"),
            "resumed_status": resumed.get("status"),
            "resumed_summary_digest": resumed.get("summary_digest"),
        },
        "injections": injections,
        "error_codes_ok": errors_ok,
        "incremental_invalidation": {
            "ok": invalidation_ok,
            "change": {"training.learning_rate": {"from": 0.15, "to": 0.2}},
            "earliest_affected_stage": earliest_invalidation,
            "before_rebuild": {
                stage: invalidated_status["stages"][stage]["status"]
                for stage in ("features", "train", "funnel", "infer")
            },
            "after_rebuild": {
                stage: rebuilt_status["stages"][stage]["status"]
                for stage in ("features", "train", "funnel", "infer")
            },
            "feature_checkpoint_seq_before": feature_seq_before,
            "feature_checkpoint_seq_after": rebuilt_status["stages"]["features"]["seq"],
            "invalidation_records": invalidation_records,
        },
        "checkpoint_atomicity": {
            "records": len(checkpoint_files),
            "monotonic_unique_sequences": checkpoint_seqs,
            "temporary_files": temp_files,
            "ok": atomic_ok,
        },
        "feature_probe": {
            "impression_id": probe_row["impression_id"],
            "bins": probe_row["bins"],
            "missing": probe_row["missing"],
            "fingerprint": probe_row["fingerprint"],
            "feature_artifact_digest": result_a["summary"]["feature_artifact_digest"],
        },
    }
    report_path = demo_root / "report.json"
    atomic_write_json(report_path, report)
    return {
        "ok": ok,
        "report_path": report_path.as_posix(),
        "report_digest": _file_digest(report_path),
        "summary_digest": result_a["summary_digest"],
        "objective_metrics": result_a["summary"]["inference"],
        "reproducibility": report["reproducibility"],
        "error_codes": {name: payload.get("code") for name, payload in injections.items()},
        "checkpoint_atomicity": report["checkpoint_atomicity"],
        "stop_resume": report["stop_resume"],
        "incremental_invalidation": {
            "ok": invalidation_ok,
            "earliest_affected_stage": earliest_invalidation,
            "before_rebuild": report["incremental_invalidation"]["before_rebuild"],
            "after_rebuild": report["incremental_invalidation"]["after_rebuild"],
        },
    }
