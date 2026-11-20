"""Deterministic local fixture generation and complete demonstration."""

from __future__ import annotations

import copy
import io
import shutil
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any

import numpy as np
import yaml

from .canonical import (
    arrays_digest,
    atomic_write_bytes,
    atomic_write_json,
    atomic_write_npz,
    canonical_digest,
    canonical_json,
    read_json,
)
from .config import load_config
from .data import load_calibration, load_weights
from .engine import AuditEngine
from .errors import (
    E_WORKSPACE_EXISTS,
    AuditError,
)
from .workspace import Workspace

LAYER_NAMES = tuple(f"linear_{index:02d}" for index in range(6))
LAYER_DIMENSIONS = (6, 10, 8, 7, 6, 5, 4)
DEMO_SEED = 20261120


def generate_demo_data(data_dir: str | Path, seed: int = DEMO_SEED, force: bool = False) -> dict[str, Any]:
    output = Path(data_dir)
    if output.exists() and any(output.iterdir()) and not force:
        raise AuditError(
            E_WORKSPACE_EXISTS,
            "demo data directory is not empty; use --force to regenerate",
            data_dir=str(output),
        )
    output.mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(seed)
    arrays: dict[str, np.ndarray] = {"layer_names": np.array(LAYER_NAMES)}
    shapes = []
    for index, name in enumerate(LAYER_NAMES):
        weight_shape = (LAYER_DIMENSIONS[index + 1], LAYER_DIMENSIONS[index])
        weight = rng.normal(0.0, 0.35, size=weight_shape).astype(np.float64)
        bias = rng.normal(0.0, 0.08, size=(weight_shape[0],)).astype(np.float64)
        arrays[f"{name}.weight"] = weight
        arrays[f"{name}.bias"] = bias
        shapes.append({"name": name, "weight": list(weight.shape), "bias": list(bias.shape)})
    arrays["shape_manifest"] = np.array(canonical_json(shapes))
    clean = rng.normal(0.0, 0.8, size=(24, LAYER_DIMENSIONS[0])).astype(np.float64)
    outlier = clean.copy()
    outlier[2, 1] = 18.0
    outlier[7, 4] = -22.0
    outlier[15, 0] = 12.0

    atomic_write_npz(output / "weights.npz", arrays)
    _write_npy(output / "calibration_clean.npy", clean)
    _write_npy(output / "calibration_outlier.npy", outlier)
    atomic_write_json(output / "layer_names.json", list(LAYER_NAMES))

    base_config = _base_config()
    incremental_config = copy.deepcopy(base_config)
    incremental_config["quantization"]["layers"]["linear_03"] = {
        "bits": 7,
        "mode": "symmetric",
        "zero_point": "exact",
    }
    over_budget_config = copy.deepcopy(base_config)
    over_budget_config["quantization"] = {
        "bits": 2,
        "mode": "symmetric",
        "zero_point": "exact",
        "layers": {"linear_01": {"mode": "asymmetric", "zero_point": "exact"}},
    }
    attribution_gap_config = copy.deepcopy(over_budget_config)
    attribution_gap_config["attribution"]["closure_tolerance"] = 0.0

    _write_yaml(output / "config.yaml", base_config)
    _write_yaml(output / "config_incremental.yaml", incremental_config)
    _write_yaml(output / "config_outlier_over_budget.yaml", over_budget_config)
    _write_yaml(output / "config_attribution_gap.yaml", attribution_gap_config)

    manifest = {
        "schema_version": 1,
        "seed": seed,
        "layer_names": list(LAYER_NAMES),
        "dimensions": list(LAYER_DIMENSIONS),
        "shapes": shapes,
        "calibration_clean_shape": list(clean.shape),
        "calibration_outlier_shape": list(outlier.shape),
        "outlier_positions": [[2, 1], [7, 4], [15, 0]],
        "weight_arrays_digest": arrays_digest(arrays),
        "calibration_clean_digest": canonical_digest(clean.tolist()),
        "calibration_outlier_digest": canonical_digest(outlier.tolist()),
        "config_digest": canonical_digest(base_config),
        "incremental_config_digest": canonical_digest(incremental_config),
        "over_budget_config_digest": canonical_digest(over_budget_config),
    }
    manifest["dataset_digest"] = canonical_digest(manifest)
    atomic_write_json(output / "manifest.json", manifest)
    return {
        "data_dir": str(output),
        "manifest": manifest,
        "files": [
            str(path.relative_to(output))
            for path in sorted(output.iterdir())
            if path.is_file()
        ],
    }


def run_demo(
    *,
    workspace_path: str | Path,
    data_dir: str | Path,
    report_path: str | Path,
    force: bool = False,
    include_faults: bool = True,
) -> dict[str, Any]:
    generated = generate_demo_data(data_dir, seed=DEMO_SEED, force=force)
    data = Path(data_dir)
    workspace_path = Path(workspace_path)
    workspace = Workspace.initialize(
        workspace_path,
        data / "weights.npz",
        data / "calibration_clean.npy",
        data / "config.yaml",
        force=force,
    )

    version_one = workspace.calibrate(mode="full")
    version_two = workspace.calibrate(
        mode="incremental",
        config_path=data / "config_incremental.yaml",
    )

    with tempfile.TemporaryDirectory(prefix="quant_audit_demo_faults_") as temporary:
        fault_root = Path(temporary)
        snapshot_audit = fault_root / "snapshot_v2_audit"
        shutil.copytree(workspace.audit_root, snapshot_audit)

        version_three = workspace.calibrate(
            mode="full",
            config_path=data / "config_outlier_over_budget.yaml",
            calibration_path=data / "calibration_outlier.npy",
        )
        active_after_rollback = workspace.query()
        historical_two = workspace.store.load_record(2)
        historical_match = canonical_json(active_after_rollback["result"]) == canonical_json(historical_two["result"])
        budget_match = canonical_json(active_after_rollback["result"]["budget"]) == canonical_json(
            historical_two["result"]["budget"]
        )

        replacement_results = _version_replacement_faults(
            workspace,
            fault_root,
            snapshot_audit,
            data,
        )
        anomaly_results = _anomaly_faults(workspace, fault_root, data)
        if not include_faults:
            replacement_results = {}
            anomaly_results = {}

        v1_record = workspace.store.load_record(1)
        v2_record = workspace.store.load_record(2)
        v3_record = workspace.store.load_record(3)
        deterministic_payload = {
            "dataset_digest": generated["manifest"]["dataset_digest"],
            "versions": [
                {
                    "version": record["version"],
                    "result_summary": record["result_summary"],
                    "usable": record["usable"],
                    "active_after_publish": record["active_after_publish"],
                }
                for record in (v1_record, v2_record, v3_record)
            ],
            "final_active_version": workspace.store.active_version(),
            "replacement_codes": {
                key: value["code"] for key, value in replacement_results.items()
            },
            "anomaly_codes": {key: value["code"] for key, value in anomaly_results.items()},
        }
        deterministic_digest = canonical_digest(deterministic_payload)

        report = {
            "ok": True,
            "demo": "deterministic quantization audit and rollback",
            "dataset": {
                "seed": DEMO_SEED,
                "layer_names": list(LAYER_NAMES),
                "dimensions": list(LAYER_DIMENSIONS),
                "dataset_digest": generated["manifest"]["dataset_digest"],
                "calibration_clean": str(data / "calibration_clean.npy"),
                "calibration_outlier": str(data / "calibration_outlier.npy"),
            },
            "versions": {
                "v1_full": _calibration_summary(version_one),
                "v2_incremental": _calibration_summary(version_two),
                "v3_over_budget": _calibration_summary(version_three),
            },
            "incremental": {
                "base_version": version_two["execution"]["incremental_base_version"],
                "earliest_recomputed_layer": version_two["execution"]["earliest_recomputed_layer"],
                "reused_layers": version_two["execution"]["reused_layers"],
                "recomputed_layers": version_two["execution"]["recomputed_layers"],
                "verified_against_full": version_two["execution"]["incremental_verified_against_full"],
            },
            "rollback": {
                "published_over_budget_version": version_three["published_version"],
                "rolled_back": version_three["rolled_back"],
                "active_version_after_rollback": workspace.store.active_version(),
                "historical_version": 2,
                "result_matches_historical": historical_match,
                "budget_matches_historical": budget_match,
                "over_budget_layers": version_three["record"]["over_budget_layers"],
            },
            "version_replacement_rejected": replacement_results,
            "anomaly_codes": anomaly_results,
            "deterministic_digest": deterministic_digest,
        }

    destination = Path(report_path)
    atomic_write_json(destination, report)
    return {
        "workspace": str(workspace_path.resolve()),
        "report": str(destination.resolve()),
        "deterministic_digest": report["deterministic_digest"],
        "versions": report["versions"],
        "incremental": report["incremental"],
        "rollback": report["rollback"],
        "version_replacement_rejected": report["version_replacement_rejected"],
        "anomaly_codes": report["anomaly_codes"],
    }


def _version_replacement_faults(
    workspace: Workspace,
    fault_root: Path,
    snapshot_audit: Path,
    data: Path,
) -> dict[str, dict[str, Any]]:
    results: dict[str, dict[str, Any]] = {}

    whole_replace = _clone_workspace(workspace.root, fault_root / "whole_replace")
    shutil.rmtree(whole_replace.audit_root)
    shutil.copytree(snapshot_audit, whole_replace.audit_root)
    results["whole_history_replaced_with_v2"] = _capture(lambda: whole_replace.query())

    head_rollback = _clone_workspace(workspace.root, fault_root / "head_rollback")
    shutil.copyfile(snapshot_audit / "HEAD.json", head_rollback.audit_root / "HEAD.json")
    results["head_version_rolled_back"] = _capture(lambda: head_rollback.query())

    summary_tamper = _clone_workspace(workspace.root, fault_root / "summary_tamper")
    summary_path = summary_tamper.audit_root / "records" / "00000003.json"
    summary_record = read_json(summary_path)
    summary_record["result_summary"] = "f" * 64
    atomic_write_json(summary_path, summary_record)
    results["result_summary_tampered"] = _capture(lambda: summary_tamper.query())

    fingerprint_tamper = _clone_workspace(workspace.root, fault_root / "fingerprint_tamper")
    fingerprint_path = fingerprint_tamper.audit_root / "records" / "00000003.json"
    fingerprint_record = read_json(fingerprint_path)
    fingerprint_record["layer_fingerprints"]["linear_00"] = "e" * 64
    atomic_write_json(fingerprint_path, fingerprint_record)
    results["layer_fingerprint_tampered"] = _capture(lambda: fingerprint_tamper.query())

    missing_stage = _clone_workspace(workspace.root, fault_root / "missing_stage")
    (missing_stage.audit_root / "stages" / "00000002.npz").unlink()
    results["missing_incremental_stage"] = _capture(
        lambda: missing_stage.calibrate(
            mode="incremental",
            base_version=2,
            config_path=data / "config_incremental.yaml",
        )
    )
    return results


def _anomaly_faults(workspace: Workspace, fault_root: Path, data: Path) -> dict[str, dict[str, Any]]:
    results: dict[str, dict[str, Any]] = {}

    nan_calibration = fault_root / "nan_calibration.npy"
    calibration = np.load(data / "calibration_clean.npy", allow_pickle=False).copy()
    calibration[0, 0] = np.nan
    _write_npy(nan_calibration, calibration)
    nan_workspace = _clone_workspace(workspace.root, fault_root / "nan_case")
    results["activation_nan"] = _capture(
        lambda: nan_workspace.calibrate(
            mode="full",
            calibration_path=nan_calibration,
            config_path=data / "config.yaml",
        )
    )

    zero_bias_workspace = _clone_workspace(workspace.root, fault_root / "zero_scale_case")
    weights = load_weights(zero_bias_workspace.data_dir / "weights.npz")
    arrays: dict[str, np.ndarray] = {"layer_names": np.array(weights.layer_names)}
    for layer in weights.layers:
        arrays[f"{layer.name}.weight"] = layer.weight
        arrays[f"{layer.name}.bias"] = layer.bias.copy()
    arrays["linear_00.bias"][:] = 0.0
    atomic_write_npz(zero_bias_workspace.data_dir / "weights.npz", arrays)
    results["zero_quantization_scale"] = _capture(
        lambda: zero_bias_workspace.calibrate(mode="full", config_path=data / "config.yaml")
    )

    weights_bundle = load_weights(data / "weights.npz")
    gap_config = load_config(data / "config_attribution_gap.yaml", weights_bundle.layer_names)
    gap_calibration = load_calibration(data / "calibration_outlier.npy", gap_config.calibration_ratio)
    gap_engine = AuditEngine(gap_config, weights_bundle, gap_calibration)
    results["attribution_closure_gap"] = _capture(lambda: gap_engine.run_full())

    incremental_config = load_config(data / "config_incremental.yaml", weights_bundle.layer_names)
    incremental_calibration = load_calibration(
        data / "calibration_clean.npy",
        incremental_config.calibration_ratio,
    )
    incremental_engine = AuditEngine(incremental_config, weights_bundle, incremental_calibration)
    previous_record, previous_arrays = workspace.store.load_stage(2)
    tampered_arrays = {key: value.copy() for key, value in previous_arrays.items()}
    tampered_arrays["quant_out_0"] = tampered_arrays["quant_out_0"] + 0.01
    results["incremental_cache_mismatch"] = _capture(
        lambda: incremental_engine.run_incremental(previous_record, tampered_arrays)
    )

    results["stale_publication"] = _capture(
        lambda: workspace.store.publish_calibration(
            base_version=1,
            result={},
            arrays={},
            inputs={},
            layer_fingerprints={},
            budget_config_fingerprint="",
            execution={},
        )
    )
    results["rollback_to_unusable_version"] = _capture(lambda: workspace.store.rollback(3))
    return results


def _capture(function: Callable[[], Any]) -> dict[str, Any]:
    try:
        function()
    except AuditError as exc:
        return {
            "code": exc.code,
            "message": exc.message,
            "details": copy.deepcopy(exc.details),
        }
    return {"code": "E_EXPECTED_FAILURE_NOT_OBSERVED", "message": "operation unexpectedly succeeded", "details": {}}


def _clone_workspace(source: Path, destination: Path) -> Workspace:
    shutil.copytree(source, destination)
    return Workspace.open(destination)


def _calibration_summary(result: dict[str, Any]) -> dict[str, Any]:
    record = result["record"]
    return {
        "published_version": result["published_version"],
        "active_version": result["active_version"],
        "usable": result["usable"],
        "rolled_back": result["rolled_back"],
        "result_summary": result["result_summary"],
        "end_to_end_relative_l2": record["result"]["end_to_end"]["relative_l2"],
        "over_budget_layers": record["over_budget_layers"],
        "budget_usable": record["result"]["budget"]["usable"],
        "execution": result["execution"],
    }


def _base_config() -> dict[str, Any]:
    budgets = {name: 0.06 for name in LAYER_NAMES}
    return {
        "schema_version": 1,
        "calibration": {"ratio": 1.0},
        "quantization": {
            "bits": 8,
            "mode": "symmetric",
            "zero_point": "exact",
            "layers": {
                "linear_02": {"bits": 6, "mode": "asymmetric", "zero_point": "exact"},
                "linear_04": {"bits": 8, "mode": "asymmetric", "zero_point": "exact"},
            },
        },
        "budget": {
            "metric": "relative_l2",
            "per_layer": budgets,
            "total": 0.05,
        },
        "attribution": {
            "closure_tolerance": 0.95,
            "upstream_share_threshold": 0.5,
        },
        "runtime": {"verify_incremental": True},
    }


def _write_npy(path: Path, array: np.ndarray) -> None:
    payload = io.BytesIO()
    np.save(payload, np.ascontiguousarray(array), allow_pickle=False)
    atomic_write_bytes(path, payload.getvalue())


def _write_yaml(path: Path, value: dict[str, Any]) -> None:
    text = yaml.safe_dump(value, sort_keys=False, allow_unicode=True, default_flow_style=False)
    atomic_write_bytes(path, text.encode("utf-8"))

