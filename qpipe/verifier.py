from __future__ import annotations

from pathlib import Path
from typing import Any

from . import PIPELINE_VERSION, SCHEMA_VERSION
from .checkpoints import load_cached_stage
from .config import STAGES, compute_stage_fingerprints, load_config_by_id
from .errors import InvalidParam, VerifyFailed
from .util import canonical_json_bytes, digest_bytes, digest_obj, load_json


OUTPUT_FILES = ("result.json", "error_report.json", "layer_errors.json", "SHA256SUMS")


def _check(checks: list[dict[str, Any]], name: str, condition: bool, detail: Any = None) -> None:
    checks.append({"name": name, "ok": bool(condition), "detail": detail})


def _load_config_for_state(root: Path, state: dict[str, Any]) -> dict[str, Any]:
    config = load_config_by_id(root, state["config_id"])
    if config is None:
        raise InvalidParam(
            "run configuration snapshot is missing",
            parameter="config_id",
            details={"config_id": state.get("config_id")},
        )
    config["config_dir"] = str(root / "configs" / state["config_id"])
    config["config_version"] = state["config_version"]
    return config


def verify_run(
    *,
    root: Path,
    run_id: str,
    compare_run_id: str | None = None,
) -> dict[str, Any]:
    run_dir = root / "runs" / run_id
    state_path = run_dir / "run_state.json"
    if not state_path.is_file():
        raise InvalidParam(
            "run state not found", parameter="run_id", details={"run_id": run_id}
        )
    state = load_json(state_path)
    config = _load_config_for_state(root, state)
    fingerprints = compute_stage_fingerprints(config)
    checks: list[dict[str, Any]] = []

    _check(
        checks,
        "stage_fingerprints_match_config",
        state.get("stage_fingerprints") == fingerprints["stages"],
        {"recorded": state.get("stage_fingerprints"), "expected": fingerprints["stages"]},
    )
    _check(
        checks,
        "pipeline_fingerprint_matches_config",
        state.get("pipeline_fingerprint") == fingerprints["pipeline"],
    )

    ledger_path = run_dir / "publish_ledger.json"
    _check(checks, "publish_ledger_present", ledger_path.is_file())
    ledger = load_json(ledger_path) if ledger_path.is_file() else None
    checkpoint_dir = run_dir / "checkpoints"
    checkpoint_paths = (
        sorted(
            checkpoint_dir.glob(
                "[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9].json"
            )
        )
        if checkpoint_dir.is_dir()
        else []
    )
    checkpoints = [load_json(path) for path in checkpoint_paths]
    layer_count = len(config["manifest"]["operator_chain"])
    expected_checkpoints = len(STAGES) * (layer_count + 1)
    _check(
        checks,
        "checkpoint_count",
        len(checkpoints) == expected_checkpoints,
        {"actual": len(checkpoints), "expected": expected_checkpoints},
    )
    _check(
        checks,
        "checkpoint_sequences_contiguous",
        [item.get("sequence") for item in checkpoints]
        == list(range(1, len(checkpoints) + 1)),
    )

    if ledger is not None:
        _check(
            checks,
            "ledger_config_id",
            ledger.get("config_id") == config["config_id"],
        )
        _check(
            checks,
            "ledger_fingerprints",
            ledger.get("stage_fingerprints") == fingerprints["stages"],
        )

    stage_payloads: dict[str, dict[str, Any]] = {}
    for stage in STAGES:
        expected_fp = fingerprints["stages"][stage]
        cached = load_cached_stage(root, stage, expected_fp)
        _check(checks, f"{stage}_cache_present", cached is not None)
        if cached is None:
            continue
        payload = cached["payload"]
        stage_payloads[stage] = payload
        _check(
            checks,
            f"{stage}_cache_config",
            cached["manifest"].get("config_id") == config["config_id"],
        )
        _check(
            checks,
            f"{stage}_cache_input_fingerprint",
            cached["manifest"].get("input_fingerprint")
            == payload.get("input_fingerprint"),
        )
        expected_layers = config["manifest"]["operator_chain"]
        _check(
            checks,
            f"{stage}_layer_set",
            list(payload.get("layers", {}).keys()) == expected_layers,
        )
        if ledger is not None:
            entry = ledger.get("stages", {}).get(stage, {})
            _check(
                checks,
                f"{stage}_ledger_complete",
                entry.get("status") == "complete",
            )
            _check(
                checks,
                f"{stage}_cache_artifact_matches_ledger",
                entry.get("stage_artifact_digest") == cached["artifact_sha256"],
            )
            published = entry.get("published_layers", [])
            _check(
                checks,
                f"{stage}_layer_sequence",
                [item.get("layer_seq") for item in published]
                == list(range(1, layer_count + 1)),
            )
            _check(
                checks,
                f"{stage}_layer_identity",
                [item.get("layer") for item in published] == expected_layers,
            )
            for item in published:
                payload_file = run_dir / item["payload_file"]
                valid = payload_file.is_file() and digest_obj(load_json(payload_file)) == item[
                    "artifact_digest"
                ]
                _check(
                    checks,
                    f"{stage}_layer_{item.get('layer_seq')}_payload_digest",
                    valid,
                )

    calibration = stage_payloads.get("calibration", {}).get("layers", {})
    for layer_id, layer_result in calibration.items():
        for role in ("weight", "activation"):
            params = layer_result[role]
            scale_ok = all(value > 0 for value in _as_list(params["scale"]))
            zero_ok = all(
                params["qmin"] <= value <= params["qmax"]
                for value in _as_list(params["zero_point"], integer=True)
            )
            _check(checks, f"calibration_{layer_id}_{role}_scale_positive", scale_ok)
            _check(checks, f"calibration_{layer_id}_{role}_zero_in_range", zero_ok)
            _check(
                checks,
                f"calibration_{layer_id}_{role}_bounds",
                params["qmin"] < params["qmax"],
            )
            observed_min = _as_list(params["observed_min"])
            observed_max = _as_list(params["observed_max"])
            representable_min = _as_list(params["representable_min"])
            representable_max = _as_list(params["representable_max"])
            coverage_ok = len(observed_min) == len(representable_min) == len(
                observed_max
            ) == len(representable_max) and all(
                rep_min <= obs_min + 1e-12 * max(1.0, abs(obs_min))
                and rep_max >= obs_max - 1e-12 * max(1.0, abs(obs_max))
                for obs_min, obs_max, rep_min, rep_max in zip(
                    observed_min,
                    observed_max,
                    representable_min,
                    representable_max,
                    strict=True,
                )
            )
            _check(
                checks,
                f"calibration_{layer_id}_{role}_covers_extrema",
                coverage_ok,
                {
                    "observed_min": observed_min,
                    "observed_max": observed_max,
                    "representable_min": representable_min,
                    "representable_max": representable_max,
                },
            )

    attribution = stage_payloads.get("attribution")
    if attribution is not None:
        consistency = attribution["contribution_consistency"]
        deviation = attribution["end_to_end"]["absolute_l1_deviation"]
        tolerance = consistency["declared_tolerance"]
        _check(
            checks,
            "sensitivity_contribution_sum",
            abs(consistency["first_order_sensitivity_sum"] - deviation) <= tolerance,
            {
                "sum": consistency["first_order_sensitivity_sum"],
                "deviation": deviation,
                "tolerance": tolerance,
            },
        )
        _check(
            checks,
            "replay_contribution_sum",
            abs(consistency["layer_perturbation_replay_sum"] - deviation) <= tolerance,
            {
                "sum": consistency["layer_perturbation_replay_sum"],
                "deviation": deviation,
                "tolerance": tolerance,
            },
        )
        _check(
            checks,
            "attribution_bases_preserved",
            all(
                "first_order_sensitivity" in item["attribution"]
                and "layer_perturbation_replay" in item["attribution"]
                for item in attribution["layers"].values()
            ),
        )
        _check(
            checks,
            "review_required_layers_preserved",
            attribution["ranking"]["review_required_layers"]
            == sorted(
                layer_id
                for layer_id, item in attribution["layers"].items()
                if item["attribution"]["review_required"]
            ),
        )

    output_dir = run_dir / "output"
    for name in OUTPUT_FILES:
        _check(checks, f"output_{name}_present", (output_dir / name).is_file())
    if all((output_dir / name).is_file() for name in OUTPUT_FILES):
        result_bytes = (output_dir / "result.json").read_bytes()
        error_bytes = (output_dir / "error_report.json").read_bytes()
        layer_bytes = (output_dir / "layer_errors.json").read_bytes()
        combined = digest_bytes(result_bytes + error_bytes + layer_bytes)
        sums_text = (output_dir / "SHA256SUMS").read_text(encoding="ascii")
        _check(
            checks,
            "output_digest_line",
            f"{combined}  pipeline-output\n" in sums_text,
            {"actual": combined},
        )
        result = load_json(output_dir / "result.json")
        error_report = load_json(output_dir / "error_report.json")
        layer_errors = load_json(output_dir / "layer_errors.json")
        _check(
            checks,
            "result_pipeline_fingerprint",
            result.get("pipeline_fingerprint") == fingerprints["pipeline"],
        )
        _check(
            checks,
            "result_layer_order",
            [item["layer"] for item in result.get("layers", [])]
            == config["manifest"]["operator_chain"],
        )
        _check(
            checks,
            "result_status_consistent",
            (result.get("status") == "pass" and not result.get("violations"))
            or (result.get("status") == "fail" and bool(result.get("violations"))),
        )
        _check(
            checks,
            "error_report_status_consistent",
            (
                result.get("status") == "pass"
                and error_report.get("status") == "pass"
                and error_report.get("code") is None
            )
            or (
                result.get("status") == "fail"
                and error_report.get("code") == "E_TOLERANCE_EXCEEDED"
            ),
        )
        _check(
            checks,
            "layer_report_matches_result",
            layer_errors.get("layers") == result.get("layers"),
        )

    byte_identical: bool | None = None
    comparison: dict[str, Any] | None = None
    if compare_run_id is not None:
        other_dir = root / "runs" / compare_run_id / "output"
        comparisons: dict[str, bool] = {}
        for name in OUTPUT_FILES:
            path = output_dir / name
            other = other_dir / name
            comparisons[name] = (
                path.is_file()
                and other.is_file()
                and path.read_bytes() == other.read_bytes()
            )
        byte_identical = all(comparisons.values())
        comparison = {"run_id": compare_run_id, "files": comparisons}
        _check(checks, "byte_identical_to_comparison_run", byte_identical, comparison)

    ok = all(item["ok"] for item in checks)
    summary = {
        "ok": ok,
        "run_id": run_id,
        "config_id": config["config_id"],
        "pipeline_version": PIPELINE_VERSION,
        "schema_version": SCHEMA_VERSION,
        "business_status": state.get("status"),
        "final_digest": state.get("final_digest"),
        "checkpoint_count": len(checkpoints),
        "checks": checks,
        "compared_run_id": compare_run_id,
        "byte_identical": byte_identical,
        "comparison": comparison,
    }
    if not ok:
        failed = [item for item in checks if not item["ok"]]
        raise VerifyFailed(
            "run verification failed",
            failed_checks=failed,
            summary=summary,
        )
    return summary


def _as_list(value: Any, *, integer: bool = False) -> list[Any]:
    if isinstance(value, list):
        return value
    return [int(value) if integer else float(value)]
