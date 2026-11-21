from __future__ import annotations

from typing import Any

import numpy as np


def validate_attribution(
    *, config: dict[str, Any], attribution_payload: dict[str, Any]
) -> dict[str, Any]:
    layer_results: dict[str, Any] = {}
    violations: list[dict[str, Any]] = []
    margins: list[dict[str, Any]] = []

    for layer in config["manifest"]["layers"]:
        layer_id = layer["id"]
        source = attribution_payload["layers"][layer_id]
        metrics = source["actual_error"]
        tolerance = layer["tolerance"]
        max_abs_limit = float(tolerance["max_abs"])
        max_rel_limit = float(tolerance["max_rel"])
        max_abs_error = float(metrics["max_abs_error"])
        relative_error = float(metrics["relative_l1_error"])
        absolute_error = np.asarray(metrics["absolute_error"], dtype=np.float64)
        abs_ok = max_abs_error <= max_abs_limit
        rel_ok = relative_error <= max_rel_limit
        abs_margin = max_abs_limit - max_abs_error
        rel_margin = max_rel_limit - relative_error
        sample_max_error = np.asarray(metrics["sample_max_abs_error"], dtype=np.float64)
        layer_result = {
            "layer": layer_id,
            "source_layer": layer_id,
            "tolerance": {
                "max_abs_error": max_abs_limit,
                "max_rel_error": max_rel_limit,
            },
            "observed": {
                "max_abs_error": max_abs_error,
                "relative_l1_error": relative_error,
                "mean_abs_error": float(metrics["mean_abs_error"]),
                "absolute_l1_error": float(metrics["absolute_l1_error"]),
            },
            "margin": {
                "max_abs_error": abs_margin,
                "max_rel_error": rel_margin,
            },
            "within_tolerance": abs_ok and rel_ok,
            "sample_margin": [
                {
                    "sample": index,
                    "max_abs_error": float(value),
                    "margin": max_abs_limit - float(value),
                    "within_tolerance": bool(value <= max_abs_limit),
                }
                for index, value in enumerate(sample_max_error)
            ],
            "out_of_tolerance_samples": [
                int(index)
                for index, value in enumerate(sample_max_error)
                if float(value) > max_abs_limit
            ],
            "out_of_tolerance_positions": [
                {
                    "sample": int(position[0]),
                    "feature": int(position[1]),
                    "absolute_error": float(
                        absolute_error[position[0], position[1]]
                    ),
                }
                for position in np.argwhere(absolute_error > max_abs_limit)
            ],
            "max_abs_position": metrics["max_abs_position"],
            "attribution": {
                "first_order_sensitivity": source["attribution"][
                    "first_order_sensitivity"
                ],
                "layer_perturbation_replay": source["attribution"][
                    "layer_perturbation_replay"
                ],
                "review_required": source["attribution"]["review_required"],
            },
        }
        layer_results[layer_id] = layer_result
        margins.append(
            {
                "layer": layer_id,
                "kind": "layer",
                "metric": "max_abs_error",
                "margin": abs_margin,
                "absolute_margin": abs(abs_margin),
                "observed": max_abs_error,
                "limit": max_abs_limit,
                "within_tolerance": abs_ok,
            }
        )
        if not abs_ok:
            violations.append(
                {
                    "layer": layer_id,
                    "metric": "max_abs_error",
                    "observed": max_abs_error,
                    "limit": max_abs_limit,
                    "excess": max_abs_error - max_abs_limit,
                    "out_of_tolerance_samples": layer_result[
                        "out_of_tolerance_samples"
                    ],
                    "out_of_tolerance_positions": layer_result[
                        "out_of_tolerance_positions"
                    ],
                }
            )
        if not rel_ok:
            violations.append(
                {
                    "layer": layer_id,
                    "metric": "relative_l1_error",
                    "observed": relative_error,
                    "limit": max_rel_limit,
                    "excess": relative_error - max_rel_limit,
                    "out_of_tolerance_samples": [],
                    "out_of_tolerance_positions": [],
                }
            )

    end_to_end_tolerance = config["manifest"].get("end_to_end_tolerance", {})
    end_to_end_limit_abs = float(end_to_end_tolerance.get("max_abs", float("inf")))
    end_to_end_limit_rel = float(end_to_end_tolerance.get("max_rel", float("inf")))
    end_to_end = attribution_payload["end_to_end"]
    end_to_end_abs = float(end_to_end["max_abs_deviation"])
    end_to_end_rel = float(end_to_end["relative_l1_deviation"])
    end_to_end_abs_margin = end_to_end_limit_abs - end_to_end_abs
    end_to_end_rel_margin = end_to_end_limit_rel - end_to_end_rel
    end_to_end_ok = (
        end_to_end_abs <= end_to_end_limit_abs
        and end_to_end_rel <= end_to_end_limit_rel
    )
    margins.append(
        {
            "layer": None,
            "kind": "end_to_end",
            "metric": "max_abs_deviation",
            "margin": end_to_end_abs_margin,
            "absolute_margin": abs(end_to_end_abs_margin),
            "observed": end_to_end_abs,
            "limit": end_to_end_limit_abs,
            "within_tolerance": end_to_end_ok,
        }
    )
    if not end_to_end_ok:
        violations.append(
            {
                "layer": None,
                "metric": "end_to_end_max_abs_deviation",
                "observed": end_to_end_abs,
                "limit": end_to_end_limit_abs,
                "excess": end_to_end_abs - end_to_end_limit_abs,
                "out_of_tolerance_samples": [],
                "out_of_tolerance_positions": [],
            }
        )

    consistency = attribution_payload["contribution_consistency"]
    attribution_consistent = bool(consistency["consistent"])
    if not attribution_consistent:
        violations.append(
            {
                "layer": None,
                "metric": "attribution_contribution_sum",
                "observed": max(
                    consistency["first_order_sensitivity_residual"],
                    consistency["layer_perturbation_replay_residual"],
                ),
                "limit": consistency["declared_tolerance"],
                "excess": 0.0,
                "out_of_tolerance_samples": [],
                "out_of_tolerance_positions": [],
            }
        )

    closest = min(
        margins,
        key=lambda item: (
            item["absolute_margin"],
            "" if item["layer"] is None else item["layer"],
            item["metric"],
        ),
    )
    ranked_violations = sorted(
        violations,
        key=lambda item: (
            -float(item["excess"]),
            "" if item["layer"] is None else item["layer"],
            item["metric"],
        ),
    )
    failed = bool(ranked_violations)
    return {
        "stage": "validation",
        "status": "fail" if failed else "pass",
        "code": "E_TOLERANCE_EXCEEDED" if failed else None,
        "end_to_end": {
            "absolute_l1_deviation": float(end_to_end["absolute_l1_deviation"]),
            "max_abs_deviation": end_to_end_abs,
            "relative_l1_deviation": end_to_end_rel,
            "sample_absolute_l1_deviation": end_to_end.get(
                "sample_absolute_l1_deviation", []
            ),
            "tolerance": {
                "max_abs_deviation": end_to_end_limit_abs,
                "max_rel_deviation": end_to_end_limit_rel,
            },
            "margin": {
                "max_abs_deviation": end_to_end_abs_margin,
                "max_rel_deviation": end_to_end_rel_margin,
            },
            "within_tolerance": end_to_end_ok,
        },
        "layers": layer_results,
        "violations": ranked_violations,
        "closest_to_tolerance": closest,
        "attribution_consistency": consistency,
        "review_required_layers": attribution_payload["ranking"][
            "review_required_layers"
        ],
        "input_fingerprint": attribution_payload["input_fingerprint"],
    }


def tolerance_error(result: dict[str, Any]) -> dict[str, Any]:
    violations = result.get("violations", [])
    first = next(
        (item for item in violations if item.get("layer") is not None),
        None,
    )
    if first is None and result.get("layers"):
        layer_id, layer = max(
            result["layers"].items(),
            key=lambda item: (item[1]["observed"]["max_abs_error"], item[0]),
        )
        first = {
            "layer": layer_id,
            "metric": "max_abs_error",
            "observed": layer["observed"]["max_abs_error"],
            "limit": layer["tolerance"]["max_abs_error"],
            "excess": layer["margin"]["max_abs_error"] * -1.0,
            "out_of_tolerance_samples": layer["out_of_tolerance_samples"],
            "out_of_tolerance_positions": layer["out_of_tolerance_positions"],
        }
    if first is None and violations:
        first = violations[0]
    return {
        "code": "E_TOLERANCE_EXCEEDED",
        "message": "one or more layer or end-to-end tolerances were exceeded",
        "layer": first.get("layer") if first else None,
        "max_deviation": first.get("observed") if first else None,
        "limit": first.get("limit") if first else None,
        "violations": violations,
        "out_of_tolerance_samples": {
            layer_id: layer["out_of_tolerance_samples"]
            for layer_id, layer in result["layers"].items()
            if layer["out_of_tolerance_samples"]
        },
        "out_of_tolerance_positions": {
            layer_id: layer["out_of_tolerance_positions"]
            for layer_id, layer in result["layers"].items()
            if layer["out_of_tolerance_positions"]
        },
        "closest_to_tolerance": result["closest_to_tolerance"],
    }
