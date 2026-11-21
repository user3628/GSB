"""Deterministic quality diagnostics and threshold sensitivity analysis."""

from __future__ import annotations

import math
from typing import Any

import numpy as np

from .errors import WorkbenchError


def _wilson_interval(successes: int, total: int, z: float) -> dict[str, float]:
    if total <= 0:
        return {"low": 0.0, "high": 0.0, "half_width": 0.0}
    probability = successes / total
    denominator = 1.0 + (z * z) / total
    center = (probability + (z * z) / (2.0 * total)) / denominator
    margin = (
        z
        * math.sqrt((probability * (1.0 - probability) / total) + (z * z) / (4.0 * total * total))
        / denominator
    )
    return {
        "low": float(max(0.0, center - margin)),
        "high": float(min(1.0, center + margin)),
        "half_width": float(margin),
    }


def _rate_record(decisions: np.ndarray, z: float) -> dict[str, Any]:
    total = int(len(decisions))
    detected = int(np.count_nonzero(decisions))
    probability = float(detected / total) if total else 0.0
    return {
        "count": total,
        "detected": detected,
        "detection_rate": probability,
        "error_band": _wilson_interval(detected, total, z),
    }


def build_diagnostics(
    score_artifact: dict[str, Any],
    declaration: dict[str, Any],
    config: dict[str, Any],
) -> dict[str, Any]:
    payload = score_artifact["payload"]
    scores = np.asarray(payload["combined_scores"], dtype=np.float64)
    decisions = np.asarray(payload["decisions"], dtype=np.int64)
    source_ids = [str(value) for value in payload["source_ids"]]
    batch_ids = np.asarray(payload["batch_ids"], dtype=np.int64)
    if scores.ndim != 1 or len(scores) != len(decisions) or len(scores) != len(source_ids):
        raise WorkbenchError("E_DATA", "score artifact arrays have inconsistent lengths", stage="diagnostics")
    if np.any(~np.isfinite(scores)):
        row = int(np.flatnonzero(~np.isfinite(scores))[0])
        raise WorkbenchError(
            "E_NUMERIC",
            "diagnostics received a non-finite score",
            stage="diagnostics",
            sample_id=source_ids[row],
            details={"row": row, "value": float(scores[row])},
        )
    if int(declaration.get("rows", -1)) != len(scores):
        raise WorkbenchError(
            "E_DATA",
            "declaration rows differ from score count",
            stage="diagnostics",
            details={"declared": declaration.get("rows"), "observed": len(scores)},
        )
    if int(declaration.get("columns", -1)) != int(payload.get("column_count", declaration.get("columns", -1))):
        # The score artifact may omit column_count; the check only applies when present.
        observed = payload.get("column_count")
        if observed is not None and int(observed) != int(declaration.get("columns", -1)):
            raise WorkbenchError(
                "E_DATA",
                "declaration columns differ from scored matrix",
                stage="diagnostics",
                details={"declared": declaration.get("columns"), "observed": observed},
            )

    diagnostics_config = config["diagnostics"]
    z = float(diagnostics_config["confidence_z"])
    quantiles = {
        f"q{str(float(value))}": float(np.quantile(scores, value, method="linear"))
        for value in diagnostics_config["quantiles"]
    }
    overall = _rate_record(decisions, z)

    source_groups: dict[str, dict[str, Any]] = {}
    for source in sorted(set(source_ids)):
        mask = np.asarray([value == source for value in source_ids], dtype=bool)
        source_groups[source] = _rate_record(decisions[mask], z)

    batch_groups: dict[str, dict[str, Any]] = {}
    for batch in sorted(set(int(value) for value in batch_ids.tolist())):
        mask = batch_ids == batch
        batch_groups[str(batch)] = _rate_record(decisions[mask], z)

    sensitivity: list[dict[str, Any]] = []
    for quantile in diagnostics_config["sensitivity_quantiles"]:
        q = float(quantile)
        threshold = float(np.quantile(scores, q, method="linear"))
        curve_decisions = scores > threshold
        curve_record = _rate_record(curve_decisions, z)
        estimated_false_positive_rate = 1.0 - q
        estimated_false_positive_count = estimated_false_positive_rate * len(scores)
        false_positive_band = _wilson_interval(
            int(round(estimated_false_positive_count)), len(scores), z
        )
        sensitivity.append(
            {
                "quantile": q,
                "threshold": threshold,
                "detected": curve_record["detected"],
                "detection_rate": curve_record["detection_rate"],
                "detection_rate_error_band": curve_record["error_band"],
                "estimated_false_positive_rate": estimated_false_positive_rate,
                "estimated_false_positive_count": estimated_false_positive_count,
                "false_positive_error_band": false_positive_band,
            }
        )

    payload_out = {
        "tier": payload["tier"],
        "row_count": int(len(scores)),
        "declaration_scope": {
            "rows": int(declaration["rows"]),
            "columns": int(declaration["columns"]),
            "missing_column_indices": list(declaration.get("missing_column_indices", [])),
            "heavy_tail_column_indices": list(declaration.get("heavy_tail_column_indices", [])),
            "expected_statistics": declaration.get("expected_statistics", {}),
        },
        "score_distribution": {
            "count": int(len(scores)),
            "minimum": float(np.min(scores)),
            "maximum": float(np.max(scores)),
            "mean": float(np.mean(scores)),
            "standard_deviation": float(np.std(scores, ddof=0)),
            "quantiles": quantiles,
            "quantile_method": "linear",
        },
        "threshold": dict(payload["threshold"]),
        "overall": overall,
        "by_source": source_groups,
        "by_batch": batch_groups,
        "sensitivity": sensitivity,
        "sample_size_error_model": {
            "method": "wilson_score_interval",
            "confidence_z": z,
            "row_count": int(len(scores)),
            "overall_half_width": overall["error_band"]["half_width"],
        },
        "score_artifact_digest": score_artifact.get("artifact_digest"),
        "diagnostic_basis": "explicit model scores, decisions and declaration values only",
    }
    return payload_out
