"""Exposure-to-conversion funnel checks without label repair or probability clipping."""

from __future__ import annotations

import math
from typing import Any

from .errors import ErrorCode
from .training import ScoreRecord


FUNNEL_FORMAT = "ctr-cvr-funnel-diagnostics-v1"


def label_diagnostics(violations: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "format": FUNNEL_FORMAT,
        "stage": "funnel",
        "violation_count": len(violations),
        "violations": violations,
        "score_checks": {
            "checked_rows": 0,
            "checks": [
                "conversion_label_requires_exposure",
            ],
        },
    }


def evaluate_scores(records: list[ScoreRecord], confidence_threshold: float) -> dict[str, Any]:
    threshold_log = math.log(confidence_threshold)
    violations: list[dict[str, Any]] = []
    checked_cvr_le_ctr = 0
    checked_conversion_interval = 0
    checked_threshold_consistency = 0
    for record in records:
        if not math.isfinite(record.log_cvr) or record.log_cvr > record.log_ctr:
            violations.append(
                {
                    "code": ErrorCode.E_FUNNEL_VIOLATION,
                    "check": "cvr_probability_not_greater_than_ctr",
                    "message": "log(CVR) must be <= log(CTR)",
                    "violated_value": {"log_ctr": record.log_ctr, "log_cvr": record.log_cvr},
                    "shard_id": record.shard_id,
                    "row_number": record.row_number,
                    "impression_id": record.impression_id,
                }
            )
        else:
            checked_cvr_le_ctr += 1

        log_conversion = record.log_conversion
        if (
            not math.isfinite(log_conversion)
            or log_conversion > 0.0
            or log_conversion > record.log_ctr
        ):
            violations.append(
                {
                    "code": ErrorCode.E_FUNNEL_VIOLATION,
                    "check": "combined_probability_in_unit_interval",
                    "message": "log(CTR*CVR) must be finite, <= 0 and <= log(CTR)",
                    "violated_value": {
                        "log_ctr": record.log_ctr,
                        "log_cvr": record.log_cvr,
                        "log_conversion": log_conversion,
                    },
                    "shard_id": record.shard_id,
                    "row_number": record.row_number,
                    "impression_id": record.impression_id,
                }
            )
        else:
            checked_conversion_interval += 1

        conversion_confident = log_conversion >= threshold_log
        ctr_confident = record.log_ctr >= threshold_log
        if conversion_confident and not ctr_confident:
            violations.append(
                {
                    "code": ErrorCode.E_FUNNEL_VIOLATION,
                    "check": "threshold_conversion_implies_exposure",
                    "message": "conversion above threshold must imply CTR above threshold",
                    "violated_value": {
                        "log_ctr": record.log_ctr,
                        "log_conversion": log_conversion,
                        "threshold_log": threshold_log,
                    },
                    "shard_id": record.shard_id,
                    "row_number": record.row_number,
                    "impression_id": record.impression_id,
                }
            )
        else:
            checked_threshold_consistency += 1

    return {
        "format": FUNNEL_FORMAT,
        "stage": "funnel",
        "violation_count": len(violations),
        "violations": violations,
        "score_checks": {
            "checked_rows": len(records),
            "cvr_le_ctr_passed": checked_cvr_le_ctr,
            "combined_interval_passed": checked_conversion_interval,
            "threshold_consistency_passed": checked_threshold_consistency,
            "confidence_threshold": confidence_threshold,
            "confidence_threshold_log": threshold_log,
            "probability_policy": "no clipping, no truncation",
            "checks": [
                "cvr_probability_not_greater_than_ctr",
                "combined_probability_in_unit_interval",
                "threshold_conversion_implies_exposure",
            ],
        },
    }
