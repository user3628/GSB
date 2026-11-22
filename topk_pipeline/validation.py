"""Final-stage output, shape, range and probabilistic consistency checks."""

from __future__ import annotations

import math
from typing import Any

from .errors import E_NUMERIC, PipelineError
from .util import digest_value


def validate_shard_output(
    filter_payload: dict[str, Any],
    normalized_payload: dict[str, Any],
    sampling_payload: dict[str, Any],
    *,
    shard_id: str,
    shard_ordinal: int,
    spec: dict[str, Any],
) -> dict[str, Any]:
    rows, k = (int(value) for value in spec["expected_shape"])
    expected_range = spec.get("expected_score_range")
    if expected_range is not None:
        low, high = (float(value) for value in expected_range)

    filter_rows = sorted(filter_payload["rows"], key=lambda item: item["row"])
    normalized_rows = sorted(normalized_payload["rows"], key=lambda item: item["row"])
    sampled_rows = sorted(sampling_payload["rows"], key=lambda item: item["row"])

    if not (len(filter_rows) == len(normalized_rows) == len(sampled_rows) == rows):
        raise PipelineError(
            code=E_NUMERIC,
            message="row count does not match the declared output shape",
            stage="validation",
            shard_id=shard_id,
            details={
                "expected_rows": rows,
                "filter_rows": len(filter_rows),
                "normalization_rows": len(normalized_rows),
                "sampling_rows": len(sampled_rows),
            },
        )

    output_rows: list[dict[str, Any]] = []
    for expected_row, (filter_row, normalized_row, sampled_row) in enumerate(
        zip(filter_rows, normalized_rows, sampled_rows)
    ):
        row = int(filter_row["row"])
        if not (int(filter_row["row"]) == int(normalized_row["row"]) == int(sampled_row["row"])):
            raise PipelineError(
                code=E_NUMERIC,
                message="row ordering mismatch between stages",
                stage="validation",
                shard_id=shard_id,
                row=row,
            )
        if len(filter_row["candidates"]) != k:
            raise PipelineError(
                code=E_NUMERIC,
                message=f"TopK candidate count must be {k}",
                stage="validation",
                shard_id=shard_id,
                row=row,
                details={"candidate_count": len(filter_row["candidates"]), "k": k},
            )

        topk_candidates: list[dict[str, Any]] = []
        for candidate in filter_row["candidates"]:
            score = float(candidate["score"])
            if not math.isfinite(score):
                raise PipelineError(
                    code=E_NUMERIC,
                    message="non-finite TopK score reached validation",
                    stage="validation",
                    shard_id=shard_id,
                    row=row,
                    details={"token": int(candidate["token"])},
                )
            if expected_range is not None and not (low <= score <= high):
                raise PipelineError(
                    code=E_NUMERIC,
                    message="TopK score is outside the declared score range",
                    stage="validation",
                    shard_id=shard_id,
                    row=row,
                    details={
                        "token": int(candidate["token"]),
                        "score": score,
                        "expected_score_range": [low, high],
                    },
                )
            topk_candidates.append(
                {
                    "token": int(candidate["token"]),
                    "score": score,
                    "rank": int(candidate["rank"]),
                    "source_column_block": int(candidate["source_column_block"]),
                    "admission_turn": int(candidate["admission_turn"]),
                    "comparison_order": int(candidate["comparison_order"]),
                }
            )

        kept = sampled_row["kept_candidates"]
        kept_tokens = {int(candidate["token"]) for candidate in kept}
        if int(sampled_row["sampled_token"]) not in kept_tokens:
            raise PipelineError(
                code=E_NUMERIC,
                message="sampled token is outside the retained nucleus set",
                stage="validation",
                shard_id=shard_id,
                row=row,
            )
        probability_sum = float(
            sum(float(candidate["probability"]) for candidate in kept)
        )
        if not math.isfinite(probability_sum) or abs(probability_sum - 1.0) > 1e-9:
            raise PipelineError(
                code=E_NUMERIC,
                message="retained probabilities do not sum to one",
                stage="validation",
                shard_id=shard_id,
                row=row,
                details={"probability_sum": probability_sum},
            )
        sampled_log_probability = float(sampled_row["sample_log_probability"])
        if not math.isfinite(sampled_log_probability):
            raise PipelineError(
                code=E_NUMERIC,
                message="sampled log probability is not finite",
                stage="validation",
                shard_id=shard_id,
                row=row,
            )

        output_rows.append(
            {
                "row": row,
                "topk_candidates": topk_candidates,
                "nucleus_enabled": bool(sampled_row["nucleus_enabled"]),
                "cutoff_rank": int(sampled_row["cutoff_rank"]),
                "equal_probability_boundary": bool(sampled_row["equal_probability_boundary"]),
                "kept_candidates": kept,
                "truncated_candidates": sampled_row["truncated_candidates"],
                "sampled_token": int(sampled_row["sampled_token"]),
                "sample_log_probability": sampled_log_probability,
            }
        )

    result: dict[str, Any] = {
        "shard_id": shard_id,
        "shard_ordinal": int(shard_ordinal),
        "shape": [rows, k],
        "k": int(k),
        "temperature": float(spec["temperature"]),
        "nucleus_enabled": bool(spec["nucleus_enabled"]),
        "top_p": float(spec["top_p"]) if spec["nucleus_enabled"] else None,
        "seed": int(spec["seed"]),
        "evaluated_elements": int(filter_payload["compared_elements"])
        + 3 * rows * int(k),
        "rows": output_rows,
    }
    result["artifact_digest"] = digest_value(result)
    return result
