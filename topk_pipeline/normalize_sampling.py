"""Log-domain normalization, nucleus truncation and deterministic sampling."""

from __future__ import annotations

import math
from typing import Any

import numpy as np

from .errors import E_INVALID_NUCLEUS, E_INVALID_TEMP, E_NUMERIC, PipelineError
from .util import as_plain_number, finite_or_negative_inf


def _validate_temperature(temperature: Any, shard_id: str) -> float:
    try:
        value = float(temperature)
    except (TypeError, ValueError):
        value = math.nan
    if not math.isfinite(value) or value <= 0.0:
        raise PipelineError(
            code=E_INVALID_TEMP,
            message=f"temperature must be finite and > 0, received {temperature!r}",
            stage="normalize",
            shard_id=shard_id,
            details={"temperature": repr(temperature)},
        )
    return value


def normalize_topk(
    filter_payload: dict[str, Any],
    *,
    temperature: Any,
    shard_id: str,
) -> dict[str, Any]:
    """Compute stable log-softmax over each row's TopK candidates."""

    temperature_value = _validate_temperature(temperature, shard_id)
    normalized_rows: list[dict[str, Any]] = []

    for row_payload in sorted(filter_payload["rows"], key=lambda item: item["row"]):
        row = int(row_payload["row"])
        candidates = row_payload["candidates"]
        if not candidates:
            raise PipelineError(
                code=E_NUMERIC,
                message="cannot normalize an empty TopK candidate set",
                stage="normalize",
                shard_id=shard_id,
                row=row,
                details={"cutoff_position": 0},
            )
        scores = np.asarray([as_plain_number(candidate["score"]) for candidate in candidates], dtype=np.float64)
        maximum = float(np.max(scores))
        if not math.isfinite(maximum):
            raise PipelineError(
                code=E_NUMERIC,
                message="row has no finite maximum score after TopK filtering",
                stage="normalize",
                shard_id=shard_id,
                row=row,
                details={"max_score": "-Infinity", "candidate_count": len(candidates)},
            )

        scaled_logits = (scores - maximum) / temperature_value
        exp_shifted = np.exp(scaled_logits)
        probability_sum = float(np.sum(exp_shifted))
        if not math.isfinite(probability_sum) or probability_sum <= 0.0:
            raise PipelineError(
                code=E_NUMERIC,
                message="log-domain normalization produced a zero or non-finite probability sum",
                stage="normalize",
                shard_id=shard_id,
                row=row,
                details={"cutoff_position": 0, "probability_sum": repr(probability_sum)},
            )
        logsumexp = math.log(probability_sum)
        probabilities = exp_shifted / probability_sum
        log_probabilities = scaled_logits - logsumexp

        out_candidates: list[dict[str, Any]] = []
        for index, candidate in enumerate(candidates):
            out_candidates.append(
                {
                    "token": int(candidate["token"]),
                    "score": finite_or_negative_inf(float(as_plain_number(candidate["score"]))),
                    "scaled_logit": finite_or_negative_inf(float(scaled_logits[index])),
                    "probability": float(probabilities[index]),
                    "log_probability": finite_or_negative_inf(float(log_probabilities[index])),
                    "rank": int(candidate["rank"]),
                    "source_column_block": int(candidate["source_column_block"]),
                    "admission_turn": int(candidate["admission_turn"]),
                    "comparison_order": int(candidate["comparison_order"]),
                }
            )
        normalized_rows.append(
            {
                "row": row,
                "max_score": maximum,
                "logsumexp": logsumexp,
                "candidates": out_candidates,
            }
        )

    return {
        "shard_id": shard_id,
        "stage": "normalize",
        "temperature": temperature_value,
        "rows": normalized_rows,
    }


def _seed_for_row(seed: int, shard_ordinal: int, row: int) -> np.random.SeedSequence:
    seed_u64 = int(seed) % (1 << 64)
    return np.random.SeedSequence([seed_u64, int(shard_ordinal), int(row)])


def nucleus_and_sample(
    normalized_payload: dict[str, Any],
    *,
    shard_id: str,
    shard_ordinal: int,
    seed: int,
    nucleus_enabled: bool,
    top_p: float,
) -> dict[str, Any]:
    """Apply deterministic top-p prefix truncation and draw one token per row."""

    threshold: float | None = None
    if nucleus_enabled:
        try:
            threshold = float(top_p)
        except (TypeError, ValueError):
            threshold = math.nan
        if not math.isfinite(threshold) or threshold <= 0.0 or threshold > 1.0:
            raise PipelineError(
                code=E_INVALID_NUCLEUS,
                message=f"nucleus top_p must be in (0, 1], received {top_p!r}",
                stage="sampling",
                shard_id=shard_id,
                details={"top_p": repr(top_p)},
            )

    sampled_rows: list[dict[str, Any]] = []
    for row_payload in sorted(normalized_payload["rows"], key=lambda item: item["row"]):
        row = int(row_payload["row"])
        candidates = row_payload["candidates"]
        probabilities = np.asarray(
            [candidate["probability"] for candidate in candidates], dtype=np.float64
        )
        if (
            probabilities.size == 0
            or not np.all(np.isfinite(probabilities))
            or np.any(probabilities < 0.0)
            or float(np.sum(probabilities)) <= 0.0
        ):
            raise PipelineError(
                code=E_NUMERIC,
                message="candidate probability vector is empty, invalid or sums to zero",
                stage="sampling",
                shard_id=shard_id,
                row=row,
                details={"cutoff_position": 0},
            )

        if nucleus_enabled:
            cumulative = np.cumsum(probabilities)
            boundary = int(np.searchsorted(cumulative, float(threshold), side="left"))
            if boundary >= len(probabilities):
                boundary = len(probabilities) - 1
            keep_length = boundary + 1
        else:
            keep_length = len(probabilities)

        # A rank-prefix cut is used even when equal probabilities straddle the
        # boundary. Ties are therefore resolved by the earlier (lower token)
        # rank, never by hash-map or worker completion order.
        equal_probability_boundary = bool(
            keep_length < len(probabilities)
            and probabilities[keep_length - 1] == probabilities[keep_length]
        )
        kept_raw = probabilities[:keep_length]
        kept_sum = float(np.sum(kept_raw))
        if not math.isfinite(kept_sum) or kept_sum <= 0.0:
            raise PipelineError(
                code=E_NUMERIC,
                message="nucleus truncation left a zero or non-finite probability mass",
                stage="sampling",
                shard_id=shard_id,
                row=row,
                details={
                    "cutoff_position": keep_length,
                    "cutoff_rank": keep_length,
                    "top_p": threshold,
                },
            )

        kept_probabilities = kept_raw / kept_sum
        rng = np.random.Generator(
            np.random.PCG64(_seed_for_row(int(seed), shard_ordinal, row))
        )
        uniform = float(rng.random())
        cumulative_kept = np.cumsum(kept_probabilities)
        selected_index = int(np.searchsorted(cumulative_kept, uniform, side="right"))
        if selected_index >= keep_length:
            selected_index = keep_length - 1
        selected_probability = float(kept_probabilities[selected_index])
        selected_candidate = candidates[selected_index]

        kept_candidates: list[dict[str, Any]] = []
        for index in range(keep_length):
            candidate = candidates[index]
            kept_candidates.append(
                {
                    "token": int(candidate["token"]),
                    "score": finite_or_negative_inf(float(as_plain_number(candidate["score"]))),
                    "rank": int(candidate["rank"]),
                    "probability": float(kept_probabilities[index]),
                    "log_probability": math.log(float(kept_probabilities[index])),
                    "source_column_block": int(candidate["source_column_block"]),
                }
            )

        truncated_candidates: list[dict[str, Any]] = []
        if nucleus_enabled:
            for index in range(keep_length, len(candidates)):
                candidate = candidates[index]
                truncated_candidates.append(
                    {
                        "token": int(candidate["token"]),
                        "score": finite_or_negative_inf(float(as_plain_number(candidate["score"]))),
                        "rank": int(candidate["rank"]),
                        "probability_before_renormalization": float(probabilities[index]),
                        "cumulative_probability_before_renormalization": float(
                            np.sum(probabilities[: index + 1])
                        ),
                        "reason": "nucleus_top_p_boundary",
                    }
                )

        sampled_rows.append(
            {
                "row": row,
                "nucleus_enabled": bool(nucleus_enabled),
                "top_p": threshold,
                "cutoff_rank": keep_length,
                "equal_probability_boundary": equal_probability_boundary,
                "kept_probability_sum_before_renormalization": kept_sum,
                "kept_candidates": kept_candidates,
                "truncated_candidates": truncated_candidates,
                "random_uniform": uniform,
                "sampled_token": int(selected_candidate["token"]),
                "sampled_rank": int(selected_candidate["rank"]),
                "sample_log_probability": math.log(selected_probability),
            }
        )

    return {
        "shard_id": shard_id,
        "stage": "sampling",
        "seed": int(seed),
        "shard_ordinal": int(shard_ordinal),
        "nucleus_enabled": bool(nucleus_enabled),
        "top_p": threshold,
        "rng": "PCG64(SeedSequence([seed_u64, shard_ordinal, row]))",
        "rows": sampled_rows,
    }

