"""Deterministic dot-product scoring and numerically guarded softmax."""

from __future__ import annotations

from typing import Any, Callable

import numpy as np

from .config import RopeConfig
from .errors import PipelineStopped, RopeError


def _numeric_error(
    shard_id: str,
    row_block_index: int,
    step_index: int,
    reason: str,
    **extra: Any,
) -> RopeError:
    details = {
        "shard_id": shard_id,
        "row_block_index": int(row_block_index),
        "step_index": int(step_index),
        "reason": reason,
    }
    details.update(extra)
    return RopeError("E_NUMERIC", f"数值校验失败: {reason}", details)


def compute_scores_and_weights(
    query_rotated: np.ndarray,
    key_rotated: np.ndarray,
    config: RopeConfig,
    *,
    shard_id: str,
    row_chunk_size: int,
    should_stop: Callable[[], bool] | None = None,
) -> dict[str, np.ndarray]:
    """Compute q·k scores and row-wise stable softmax.

    The dot product deliberately uses a row-wise reduction rather than a
    matrix-multiply so that changing the execution row block cannot alter
    floating-point accumulation order.
    """

    if query_rotated.shape != key_rotated.shape or query_rotated.ndim != 4:
        raise ValueError("rotated query/key must be matching 4D tensors")
    batch, heads, sequence_length, head_dim = query_rotated.shape
    if row_chunk_size <= 0:
        raise ValueError("row_chunk_size must be positive")
    row_count = batch * heads * sequence_length
    scores = np.empty((batch, heads, sequence_length, sequence_length), dtype=np.float64)
    weights = np.empty_like(scores)
    norm_factors = np.empty((batch, heads, sequence_length), dtype=np.float64)

    row_block_index = 0
    for flat_start in range(0, row_count, row_chunk_size):
        flat_end = min(row_count, flat_start + row_chunk_size)
        block_size = flat_end - flat_start
        for step_index, flat_row in enumerate(range(flat_start, flat_end)):
            b = flat_row // (heads * sequence_length)
            remainder = flat_row % (heads * sequence_length)
            h = remainder // sequence_length
            query_position = remainder % sequence_length
            query_row = query_rotated[b, h, query_position]
            key_matrix = key_rotated[b, h]
            if not np.all(np.isfinite(query_row)):
                raise _numeric_error(
                    shard_id,
                    row_block_index,
                    step_index,
                    "non_finite_query",
                    batch_index=int(b),
                    head_index=int(h),
                    query_position=int(query_position),
                )
            if not np.all(np.isfinite(key_matrix)):
                raise _numeric_error(
                    shard_id,
                    row_block_index,
                    step_index,
                    "non_finite_key",
                    batch_index=int(b),
                    head_index=int(h),
                    query_position=int(query_position),
                )

            score_row = np.sum(key_matrix * query_row[None, :], axis=1, dtype=np.float64)
            if config.causal_mask:
                score_row[query_position + 1 :] = -np.inf
            finite_slice = score_row[: query_position + 1] if config.causal_mask else score_row
            if not np.all(np.isfinite(finite_slice)):
                raise _numeric_error(
                    shard_id,
                    row_block_index,
                    step_index,
                    "non_finite_score",
                    batch_index=int(b),
                    head_index=int(h),
                    query_position=int(query_position),
                )

            row_max = np.max(finite_slice)
            shifted = np.full(sequence_length, -np.inf, dtype=np.float64)
            if config.causal_mask:
                shifted[: query_position + 1] = score_row[: query_position + 1] - row_max
            else:
                shifted[:] = score_row - row_max
            exponential = np.exp(shifted)
            norm_factor = np.sum(exponential, dtype=np.float64)
            if not np.isfinite(norm_factor) or norm_factor <= 0.0:
                raise _numeric_error(
                    shard_id,
                    row_block_index,
                    step_index,
                    "normalization_factor_zero_or_non_finite",
                    batch_index=int(b),
                    head_index=int(h),
                    query_position=int(query_position),
                    norm_factor=float(norm_factor),
                )
            weight_row = exponential / norm_factor
            if not np.all(np.isfinite(weight_row)):
                raise _numeric_error(
                    shard_id,
                    row_block_index,
                    step_index,
                    "non_finite_weight",
                    batch_index=int(b),
                    head_index=int(h),
                    query_position=int(query_position),
                )

            scores[b, h, query_position] = score_row
            weights[b, h, query_position] = weight_row
            norm_factors[b, h, query_position] = norm_factor

        if should_stop is not None and should_stop():
            raise PipelineStopped(
                "scoring_normalization", f"row_block:{row_block_index}"
            )
        row_block_index += 1

    return {
        "scores": scores,
        "attention_weights": weights,
        "normalization_factors": norm_factors,
    }


def scoring_statistics(arrays: dict[str, np.ndarray], *, shard_id: str) -> dict[str, Any]:
    scores = arrays["scores"]
    weights = arrays["attention_weights"]
    norm = arrays["normalization_factors"]
    finite_scores = scores[np.isfinite(scores)]
    issue_count = int(weights.size - np.count_nonzero(np.isfinite(weights)))
    if issue_count:
        raise RopeError(
            "E_NUMERIC",
            "注意力权重包含非有限值",
            {"shard_id": shard_id, "non_finite_count": issue_count},
        )
    return {
        "shard_id": shard_id,
        "score_count": int(scores.size),
        "finite_score_count": int(finite_scores.size),
        "score_min": float(np.min(finite_scores)),
        "score_max": float(np.max(finite_scores)),
        "score_mean": float(np.mean(finite_scores)),
        "weight_min": float(np.min(weights)),
        "weight_max": float(np.max(weights)),
        "weight_mean": float(np.mean(weights)),
        "normalization_min": float(np.min(norm)),
        "normalization_max": float(np.max(norm)),
        "normalization_mean": float(np.mean(norm)),
        "row_sum_max_error": float(
            np.max(np.abs(np.sum(weights, axis=-1, dtype=np.float64) - 1.0))
        ),
    }
