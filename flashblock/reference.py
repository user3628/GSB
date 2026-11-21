"""A straightforward row-by-row full attention reference implementation."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from .config import AttentionConfig
from .core import validate_and_prepare
from .numeric import NumericStats, validate_attention_inputs


@dataclass
class ReferenceResult:
    output: np.ndarray
    row_max: np.ndarray
    row_norm: np.ndarray
    numeric_stats: NumericStats


def reference_attention(
    q: np.ndarray,
    k: np.ndarray,
    v: np.ndarray,
    config: AttentionConfig,
    *,
    shard_id: str = "default",
) -> ReferenceResult:
    q64, k64, v64 = validate_and_prepare(q, k, v)
    validate_attention_inputs(q64, k64, v64, config, shard_id)
    batch, heads, q_length, head_dim = q64.shape
    key_length = k64.shape[2]
    output = np.zeros((batch, heads, q_length, head_dim), dtype=np.float64)
    row_max = np.full((batch, heads, q_length), -np.inf, dtype=np.float64)
    row_norm = np.zeros((batch, heads, q_length), dtype=np.float64)
    stats = NumericStats()
    scale = 1.0 / float(np.sqrt(head_dim))
    key_positions = np.arange(key_length, dtype=np.int64)[None, :]

    for batch_index in range(batch):
        for head_index in range(heads):
            for query_index in range(q_length):
                with np.errstate(over="ignore", invalid="ignore", divide="ignore", under="ignore"):
                    raw = (q64[batch_index, head_index, query_index] @ k64[batch_index, head_index].T) * scale
                if config.causal:
                    valid = key_positions[0] <= query_index
                else:
                    valid = np.ones(key_length, dtype=bool)
                valid_raw = raw[valid]
                finite = np.isfinite(valid_raw)
                abs_values = np.abs(valid_raw)
                clamped = int(np.count_nonzero((abs_values > config.score_clamp) | ~finite))
                extreme = int(np.count_nonzero(abs_values > config.extreme_threshold))
                local_stats = NumericStats(
                    clamped_values=clamped,
                    extreme_values=extreme,
                    max_abs_raw_logit=float(np.max(abs_values)) if abs_values.size else 0.0,
                    rows_with_extremes=1 if extreme else 0,
                )
                stats = stats.merge(local_stats)
                scores = np.clip(raw, -config.score_clamp, config.score_clamp)
                scores = scores[valid]
                if not valid.any():
                    continue
                row_maximum = float(np.max(scores))
                weights = np.exp(scores - row_maximum)
                normalizer = float(np.sum(weights))
                if not np.isfinite(normalizer) or normalizer <= 0.0:
                    continue
                output[batch_index, head_index, query_index] = (
                    weights @ v64[batch_index, head_index, valid]
                ) / normalizer
                row_max[batch_index, head_index, query_index] = row_maximum
                row_norm[batch_index, head_index, query_index] = normalizer

    return ReferenceResult(
        output=np.ascontiguousarray(output),
        row_max=np.ascontiguousarray(row_max),
        row_norm=np.ascontiguousarray(row_norm),
        numeric_stats=stats,
    )

