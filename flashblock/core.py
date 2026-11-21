"""NumPy implementation of blockwise, online-softmax attention."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any, Mapping

import numpy as np

from .config import AttentionConfig
from .digest import array_digest, sha256_arrays, sha256_json, update_array_hash
from .errors import fail
from .numeric import NumericStats, ensure_finite, validate_attention_inputs
from .scheduler import RowBlockPlan, plan_row_block
from .state import PartialBlockState, RowBlockState


@dataclass
class InputFingerprints:
    q: str
    kv: str
    input: str
    config: str

    def to_dict(self) -> dict[str, str]:
        return {
            "q_fingerprint": self.q,
            "kv_fingerprint": self.kv,
            "input_fingerprint": self.input,
            "config_fingerprint": self.config,
        }


@dataclass
class CoreAttentionResult:
    shard_id: str
    output: np.ndarray
    row_max: np.ndarray
    row_norm: np.ndarray
    states: list[RowBlockState]
    plans: list[RowBlockPlan]
    numeric_stats: NumericStats
    input_fingerprints: InputFingerprints


def validate_and_prepare(
    q: np.ndarray,
    k: np.ndarray,
    v: np.ndarray,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    arrays = []
    for name, array in (("q", q), ("k", k), ("v", v)):
        value = np.asarray(array)
        if value.ndim != 4:
            fail("E_SHAPE", f"{name} must be 4-D [batch, heads, sequence, head_dim]", actual_shape=list(value.shape))
        if not np.issubdtype(value.dtype, np.floating):
            fail("E_SHAPE", f"{name} must be floating point", dtype=str(value.dtype))
        arrays.append(np.array(value, dtype=np.float64, copy=True, order="C"))
    q64, k64, v64 = arrays
    if q64.shape[:2] != k64.shape[:2] or q64.shape[:2] != v64.shape[:2]:
        fail(
            "E_SHAPE",
            "Q/K/V batch and head dimensions must match",
            q_shape=list(q64.shape),
            k_shape=list(k64.shape),
            v_shape=list(v64.shape),
        )
    if q64.shape[3] != k64.shape[3] or q64.shape[3] != v64.shape[3]:
        fail(
            "E_SHAPE",
            "Q/K/V head dimensions must match",
            q_shape=list(q64.shape),
            k_shape=list(k64.shape),
            v_shape=list(v64.shape),
        )
    if k64.shape[2] != v64.shape[2]:
        fail(
            "E_SHAPE",
            "K/V sequence lengths must match",
            k_shape=list(k64.shape),
            v_shape=list(v64.shape),
        )
    if min(q64.shape) <= 0:
        fail("E_SHAPE", "All attention dimensions must be non-zero", shape=list(q64.shape))
    return q64, k64, v64


def build_fingerprints(q: np.ndarray, k: np.ndarray, v: np.ndarray, config: AttentionConfig) -> InputFingerprints:
    q_digest = array_digest(q)
    kv_digest = sha256_arrays({"k": k, "v": v})
    input_digest = sha256_json(
        {
            "format": "flashblock/input-v1",
            "q": q_digest,
            "kv": kv_digest,
            "shape": {
                "q": list(q.shape),
                "k": list(k.shape),
                "v": list(v.shape),
            },
            "config": config.semantic_fingerprint,
        }
    )
    return InputFingerprints(
        q=q_digest,
        kv=kv_digest,
        input=input_digest,
        config=config.semantic_fingerprint,
    )


def column_block_count(config: AttentionConfig, key_length: int) -> int:
    return (key_length + config.col_block_size - 1) // config.col_block_size


def needed_column_blocks(
    config: AttentionConfig,
    key_length: int,
    q_end: int,
) -> int:
    total = column_block_count(config, key_length)
    if not config.causal:
        return total
    last_key = min(key_length - 1, q_end - 1)
    if last_key < 0:
        return 0
    return last_key // config.col_block_size + 1


def partial_dependency_fingerprint(
    q_block: np.ndarray,
    k_block: np.ndarray,
    v_block: np.ndarray,
    *,
    config: AttentionConfig,
    q_start: int,
    q_end: int,
    key_start: int,
    key_end: int,
    scale: float,
) -> str:
    hasher = hashlib.sha256()
    descriptor = {
        "format": "flashblock/partial-dependency-v1",
        "config": config.semantic_fingerprint,
        "q_start": q_start,
        "q_end": q_end,
        "key_start": key_start,
        "key_end": key_end,
        "causal": config.causal,
        "scale": scale,
    }
    hasher.update(sha256_json(descriptor).encode("ascii"))
    update_array_hash(hasher, "q", q_block)
    update_array_hash(hasher, "k", k_block)
    update_array_hash(hasher, "v", v_block)
    return hasher.hexdigest()


def _combine_stats(stats: list[NumericStats]) -> NumericStats:
    result = NumericStats()
    for item in stats:
        result = result.merge(item)
    return result


def _compute_partial_chunk(
    q_block: np.ndarray,
    k_block: np.ndarray,
    v_block: np.ndarray,
    *,
    shard_id: str,
    row_block: int,
    col_block: int,
    q_start: int,
    q_end: int,
    key_start: int,
    key_end: int,
    scale: float,
    config: AttentionConfig,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, NumericStats]:
    with np.errstate(over="ignore", invalid="ignore", divide="ignore", under="ignore"):
        raw_scores = np.matmul(q_block, np.swapaxes(k_block, -1, -2)) * scale

    if np.isnan(raw_scores).any():
        location = np.argwhere(np.isnan(raw_scores))[0]
        fail(
            "E_NUMERIC",
            "NaN produced by QK dot product",
            shard_id=shard_id,
            row_block=row_block,
            col_block=col_block,
            batch=int(location[0]),
            head=int(location[1]),
            local_query=int(location[2]),
            local_key=int(location[3]),
        )

    q_positions = np.arange(q_start, q_end, dtype=np.int64)[:, None]
    key_positions = np.arange(key_start, key_end, dtype=np.int64)[None, :]
    if config.causal:
        valid_mask = key_positions <= q_positions
    else:
        valid_mask = np.ones((q_end - q_start, key_end - key_start), dtype=bool)

    valid_4d = valid_mask[None, None, :, :]
    expanded_valid = np.broadcast_to(valid_4d, raw_scores.shape)
    raw_valid = raw_scores[expanded_valid]
    abscissa = np.abs(raw_valid)
    extreme_mask = abscissa > config.extreme_threshold
    clamp_mask = (raw_valid > config.score_clamp) | (raw_valid < -config.score_clamp)
    clamped_mask = np.logical_or(clamp_mask, ~np.isfinite(raw_valid))
    clamped_count = int(np.count_nonzero(clamped_mask))
    extreme_count = int(np.count_nonzero(extreme_mask))
    max_abs = float(np.max(abscissa)) if abscissa.size else 0.0
    rows_with_extremes = 0
    first_clamped: dict[str, int] | None = None
    if q_end > q_start and key_end > key_start:
        # Masking is applied before counting so masked columns never contribute.
        masked_extreme = np.where(valid_4d, np.abs(raw_scores), 0.0) > config.extreme_threshold
        rows_with_extremes = int(np.count_nonzero(np.any(masked_extreme, axis=-1)))
        clamped_positions = np.argwhere(valid_4d & ((np.abs(raw_scores) > config.score_clamp) | ~np.isfinite(raw_scores)))
        if clamped_positions.size:
            loc = clamped_positions[0]
            first_clamped = {
                "batch": int(loc[0]),
                "head": int(loc[1]),
                "local_query": int(loc[2]),
                "local_key": int(loc[3]),
            }
    stats = NumericStats(
        clamped_values=clamped_count,
        extreme_values=extreme_count,
        max_abs_raw_logit=max_abs,
        rows_with_extremes=rows_with_extremes,
        first_clamped=first_clamped,
    )

    scores = np.clip(raw_scores, -config.score_clamp, config.score_clamp)
    scores = np.where(valid_4d, scores, -np.inf)
    block_max = np.max(scores, axis=-1)
    valid_any = np.isfinite(block_max)
    max_safe = np.where(valid_any, block_max, 0.0)
    shifted = np.where(valid_4d, scores - max_safe[..., None], -np.inf)
    with np.errstate(over="ignore", invalid="ignore", divide="ignore", under="ignore"):
        weights = np.exp(shifted)
        row_sum = np.sum(weights, axis=-1)
        weighted_output = np.matmul(weights, v_block)

    if not np.isfinite(row_sum).all() or not np.isfinite(weighted_output).all():
        fail(
            "E_NUMERIC",
            "Non-finite online-softmax accumulation",
            shard_id=shard_id,
            row_block=row_block,
            col_block=col_block,
        )
    if (row_sum < 0.0).any() or (valid_any & (row_sum <= 0.0)).any():
        fail(
            "E_NUMERIC",
            "Zero or negative online-softmax normalizer",
            shard_id=shard_id,
            row_block=row_block,
            col_block=col_block,
            key_interval=[key_start, key_end],
        )
    row_sum = np.where(valid_any, row_sum, 0.0)
    weighted_output = np.where(valid_any[..., None], weighted_output, 0.0)
    partial_max = np.where(valid_any, block_max, -np.inf)
    return (
        partial_max,
        np.ascontiguousarray(row_sum),
        np.ascontiguousarray(weighted_output),
        stats,
    )


def compute_partial(
    q: np.ndarray,
    k: np.ndarray,
    v: np.ndarray,
    *,
    config: AttentionConfig,
    shard_id: str,
    row_block: int,
    col_block: int,
    q_start: int,
    q_end: int,
    key_start: int,
    key_end: int,
    batch_chunks: tuple[tuple[int, int], ...],
) -> PartialBlockState:
    head_dim = q.shape[3]
    scale = 1.0 / float(np.sqrt(head_dim))
    q_block = q[:, :, q_start:q_end, :]
    k_block = k[:, :, key_start:key_end, :]
    v_block = v[:, :, key_start:key_end, :]
    dependency = partial_dependency_fingerprint(
        q_block,
        k_block,
        v_block,
        config=config,
        q_start=q_start,
        q_end=q_end,
        key_start=key_start,
        key_end=key_end,
        scale=scale,
    )

    m_chunks: list[np.ndarray] = []
    l_chunks: list[np.ndarray] = []
    o_chunks: list[np.ndarray] = []
    all_stats: list[NumericStats] = []
    for batch_start, batch_end in batch_chunks:
        m_chunk, l_chunk, o_chunk, stats = _compute_partial_chunk(
            q_block[batch_start:batch_end],
            k_block[batch_start:batch_end],
            v_block[batch_start:batch_end],
            shard_id=shard_id,
            row_block=row_block,
            col_block=col_block,
            q_start=q_start,
            q_end=q_end,
            key_start=key_start,
            key_end=key_end,
            scale=scale,
            config=config,
        )
        m_chunks.append(m_chunk)
        l_chunks.append(l_chunk)
        o_chunks.append(o_chunk)
        all_stats.append(stats)

    return PartialBlockState(
        col_block=col_block,
        key_start=key_start,
        key_end=key_end,
        q_start=q_start,
        q_end=q_end,
        m=np.ascontiguousarray(np.concatenate(m_chunks, axis=0)),
        l=np.ascontiguousarray(np.concatenate(l_chunks, axis=0)),
        o=np.ascontiguousarray(np.concatenate(o_chunks, axis=0)),
        dependency_fingerprint=dependency,
        numeric_stats=_combine_stats(all_stats),
    )


def merge_partials(partials: list[PartialBlockState], *, shard_id: str, row_block: int) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    if not partials:
        fail(
            "E_NUMERIC",
            "No column-block partials available for row block",
            shard_id=shard_id,
            row_block=row_block,
            col_block=0,
        )
    ordered = sorted(partials, key=lambda item: (item.key_start, item.col_block))
    sample = ordered[0].m
    running_max = np.full(sample.shape, -np.inf, dtype=np.float64)
    running_sum = np.zeros(sample.shape, dtype=np.float64)
    running_output = np.zeros(ordered[0].o.shape, dtype=np.float64)

    for partial in ordered:
        if partial.m.shape != running_max.shape or partial.l.shape != running_sum.shape:
            fail(
                "E_SHAPE",
                "Column-block partial state shape mismatch",
                shard_id=shard_id,
                row_block=row_block,
                col_block=partial.col_block,
            )
        local_max = partial.m
        new_max = np.maximum(running_max, local_max)
        max_safe = np.where(np.isfinite(new_max), new_max, 0.0)
        with np.errstate(over="ignore", invalid="ignore", divide="ignore", under="ignore"):
            old_scale = np.where(np.isfinite(running_max), np.exp(running_max - max_safe), 0.0)
            local_scale = np.where(np.isfinite(local_max), np.exp(local_max - max_safe), 0.0)
            running_sum = running_sum * old_scale + partial.l * local_scale
            running_output = (
                running_output * old_scale[..., None]
                + partial.o * local_scale[..., None]
            )
        running_max = new_max

    running_sum = np.ascontiguousarray(running_sum)
    running_output = np.ascontiguousarray(running_output)
    if not np.isfinite(running_sum).all() or not np.isfinite(running_output).all():
        fail(
            "E_NUMERIC",
            "Non-finite merged online-softmax state",
            shard_id=shard_id,
            row_block=row_block,
            col_block=ordered[-1].col_block,
            partial_blocks=len(ordered),
        )
    if (running_sum <= 0.0).any():
        fail(
            "E_NUMERIC",
            "Zero normalizer after merging column blocks",
            shard_id=shard_id,
            row_block=row_block,
            col_block=ordered[-1].col_block,
            partial_blocks=len(ordered),
        )
    return np.ascontiguousarray(running_max), running_sum, running_output


def compute_row_block(
    q: np.ndarray,
    k: np.ndarray,
    v: np.ndarray,
    *,
    config: AttentionConfig,
    shard_id: str,
    row_block: int,
    plan: RowBlockPlan | None = None,
    reused_partials: Mapping[int, PartialBlockState] | None = None,
    force_recompute_cols: set[int] | None = None,
    fingerprints: InputFingerprints | None = None,
) -> tuple[RowBlockState, dict[str, Any]]:
    batch, heads, q_length, head_dim = q.shape
    key_length = k.shape[2]
    q_start = row_block * config.row_block_size
    q_end = min(q_start + config.row_block_size, q_length)
    if q_start >= q_length:
        fail("E_SHAPE", "Row block is outside Q sequence", shard_id=shard_id, row_block=row_block)
    n_columns = needed_column_blocks(config, key_length, q_end)
    if n_columns <= 0:
        fail("E_NUMERIC", "No visible key column blocks", shard_id=shard_id, row_block=row_block)
    if plan is None:
        plan = plan_row_block(
            config,
            shard_id=shard_id,
            batch=batch,
            heads=heads,
            head_dim=head_dim,
            key_length=key_length,
            q_start=q_start,
            q_end=q_end,
            row_block=row_block,
            column_blocks=n_columns,
        )
    if plan.column_blocks != n_columns or plan.q_rows != q_end - q_start:
        fail(
            "E_INVALID_STAGE",
            "Provided memory plan does not match row block shape",
            shard_id=shard_id,
            row_block=row_block,
            plan_columns=plan.column_blocks,
            expected_columns=n_columns,
        )

    if fingerprints is None:
        fingerprints = build_fingerprints(q, k, v, config)
    q_fingerprint = array_digest(q[:, :, q_start:q_end, :])
    scale = 1.0 / float(np.sqrt(head_dim))
    reused_map = dict(reused_partials or {})
    forced = set(force_recompute_cols or ())
    partials: list[PartialBlockState] = []
    reused_columns: list[int] = []
    recomputed_columns: list[int] = []
    recompute_reasons: dict[int, str] = {}

    for col_block in range(n_columns):
        key_start = col_block * config.col_block_size
        key_end = min(key_start + config.col_block_size, key_length)
        dependency = partial_dependency_fingerprint(
            q[:, :, q_start:q_end, :],
            k[:, :, key_start:key_end, :],
            v[:, :, key_start:key_end, :],
            config=config,
            q_start=q_start,
            q_end=q_end,
            key_start=key_start,
            key_end=key_end,
            scale=scale,
        )
        previous = reused_map.get(col_block)
        reason: str | None = None
        if col_block in forced:
            reason = "affected_key_interval"
        elif previous is None:
            reason = "partial_missing"
        elif previous.dependency_fingerprint != dependency:
            reason = "dependency_fingerprint_changed"
        elif (previous.q_start, previous.q_end, previous.key_start, previous.key_end) != (
            q_start,
            q_end,
            key_start,
            key_end,
        ):
            reason = "block_coordinates_changed"

        if reason is None and previous is not None:
            partials.append(previous)
            reused_columns.append(col_block)
            continue

        partial = compute_partial(
            q,
            k,
            v,
            config=config,
            shard_id=shard_id,
            row_block=row_block,
            col_block=col_block,
            q_start=q_start,
            q_end=q_end,
            key_start=key_start,
            key_end=key_end,
            batch_chunks=plan.batch_chunks,
        )
        partials.append(partial)
        recomputed_columns.append(col_block)
        recompute_reasons[col_block] = reason or "not_available"

    running_max, running_sum, running_output = merge_partials(
        partials,
        shard_id=shard_id,
        row_block=row_block,
    )
    normalized = running_output / running_sum[..., None]
    ensure_finite(
        normalized,
        shard_id=shard_id,
        row_block=row_block,
        col_block=n_columns - 1,
        stage="output normalization",
    )
    stats = _combine_stats([partial.numeric_stats for partial in partials])
    state = RowBlockState(
        shard_id=shard_id,
        row_block=row_block,
        q_start=q_start,
        q_end=q_end,
        key_length=key_length,
        partials=partials,
        m=running_max,
        l=running_sum,
        o=running_output,
        config_fingerprint=config.semantic_fingerprint,
        q_fingerprint=q_fingerprint,
        kv_fingerprint=fingerprints.kv,
        input_fingerprint=fingerprints.input,
        numeric_stats=stats,
        run_id="",
        sequence=0,
    )
    state.validate()
    diagnostics = {
        "row_block": row_block,
        "q_interval": [q_start, q_end],
        "column_blocks": n_columns,
        "reused_columns": reused_columns,
        "recomputed_columns": recomputed_columns,
        "recompute_reasons": {str(key): value for key, value in sorted(recompute_reasons.items())},
        "plan": plan.to_dict(),
    }
    return state, diagnostics


def assemble_outputs(states: list[Any], q_shape: tuple[int, ...]) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    if not states:
        fail("E_INVALID_STAGE", "Cannot assemble output without row-block states")
    batch, heads, q_length, head_dim = q_shape
    output = np.empty((batch, heads, q_length, head_dim), dtype=np.float64)
    row_max = np.full((batch, heads, q_length), -np.inf, dtype=np.float64)
    row_norm = np.zeros((batch, heads, q_length), dtype=np.float64)
    covered = 0
    for state in sorted(states, key=lambda item: item.row_block):
        if state.row_block != covered:
            fail(
                "E_INVALID_STAGE",
                "Row-block states are not contiguous",
                expected_row_block=covered,
                actual_row_block=state.row_block,
            )
        output[:, :, state.q_start:state.q_end, :] = state.output()
        row_max[:, :, state.q_start:state.q_end] = state.m
        row_norm[:, :, state.q_start:state.q_end] = state.l
        covered += 1
    expected_blocks = (q_length + max(1, states[0].q_end - states[0].q_start) - 1) // max(1, states[0].q_end - states[0].q_start)
    if covered != expected_blocks and states[-1].q_end != q_length:
        fail(
            "E_INVALID_STAGE",
            "Incomplete row-block state set",
            covered_row_blocks=covered,
            q_length=q_length,
        )
    if not (np.isfinite(output).all() and np.isfinite(row_norm).all()):
        fail("E_NUMERIC", "Non-finite assembled attention output")
    return (
        np.ascontiguousarray(output),
        np.ascontiguousarray(row_max),
        np.ascontiguousarray(row_norm),
    )


def full_attention(
    q: np.ndarray,
    k: np.ndarray,
    v: np.ndarray,
    config: AttentionConfig,
    *,
    shard_id: str = "default",
) -> CoreAttentionResult:
    q64, k64, v64 = validate_and_prepare(q, k, v)
    validate_attention_inputs(q64, k64, v64, config, shard_id)
    fingerprints = build_fingerprints(q64, k64, v64, config)
    batch, heads, q_length, head_dim = q64.shape
    row_blocks = (q_length + config.row_block_size - 1) // config.row_block_size
    states: list[RowBlockState] = []
    plans: list[RowBlockPlan] = []
    key_length = k64.shape[2]
    for row_block in range(row_blocks):
        q_start = row_block * config.row_block_size
        q_end = min(q_start + config.row_block_size, q_length)
        columns = needed_column_blocks(config, key_length, q_end)
        plan = plan_row_block(
            config,
            shard_id=shard_id,
            batch=batch,
            heads=heads,
            head_dim=head_dim,
            key_length=key_length,
            q_start=q_start,
            q_end=q_end,
            row_block=row_block,
            column_blocks=columns,
        )
        state, _ = compute_row_block(
            q64,
            k64,
            v64,
            config=config,
            shard_id=shard_id,
            row_block=row_block,
            plan=plan,
            fingerprints=fingerprints,
        )
        states.append(state)
        plans.append(plan)
    output, row_max, row_norm = assemble_outputs(states, q64.shape)
    stats = _combine_stats([state.numeric_stats for state in states])
    return CoreAttentionResult(
        shard_id=shard_id,
        output=output,
        row_max=row_max,
        row_norm=row_norm,
        states=states,
        plans=plans,
        numeric_stats=stats,
        input_fingerprints=fingerprints,
    )


def arrays_byte_equal(left: np.ndarray, right: np.ndarray) -> bool:
    left_c = np.ascontiguousarray(left)
    right_c = np.ascontiguousarray(right)
    return left_c.dtype == right_c.dtype and left_c.shape == right_c.shape and left_c.tobytes() == right_c.tobytes()


def max_abs_deviation(left: np.ndarray, right: np.ndarray) -> float:
    left64 = np.asarray(left, dtype=np.float64)
    right64 = np.asarray(right, dtype=np.float64)
    if left64.shape != right64.shape:
        return float("inf")
    with np.errstate(invalid="ignore", over="ignore"):
        delta = np.abs(left64 - right64)
    return float(np.nanmax(delta)) if delta.size else 0.0

