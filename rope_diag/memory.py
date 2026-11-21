"""Explicit peak-memory planning before large NumPy allocations."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from .config import RopeConfig
from .data import ShardData
from .errors import RopeError


FLOAT64_BYTES = 8


@dataclass(frozen=True)
class MemoryDecision:
    stage: str
    shard_id: str
    budget_bytes: int
    fixed_bytes: int
    working_bytes: int
    estimated_peak_bytes: int
    mode: str
    batch_chunk_size: int
    row_chunk_size: int
    chunk_count: int
    detail: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _memory_error(
    *,
    stage: str,
    shard_id: str,
    config: RopeConfig,
    fixed_bytes: int,
    minimum_working_bytes: int,
    detail: dict[str, Any],
) -> RopeError:
    required = fixed_bytes + minimum_working_bytes
    return RopeError(
        "E_MEMORY_BUDGET",
        "峰值内存预算不足，无法在不溢出的前提下执行阶段",
        {
            "stage": stage,
            "shard_id": shard_id,
            "budget_bytes": int(config.memory_budget_bytes),
            "fixed_output_bytes": int(fixed_bytes),
            "minimum_working_bytes": int(minimum_working_bytes),
            "required_bytes": int(required),
            "over_budget_bytes": int(required - config.memory_budget_bytes),
            "detail": detail,
        },
    )


def plan_frequency(shard: ShardData, config: RopeConfig) -> MemoryDecision:
    angle_bytes = shard.sequence_length * shard.head_dim * FLOAT64_BYTES
    auxiliary_bytes = shard.head_dim * FLOAT64_BYTES * 5
    total = angle_bytes + auxiliary_bytes
    if total > config.memory_budget_bytes:
        raise _memory_error(
            stage="frequency_generation",
            shard_id=shard.shard_id,
            config=config,
            fixed_bytes=angle_bytes,
            minimum_working_bytes=auxiliary_bytes,
            detail={"angle_table_bytes": angle_bytes, "auxiliary_bytes": auxiliary_bytes},
        )
    return MemoryDecision(
        stage="frequency_generation",
        shard_id=shard.shard_id,
        budget_bytes=config.memory_budget_bytes,
        fixed_bytes=angle_bytes,
        working_bytes=auxiliary_bytes,
        estimated_peak_bytes=total,
        mode="single",
        batch_chunk_size=1,
        row_chunk_size=shard.sequence_length,
        chunk_count=1,
        detail={"angle_table_bytes": angle_bytes, "auxiliary_bytes": auxiliary_bytes},
    )


def plan_rotation(shard: ShardData, config: RopeConfig) -> MemoryDecision:
    batch, heads, sequence_length, head_dim = shard.shape
    input_bytes = 2 * batch * heads * sequence_length * head_dim * FLOAT64_BYTES
    output_bytes = 2 * batch * heads * sequence_length * head_dim * FLOAT64_BYTES
    angle_bytes = sequence_length * head_dim * FLOAT64_BYTES
    cos_sin_bytes = 2 * sequence_length * head_dim * FLOAT64_BYTES
    fixed = input_bytes + output_bytes + angle_bytes + cos_sin_bytes
    # The block-wise rotation keeps even/odd operands plus four temporary
    # result arrays alive for the active batch chunk.
    per_batch_work = heads * sequence_length * head_dim * FLOAT64_BYTES * 6
    minimum_work = per_batch_work
    full_work = batch * per_batch_work
    detail = {
        "input_qk_bytes": input_bytes,
        "rotated_qk_output_bytes": output_bytes,
        "angle_table_bytes": angle_bytes,
        "cos_sin_tables_bytes": cos_sin_bytes,
        "per_batch_workspace_bytes": per_batch_work,
    }
    if fixed + minimum_work > config.memory_budget_bytes:
        raise _memory_error(
            stage="block_rotation",
            shard_id=shard.shard_id,
            config=config,
            fixed_bytes=fixed,
            minimum_working_bytes=minimum_work,
            detail=detail,
        )
    if fixed + full_work <= config.memory_budget_bytes:
        return MemoryDecision(
            stage="block_rotation",
            shard_id=shard.shard_id,
            budget_bytes=config.memory_budget_bytes,
            fixed_bytes=fixed,
            working_bytes=full_work,
            estimated_peak_bytes=fixed + full_work,
            mode="single",
            batch_chunk_size=batch,
            row_chunk_size=0,
            chunk_count=1,
            detail=detail,
        )

    available = config.memory_budget_bytes - fixed
    chosen = 1
    for candidate in range(batch, 0, -1):
        if candidate * per_batch_work <= available:
            chosen = candidate
            break
    working = chosen * per_batch_work
    return MemoryDecision(
        stage="block_rotation",
        shard_id=shard.shard_id,
        budget_bytes=config.memory_budget_bytes,
        fixed_bytes=fixed,
        working_bytes=working,
        estimated_peak_bytes=fixed + working,
        mode="batch_chunks",
        batch_chunk_size=chosen,
        row_chunk_size=0,
        chunk_count=(batch + chosen - 1) // chosen,
        detail=detail,
    )


def plan_scoring(shard: ShardData, config: RopeConfig) -> MemoryDecision:
    batch, heads, sequence_length, head_dim = shard.shape
    input_bytes = 2 * batch * heads * sequence_length * head_dim * FLOAT64_BYTES
    scores_bytes = batch * heads * sequence_length * sequence_length * FLOAT64_BYTES
    weights_bytes = scores_bytes
    norm_bytes = batch * heads * sequence_length * FLOAT64_BYTES
    fixed = input_bytes + scores_bytes + weights_bytes + norm_bytes
    row_count = batch * heads * sequence_length
    # A row owns an [S, D] product matrix plus score/exp/weight/normalization
    # vectors. This conservative estimate is checked before allocation.
    per_row_work = sequence_length * (head_dim + 6) * FLOAT64_BYTES + head_dim * FLOAT64_BYTES
    minimum_work = per_row_work
    full_work = row_count * per_row_work
    detail = {
        "input_qk_bytes": input_bytes,
        "scores_output_bytes": scores_bytes,
        "weights_output_bytes": weights_bytes,
        "normalization_output_bytes": norm_bytes,
        "per_row_workspace_bytes": per_row_work,
        "total_rows": row_count,
    }
    if fixed + minimum_work > config.memory_budget_bytes:
        raise _memory_error(
            stage="scoring_normalization",
            shard_id=shard.shard_id,
            config=config,
            fixed_bytes=fixed,
            minimum_working_bytes=minimum_work,
            detail=detail,
        )
    if fixed + full_work <= config.memory_budget_bytes:
        return MemoryDecision(
            stage="scoring_normalization",
            shard_id=shard.shard_id,
            budget_bytes=config.memory_budget_bytes,
            fixed_bytes=fixed,
            working_bytes=full_work,
            estimated_peak_bytes=fixed + full_work,
            mode="single",
            batch_chunk_size=batch,
            row_chunk_size=row_count,
            chunk_count=1,
            detail=detail,
        )

    available = config.memory_budget_bytes - fixed
    upper = min(config.row_chunk_size, row_count)
    chosen = 0
    for candidate in range(upper, 0, -1):
        if candidate * per_row_work <= available:
            chosen = candidate
            break
    if chosen == 0:
        raise _memory_error(
            stage="scoring_normalization",
            shard_id=shard.shard_id,
            config=config,
            fixed_bytes=fixed,
            minimum_working_bytes=minimum_work,
            detail=detail,
        )
    working = chosen * per_row_work
    return MemoryDecision(
        stage="scoring_normalization",
        shard_id=shard.shard_id,
        budget_bytes=config.memory_budget_bytes,
        fixed_bytes=fixed,
        working_bytes=working,
        estimated_peak_bytes=fixed + working,
        mode="row_blocks",
        batch_chunk_size=batch,
        row_chunk_size=chosen,
        chunk_count=(row_count + chosen - 1) // chosen,
        detail=detail,
    )
