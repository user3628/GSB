"""Deterministic peak-memory estimation and batch-chunk planning."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from .config import AttentionConfig
from .errors import fail


@dataclass(frozen=True)
class WorkingSetBreakdown:
    q_block_bytes: int
    kv_slices_bytes: int
    score_block_bytes: int
    merge_buffers_bytes: int
    retained_local_state_bytes: int
    subtotal_bytes: int
    workspace_factor: float
    estimated_peak_bytes: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class RowBlockPlan:
    row_block: int
    batch_chunks: tuple[tuple[int, int], ...]
    estimated_peak_bytes: int
    breakdown: WorkingSetBreakdown
    decision: str
    split_reason: str
    working_set_bytes: int
    budget_bytes: int | None
    q_rows: int
    column_blocks: int

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["batch_chunks"] = [list(chunk) for chunk in self.batch_chunks]
        value["breakdown"] = self.breakdown.to_dict()
        return value


def _estimate(
    config: AttentionConfig,
    *,
    batch_chunk: int,
    heads: int,
    q_rows: int,
    key_length: int,
    head_dim: int,
    column_blocks: int,
) -> WorkingSetBreakdown:
    itemsize = 8
    col_width = min(config.col_block_size, max(1, key_length))
    q_block = batch_chunk * heads * q_rows * head_dim * itemsize
    kv_slices = 2 * batch_chunk * heads * col_width * head_dim * itemsize
    scores = 3 * batch_chunk * heads * q_rows * col_width * itemsize
    merge = (4 * batch_chunk * heads * q_rows * head_dim + 2 * batch_chunk * heads * q_rows) * itemsize
    # Every column-block partial is retained until the row block checkpoint is
    # published, so this is part of the controlled working set.
    retained = column_blocks * (
        batch_chunk * heads * q_rows * head_dim * itemsize
        + 2 * batch_chunk * heads * q_rows * itemsize
    )
    subtotal = q_block + kv_slices + scores + merge + retained
    peak = int(subtotal * config.workspace_factor + 0.999999)
    return WorkingSetBreakdown(
        q_block_bytes=int(q_block),
        kv_slices_bytes=int(kv_slices),
        score_block_bytes=int(scores),
        merge_buffers_bytes=int(merge),
        retained_local_state_bytes=int(retained),
        subtotal_bytes=int(subtotal),
        workspace_factor=float(config.workspace_factor),
        estimated_peak_bytes=peak,
    )


def plan_row_block(
    config: AttentionConfig,
    *,
    shard_id: str,
    batch: int,
    heads: int,
    head_dim: int,
    key_length: int,
    q_start: int,
    q_end: int,
    row_block: int,
    column_blocks: int,
) -> RowBlockPlan:
    q_rows = q_end - q_start
    if batch <= 0 or heads <= 0 or head_dim <= 0 or q_rows <= 0 or column_blocks <= 0:
        fail(
            "E_MEMORY_BUDGET",
            "Invalid dimensions for memory planning",
            shard_id=shard_id,
            row_block=row_block,
            batch=batch,
            heads=heads,
            head_dim=head_dim,
            q_rows=q_rows,
            column_blocks=column_blocks,
        )

    budget = config.memory_budget_bytes
    full = _estimate(
        config,
        batch_chunk=batch,
        heads=heads,
        q_rows=q_rows,
        key_length=key_length,
        head_dim=head_dim,
        column_blocks=column_blocks,
    )
    if budget is None:
        return RowBlockPlan(
            row_block=row_block,
            batch_chunks=((0, batch),),
            estimated_peak_bytes=full.estimated_peak_bytes,
            breakdown=full,
            decision="no_budget_limit",
            split_reason="memory_budget_disabled",
            working_set_bytes=full.estimated_peak_bytes,
            budget_bytes=None,
            q_rows=q_rows,
            column_blocks=column_blocks,
        )

    if full.estimated_peak_bytes <= budget:
        return RowBlockPlan(
            row_block=row_block,
            batch_chunks=((0, batch),),
            estimated_peak_bytes=full.estimated_peak_bytes,
            breakdown=full,
            decision="fits_without_split",
            split_reason="estimated_peak_within_budget",
            working_set_bytes=full.estimated_peak_bytes,
            budget_bytes=budget,
            q_rows=q_rows,
            column_blocks=column_blocks,
        )

    for chunk_size in range(batch - 1, 0, -1):
        estimate = _estimate(
            config,
            batch_chunk=chunk_size,
            heads=heads,
            q_rows=q_rows,
            key_length=key_length,
            head_dim=head_dim,
            column_blocks=column_blocks,
        )
        if estimate.estimated_peak_bytes <= budget:
            chunks = tuple(
                (start, min(start + chunk_size, batch)) for start in range(0, batch, chunk_size)
            )
            return RowBlockPlan(
                row_block=row_block,
                batch_chunks=chunks,
                estimated_peak_bytes=estimate.estimated_peak_bytes,
                breakdown=estimate,
                decision="batch_split_required",
                split_reason=(
                    f"full_batch_estimate={full.estimated_peak_bytes} exceeds "
                    f"budget={budget}; using batch chunks of {chunk_size}"
                ),
                working_set_bytes=estimate.estimated_peak_bytes,
                budget_bytes=budget,
                q_rows=q_rows,
                column_blocks=column_blocks,
            )

    minimum = _estimate(
        config,
        batch_chunk=1,
        heads=heads,
        q_rows=q_rows,
        key_length=key_length,
        head_dim=head_dim,
        column_blocks=column_blocks,
    )
    fail(
        "E_MEMORY_BUDGET",
        "Requested peak-memory budget is below the minimum row-block working set",
        shard_id=shard_id,
        row_block=row_block,
        budget_bytes=budget,
        required_bytes=minimum.estimated_peak_bytes,
        breakdown=minimum.to_dict(),
        possible_actions=["increase_memory_budget", "decrease_row_block_size"],
        row_block_interval=[q_start, q_end],
        column_blocks=column_blocks,
    )

