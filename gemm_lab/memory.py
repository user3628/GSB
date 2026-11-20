from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .blocking import BlockPlan
from .errors import BusinessCode, fail


FIXED_OVERHEAD_BYTES = 64 * 1024


@dataclass(frozen=True)
class MemoryShard:
    batch_start: int
    batch_stop: int
    batch_count: int
    estimated_peak_bytes: int
    budget_remaining_bytes: int

    def to_dict(self) -> dict[str, int]:
        return {
            "batch_start": self.batch_start,
            "batch_stop": self.batch_stop,
            "batch_count": self.batch_count,
            "estimated_peak_bytes": self.estimated_peak_bytes,
            "budget_remaining_bytes": self.budget_remaining_bytes,
        }


@dataclass(frozen=True)
class MemorySchedule:
    budget_bytes: int
    output_bytes: int
    cache_workspace_bytes: int
    register_workspace_bytes: int
    fixed_overhead_bytes: int
    per_batch_operand_bytes: int
    batch: int
    slice_size: int
    full_batch_peak_bytes: int
    sliced: bool
    shards: tuple[MemoryShard, ...]
    accounting: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "budget_bytes": self.budget_bytes,
            "output_bytes": self.output_bytes,
            "cache_workspace_bytes": self.cache_workspace_bytes,
            "register_workspace_bytes": self.register_workspace_bytes,
            "fixed_overhead_bytes": self.fixed_overhead_bytes,
            "per_batch_operand_bytes": self.per_batch_operand_bytes,
            "batch": self.batch,
            "slice_size": self.slice_size,
            "full_batch_peak_bytes": self.full_batch_peak_bytes,
            "sliced": self.sliced,
            "shards": [shard.to_dict() for shard in self.shards],
            "accounting": self.accounting,
        }


def plan_memory(
    *,
    batch: int,
    m: int,
    n: int,
    k: int,
    itemsize: int,
    plan: BlockPlan,
    budget_bytes: int | None,
) -> MemorySchedule:
    """Plan incremental execution memory before allocating the output.

    The accounting includes the full caller-visible output buffer, cache/register
    working arrays, fixed runtime overhead, and the selected BMM operand slice.
    Caller-owned source arrays are already resident and are not double-counted;
    the operand-slice term models a materialized contiguous batch slice.
    """

    cache_bytes = plan.cache_workspace_bytes(itemsize)
    register_bytes = plan.register_workspace_bytes(itemsize)
    output_bytes = batch * m * n * itemsize
    per_batch_operand = (m * k + k * n) * itemsize
    base = output_bytes + cache_bytes + register_bytes + FIXED_OVERHEAD_BYTES
    full_peak = base + batch * per_batch_operand
    if budget_bytes is None:
        budget_bytes = max(full_peak, base + per_batch_operand)
    if budget_bytes <= 0:
        fail(
            BusinessCode.E_MEMORY_BUDGET,
            "memory budget must be positive",
            budget_bytes=budget_bytes,
        )
    if base + per_batch_operand > budget_bytes:
        fail(
            BusinessCode.E_MEMORY_BUDGET,
            "memory budget cannot hold one output buffer plus one batch slice",
            budget_bytes=budget_bytes,
            required_minimum_bytes=base + per_batch_operand,
            deficit_bytes=base + per_batch_operand - budget_bytes,
            breakdown={
                "output_bytes": output_bytes,
                "cache_workspace_bytes": cache_bytes,
                "register_workspace_bytes": register_bytes,
                "fixed_overhead_bytes": FIXED_OVERHEAD_BYTES,
                "one_batch_operand_bytes": per_batch_operand,
            },
        )
    if full_peak <= budget_bytes or batch == 1:
        slice_size = batch
    else:
        slice_size = max(1, (budget_bytes - base) // per_batch_operand)
        slice_size = min(batch, int(slice_size))
    shards: list[MemoryShard] = []
    for batch_start in range(0, batch, slice_size):
        batch_stop = min(batch_start + slice_size, batch)
        count = batch_stop - batch_start
        estimated = base + count * per_batch_operand
        shards.append(
            MemoryShard(
                batch_start=batch_start,
                batch_stop=batch_stop,
                batch_count=count,
                estimated_peak_bytes=estimated,
                budget_remaining_bytes=budget_bytes - estimated,
            )
        )
    return MemorySchedule(
        budget_bytes=budget_bytes,
        output_bytes=output_bytes,
        cache_workspace_bytes=cache_bytes,
        register_workspace_bytes=register_bytes,
        fixed_overhead_bytes=FIXED_OVERHEAD_BYTES,
        per_batch_operand_bytes=per_batch_operand,
        batch=batch,
        slice_size=slice_size,
        full_batch_peak_bytes=full_peak,
        sliced=len(shards) > 1,
        shards=tuple(shards),
        accounting=(
            "full output buffer + cache/register workspace + fixed overhead + "
            "materialized BMM operand slice; caller-owned inputs already resident"
        ),
    )

