from __future__ import annotations

import math
from dataclasses import dataclass
from enum import Enum
from typing import Any

import numpy as np

from .blocking import BlockPlan
from .errors import BusinessCode, fail


class SumStrategy(str, Enum):
    SEQUENTIAL = "sequential"
    PAIRWISE = "pairwise"
    COMPENSATED = "compensated"


def normalize_strategy(strategy: SumStrategy | str) -> SumStrategy:
    if isinstance(strategy, SumStrategy):
        return strategy
    aliases = {
        "sequential": SumStrategy.SEQUENTIAL,
        "seq": SumStrategy.SEQUENTIAL,
        "pairwise": SumStrategy.PAIRWISE,
        "pair": SumStrategy.PAIRWISE,
        "compensated": SumStrategy.COMPENSATED,
        "kahan": SumStrategy.COMPENSATED,
    }
    if not isinstance(strategy, str) or strategy.strip().lower() not in aliases:
        fail(
            BusinessCode.E_INVALID_SHAPE,
            "sum strategy must be sequential, pairwise, or compensated",
            parameter="sum_strategy",
            value=repr(strategy),
        )
    return aliases[strategy.strip().lower()]


@dataclass(frozen=True)
class KernelInfo:
    sum_strategy: str
    accumulation_mode: str
    plan: BlockPlan
    batches_executed: int
    register_tiles: int
    cache_tiles: int
    microkernel_calls: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "sum_strategy": self.sum_strategy,
            "accumulation_mode": self.accumulation_mode,
            "plan": self.plan.to_dict(),
            "batches_executed": self.batches_executed,
            "register_tiles": self.register_tiles,
            "cache_tiles": self.cache_tiles,
            "microkernel_calls": self.microkernel_calls,
        }


@dataclass
class _Counters:
    register_tiles: int = 0
    cache_tiles: int = 0
    microkernel_calls: int = 0


def choose_accumulation_mode(m: int, n: int, k: int, requested: str = "auto") -> str:
    """Resolve auto mode; scalar is selected only for cheap per-K iteration."""

    if requested not in {"auto", "panel", "scalar"}:
        fail(
            BusinessCode.E_INVALID_SHAPE,
            "accumulation_mode must be auto, panel, or scalar",
            parameter="accumulation_mode",
            value=requested,
        )
    if requested != "auto":
        return requested
    return "scalar" if m * n * k <= 1_000_000 and k <= 256 else "panel"


def _pairwise_tree(values: list[np.ndarray]) -> np.ndarray:
    """Reduce values as a balanced tree, preserving an orphan at each level."""

    if not values:
        raise ValueError("pairwise sum needs at least one value")
    work = values
    while len(work) > 1:
        next_level: list[np.ndarray] = []
        boundary = len(work) - (len(work) % 2)
        for index in range(0, boundary, 2):
            next_level.append(work[index] + work[index + 1])
        if boundary != len(work):
            next_level.append(work[-1])
        work = next_level
    return work[0]


def _compensated_tree(values: list[np.ndarray], shape: tuple[int, int], dtype: np.dtype) -> np.ndarray:
    if not values:
        return np.zeros(shape, dtype=dtype)
    if not all(np.all(np.isfinite(value)) for value in values):
        total = np.zeros(shape, dtype=dtype)
        for value in values:
            total = total + value
        return total
    total = np.zeros(shape, dtype=dtype)
    compensation = np.zeros(shape, dtype=dtype)
    for value in values:
        corrected = value - compensation
        updated = total + corrected
        compensation = (updated - total) - corrected
        total = updated
    return total


def _scalar_register_tile(
    a_tile: np.ndarray,
    b_tile: np.ndarray,
    strategy: SumStrategy,
    dtype: np.dtype,
) -> np.ndarray:
    mrl, klen = a_tile.shape
    _, nrl = b_tile.shape
    with np.errstate(invalid="ignore", over="ignore", under="ignore", divide="ignore"):
        if strategy is SumStrategy.SEQUENTIAL:
            acc = np.zeros((mrl, nrl), dtype=dtype)
            for kk in range(klen):
                acc += a_tile[:, kk, None] * b_tile[kk, None, :]
            return acc
        if strategy is SumStrategy.PAIRWISE:
            terms = [a_tile[:, kk, None] * b_tile[kk, None, :] for kk in range(klen)]
            return _pairwise_tree(terms)
        terms = [a_tile[:, kk, None] * b_tile[kk, None, :] for kk in range(klen)]
        return _compensated_tree(terms, (mrl, nrl), dtype)


def _panel_register_tile(
    a_tile: np.ndarray,
    b_tile: np.ndarray,
    kc: int,
    strategy: SumStrategy,
    dtype: np.dtype,
) -> tuple[np.ndarray, int]:
    mrl, klen = a_tile.shape
    _, nrl = b_tile.shape
    with np.errstate(invalid="ignore", over="ignore", under="ignore", divide="ignore"):
        partials: list[np.ndarray] = []
        for k0 in range(0, klen, kc):
            k1 = min(k0 + kc, klen)
            partials.append(a_tile[:, k0:k1] @ b_tile[k0:k1, :])
        if strategy is SumStrategy.SEQUENTIAL:
            acc = np.zeros((mrl, nrl), dtype=dtype)
            for partial in partials:
                acc += partial
            return acc, len(partials)
        if strategy is SumStrategy.PAIRWISE:
            return _pairwise_tree(partials), len(partials)
        return _compensated_tree(partials, (mrl, nrl), dtype), len(partials)


def gemm_2d_into(
    a: np.ndarray,
    b: np.ndarray,
    out: np.ndarray,
    *,
    plan: BlockPlan,
    strategy: SumStrategy | str = SumStrategy.SEQUENTIAL,
    accumulation_mode: str = "auto",
) -> tuple[np.ndarray, KernelInfo]:
    """Compute one logical GEMM into ``out`` using the blocked kernel.

    ``a`` and ``b`` must already be logical views with shapes ``(M, K)`` and
    ``(K, N)``.  Every cache and register tile obtains a fresh accumulator.
    """

    normalized = normalize_strategy(strategy)
    m, k = a.shape
    k2, n = b.shape
    if k != k2 or out.shape != (m, n):
        fail(
            BusinessCode.E_INVALID_SHAPE,
            "kernel logical views do not agree",
            a_shape=list(a.shape),
            b_shape=list(b.shape),
            out_shape=list(out.shape),
        )
    mode = choose_accumulation_mode(m, n, k, accumulation_mode)
    dtype = np.dtype(a.dtype)
    counters = _Counters()

    for i0 in range(0, m, plan.mc):
        i1 = min(i0 + plan.mc, m)
        for j0 in range(0, n, plan.nc):
            j1 = min(j0 + plan.nc, n)
            counters.cache_tiles += 1
            for ii in range(i0, i1, plan.mr):
                ii_end = min(ii + plan.mr, i1)
                for jj in range(j0, j1, plan.nr):
                    jj_end = min(jj + plan.nr, j1)
                    counters.register_tiles += 1
                    a_tile = a[ii:ii_end, :]
                    b_tile = b[:, jj:jj_end]
                    if mode == "scalar":
                        acc = _scalar_register_tile(a_tile, b_tile, normalized, dtype)
                        counters.microkernel_calls += k
                    else:
                        acc, panels = _panel_register_tile(
                            a_tile, b_tile, plan.kc, normalized, dtype
                        )
                        counters.microkernel_calls += panels
                    out[ii:ii_end, jj:jj_end] = acc

    info = KernelInfo(
        sum_strategy=normalized.value,
        accumulation_mode=mode,
        plan=plan,
        batches_executed=1,
        register_tiles=counters.register_tiles,
        cache_tiles=counters.cache_tiles,
        microkernel_calls=counters.microkernel_calls,
    )
    return out, info


def bmm_into(
    a: np.ndarray,
    b: np.ndarray,
    out: np.ndarray,
    *,
    plan: BlockPlan,
    strategy: SumStrategy | str = SumStrategy.SEQUENTIAL,
    accumulation_mode: str = "auto",
    batch_start: int = 0,
    batch_stop: int | None = None,
) -> tuple[np.ndarray, KernelInfo]:
    """Compute a contiguous batch slice with a fresh kernel call per matrix.

    The batch dimension is the outermost loop.  No accumulator is reused by a
    later batch, which makes each batch bitwise identical to a standalone GEMM.
    """

    if a.ndim != 3 or b.ndim != 3 or out.ndim != 3:
        fail(
            BusinessCode.E_INVALID_SHAPE,
            "BMM logical views must be 3-D",
            a_shape=list(a.shape),
            b_shape=list(b.shape),
            out_shape=list(out.shape),
        )
    batch = a.shape[0]
    if b.shape[0] != batch or out.shape[0] != batch:
        fail(
            BusinessCode.E_BATCH_MISMATCH,
            "BMM batch dimensions differ in kernel",
            batch_a=batch,
            batch_b=int(b.shape[0]),
            batch_out=int(out.shape[0]),
        )
    if batch_stop is None:
        batch_stop = batch
    if not (0 <= batch_start <= batch_stop <= batch):
        fail(
            BusinessCode.E_INVALID_SHAPE,
            "invalid BMM batch slice",
            batch_start=batch_start,
            batch_stop=batch_stop,
            batch=batch,
        )
    aggregate = _Counters()
    mode = choose_accumulation_mode(
        int(out.shape[1]), int(out.shape[2]), int(a.shape[-1]), accumulation_mode
    )
    last_info: KernelInfo | None = None
    for batch_index in range(batch_start, batch_stop):
        _, last_info = gemm_2d_into(
            a[batch_index],
            b[batch_index],
            out[batch_index],
            plan=plan,
            strategy=strategy,
            accumulation_mode=accumulation_mode,
        )
        aggregate.register_tiles += last_info.register_tiles
        aggregate.cache_tiles += last_info.cache_tiles
        aggregate.microkernel_calls += last_info.microkernel_calls
    if last_info is None:
        fail(
            BusinessCode.E_INVALID_SHAPE,
            "BMM batch slice must contain at least one matrix",
            batch_start=batch_start,
            batch_stop=batch_stop,
        )
    info = KernelInfo(
        sum_strategy=last_info.sum_strategy,
        accumulation_mode=mode,
        plan=plan,
        batches_executed=batch_stop - batch_start,
        register_tiles=aggregate.register_tiles,
        cache_tiles=aggregate.cache_tiles,
        microkernel_calls=aggregate.microkernel_calls,
    )
    return out, info


def strategy_error_order(strategy: SumStrategy | str, k: int) -> dict[str, Any]:
    normalized = normalize_strategy(strategy)
    if normalized is SumStrategy.SEQUENTIAL:
        order = k
        description = "gamma_K: K sequential additions"
    elif normalized is SumStrategy.PAIRWISE:
        order = max(1, math.ceil(math.log2(max(1, k))) + 1)
        description = "gamma_ceil(log2(K))+1: balanced pairwise tree"
    else:
        order = 2
        description = "2u*sum(abs(products)) + O(u^2): compensated summation"
    return {
        "strategy": normalized.value,
        "k": k,
        "summation_order": order,
        "description": description,
    }

