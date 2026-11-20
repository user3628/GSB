from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from typing import Any, Iterable

import numpy as np

from .errors import BusinessCode, GemmError, fail


DEFAULT_L1_BYTES = 32 * 1024
DEFAULT_L2_BYTES = 1024 * 1024

MAX_BLOCKS = {"mr": 32, "nr": 64, "mc": 2048, "kc": 1024, "nc": 4096}


def _ceil_div(value: int, divisor: int) -> int:
    return (value + divisor - 1) // divisor


@dataclass(frozen=True)
class BlockPlan:
    """Three-level blocked GEMM plan.

    ``MC x KC`` and ``KC x NC`` are cache blocks; ``MR x NR`` is the register
    accumulator tile.  Partial blocks are supported at all matrix edges.
    """

    mr: int
    nr: int
    mc: int
    kc: int
    nc: int

    @property
    def signature(self) -> str:
        text = f"mr={self.mr},nr={self.nr},mc={self.mc},kc={self.kc},nc={self.nc}"
        return hashlib.sha256(text.encode("ascii")).hexdigest()[:20]

    @property
    def short(self) -> str:
        return f"{self.mr}x{self.nr}/{self.mc}x{self.kc}x{self.nc}"

    def register_workspace_bytes(self, itemsize: int) -> int:
        return (self.mr * self.kc + self.kc * self.nr + self.mr * self.nr) * itemsize

    def cache_workspace_bytes(self, itemsize: int) -> int:
        return (self.mc * self.kc + self.kc * self.nc + self.mc * self.nc) * itemsize

    def to_dict(self) -> dict[str, int]:
        return {
            "mr": self.mr,
            "nr": self.nr,
            "mc": self.mc,
            "kc": self.kc,
            "nc": self.nc,
            "signature": self.signature,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "BlockPlan":
        try:
            plan = cls(
                mr=int(value["mr"]),
                nr=int(value["nr"]),
                mc=int(value["mc"]),
                kc=int(value["kc"]),
                nc=int(value["nc"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            fail(
                BusinessCode.E_INVALID_SHAPE,
                "invalid block plan mapping",
                reason="malformed_plan",
                error=str(exc),
            )
        validate_plan(plan, m=1, n=1, k=1)
        return plan


@dataclass(frozen=True)
class PlanSelection:
    plan: BlockPlan
    score: float
    workload_class: str
    reason: str
    l1_budget_bytes: int
    l2_budget_bytes: int
    itemsize: int
    metrics: dict[str, float | int]
    normalized_components: dict[str, float]
    weights: dict[str, float]
    candidates_considered: int
    candidates_feasible: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "plan": self.plan.to_dict(),
            "score": round(self.score, 8),
            "workload_class": self.workload_class,
            "reason": self.reason,
            "l1_budget_bytes": self.l1_budget_bytes,
            "l2_budget_bytes": self.l2_budget_bytes,
            "itemsize": self.itemsize,
            "metrics": {k: _json_number(v) for k, v in self.metrics.items()},
            "normalized_components": {
                k: round(float(v), 8) for k, v in self.normalized_components.items()
            },
            "weights": {k: round(float(v), 8) for k, v in self.weights.items()},
            "candidates_considered": self.candidates_considered,
            "candidates_feasible": self.candidates_feasible,
        }


def _json_number(value: float | int) -> float | int:
    if isinstance(value, float):
        return round(value, 8)
    return value


def _plan_metrics(plan: BlockPlan, m: int, n: int, k: int, itemsize: int) -> dict[str, float | int]:
    """Estimate compulsory traffic and cache/register working sets for a plan."""

    m_tiles = _ceil_div(m, plan.mc)
    n_tiles = _ceil_div(n, plan.nc)
    k_tiles = _ceil_div(k, plan.kc)
    microtiles_m = _ceil_div(m, plan.mr)
    microtiles_n = _ceil_div(n, plan.nr)
    microkernel_calls = microtiles_m * microtiles_n * k_tiles

    a_elements = m * k * n_tiles
    b_elements = k * n * m_tiles
    output_elements = m * n
    traffic_elements = a_elements + b_elements + output_elements

    padded_m = m_tiles * plan.mc
    padded_n = n_tiles * plan.nc
    padded_k = k_tiles * plan.kc
    edge_waste_fraction = (padded_m * padded_n * padded_k) / float(m * n * k) - 1.0

    register_bytes = plan.register_workspace_bytes(itemsize)
    cache_bytes = plan.cache_workspace_bytes(itemsize)
    parallel_tiles = m_tiles * n_tiles
    return {
        "m_tiles": m_tiles,
        "n_tiles": n_tiles,
        "k_tiles": k_tiles,
        "parallel_tiles": parallel_tiles,
        "microtiles_m": microtiles_m,
        "microtiles_n": microtiles_n,
        "microkernel_calls": microkernel_calls,
        "estimated_traffic_bytes": traffic_elements * itemsize,
        "traffic_per_mnk": traffic_elements / float(m * n * k),
        "edge_waste_fraction": edge_waste_fraction,
        "register_workspace_bytes": register_bytes,
        "cache_workspace_bytes": cache_bytes,
    }


def validate_plan(
    plan: BlockPlan,
    *,
    m: int,
    n: int,
    k: int,
    l1_budget_bytes: int = DEFAULT_L1_BYTES,
    l2_budget_bytes: int = DEFAULT_L2_BYTES,
    enforce_budgets: bool = False,
    itemsize: int = 8,
) -> dict[str, Any]:
    """Validate explicit dimensions and block-boundary divisibility.

    The logical dimensions may be non-divisible; edge tiles are handled by the
    kernel.  Cache tiles must be whole multiples of register tiles
    (``MC % MR == 0`` and ``NC % NR == 0``) so register boundaries never cross
    a cache boundary.
    """

    values = plan.to_dict()
    for name in ("mr", "nr", "mc", "kc", "nc"):
        value = int(values[name])
        if value <= 0:
            fail(
                BusinessCode.E_INVALID_SHAPE,
                f"block dimension {name} must be positive",
                reason="non_positive_block",
                parameter=name,
                value=value,
            )
        if value > MAX_BLOCKS[name]:
            fail(
                BusinessCode.E_INVALID_SHAPE,
                f"block dimension {name} exceeds hard upper bound",
                reason="block_size_limit",
                parameter=name,
                value=value,
                maximum=MAX_BLOCKS[name],
            )
    if plan.mc % plan.mr != 0:
        fail(
            BusinessCode.E_INVALID_SHAPE,
            "MC must be divisible by MR",
            reason="cache_register_divisibility",
            mc=plan.mc,
            mr=plan.mr,
        )
    if plan.nc % plan.nr != 0:
        fail(
            BusinessCode.E_INVALID_SHAPE,
            "NC must be divisible by NR",
            reason="cache_register_divisibility",
            nc=plan.nc,
            nr=plan.nr,
        )
    if plan.mr > plan.mc or plan.nr > plan.nc:
        fail(
            BusinessCode.E_INVALID_SHAPE,
            "register tile cannot exceed its enclosing cache tile",
            reason="register_exceeds_cache",
            plan=plan.to_dict(),
        )

    register_bytes = plan.register_workspace_bytes(itemsize)
    cache_bytes = plan.cache_workspace_bytes(itemsize)
    if enforce_budgets and register_bytes > l1_budget_bytes:
        fail(
            BusinessCode.E_INVALID_SHAPE,
            "explicit plan exceeds L1 working-set budget",
            reason="l1_budget_exceeded",
            register_workspace_bytes=register_bytes,
            l1_budget_bytes=l1_budget_bytes,
        )
    if enforce_budgets and cache_bytes > l2_budget_bytes:
        fail(
            BusinessCode.E_INVALID_SHAPE,
            "explicit plan exceeds L2 working-set budget",
            reason="l2_budget_exceeded",
            cache_workspace_bytes=cache_bytes,
            l2_budget_bytes=l2_budget_bytes,
        )
    metrics = _plan_metrics(plan, m, n, k, itemsize)
    return {
        "valid": True,
        "edge_tiles": {
            "m": m % plan.mc != 0,
            "n": n % plan.nc != 0,
            "k": k % plan.kc != 0,
        },
        "register_workspace_bytes": register_bytes,
        "cache_workspace_bytes": cache_bytes,
        "within_l1_budget": register_bytes <= l1_budget_bytes,
        "within_l2_budget": cache_bytes <= l2_budget_bytes,
        "metrics": metrics,
    }


def _candidate_values(maximum: int) -> list[int]:
    if maximum <= 0:
        return []
    values = {maximum}
    for value in (4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048):
        if value <= maximum:
            values.add(value)
    return sorted(values)


def _iter_candidates(m: int, n: int, k: int, itemsize: int, l1: int, l2: int) -> Iterable[BlockPlan]:
    seen: set[BlockPlan] = set()
    mr_values = (1, 2, 4, 8, 16)
    nr_values = (1, 2, 4, 8, 16, 32)
    mc_values = _candidate_values(min(m, MAX_BLOCKS["mc"]))
    nc_values = _candidate_values(min(n, MAX_BLOCKS["nc"]))
    kc_values = sorted({min(value, k, MAX_BLOCKS["kc"]) for value in (16, 32, 64, 128, 256, 512)})
    for mr in mr_values:
        for nr in nr_values:
            for mc in mc_values:
                if mc % mr:
                    continue
                for nc in nc_values:
                    if nc % nr:
                        continue
                    for kc in kc_values:
                        plan = BlockPlan(mr=mr, nr=nr, mc=mc, kc=kc, nc=nc)
                        if plan in seen:
                            continue
                        seen.add(plan)
                        if plan.register_workspace_bytes(itemsize) > l1:
                            continue
                        if plan.cache_workspace_bytes(itemsize) > l2:
                            continue
                        yield plan


def _normalize(values: list[float], *, lower_is_better: bool, logarithmic: bool = False) -> list[float]:
    if logarithmic:
        values = [math.log1p(max(0.0, value)) for value in values]
    low = min(values)
    high = max(values)
    if high - low <= 1e-15:
        return [0.0 for _ in values]
    normalized = [(value - low) / (high - low) for value in values]
    if lower_is_better:
        return [1.0 - value for value in normalized]
    return normalized


def select_block_plan(
    *,
    m: int,
    n: int,
    k: int,
    batch: int = 1,
    itemsize: int = 8,
    l1_bytes: int = DEFAULT_L1_BYTES,
    l2_bytes: int = DEFAULT_L2_BYTES,
    parallel_workers: int = 1,
) -> PlanSelection:
    """Choose a cache/register plan by predicted traffic and overhead score."""

    if min(m, n, k, batch) <= 0:
        fail(
            BusinessCode.E_INVALID_SHAPE,
            "shape and batch dimensions must be positive",
            m=m,
            n=n,
            k=k,
            batch=batch,
        )
    if l1_bytes <= 0 or l2_bytes < l1_bytes:
        fail(
            BusinessCode.E_INVALID_SHAPE,
            "cache budgets must satisfy 0 < L1 <= L2",
            reason="invalid_cache_budget",
            l1_budget_bytes=l1_bytes,
            l2_budget_bytes=l2_bytes,
        )

    candidates = list(_iter_candidates(m, n, k, itemsize, l1_bytes, l2_bytes))
    if not candidates:
        fail(
            BusinessCode.E_INVALID_SHAPE,
            "no block plan fits the configured cache budgets",
            reason="no_feasible_plan",
            l1_budget_bytes=l1_bytes,
            l2_budget_bytes=l2_bytes,
        )
    metrics = [_plan_metrics(plan, m, n, k, itemsize) for plan in candidates]
    traffic = [float(item["estimated_traffic_bytes"]) for item in metrics]
    calls = [float(item["microkernel_calls"]) for item in metrics]
    cache_pressure = [
        float(item["cache_workspace_bytes"]) / float(l2_bytes) for item in metrics
    ]
    parallel = [float(item["parallel_tiles"]) for item in metrics]
    waste = [float(item["edge_waste_fraction"]) for item in metrics]

    workload_mnk = m * n * k * batch
    workload_class = "small" if workload_mnk <= 2_000_000 else "large" if workload_mnk >= 100_000_000 else "medium"
    if workload_class == "small":
        weights = {"traffic": 0.45, "microkernel_calls": 0.40, "cache_pressure": 0.05, "parallel": 0.04, "edge_waste": 0.06}
    elif workload_class == "large":
        weights = {"traffic": 0.60, "microkernel_calls": 0.16, "cache_pressure": 0.12, "parallel": 0.04, "edge_waste": 0.08}
    else:
        weights = {"traffic": 0.53, "microkernel_calls": 0.22, "cache_pressure": 0.10, "parallel": 0.05, "edge_waste": 0.10}

    n_traffic = _normalize(traffic, lower_is_better=True, logarithmic=True)
    n_calls = _normalize(calls, lower_is_better=True, logarithmic=True)
    n_cache = _normalize(cache_pressure, lower_is_better=True, logarithmic=True)
    parallel_target = max(1, int(parallel_workers))
    n_parallel = [min(value / parallel_target, 1.0) for value in parallel]
    n_waste = _normalize(waste, lower_is_better=True)
    total_candidates = len(candidates)
    scores: list[float] = []
    for idx in range(total_candidates):
        score = (
            weights["traffic"] * n_traffic[idx]
            + weights["microkernel_calls"] * n_calls[idx]
            + weights["cache_pressure"] * n_cache[idx]
            + weights["parallel"] * n_parallel[idx]
            + weights["edge_waste"] * n_waste[idx]
        )
        scores.append(score)

    best_idx = min(
        range(total_candidates),
        key=lambda index: (
            -scores[index],
            metrics[index]["microkernel_calls"],
            metrics[index]["estimated_traffic_bytes"],
            candidates[index].short,
        ),
    )
    selected = candidates[best_idx]
    selected_metrics = dict(metrics[best_idx])
    selected_metrics["parallel_workers"] = parallel_target
    normalized = {
        "traffic": n_traffic[best_idx],
        "microkernel_calls": n_calls[best_idx],
        "cache_pressure": n_cache[best_idx],
        "parallel": n_parallel[best_idx],
        "edge_waste": n_waste[best_idx],
    }
    reason = (
        f"{workload_class} workload: chose {selected.short}; "
        f"predicted traffic {selected_metrics['traffic_per_mnk']:.3f}x M*N*K, "
        f"{selected_metrics['microkernel_calls']} register-panel calls, "
        f"cache workspace {selected_metrics['cache_workspace_bytes']} B <= {l2_bytes} B, "
        f"{selected_metrics['parallel_tiles']} parallel cache tiles for "
        f"{parallel_target} workers"
    )
    return PlanSelection(
        plan=selected,
        score=float(scores[best_idx]),
        workload_class=workload_class,
        reason=reason,
        l1_budget_bytes=l1_bytes,
        l2_budget_bytes=l2_bytes,
        itemsize=itemsize,
        metrics=selected_metrics,
        normalized_components=normalized,
        weights=weights,
        candidates_considered=total_candidates,
        candidates_feasible=total_candidates,
    )


def explicit_selection(
    plan: BlockPlan,
    *,
    m: int,
    n: int,
    k: int,
    itemsize: int,
    l1_bytes: int,
    l2_bytes: int,
) -> PlanSelection:
    validation = validate_plan(
        plan,
        m=m,
        n=n,
        k=k,
        l1_budget_bytes=l1_bytes,
        l2_budget_bytes=l2_bytes,
        enforce_budgets=False,
        itemsize=itemsize,
    )
    metrics = validation["metrics"]
    reason = (
        "explicit plan validated: MC%MR==0, NC%NR==0, dimensions within hard caps; "
        f"edge tiles M/N/K={validation['edge_tiles']}; "
        f"L1={validation['register_workspace_bytes']} B, L2={validation['cache_workspace_bytes']} B"
    )
    return PlanSelection(
        plan=plan,
        score=1.0,
        workload_class="explicit",
        reason=reason,
        l1_budget_bytes=l1_bytes,
        l2_budget_bytes=l2_bytes,
        itemsize=itemsize,
        metrics=metrics,
        normalized_components={},
        weights={},
        candidates_considered=1,
        candidates_feasible=1,
    )

