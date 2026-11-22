"""Deterministic cache-aware selection of three-level GEMM tile plans."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np

from .errors import BlockGemmError, E_INVALID_CONFIG
from .fingerprint import sha256_json


DEFAULT_L1_CACHE_KIB = 64
DEFAULT_L2_CACHE_KIB = 1024
TARGET_PARALLEL_BLOCKS = 16


@dataclass(frozen=True, order=True)
class TilePlan:
    """A deterministic MC/KC/NC cache plan with an MR/NR register plan."""

    mr: int
    nr: int
    mc: int
    kc: int
    nc: int

    def __post_init__(self) -> None:
        values = (self.mr, self.nr, self.mc, self.kc, self.nc)
        if any(v <= 0 for v in values):
            raise ValueError("all tile dimensions must be positive")
        if self.mc % self.mr or self.nc % self.nr:
            raise ValueError("MC must be divisible by MR and NC by NR")

    def l1_working_set_bytes(self, itemsize: int) -> int:
        # One MR x NR register accumulator plus its MCxKC/KCxNR stream windows.
        return (self.mr * self.kc + self.kc * self.nr + self.mr * self.nr) * itemsize

    def l2_working_set_bytes(self, itemsize: int) -> int:
        # A MCxKC panel, a KCxNC panel, and the MCxNC output cache block.
        return (self.mc * self.kc + self.kc * self.nc + self.mc * self.nc) * itemsize

    def to_dict(self) -> dict[str, int]:
        return {
            "MR": self.mr,
            "NR": self.nr,
            "MC": self.mc,
            "KC": self.kc,
            "NC": self.nc,
        }

    def key(self) -> tuple[int, int, int, int, int]:
        return (self.mr, self.nr, self.mc, self.kc, self.nc)

    def fingerprint(self) -> str:
        return sha256_json(
            {
                "schema": "blockgemm.tile-plan.v1",
                **self.to_dict(),
            }
        )


@dataclass(frozen=True)
class CandidateScore:
    plan: TilePlan
    score: float
    details: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "plan": self.plan.to_dict(),
            "plan_fingerprint": self.plan.fingerprint(),
            "score": self.score,
            "details": self.details,
        }


@dataclass(frozen=True)
class PlanSelection:
    plan: TilePlan
    l1_cache_kib: int
    l2_cache_kib: int
    selected: CandidateScore
    candidates: list[CandidateScore]
    rejected_counts: dict[str, int]

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": "blockgemm.plan-selection.v1",
            "selected": self.selected.to_dict(),
            "plan_fingerprint": self.plan.fingerprint(),
            "cache_budget": {
                "l1_cache_kib": self.l1_cache_kib,
                "l2_cache_kib": self.l2_cache_kib,
                "l1_cache_bytes": self.l1_cache_kib * 1024,
                "l2_cache_bytes": self.l2_cache_kib * 1024,
            },
            "candidate_count": len(self.candidates) + sum(self.rejected_counts.values()),
            "feasible_candidate_count": len(self.candidates),
            "rejected_counts": dict(sorted(self.rejected_counts.items())),
            "top_candidates": [c.to_dict() for c in self.candidates[:5]],
            "reason": (
                "Chose the lowest deterministic score among candidates that fit "
                "both L1 and L2 working-set budgets; score blends predicted DRAM "
                "traffic, cache pressure, tile-capacity waste, and parallel-block "
                "shortfall against a target of 16 output blocks."
            ),
        }


def _ceil_div(a: int, b: int) -> int:
    return (a + b - 1) // b


def _candidate_plans() -> list[TilePlan]:
    plans: list[TilePlan] = []
    for mr, nr in ((4, 4), (8, 4), (8, 8)):
        for mc in (32, 64, 96, 128, 192, 256):
            for nc in (32, 64, 96, 128, 192, 256):
                if mc % mr or nc % nr:
                    continue
                for kc in (32, 64, 128, 256):
                    plans.append(TilePlan(mr, nr, mc, kc, nc))
    return plans


def _score_candidate(
    plan: TilePlan,
    *,
    m: int,
    n: int,
    k: int,
    batch: int,
    itemsize: int,
    l1_bytes: int,
    l2_bytes: int,
) -> CandidateScore:
    tiles_m = _ceil_div(m, plan.mc)
    tiles_n = _ceil_div(n, plan.nc)
    k_steps = _ceil_div(k, plan.kc)
    parallel_blocks = tiles_m * tiles_n * batch

    traffic_elems = 0
    for _batch_index in range(batch):
        for tile_m in range(tiles_m):
            tile_h = min(plan.mc, m - tile_m * plan.mc)
            for tile_n in range(tiles_n):
                tile_w = min(plan.nc, n - tile_n * plan.nc)
                for k_step in range(k_steps):
                    kc = min(plan.kc, k - k_step * plan.kc)
                    traffic_elems += tile_h * kc + kc * tile_w
                traffic_elems += tile_h * tile_w

    ideal_elems = (m * k + k * n + m * n) * batch
    normalized_traffic = traffic_elems / max(ideal_elems, 1)
    l1_ws = plan.l1_working_set_bytes(itemsize)
    l2_ws = plan.l2_working_set_bytes(itemsize)
    l1_pressure = l1_ws / l1_bytes
    l2_pressure = l2_ws / l2_bytes
    capacity = tiles_m * plan.mc * tiles_n * plan.nc * batch
    tile_waste = capacity / max(m * n * batch, 1) - 1.0
    parallel_shortfall = max(0.0, TARGET_PARALLEL_BLOCKS - parallel_blocks) / TARGET_PARALLEL_BLOCKS
    microtiles_m = _ceil_div(plan.mc, plan.mr)
    microtiles_n = _ceil_div(plan.nc, plan.nr)
    estimated_microkernel_calls = (
        tiles_m * tiles_n * batch * microtiles_m * microtiles_n * k_steps
    )
    microkernel_overhead = 1e-6 * estimated_microkernel_calls

    score = (
        normalized_traffic
        + 0.12 * l2_pressure
        + 0.04 * l1_pressure
        + 0.20 * tile_waste
        + 0.08 * parallel_shortfall
        + microkernel_overhead
    )
    return CandidateScore(
        plan=plan,
        score=score,
        details={
            "tiles_m": tiles_m,
            "tiles_n": tiles_n,
            "k_steps": k_steps,
            "parallel_blocks": parallel_blocks,
            "predicted_traffic_elements": traffic_elems,
            "predicted_traffic_bytes": traffic_elems * itemsize,
            "ideal_traffic_elements": ideal_elems,
            "normalized_traffic": normalized_traffic,
            "l1_working_set_bytes": l1_ws,
            "l2_working_set_bytes": l2_ws,
            "l1_pressure": l1_pressure,
            "l2_pressure": l2_pressure,
            "tile_capacity_waste_ratio": tile_waste,
            "parallel_shortfall": parallel_shortfall,
            "estimated_microkernel_calls": estimated_microkernel_calls,
            "microkernel_call_overhead": microkernel_overhead,
        },
    )


def select_tiling(
    *,
    m: int,
    n: int,
    k: int,
    batch: int = 1,
    dtype: str | np.dtype = "float64",
    l1_cache_kib: int = DEFAULT_L1_CACHE_KIB,
    l2_cache_kib: int = DEFAULT_L2_CACHE_KIB,
) -> PlanSelection:
    """Select the same plan for the same shape, dtype, and cache budgets."""

    if min(m, n, k, batch) <= 0:
        raise BlockGemmError(
            E_INVALID_CONFIG,
            "M, N, K, and batch must all be positive",
            reason="non_positive_problem_size",
            details={"M": m, "N": n, "K": k, "batch": batch},
        )
    if l1_cache_kib <= 0 or l2_cache_kib <= 0:
        raise BlockGemmError(
            E_INVALID_CONFIG,
            "cache budgets must be positive",
            reason="non_positive_cache_budget",
            details={"l1_cache_kib": l1_cache_kib, "l2_cache_kib": l2_cache_kib},
        )
    itemsize = np.dtype(dtype).itemsize
    l1_bytes = int(l1_cache_kib) * 1024
    l2_bytes = int(l2_cache_kib) * 1024

    feasible: list[CandidateScore] = []
    rejected = {"l1_budget": 0, "l2_budget": 0}
    for plan in _candidate_plans():
        l1_ws = plan.l1_working_set_bytes(itemsize)
        l2_ws = plan.l2_working_set_bytes(itemsize)
        if l1_ws > l1_bytes:
            rejected["l1_budget"] += 1
            continue
        if l2_ws > l2_bytes:
            rejected["l2_budget"] += 1
            continue
        feasible.append(
            _score_candidate(
                plan,
                m=int(m),
                n=int(n),
                k=int(k),
                batch=int(batch),
                itemsize=itemsize,
                l1_bytes=l1_bytes,
                l2_bytes=l2_bytes,
            )
        )

    if not feasible:
        raise BlockGemmError(
            E_INVALID_CONFIG,
            "no tile candidate fits the configured L1/L2 cache budgets",
            reason="no_cache_feasible_tiling",
            details={
                "M": m,
                "N": n,
                "K": k,
                "batch": batch,
                "dtype": str(np.dtype(dtype)),
                "l1_cache_kib": l1_cache_kib,
                "l2_cache_kib": l2_cache_kib,
                "rejected_counts": rejected,
                "smallest_candidate": TilePlan(4, 4, 32, 32, 32).to_dict(),
            },
        )

    feasible.sort(key=lambda item: (round(item.score, 15), item.plan.key()))
    chosen = feasible[0]
    # Keep a small deterministic candidate table in persisted plan reports.
    top = feasible[:10]
    return PlanSelection(
        plan=chosen.plan,
        l1_cache_kib=int(l1_cache_kib),
        l2_cache_kib=int(l2_cache_kib),
        selected=chosen,
        candidates=top,
        rejected_counts=rejected,
    )
