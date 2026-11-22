"""Exact and adaptive stratified election algorithms.

The adaptive path deliberately keeps the decisive highest-priority bucket
exhaustive.  Lower-priority buckets are sampled only for population estimates
and confidence reporting.  This gives a deterministic winner identical to an
exact scan while making the bounded comparison work and the estimation scope
explicit in the response.
"""

from __future__ import annotations

import hashlib
import heapq
import math
from dataclasses import dataclass
from typing import Any, Iterable, Sequence

from .config import CoordinatorConfig
from .models import NodeRecord, NodeStatus, digest_json

try:  # NumPy is part of the requested environment; pure-stdlib fallback remains.
    import numpy as np
except Exception:  # pragma: no cover - exercised only on reduced installations
    np = None  # type: ignore[assignment]


class NoEligibleCandidateError(Exception):
    def __init__(self, *, candidate_count: int, algorithm: str) -> None:
        super().__init__("no heartbeat-fresh eligible candidate")
        self.candidate_count = candidate_count
        self.algorithm = algorithm


@dataclass(frozen=True, slots=True)
class ElectionResult:
    winner: NodeRecord
    metadata: dict[str, Any]


def _is_eligible(node: NodeRecord, now_ms: int, config: CoordinatorConfig) -> bool:
    return (
        config.priority_min <= node.priority <= config.priority_max
        and node.status is not NodeStatus.SUSPECT
        and node.heartbeat_fresh(now_ms, config.heartbeat_timeout_ms)
    )


def _wilson_interval(successes: int, total: int, z: float = 1.959963984540054) -> tuple[float, float]:
    if total <= 0:
        return (0.0, 0.0)
    p = successes / total
    denominator = 1.0 + z * z / total
    center = (p + z * z / (2.0 * total)) / denominator
    margin = (
        z
        * math.sqrt((p * (1.0 - p) + z * z / (4.0 * total)) / total)
        / denominator
    )
    return (max(0.0, center - margin), min(1.0, center + margin))


def _sample_key(seed: str, node_id: str) -> str:
    return hashlib.sha256(f"{seed}|{node_id}".encode("utf-8")).hexdigest()


def _candidate_digest(nodes: Sequence[NodeRecord]) -> str:
    return digest_json(
        [
            {
                "id": node.id,
                "priority": node.priority,
                "lease_ttl_ms": node.lease_ttl_ms,
                "last_heartbeat_ms": node.last_heartbeat_ms,
                "status": node.status.value,
            }
            for node in sorted(nodes, key=lambda item: item.id)
        ]
    )


def elect(
    nodes: Sequence[NodeRecord],
    *,
    now_ms: int,
    config: CoordinatorConfig,
    seed_extra: dict[str, Any] | None = None,
) -> ElectionResult:
    candidate_count = len(nodes)
    if candidate_count == 0:
        raise NoEligibleCandidateError(
            candidate_count=0,
            algorithm="exact_full_scan",
        )

    if candidate_count <= config.exact_limit:
        return _elect_exact(nodes, now_ms=now_ms, config=config)
    return _elect_stratified(
        nodes,
        now_ms=now_ms,
        config=config,
        seed_extra=seed_extra or {},
    )


def _elect_exact(
    nodes: Sequence[NodeRecord], *, now_ms: int, config: CoordinatorConfig
) -> ElectionResult:
    eligible = [node for node in nodes if _is_eligible(node, now_ms, config)]
    if not eligible:
        raise NoEligibleCandidateError(
            candidate_count=len(nodes),
            algorithm="exact_full_scan",
        )
    ranked = sorted(eligible, key=lambda node: (-node.priority, node.id))
    winner = ranked[0]
    tied_top = [node for node in ranked if node.priority == winner.priority]
    metadata: dict[str, Any] = {
        "algorithm": "exact_full_scan",
        "estimated": False,
        "estimated_winner": False,
        "candidate_count": len(nodes),
        "eligible_count": len(eligible),
        "exact_limit": config.exact_limit,
        "criterion_order": ["priority_desc", "node_id_asc"],
        "winner_evidence": {
            "node_id": winner.id,
            "priority": winner.priority,
            "top_priority_tie_count": len(tied_top),
            "tie_break": "lexicographically_smallest_node_id",
        },
        "comparisons": max(0, len(eligible) - 1),
        "candidate_set_digest": _candidate_digest(nodes),
        "buckets": _exact_bucket_summary(nodes, now_ms, config),
        "difference_vs_exact": {
            "same_winner": True,
            "basis": "this run is the exact full scan",
        },
    }
    return ElectionResult(winner=winner, metadata=metadata)


def _elect_stratified(
    nodes: Sequence[NodeRecord],
    *,
    now_ms: int,
    config: CoordinatorConfig,
    seed_extra: dict[str, Any],
) -> ElectionResult:
    buckets: dict[int, list[NodeRecord]] = {}
    for node in nodes:
        buckets.setdefault(node.priority, []).append(node)
    priorities = sorted(buckets, reverse=True)
    seed = digest_json(
        {
            "algorithm": "priority_stratified_sampling",
            "seed_extra": seed_extra,
            "population": [
                [node.id, node.priority, node.last_heartbeat_ms, node.status.value]
                for node in sorted(nodes, key=lambda item: item.id)
            ],
        }
    )

    evaluated_count = 0
    bucket_output: list[dict[str, Any]] = []
    decisive_priority: int | None = None
    winner: NodeRecord | None = None
    decisive_eligible: list[NodeRecord] = []
    lower_estimates: list[float] = []
    lower_variances: list[float] = []

    for position, priority in enumerate(priorities):
        members = buckets[priority]
        if decisive_priority is None:
            eligible = [node for node in members if _is_eligible(node, now_ms, config)]
            evaluated_count += len(members)
            bucket_record: dict[str, Any] = {
                "priority": priority,
                "population": len(members),
                "eligible_evaluated": len(eligible),
                "fully_evaluated": True,
                "evaluation_rate": 1.0,
                "position_from_top": position,
            }
            if eligible:
                decisive_priority = priority
                decisive_eligible = eligible
                winner = min(eligible, key=lambda node: node.id)
                bucket_record.update(
                    {
                        "role": "decisive",
                        "winner_candidate": winner.id,
                        "top_tie_count": len(eligible),
                    }
                )
            else:
                bucket_record["role"] = "ruled_out"
            bucket_output.append(bucket_record)
            continue

        population = len(members)
        sample_size = min(
            population,
            max(
                config.sample_min_per_bucket,
                math.ceil(population * config.sample_fraction),
            ),
        )
        sampled = heapq.nsmallest(
            sample_size,
            members,
            key=lambda node: _sample_key(seed, node.id),
        )
        evaluated_count += len(sampled)
        sampled_eligible = sum(
            1 for node in sampled if _is_eligible(node, now_ms, config)
        )
        p = sampled_eligible / len(sampled) if sampled else 0.0
        lower, upper = _wilson_interval(sampled_eligible, len(sampled))
        estimate = p * population
        variance = population * population * p * (1.0 - p) / max(1, len(sampled))
        lower_estimates.append(estimate)
        lower_variances.append(variance)
        bucket_output.append(
            {
                "priority": priority,
                "population": population,
                "sampled": len(sampled),
                "sampled_eligible": sampled_eligible,
                "estimated_eligible": round(estimate, 3),
                "estimated_eligible_ratio": round(p, 6),
                "eligible_ratio_wilson_95": [
                    round(lower, 6),
                    round(upper, 6),
                ],
                "fully_evaluated": False,
                "evaluation_rate": (
                    round(len(sampled) / population, 6) if population else 0.0
                ),
                "position_from_top": position,
                "role": "lower_priority_sample",
            }
        )

    if winner is None or decisive_priority is None:
        # Determining that no bucket is decisive necessarily inspects all buckets.
        raise NoEligibleCandidateError(
            candidate_count=len(nodes),
            algorithm="priority_stratified_sampling",
        )

    if np is not None and lower_estimates:
        estimated_lower_total = float(np.sum(np.asarray(lower_estimates, dtype=float)))
        estimated_lower_std = float(
            np.sqrt(np.sum(np.asarray(lower_variances, dtype=float)))
        )
    else:
        estimated_lower_total = float(sum(lower_estimates))
        estimated_lower_std = math.sqrt(sum(lower_variances))

    exact_evaluated_eligible = sum(
        int(bucket.get("eligible_evaluated", 0))
        for bucket in bucket_output
        if bucket.get("fully_evaluated")
    )
    fully_evaluated_count = sum(
        int(bucket["population"])
        for bucket in bucket_output
        if bucket.get("fully_evaluated")
    )
    estimated_total_eligible = exact_evaluated_eligible + estimated_lower_total
    sampling_rate = evaluated_count / len(nodes) if nodes else 0.0
    metadata: dict[str, Any] = {
        "algorithm": "priority_stratified_sampling",
        "estimated": True,
        "estimated_winner": True,
        "candidate_count": len(nodes),
        "eligible_count_estimate": round(estimated_total_eligible, 3),
        "eligible_count_estimate_stddev": round(estimated_lower_std, 3),
        "exact_limit": config.exact_limit,
        "sample_fraction": config.sample_fraction,
        "sample_seed": seed,
        "sampling_rate": round(sampling_rate, 6),
        "sampled_candidate_count": evaluated_count,
        "fully_evaluated_candidate_count": fully_evaluated_count,
        "decisive_priority": decisive_priority,
        "criterion_order": ["priority_desc", "node_id_asc"],
        "winner_evidence": {
            "node_id": winner.id,
            "priority": winner.priority,
            "top_priority_tie_count": len(decisive_eligible),
            "tie_break": "lexicographically_smallest_node_id",
            "decisive_bucket_exhaustive": True,
        },
        "buckets": bucket_output,
        "confidence": {
            "level": 0.95,
            "winner_confidence": 1.0,
            "basis": (
                "The decisive highest-priority eligible bucket is fully scanned; "
                "lower-priority buckets cannot outrank it. Lower bucket eligible "
                "population estimates use deterministic sampling and Wilson 95% intervals."
            ),
            "estimated_scope": "lower_priority_bucket_population_only",
        },
        "certification": {
            "winner_certified_by_decisive_bucket": True,
            "note": (
                "Sampling path is explicitly estimated, but the winner identity is "
                "deterministically certified by exhaustive decisive-bucket comparison."
            ),
        },
        "comparisons": max(0, evaluated_count - 1),
        "candidate_set_digest": _candidate_digest(nodes),
        "difference_vs_exact": {
            "same_winner": True,
            "basis": "decisive priority bucket is exhaustively compared",
            "compared_items": ["priority", "node_id", "heartbeat_freshness", "SUSPECT_status"],
        },
    }
    return ElectionResult(winner=winner, metadata=metadata)


def _exact_bucket_summary(
    nodes: Iterable[NodeRecord], now_ms: int, config: CoordinatorConfig
) -> list[dict[str, Any]]:
    buckets: dict[int, list[NodeRecord]] = {}
    for node in nodes:
        buckets.setdefault(node.priority, []).append(node)
    result = []
    for priority in sorted(buckets, reverse=True):
        members = buckets[priority]
        eligible = [node for node in members if _is_eligible(node, now_ms, config)]
        result.append(
            {
                "priority": priority,
                "population": len(members),
                "eligible": len(eligible),
                "fully_evaluated": True,
                "evaluation_rate": 1.0,
            }
        )
    return result
