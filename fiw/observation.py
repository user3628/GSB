"""Per-round endpoint observation conclusions with upstream propagation.

Conclusions are computed from the committed trajectory of the round only;
no uncommitted intermediate observations are ever consulted.
"""

from __future__ import annotations

from .catalog import topo_order
from .models import (
    CONCLUSION_NORMAL,
    CONCLUSION_SEVERITY,
    CONCLUSION_UNREACHABLE,
    CONCLUSION_UNSTABLE,
    FAULT_CONNECTION_REFUSED,
    FAULT_CONNECTION_TIMEOUT,
    FAULT_CORRUPT_BODY,
    OUTCOME_INJECTED,
    OUTCOME_TIMEOUT,
)

WEIGHT_UNSTABLE_THRESHOLD = 0.5

_FAULT_IMPACT = {
    FAULT_CONNECTION_REFUSED: CONCLUSION_UNREACHABLE,
    FAULT_CONNECTION_TIMEOUT: CONCLUSION_UNREACHABLE,
    FAULT_CORRUPT_BODY: CONCLUSION_UNSTABLE,
}

_LEVELS = [CONCLUSION_NORMAL, CONCLUSION_UNSTABLE, CONCLUSION_UNREACHABLE]


def _escalate(conclusion):
    idx = min(CONCLUSION_SEVERITY[conclusion] + 1, len(_LEVELS) - 1)
    return _LEVELS[idx]


def compute_observations(catalog, trajectory):
    """Return {endpoint_id: observation} for every endpoint in the catalog."""
    by_id = catalog.by_id
    impacts = {}
    for entry in trajectory:
        if entry["outcome"] == OUTCOME_INJECTED or (
                entry["outcome"] == OUTCOME_TIMEOUT):
            level = _FAULT_IMPACT[entry["fault_type"]]
            prev = impacts.get(entry["target"])
            if prev is None or CONCLUSION_SEVERITY[level] > CONCLUSION_SEVERITY[prev[0]]:
                impacts[entry["target"]] = (level, entry)

    observations = {}
    for eid in topo_order(catalog):
        ep = by_id[eid]
        reasons = []
        propagation = []
        conclusion = CONCLUSION_NORMAL
        if ep.weight < WEIGHT_UNSTABLE_THRESHOLD:
            conclusion = CONCLUSION_UNSTABLE
            reasons.append(f"health weight {ep.weight} below "
                           f"{WEIGHT_UNSTABLE_THRESHOLD} baseline threshold")
        if eid in impacts:
            level, entry = impacts[eid]
            if CONCLUSION_SEVERITY[level] > CONCLUSION_SEVERITY[conclusion]:
                conclusion = level
            reasons.append(f"injection {entry['item_id']} ({entry['fault_type']}) "
                           f"confirmed with outcome {entry['outcome']}")
        for up in ep.upstreams:
            upstream_conclusion = observations[up]["conclusion"]
            if upstream_conclusion == CONCLUSION_UNREACHABLE:
                downgraded = _escalate(conclusion)
                propagation.append({
                    "upstream": up,
                    "upstream_conclusion": upstream_conclusion,
                    "rule": "upstream_unreachable_downgrade",
                    "from": conclusion,
                    "to": downgraded,
                })
                conclusion = downgraded
        observations[eid] = {
            "endpoint": eid,
            "node": ep.node,
            "weight": ep.weight,
            "conclusion": conclusion,
            "reasons": reasons,
            "propagation": propagation,
        }
    return observations
