from __future__ import annotations

from collections import deque
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from .domain import HOP_BASE, MAX_HOPS, SCORE_SCALE, SCORE_TOLERANCE


class FixedPointContractError(ValueError):
    """Raised when a fixed-point intermediate is outside its documented range."""


def _bounded(value: int, low: int, high: int, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise FixedPointContractError(f"{label} must be an integer")
    if value < low or value > high:
        raise FixedPointContractError(f"{label} out of range: {value} not in [{low}, {high}]")
    return value


def hop_component(hops: int) -> int:
    """Return the bounded downhill component for a positive topology hop count."""
    hops = _bounded(hops, 1, MAX_HOPS, "hops")
    value = HOP_BASE // (1 << hops)
    return _bounded(value, 0, SCORE_SCALE, "hop_component")


def time_component(delta_ms: int, window_ms: int) -> int:
    """Return round-to-nearest fixed-point proximity for a closed time window.

    Both endpoints are inclusive.  delta == 0 maps to SCORE_SCALE and
    delta == window_ms maps to zero.
    """
    delta_ms = _bounded(delta_ms, 0, window_ms, "delta_ms")
    window_ms = _bounded(window_ms, 1, 2**63 - 1, "window_ms")
    numerator = (window_ms - delta_ms) * SCORE_SCALE + window_ms // 2
    value = numerator // window_ms
    return _bounded(value, 0, SCORE_SCALE, "time_component")


def combine_components(hop_value: int, time_value: int) -> int:
    hop_value = _bounded(hop_value, 0, SCORE_SCALE, "hop_component")
    time_value = _bounded(time_value, 0, SCORE_SCALE, "time_component")
    value = (hop_value * time_value + SCORE_SCALE // 2) // SCORE_SCALE
    return _bounded(value, 0, SCORE_SCALE, "score_fixed")


@dataclass(frozen=True)
class ScoreResult:
    hops: int
    delta_ms: int
    hop_component: int
    time_component: int
    score_fixed: int


def score_pair(
    *,
    hops: int,
    source_timestamp_ms: int,
    candidate_timestamp_ms: int,
    time_window_ms: int,
) -> ScoreResult:
    """Score an effect alert against an earlier-or-equal root-cause alert.

    The root-cause time window is closed: candidate timestamps in
    [source_timestamp_ms - time_window_ms, source_timestamp_ms] are eligible.
    """
    source_ts = _bounded(source_timestamp_ms, 0, 2**63 - 1, "source_timestamp_ms")
    candidate_ts = _bounded(candidate_timestamp_ms, 0, 2**63 - 1, "candidate_timestamp_ms")
    time_window_ms = _bounded(time_window_ms, 1, 2**63 - 1, "time_window_ms")
    if candidate_ts > source_ts:
        raise FixedPointContractError("candidate timestamp must not be later than the source alert")
    delta = source_ts - candidate_ts
    if delta > time_window_ms:
        raise FixedPointContractError("candidate is outside the closed time window")
    h = hop_component(hops)
    t = time_component(delta, time_window_ms)
    s = combine_components(h, t)
    return ScoreResult(hops, delta, h, t, s)


def shortest_downstream_distances(
    adjacency: Mapping[str, Iterable[str]],
    source: str,
    *,
    max_hops: int = MAX_HOPS,
) -> dict[str, int]:
    """BFS shortest downstream distances, excluding the source at hop zero."""
    distances: dict[str, int] = {}
    queue: deque[tuple[str, int]] = deque()
    for target in sorted(set(adjacency.get(source, ()))):
        if target != source:
            queue.append((target, 1))
    while queue:
        service, hops = queue.popleft()
        if hops > max_hops:
            continue
        prior = distances.get(service)
        if prior is not None and prior <= hops:
            continue
        distances[service] = hops
        for target in sorted(set(adjacency.get(service, ()))):
            if target != service and target not in distances:
                queue.append((target, hops + 1))
    return distances


def scores_within_tolerance(left: int, right: int, tolerance: int = SCORE_TOLERANCE) -> bool:
    if tolerance < 0:
        raise ValueError("tolerance must be non-negative")
    return abs(left - right) <= tolerance


def stable_tie_key(row: Mapping[str, Any]) -> tuple[str, str, str]:
    return (
        str(row.get("candidate_service", "")),
        str(row.get("candidate_fingerprint", "")),
        str(row.get("candidate_alert_uid", "")),
    )


def rank_with_tolerance(
    rows: Sequence[Mapping[str, Any]],
    *,
    tolerance: int = SCORE_TOLERANCE,
) -> list[dict[str, Any]]:
    """Apply deterministic tolerance clustering and stable tie ordering.

    Scores are first ordered by exact value.  A cluster starts at the highest
    exact score and accepts following rows while leader_score - score <=
    tolerance.  Each cluster is then ordered by (service, fingerprint, uid) and
    receives sequential ranks.  This makes the result transitive and stable;
    the raw score is never rewritten.
    """
    if tolerance < 0:
        raise ValueError("tolerance must be non-negative")
    prepared = [dict(row) for row in rows]
    for row in prepared:
        score = row.get("score_fixed")
        _bounded(score, 0, SCORE_SCALE, "score_fixed")
    exact = sorted(prepared, key=lambda r: (-int(r["score_fixed"]), stable_tie_key(r)))
    clusters: list[tuple[int, list[dict[str, Any]]]] = []
    for row in exact:
        score = int(row["score_fixed"])
        if not clusters or clusters[-1][0] - score > tolerance:
            clusters.append((score, [row]))
        else:
            clusters[-1][1].append(row)
    ranked: list[dict[str, Any]] = []
    rank = 1
    for _, cluster in clusters:
        for row in sorted(cluster, key=stable_tie_key):
            row["rank"] = rank
            ranked.append(row)
            rank += 1
    return ranked
