from __future__ import annotations

import math
from typing import Any, Callable, Iterable

from .canonical import digest_obj
from .config import ArbitrationConfig, ZONE_ORDER
from .errors import PipelineStopped


def _overlap(start_a: int, end_a: int, start_b: int, end_b: int) -> int:
    return max(0, min(end_a, end_b) - max(start_a, start_b))


def _choose_interval(
    intervals: list[list[int]],
    cluster_start: int,
    cluster_end: int,
    search_window: int,
) -> list[int] | None:
    window_start = max(0, cluster_start - search_window)
    window_end = cluster_end + search_window
    best: tuple[int, int, int] | None = None
    best_interval: list[int] | None = None
    for interval in intervals:
        start, end = interval
        overlap = _overlap(start, end, cluster_start, cluster_end)
        window_overlap = _overlap(start, end, window_start, window_end)
        if window_overlap == 0:
            continue
        distance = abs(start - cluster_start)
        rank = (overlap, window_overlap, -distance)
        if best is None or rank > best:
            best = rank
            best_interval = interval
    return best_interval


def _group_hits(hits: list[dict[str, Any]], window_ms: int) -> list[list[dict[str, Any]]]:
    ordered = sorted(
        hits,
        key=lambda hit: (
            hit["word"],
            hit["start_ms"],
            hit["end_ms"],
            hit["zone_id"],
            hit["hit_id"],
        ),
    )
    groups: list[list[dict[str, Any]]] = []
    for hit in ordered:
        if not groups:
            groups.append([hit])
            continue
        current = groups[-1]
        same_word = current[0]["word"] == hit["word"]
        close = hit["start_ms"] - current[0]["start_ms"] <= window_ms
        if same_word and close:
            current.append(hit)
        else:
            groups.append([hit])
    return groups


def _priority_score(priorities: dict[str, int], zone: str, participants: list[str]) -> float:
    values = [priorities[item] for item in participants]
    low = min(values)
    high = max(values)
    if high == low:
        return 1.0
    return round((high - priorities[zone]) / (high - low), 9)


def _distance(geometry: dict[str, tuple[float, float, float]], first: str, second: str) -> float:
    return math.dist(geometry[first], geometry[second])


def arbitrate_shard(
    shard_id: str,
    activity: dict[str, list[list[int]]],
    hits: list[dict[str, Any]],
    priorities: dict[str, int],
    config: ArbitrationConfig,
) -> dict[str, Any]:
    assignments: list[dict[str, Any]] = []
    for cluster_index, group in enumerate(_group_hits(hits, config.cluster_window_ms)):
        cluster_start = min(hit["start_ms"] for hit in group)
        cluster_end = max(hit["end_ms"] for hit in group)
        hit_by_zone: dict[str, list[dict[str, Any]]] = {}
        for hit in group:
            hit_by_zone.setdefault(hit["zone_id"], []).append(hit)
        onset_by_zone: dict[str, int] = {}
        interval_by_zone: dict[str, list[int]] = {}
        for zone_id in ZONE_ORDER:
            interval = _choose_interval(
                activity.get(zone_id, []),
                cluster_start,
                cluster_end,
                config.hit_search_window_ms,
            )
            if interval is not None:
                interval_by_zone[zone_id] = interval
                onset_by_zone[zone_id] = interval[0]
            elif zone_id in hit_by_zone:
                fallback_start = min(hit["start_ms"] for hit in hit_by_zone[zone_id])
                onset_by_zone[zone_id] = fallback_start
        participants = [zone_id for zone_id in ZONE_ORDER if zone_id in onset_by_zone]
        hit_strength = {
            zone_id: round(
                max((hit["strength"] for hit in hit_by_zone.get(zone_id, [])), default=0.0),
                9,
            )
            for zone_id in participants
        }
        component_scores: dict[str, dict[str, Any]] = {}
        for hypothesis in participants:
            pair_details: list[dict[str, Any]] = []
            compatibilities: list[float] = []
            for first_index, first in enumerate(participants):
                for second in participants[first_index + 1 :]:
                    observed_delay = onset_by_zone[first] - onset_by_zone[second]
                    distance_difference = _distance(config.geometry_m, hypothesis, first) - _distance(
                        config.geometry_m, hypothesis, second
                    )
                    expected_delay = distance_difference / config.speed_of_sound_m_s * 1000.0
                    error = abs(observed_delay - expected_delay)
                    compatibility = math.exp(-error / config.tdoa_tolerance_ms)
                    compatibilities.append(compatibility)
                    pair_details.append(
                        {
                            "zones": [first, second],
                            "observed_delay_ms": observed_delay,
                            "distance_difference_m": round(distance_difference, 9),
                            "expected_delay_ms": round(expected_delay, 9),
                            "error_ms": round(error, 9),
                            "compatibility": round(compatibility, 9),
                        }
                    )
            tdoa_score = (
                round(sum(compatibilities) / len(compatibilities), 9)
                if compatibilities
                else config.neutral_tdoa_score
            )
            priority_score = _priority_score(priorities, hypothesis, participants)
            hit_score = hit_strength[hypothesis]
            total = (
                config.priority_weight * priority_score
                + config.hit_weight * hit_score
                + config.tdoa_weight * tdoa_score
            )
            component_scores[hypothesis] = {
                "priority": priorities[hypothesis],
                "priority_score": priority_score,
                "hit_strength": hit_score,
                "tdoa_score": tdoa_score,
                "total": round(total, 9),
                "pair_scores": pair_details,
            }
        ranked = sorted(
            participants,
            key=lambda zone: (
                -component_scores[zone]["total"],
                priorities[zone],
                ZONE_ORDER.index(zone),
            ),
        )
        top_zone = ranked[0]
        second_zone = ranked[1] if len(ranked) > 1 else None
        top_score = component_scores[top_zone]["total"]
        second_score = component_scores[second_zone]["total"] if second_zone else None
        gap = round(top_score - second_score, 9) if second_score is not None else None
        review = gap is not None and gap < config.review_margin
        status = "review" if review else "assigned"
        responsible_zone = None if review else top_zone
        explanation_hypothesis = top_zone
        pair_details = component_scores[explanation_hypothesis]["pair_scores"]
        assignment_identity = {
            "shard_id": shard_id,
            "word": group[0]["word"],
            "cluster_start_ms": cluster_start,
            "cluster_end_ms": cluster_end,
            "hits": sorted(hit["hit_id"] for hit in group),
        }
        assignments.append(
            {
                "assignment_id": "cmd-"
                + digest_obj(assignment_identity)[:16],
                "shard_id": shard_id,
                "cluster_index": cluster_index,
                "word": group[0]["word"],
                "cluster_start_ms": cluster_start,
                "cluster_end_ms": cluster_end,
                "status": status,
                "responsible_zone": responsible_zone,
                "top_candidate": top_zone,
                "second_candidate": second_zone,
                "score_gap": gap,
                "review_margin": config.review_margin,
                "participating_zones": participants,
                "active_interval_by_zone": interval_by_zone,
                "observed_onset_by_zone": onset_by_zone,
                "component_scores": component_scores,
                "pair_delay_details": pair_details,
                "source_hit_ids": sorted(hit["hit_id"] for hit in group),
            }
        )
    assignments.sort(
        key=lambda item: (
            item["cluster_start_ms"],
            item["cluster_end_ms"],
            item["word"],
            item["assignment_id"],
        )
    )
    for index, assignment in enumerate(assignments):
        assignment["ordinal"] = index
    return {
        "shard_id": shard_id,
        "assignments": assignments,
        "assignment_count": len(assignments),
        "review_count": sum(1 for item in assignments if item["status"] == "review"),
        "activity_digest": digest_obj(activity),
        "priorities_digest": digest_obj(priorities),
        "word_hits_digest": digest_obj(hits),
    }


def compute_arbitration(
    shards: list[dict[str, Any]],
    effective_activity: dict[str, dict[str, list[list[int]]]],
    priorities: dict[str, dict[str, int]],
    word_hits: dict[str, list[dict[str, Any]]],
    config: ArbitrationConfig,
    *,
    should_stop: Callable[[], bool] | None = None,
) -> dict[str, Any]:
    results: dict[str, Any] = {}
    for shard in shards:
        if should_stop is not None and should_stop():
            raise PipelineStopped()
        shard_id = shard["shard_id"]
        results[shard_id] = arbitrate_shard(
            shard_id,
            effective_activity[shard_id],
            word_hits[shard_id],
            priorities[shard_id],
            config,
        )
    return {
        "config_digest": digest_obj(
            {
                "arbitration": {
                    "speed_of_sound_m_s": config.speed_of_sound_m_s,
                    "geometry_m": config.geometry_m,
                    "priority_weight": config.priority_weight,
                    "hit_weight": config.hit_weight,
                    "tdoa_weight": config.tdoa_weight,
                    "tdoa_tolerance_ms": config.tdoa_tolerance_ms,
                    "review_margin": config.review_margin,
                    "cluster_window_ms": config.cluster_window_ms,
                    "hit_search_window_ms": config.hit_search_window_ms,
                    "neutral_tdoa_score": config.neutral_tdoa_score,
                }
            }
        ),
        "shards": results,
    }
