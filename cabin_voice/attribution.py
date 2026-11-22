"""Cross-zone arrival-delay evidence and command ownership arbitration."""

from __future__ import annotations

import math
from typing import Any, Iterable

from .errors import CabinVoiceError


def _zone_geometry(manifest: dict[str, Any]) -> tuple[dict[str, dict[str, Any]], dict[str, float]]:
    microphone = manifest.get("microphone", {"x_m": 0.0, "y_m": 0.0})
    mx = float(microphone["x_m"])
    my = float(microphone["y_m"])
    zones = {str(zone["zone_id"]): zone for zone in manifest["zones"]}
    distances = {
        zone_id: math.hypot(float(zone["x_m"]) - mx, float(zone["y_m"]) - my)
        for zone_id, zone in zones.items()
    }
    return zones, distances


def _interval_id(zone_id: str, interval: dict[str, Any]) -> str:
    return f"{zone_id}:{interval['start_ms']}-{interval['end_ms']}"


def estimate_delays(
    detection: dict[str, Any],
    manifest: dict[str, Any],
    arrival: dict[str, Any],
) -> dict[str, Any]:
    """Compute overlapping zone-pair onset differences and geometry residuals."""

    zones, distances = _zone_geometry(manifest)
    speed = float(arrival["speed_of_sound_mps"])
    sigma = float(arrival["delay_sigma_ms"])
    intervals = {
        str(zone["zone_id"]): list(zone["effective_intervals"])
        for zone in detection["zones"]
    }
    ordered_zones = sorted(intervals)
    pairs: list[dict[str, Any]] = []
    for left_index, left_zone in enumerate(ordered_zones):
        for right_zone in ordered_zones[left_index + 1 :]:
            for left_interval in intervals[left_zone]:
                for right_interval in intervals[right_zone]:
                    overlap_start = max(left_interval["start_ms"], right_interval["start_ms"])
                    overlap_end = min(left_interval["end_ms"], right_interval["end_ms"])
                    if overlap_end <= overlap_start:
                        continue
                    observed_delay_ms = int(right_interval["start_ms"] - left_interval["start_ms"])
                    expected_delay_ms = (distances[right_zone] - distances[left_zone]) / speed * 1000.0
                    residual_ms = observed_delay_ms - expected_delay_ms
                    penalty = 0.5 * (residual_ms / sigma) ** 2
                    pair = {
                        "a_zone": left_zone,
                        "b_zone": right_zone,
                        "a_interval_id": _interval_id(left_zone, left_interval),
                        "b_interval_id": _interval_id(right_zone, right_interval),
                        "overlap_ms": overlap_end - overlap_start,
                        "observed_delay_ms": observed_delay_ms,
                        "expected_delay_ms": round(expected_delay_ms, 6),
                        "residual_ms": round(residual_ms, 6),
                        "normalized_penalty": round(penalty, 9),
                    }
                    if not math.isfinite(penalty):
                        raise CabinVoiceError(
                            "E_NONFINITE_SCORE",
                            "delay score is not finite",
                            {
                                "zone_id": left_zone,
                                "other_zone_id": right_zone,
                                "parameter": "arrival.delay_sigma_ms",
                                "score": penalty,
                            },
                        )
                    pairs.append(pair)
    pair_scores = {
        zone_id: {
            "distance_to_microphone_m": round(distances[zone_id], 9),
            "x_m": float(zones[zone_id]["x_m"]),
            "y_m": float(zones[zone_id]["y_m"]),
        }
        for zone_id in ordered_zones
    }
    return {
        "speed_of_sound_mps": speed,
        "delay_sigma_ms": sigma,
        "zone_geometry": pair_scores,
        "pair_delays": pairs,
    }


def _select_interval(
    intervals: Iterable[dict[str, Any]], anchor_ms: int | None
) -> dict[str, Any] | None:
    values = list(intervals)
    if not values:
        return None
    if anchor_ms is not None:
        containing = [
            interval
            for interval in values
            if interval["start_ms"] <= anchor_ms < interval["end_ms"]
        ]
        if containing:
            return min(containing, key=lambda item: (item["end_ms"] - item["start_ms"], item["start_ms"]))
        return min(
            values,
            key=lambda item: (
                min(abs(anchor_ms - item["start_ms"]), abs(anchor_ms - item["end_ms"])),
                item["start_ms"],
            ),
        )
    return values[0]


def _delay_evidence(
    zone_id: str,
    interval_id: str | None,
    delay_result: dict[str, Any],
) -> list[dict[str, Any]]:
    if interval_id is None:
        return []
    evidence: list[dict[str, Any]] = []
    for pair in delay_result["pair_delays"]:
        if pair["a_zone"] == zone_id and pair["a_interval_id"] == interval_id:
            evidence.append(
                {
                    "other_zone_id": pair["b_zone"],
                    "observed_delay_ms": pair["observed_delay_ms"],
                    "expected_delay_ms": pair["expected_delay_ms"],
                    "residual_ms": pair["residual_ms"],
                    "normalized_penalty": pair["normalized_penalty"],
                }
            )
        elif pair["b_zone"] == zone_id and pair["b_interval_id"] == interval_id:
            evidence.append(
                {
                    "other_zone_id": pair["a_zone"],
                    "observed_delay_ms": -pair["observed_delay_ms"],
                    "expected_delay_ms": -pair["expected_delay_ms"],
                    "residual_ms": -pair["residual_ms"],
                    "normalized_penalty": pair["normalized_penalty"],
                }
            )
    return evidence


def _hit_anchor(hits: list[dict[str, Any]]) -> int | None:
    values = [int(hit["at_ms"]) for hit in hits if "at_ms" in hit]
    return min(values) if values else None


def arbitrate_commands(
    chunk: dict[str, Any],
    detection: dict[str, Any],
    delay_result: dict[str, Any],
    hits_by_zone: dict[str, list[dict[str, Any]]],
    priorities: dict[str, int],
    attribution_config: dict[str, Any],
) -> dict[str, Any]:
    """Assign each suspected term to one zone, or expose a review pair."""

    effective = {
        str(zone["zone_id"]): list(zone["effective_intervals"])
        for zone in detection["zones"]
    }
    grouped: dict[str, list[dict[str, Any]]] = {}
    for zone_id in sorted(hits_by_zone):
        for hit in hits_by_zone[zone_id]:
            grouped.setdefault(str(hit["term"]), []).append({"zone_id": zone_id, **hit})

    priority_weight = float(attribution_config["priority_weight"])
    floor = float(attribution_config["lexicon_floor"])
    review_threshold = float(attribution_config["review_gap_threshold"])
    events: list[dict[str, Any]] = []
    for term in sorted(grouped):
        by_zone: dict[str, list[dict[str, Any]]] = {}
        for hit in grouped[term]:
            by_zone.setdefault(str(hit["zone_id"]), []).append(hit)
        candidates: list[dict[str, Any]] = []
        for zone_id in sorted(by_zone):
            hits = by_zone[zone_id]
            strongest = max(hits, key=lambda item: (float(item["strength"]), item.get("at_ms", -1)))
            anchor = _hit_anchor(hits)
            interval = _select_interval(effective.get(zone_id, []), anchor)
            interval_id = _interval_id(zone_id, interval) if interval else None
            evidence = _delay_evidence(zone_id, interval_id, delay_result)
            mean_penalty = (
                sum(item["normalized_penalty"] for item in evidence) / len(evidence)
                if evidence
                else 0.0
            )
            priority = max(1, int(priorities.get(zone_id, 1)))
            strength = float(strongest["strength"])
            lexicon_score = math.log(max(strength, floor))
            priority_adjustment = priority_weight * math.log(priority / 100.0)
            delay_score = -mean_penalty
            score = lexicon_score + priority_adjustment + delay_score
            if not math.isfinite(score):
                raise CabinVoiceError(
                    "E_NONFINITE_SCORE",
                    "attribution score is not finite",
                    {
                        "chunk_id": chunk["chunk_id"],
                        "zone_id": zone_id,
                        "term": term,
                        "score": score,
                    },
                )
            candidates.append(
                {
                    "zone_id": zone_id,
                    "strength": strength,
                    "hit_count": len(hits),
                    "anchor_ms": anchor,
                    "active_interval": interval,
                    "score": round(score, 9),
                    "evidence": {
                        "lexicon_log_score": round(lexicon_score, 9),
                        "priority": priority,
                        "priority_log_adjustment": round(priority_adjustment, 9),
                        "delay_score": round(delay_score, 9),
                        "delay_observations": evidence,
                    },
                }
            )
        candidates.sort(key=lambda item: (-item["score"], -item["evidence"]["priority"], item["zone_id"]))
        top_two = candidates[:2]
        gap = (
            round(top_two[0]["score"] - top_two[1]["score"], 9)
            if len(top_two) == 2
            else None
        )
        needs_review = len(top_two) >= 2 and gap is not None and gap < review_threshold
        event_id = f"{chunk['chunk_id']}:{term}"
        event = {
            "event_id": event_id,
            "chunk_id": chunk["chunk_id"],
            "term": term,
            "status": "needs_review" if needs_review else "assigned",
            "responsible_zone": None if needs_review else candidates[0]["zone_id"],
            "score_gap": gap,
            "candidate_count": len(candidates),
            "candidates": candidates,
            "review": (
                {
                    "reason": "top_two_score_gap_below_threshold",
                    "threshold": review_threshold,
                    "retained_zones": [top_two[0]["zone_id"], top_two[1]["zone_id"]],
                    "evidence": [
                        {
                            "zone_id": candidate["zone_id"],
                            "score": candidate["score"],
                            "evidence": candidate["evidence"],
                        }
                        for candidate in top_two
                    ],
                }
                if needs_review
                else None
            ),
        }
        events.append(event)
    return {
        "chunk_id": chunk["chunk_id"],
        "events": events,
        "assigned_count": sum(1 for event in events if event["status"] == "assigned"),
        "review_count": sum(1 for event in events if event["status"] == "needs_review"),
    }
