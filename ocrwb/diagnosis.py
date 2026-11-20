"""Diagnostic aggregation and metric statistics.

All metrics derive strictly from manifest truth and explicit stage outputs;
nothing is inferred from missing information.  Every floating point value is
rounded to the configured decimals so the summary is byte-stable.
"""

from __future__ import annotations

import math

from .config import WorkbenchConfig
from .hashing import digest_json


def iou(box_a: dict, box_b: dict) -> float:
    ix0 = max(box_a["x0"], box_b["x0"])
    iy0 = max(box_a["y0"], box_b["y0"])
    ix1 = min(box_a["x1"], box_b["x1"])
    iy1 = min(box_a["y1"], box_b["y1"])
    inter_w = max(0.0, ix1 - ix0)
    inter_h = max(0.0, iy1 - iy0)
    inter = inter_w * inter_h
    area_a = max(0.0, box_a["x1"] - box_a["x0"]) * max(0.0, box_a["y1"] - box_a["y0"])
    area_b = max(0.0, box_b["x1"] - box_b["x0"]) * max(0.0, box_b["y1"] - box_b["y0"])
    union = area_a + area_b - inter
    return inter / union if union > 0 else 0.0


def wilson_interval(successes: int, trials: int, z: float = 1.96) -> tuple[float, float]:
    """Wilson score interval half-width for a binomial proportion."""
    if trials == 0:
        return 0.0, 0.0
    phat = successes / trials
    denom = 1 + z * z / trials
    center = (phat + z * z / (2 * trials)) / denom
    half = (z * math.sqrt((phat * (1 - phat) + z * z / (4 * trials)) / trials)) / denom
    return max(0.0, center - half), min(1.0, center + half)


def _r(value: float, decimals: int) -> float:
    return round(float(value), decimals)


def _match_candidates(shard: dict, detection: dict, threshold: float) -> list[dict]:
    """Greedy one-to-one IoU matching between truth boxes and candidates."""
    pairs = []
    for truth_index, truth in enumerate(shard["truth"]):
        for candidate_index, candidate in enumerate(detection["candidates"]):
            score = iou(truth["box"], candidate["box"])
            if score >= threshold:
                pairs.append((score, truth_index, candidate_index))
    pairs.sort(key=lambda item: (-item[0], item[1], item[2]))
    matched_truth: set[int] = set()
    matched_candidate: set[int] = set()
    matches = []
    for score, truth_index, candidate_index in pairs:
        if truth_index in matched_truth or candidate_index in matched_candidate:
            continue
        matched_truth.add(truth_index)
        matched_candidate.add(candidate_index)
        matches.append(
            {
                "truth_index": truth_index,
                "candidate_index": candidate_index,
                "candidate_id": detection["candidates"][candidate_index]["candidate_id"],
                "iou": score,
            }
        )
    return matches


def aggregate(
    manifest: dict,
    detections: dict[str, dict],
    decisions: dict[str, dict],
    recognitions: dict[str, dict],
    config: WorkbenchConfig,
) -> dict:
    """Build the full per-shard, per-group and global diagnostic report."""
    decimals = config.confidence_decimals
    per_shard: list[dict] = []
    group_accum = {group["name"]: _new_accumulator() for group in manifest["groups"]}
    global_accum = _new_accumulator()
    strategy_hits: dict[str, int] = {}

    for shard in manifest["shards"]:
        image_id = shard["image_id"]
        detection = detections[image_id]
        decision = decisions[image_id]
        recognition = recognitions[image_id]
        matches = _match_candidates(shard, detection, config.iou_match_threshold)
        recognition_by_id = {item["candidate_id"]: item for item in recognition["results"]}

        truth_total = len(shard["truth"])
        detected = len(matches)
        exact_hits = 0
        char_successes = 0
        char_trials = 0
        distance_values = []
        for match in matches:
            truth = shard["truth"][match["truth_index"]]
            recognized = recognition_by_id.get(match["candidate_id"])
            if recognized is None:
                continue
            distance_values.append(recognized["mean_distance"])
            if recognized["text"] == truth["text"]:
                exact_hits += 1
            for actual, expected in zip(recognized["text"], truth["text"]):
                char_trials += 1
                if actual == expected:
                    char_successes += 1

        recall_low, recall_high = wilson_interval(detected, truth_total)
        exact_low, exact_high = wilson_interval(exact_hits, detected)
        char_low, char_high = wilson_interval(char_successes, char_trials)
        record = {
            "image_id": image_id,
            "group": shard["group"],
            "truth_count": truth_total,
            "candidate_count": detection["candidate_count"],
            "detected_count": detected,
            "detection_recall": _r(detected / truth_total, decimals) if truth_total else 0.0,
            "recall_error_band": [_r(recall_low, decimals), _r(recall_high, decimals)],
            "exact_match_accuracy": _r(exact_hits / detected, decimals) if detected else 0.0,
            "exact_error_band": [_r(exact_low, decimals), _r(exact_high, decimals)],
            "character_accuracy": _r(char_successes / char_trials, decimals) if char_trials else 0.0,
            "character_error_band": [_r(char_low, decimals), _r(char_high, decimals)],
            "mean_match_distance": _r(sum(distance_values) / len(distance_values), decimals)
            if distance_values
            else 0.0,
            "detect_strategy": decision["detect_strategy"],
            "recognize_strategy": decision["recognize_strategy"],
            "detect_cost": decision["detect_cost"],
            "recognize_cost": decision["recognize_cost"],
            "detect_budget": decision["detect_budget"],
            "recognize_budget": decision["recognize_budget"],
            "detect_errors": detection["errors"],
            "recognize_errors": recognition["errors"],
            "matches": matches,
        }
        per_shard.append(record)
        _accumulate(group_accum[shard["group"]], record, truth_total, detected, exact_hits, char_successes, char_trials, distance_values)
        _accumulate(global_accum, record, truth_total, detected, exact_hits, char_successes, char_trials, distance_values)
        for key in (decision["detect_strategy"], decision["recognize_strategy"]):
            strategy_hits[key] = strategy_hits.get(key, 0) + 1

    groups = []
    for group in manifest["groups"]:
        groups.append(_finalize_accumulator(group_accum[group["name"]], group["name"], decimals))
    report = {
        "manifest_seed": manifest["generator_seed"],
        "config_version": config.version(),
        "iou_threshold": config.iou_match_threshold,
        "per_shard": per_shard,
        "groups": groups,
        "global": _finalize_accumulator(global_accum, "global", decimals),
        "strategy_hits": dict(sorted(strategy_hits.items())),
        "error_counts": _count_errors(per_shard),
    }
    report["summary_digest"] = digest_json(report)[:16]
    return report


def _new_accumulator() -> dict:
    return {
        "shards": 0,
        "truth": 0,
        "detected": 0,
        "exact": 0,
        "char_successes": 0,
        "char_trials": 0,
        "distances": [],
        "detect_cost": 0,
        "recognize_cost": 0,
        "detect_budget": 0,
        "recognize_budget": 0,
        "candidates": 0,
    }


def _accumulate(accumulator, record, truth_total, detected, exact_hits, char_successes, char_trials, distance_values):
    accumulator["shards"] += 1
    accumulator["truth"] += truth_total
    accumulator["detected"] += detected
    accumulator["exact"] += exact_hits
    accumulator["char_successes"] += char_successes
    accumulator["char_trials"] += char_trials
    accumulator["distances"].extend(distance_values)
    accumulator["detect_cost"] += record["detect_cost"]
    accumulator["recognize_cost"] += record["recognize_cost"]
    accumulator["detect_budget"] += record["detect_budget"]
    accumulator["recognize_budget"] += record["recognize_budget"]
    accumulator["candidates"] += record["candidate_count"]


def _finalize_accumulator(accumulator, name, decimals) -> dict:
    recall_low, recall_high = wilson_interval(accumulator["detected"], accumulator["truth"])
    exact_low, exact_high = wilson_interval(accumulator["exact"], accumulator["detected"])
    char_low, char_high = wilson_interval(accumulator["char_successes"], accumulator["char_trials"])
    distances = accumulator["distances"]
    return {
        "name": name,
        "shard_count": accumulator["shards"],
        "truth_count": accumulator["truth"],
        "candidate_count": accumulator["candidates"],
        "detected_count": accumulator["detected"],
        "detection_recall": _r(accumulator["detected"] / accumulator["truth"], decimals)
        if accumulator["truth"]
        else 0.0,
        "recall_error_band": [_r(recall_low, decimals), _r(recall_high, decimals)],
        "exact_match_accuracy": _r(accumulator["exact"] / accumulator["detected"], decimals)
        if accumulator["detected"]
        else 0.0,
        "exact_error_band": [_r(exact_low, decimals), _r(exact_high, decimals)],
        "character_accuracy": _r(
            accumulator["char_successes"] / accumulator["char_trials"], decimals
        )
        if accumulator["char_trials"]
        else 0.0,
        "character_error_band": [_r(char_low, decimals), _r(char_high, decimals)],
        "mean_match_distance": _r(sum(distances) / len(distances), decimals) if distances else 0.0,
        "detect_cost": accumulator["detect_cost"],
        "recognize_cost": accumulator["recognize_cost"],
        "detect_budget": accumulator["detect_budget"],
        "recognize_budget": accumulator["recognize_budget"],
        "budget_headroom": {
            "detect": accumulator["detect_budget"] - accumulator["detect_cost"],
            "recognize": accumulator["recognize_budget"] - accumulator["recognize_cost"],
        },
    }


def _count_errors(per_shard: list[dict]) -> dict:
    counts: dict[str, int] = {}
    for record in per_shard:
        for error in record["detect_errors"] + record["recognize_errors"]:
            counts[error["code"]] = counts.get(error["code"], 0) + 1
    return dict(sorted(counts.items()))
