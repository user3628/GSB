from __future__ import annotations

import math
from typing import Any

from .canonical import digest_value
from .errors import PipelineError


STRATEGIES = ("per_frame_recompute", "segment_cache", "full_cache")


def frame_nbytes(width: int, height: int) -> int:
    return width * height * 3 * 4


def _candidate(
    name: str,
    *,
    estimated_peak_bytes: int,
    budget_bytes: int,
    estimated_work: int,
    max_work: int,
    simplicity: int,
    base_score: float,
) -> dict[str, Any]:
    memory_score = 100.0 * max(0.0, 1.0 - estimated_peak_bytes / max(budget_bytes, 1))
    compute_score = 100.0 * max(0.0, 1.0 - estimated_work / max(max_work, 1))
    score = 0.45 * memory_score + 0.40 * compute_score + 0.15 * simplicity + base_score
    return {
        "strategy": name,
        "estimated_peak_bytes": estimated_peak_bytes,
        "estimated_work_units": estimated_work,
        "scores": {
            "memory": round(memory_score, 4),
            "compute": round(compute_score, 4),
            "simplicity": round(simplicity, 4),
            "base_adjustment": round(base_score, 4),
            "total": round(score, 4),
        },
        "viable": True,
        "eliminated_reason": None,
    }


def select_cache_policy(
    *,
    shot_count: int,
    total_frames: int,
    width: int,
    height: int,
    max_shot_frames: int,
    config: dict[str, Any],
) -> dict[str, Any]:
    cache_config = config["cache"]
    budget = int(cache_config["memory_budget_bytes"])
    frame_bytes = frame_nbytes(width, height)
    workspace = int(math.ceil(cache_config["workspace_frame_factor"] * frame_bytes))
    image_bytes = width * height * 3
    minimum_working = workspace + 2 * image_bytes
    small_threshold = int(cache_config["small_frame_threshold"])
    full_min_frames = int(cache_config["full_cache_min_frames"])
    fill = float(cache_config["segment_fill_ratio"])

    recompute_peak = minimum_working + 2 * frame_bytes
    segment_target = 2 * max_shot_frames * frame_bytes
    segment_peak = minimum_working + segment_target
    full_peak = minimum_working + total_frames * frame_bytes

    maximum_work = max(total_frames, shot_count, 1)
    candidates: list[dict[str, Any]] = []
    candidates.append(
        _candidate(
            "per_frame_recompute",
            estimated_peak_bytes=recompute_peak,
            budget_bytes=budget,
            estimated_work=total_frames,
            max_work=maximum_work,
            simplicity=100,
            base_score=5.0 if total_frames < small_threshold else 0.0,
        )
    )
    segment_work = shot_count + int(math.ceil(total_frames * 0.10))
    segment_score_penalty = -30.0 if total_frames < small_threshold else 0.0
    candidates.append(
        _candidate(
            "segment_cache",
            estimated_peak_bytes=segment_peak,
            budget_bytes=budget,
            estimated_work=segment_work,
            max_work=maximum_work,
            simplicity=70,
            base_score=segment_score_penalty,
        )
    )
    full_work = max(1, int(math.ceil(shot_count * 0.15)))
    full_score_penalty = -45.0 if total_frames < small_threshold else -10.0
    candidates.append(
        _candidate(
            "full_cache",
            estimated_peak_bytes=full_peak,
            budget_bytes=budget,
            estimated_work=full_work,
            max_work=maximum_work,
            simplicity=35,
            base_score=full_score_penalty,
        )
    )

    segment_cache_frames = min(
        2 * max_shot_frames,
        int(math.floor((budget * fill - minimum_working) / frame_bytes)),
    )
    for candidate in candidates:
        if candidate["estimated_peak_bytes"] > budget:
            candidate["viable"] = False
            candidate["eliminated_reason"] = (
                f"estimated peak {candidate['estimated_peak_bytes']} exceeds budget {budget}"
            )
        elif candidate["strategy"] == "full_cache" and total_frames < full_min_frames:
            candidate["viable"] = False
            candidate["eliminated_reason"] = (
                f"full cache is unnecessary for {total_frames} frames; minimum is {full_min_frames}"
            )
        elif candidate["strategy"] == "segment_cache" and segment_cache_frames < 2 * max_shot_frames:
            candidate["viable"] = False
            candidate["eliminated_reason"] = (
                "memory budget cannot hold two adjacent shot segments required by crossfade"
            )

    viable = [candidate for candidate in candidates if candidate["viable"]]
    if not viable:
        raise PipelineError(
            "E_INVALID_CONFIG",
            "memory budget is too small for any safe frame strategy",
            details={"memory_budget_bytes": budget, "frame_bytes": frame_bytes},
        )
    selected = max(viable, key=lambda item: (item["scores"]["total"], -STRATEGIES.index(item["strategy"])))
    for candidate in candidates:
        if candidate is selected:
            candidate["selected"] = True
        else:
            candidate["selected"] = False
            if candidate["eliminated_reason"] is None:
                candidate["eliminated_reason"] = "lower total score than selected strategy"
    reasons = {
        "per_frame_recompute": "minimal resident frame set; avoids speculative caching for small workloads",
        "segment_cache": "batches adjacent shot frames while keeping the measured cache below the memory budget",
        "full_cache": "all frames fit safely and repeated cross-stage accesses justify retaining them",
    }
    plan = {
        "stage": "algorithm_plan",
        "strategy": selected["strategy"],
        "reason": reasons[selected["strategy"]],
        "inputs": {
            "shot_count": shot_count,
            "total_frames": total_frames,
            "width": width,
            "height": height,
            "frame_bytes": frame_bytes,
            "max_shot_frames": max_shot_frames,
            "memory_budget_bytes": budget,
        },
        "limits": {
            "segment_cache_frames": max(0, segment_cache_frames),
            "segment_cache_bytes": max(0, segment_cache_frames * frame_bytes),
            "estimated_working_bytes": minimum_working,
        },
        "candidates": candidates,
    }
    plan["selection_digest"] = digest_value(plan)
    return plan
