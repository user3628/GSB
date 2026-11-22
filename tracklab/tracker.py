"""Stage 3: deterministic trajectory evolution, occlusion detection, and recovery."""

from __future__ import annotations

import copy
import math
from typing import Any, Sequence

import numpy as np

from .appearance import model_from_region
from .canonical import digest, stable_float
from .config import Settings
from .geometry import box_center, center_error_normalized, iou
from .scoring import blend_template_model, build_candidate_grid, score_candidates


def _box_with_prediction(
    box: Sequence[float],
    velocity: Sequence[float],
    size_velocity: Sequence[float],
) -> list[float]:
    x, y, w, h = (float(v) for v in box)
    vx, vy = (float(v) for v in velocity)
    dw, dh = (float(v) for v in size_velocity)
    return [
        stable_float(x + vx),
        stable_float(y + vy),
        stable_float(max(0.01, w + dw)),
        stable_float(max(0.01, h + dh)),
    ]


def _velocity_between(
    previous_box: Sequence[float],
    current_box: Sequence[float],
    gap: int = 1,
    *,
    previous_velocity: Sequence[float] | None = None,
    previous_size_velocity: Sequence[float] | None = None,
    smoothing: float = 0.0,
    max_velocity_ratio: float = 1.0,
    max_size_velocity_ratio: float = 1.0,
) -> tuple[list[float], list[float]]:
    gap = max(1, int(gap))
    px, py = box_center(previous_box)
    cx, cy = box_center(current_box)
    measured_velocity = [(cx - px) / gap, (cy - py) / gap]
    measured_size_velocity = [
        (float(current_box[2]) - float(previous_box[2])) / gap,
        (float(current_box[3]) - float(previous_box[3])) / gap,
    ]
    old_velocity = list(previous_velocity or [0.0, 0.0])
    old_size_velocity = list(previous_size_velocity or [0.0, 0.0])
    smoothing = min(1.0, max(0.0, float(smoothing)))
    velocity = [smoothing * old_velocity[i] + (1.0 - smoothing) * measured_velocity[i] for i in range(2)]
    size_velocity = [
        smoothing * old_size_velocity[i] + (1.0 - smoothing) * measured_size_velocity[i] for i in range(2)
    ]
    max_vx = max_velocity_ratio * float(current_box[2])
    max_vy = max_velocity_ratio * float(current_box[3])
    max_vw = max_size_velocity_ratio * float(current_box[2])
    max_vh = max_size_velocity_ratio * float(current_box[3])
    velocity = [max(-max_vx, min(max_vx, velocity[0])), max(-max_vy, min(max_vy, velocity[1]))]
    size_velocity = [max(-max_vw, min(max_vw, size_velocity[0])), max(-max_vh, min(max_vh, size_velocity[1]))]
    return (
        [stable_float(v) for v in velocity],
        [stable_float(v) for v in size_velocity],
    )


def initialize_state_and_template(
    frame: np.ndarray,
    sequence: dict[str, Any],
    settings: Settings,
) -> dict[str, Any]:
    sequence_id = str(sequence["sequence_id"])
    initial_box = [stable_float(float(v)) for v in sequence["initial_box"]]
    region = model_from_region(
        frame,
        initial_box,
        settings=settings,
        sequence_id=sequence_id,
        frame_index=0,
        label="initial_anchor_box",
        strict=True,
    )
    assert region is not None
    template = {
        "source_frame": 0,
        "source_box_xywh_norm": initial_box,
        "model": region["model"],
    }
    return {
        "schema_version": 1,
        "sequence_id": sequence_id,
        "frame_index": -1,
        "box_xywh_norm": initial_box,
        "velocity_per_frame": [0.0, 0.0],
        "size_velocity_per_frame": [0.0, 0.0],
        "template": template,
        "occluded": False,
        "low_streak": 0,
        "occlusion_anchor": None,
        "occlusion_start_frame": None,
        "last_confident_box_xywh_norm": initial_box,
        "last_confident_frame": 0,
        "last_response_score": None,
        "last_candidate_record_digest": None,
    }


def _copy_state_anchor(state: dict[str, Any]) -> dict[str, Any]:
    return {
        "frame": int(state["last_confident_frame"]),
        "box_xywh_norm": [stable_float(float(v)) for v in state["last_confident_box_xywh_norm"]],
        "velocity_per_frame": [stable_float(float(v)) for v in state["velocity_per_frame"]],
        "size_velocity_per_frame": [stable_float(float(v)) for v in state["size_velocity_per_frame"]],
        "template_model_digest": state["template"]["model"]["model_digest"],
    }


def evolve_frame(
    frame: np.ndarray,
    sequence: dict[str, Any],
    frame_index: int,
    prior_state: dict[str, Any],
    settings: Settings,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    """Run one indivisible frame unit. Reference truth is used only for validation."""
    sequence_id = str(sequence["sequence_id"])
    reference_box = [stable_float(float(v)) for v in sequence["frames"][frame_index]["box_xywh_norm"]]
    tcfg = settings.tracking
    update_threshold = float(sequence["appearance_update_threshold"])
    confidence_floor = float(sequence["trace_confidence_floor"])
    recovery_margin = float(tcfg["recovery_margin"])
    patience = max(1, int(tcfg["occlusion_patience"]))
    alpha = float(tcfg["template_update_alpha"])
    expansion = float(sequence["search_window_expansion"])
    recovery_multiplier = float(tcfg["recovery_search_expansion_multiplier"])

    state = copy.deepcopy(prior_state)
    state["frame_index"] = frame_index
    sequence_view = dict(sequence)
    sequence_view["_current_frame_index"] = frame_index
    was_occluded = bool(state["occluded"])
    events: list[dict[str, Any]] = []

    if was_occluded and state["occlusion_anchor"] is not None:
        anchor = state["occlusion_anchor"]
        predicted_box = [stable_float(float(v)) for v in anchor["box_xywh_norm"]]
        sampling_expansion = expansion * recovery_multiplier
    else:
        predicted_box = _box_with_prediction(
            state["box_xywh_norm"],
            state["velocity_per_frame"],
            state["size_velocity_per_frame"],
        )
        sampling_expansion = expansion

    candidates, grid_info = build_candidate_grid(
        predicted_box,
        sequence_view,
        settings,
        expansion_override=sampling_expansion,
    )
    template = state["template"]["model"]
    candidate_record = score_candidates(
        frame,
        template,
        candidates,
        predicted_box,
        grid_info,
        sequence_view,
        settings,
        template_source_frame=int(state["template"]["source_frame"]),
    )
    best = candidate_record["best_candidate"]
    best_box = [stable_float(float(v)) for v in best["box_xywh_norm"]]
    best_response = float(best["response_score"])
    template_updated = False
    template_before_digest = template["model_digest"]
    template_after_digest = template_before_digest
    accepted_box = [stable_float(float(v)) for v in state["box_xywh_norm"]]
    state_action = "hold"
    recovery_record: dict[str, Any] | None = None
    occlusion_started: dict[str, Any] | None = None
    occlusion_closed: dict[str, Any] | None = None

    if not was_occluded:
        if best_response < confidence_floor:
            state["low_streak"] = int(state["low_streak"]) + 1
            state_action = "hold_low_confidence"
            accepted_box = predicted_box
            if state["low_streak"] >= patience:
                state["occluded"] = True
                state["occlusion_anchor"] = _copy_state_anchor(state)
                state["occlusion_start_frame"] = frame_index - patience + 1
                state_action = "occlusion_entered"
                occlusion_started = {
                    "type": "occlusion_started",
                    "frame_index": frame_index,
                    "start_frame": state["occlusion_start_frame"],
                    "anchor_frame": state["occlusion_anchor"]["frame"],
                    "anchor_box_xywh_norm": state["occlusion_anchor"]["box_xywh_norm"],
                    "reason": f"{patience} consecutive responses below confidence floor",
                    "confidence_floor": stable_float(confidence_floor),
                    "responses": [stable_float(best_response)],
                }
                events.append(occlusion_started)
        else:
            state["low_streak"] = 0
            state_action = "accepted"
            accepted_box = best_box
            previous_box = state["box_xywh_norm"]
            previous_frame = int(state["frame_index"]) - 1 if frame_index > 0 else -1
            gap = max(1, frame_index - int(state["last_confident_frame"]))
            velocity, size_velocity = _velocity_between(
                state["last_confident_box_xywh_norm"],
                best_box,
                gap,
                previous_velocity=state["velocity_per_frame"],
                previous_size_velocity=state["size_velocity_per_frame"],
                smoothing=float(tcfg["velocity_smoothing"]),
                max_velocity_ratio=float(tcfg["max_velocity_ratio"]),
                max_size_velocity_ratio=float(tcfg["max_size_velocity_ratio"]),
            )
            state["velocity_per_frame"] = velocity
            state["size_velocity_per_frame"] = size_velocity
            state["last_confident_box_xywh_norm"] = best_box
            state["last_confident_frame"] = frame_index
            if best_response >= update_threshold:
                new_region = model_from_region(
                    frame,
                    best_box,
                    settings=settings,
                    sequence_id=sequence_id,
                    frame_index=frame_index,
                    label="accepted_target_box",
                    strict=True,
                )
                assert new_region is not None
                blended = blend_template_model(template, new_region["model"], alpha)
                state["template"] = {
                    "source_frame": frame_index,
                    "source_box_xywh_norm": best_box,
                    "model": blended,
                }
                template_updated = True
                template_after_digest = blended["model_digest"]
                events.append(
                    {
                        "type": "template_updated",
                        "frame_index": frame_index,
                        "alpha": stable_float(alpha),
                        "update_threshold": stable_float(update_threshold),
                        "response_score": stable_float(best_response),
                        "template_before_digest": template_before_digest,
                        "template_after_digest": template_after_digest,
                        "source_box_xywh_norm": best_box,
                    }
                )
    else:
        anchor = state["occlusion_anchor"]
        assert anchor is not None
        if best_response >= confidence_floor + recovery_margin:
            accepted_box = best_box
            state_action = "occlusion_recovered"
            start = int(state["occlusion_start_frame"])
            end = frame_index - 1
            gap = max(1, frame_index - int(anchor["frame"]))
            velocity, size_velocity = _velocity_between(
                anchor["box_xywh_norm"],
                best_box,
                gap,
                previous_velocity=anchor.get("velocity_per_frame"),
                previous_size_velocity=anchor.get("size_velocity_per_frame"),
                smoothing=float(tcfg["velocity_smoothing"]),
                max_velocity_ratio=float(tcfg["max_velocity_ratio"]),
                max_size_velocity_ratio=float(tcfg["max_size_velocity_ratio"]),
            )
            state["velocity_per_frame"] = velocity
            state["size_velocity_per_frame"] = size_velocity
            state["occluded"] = False
            state["low_streak"] = 0
            state["last_confident_box_xywh_norm"] = best_box
            state["last_confident_frame"] = frame_index
            recovery_record = {
                "type": "occlusion_recovered",
                "frame_index": frame_index,
                "start_frame": start,
                "end_frame": end,
                "anchor_frame": int(anchor["frame"]),
                "anchor_box_xywh_norm": anchor["box_xywh_norm"],
                "history_frames": list(range(int(anchor["frame"]), frame_index)),
                "search_window_box_xywh_norm": grid_info["search_window_box_xywh_norm"],
                "search_window_expansion": grid_info["search_window_expansion"],
                "candidate_count": candidate_record["candidate_count"],
                "response_score": stable_float(best_response),
                "recovery_threshold": stable_float(confidence_floor + recovery_margin),
                "basis": "best candidate response above recovery threshold inside interruption-consistent search window",
            }
            occlusion_closed = {
                "type": "occlusion_closed",
                "frame_index": frame_index,
                "start_frame": start,
                "end_frame": end,
                "recovery_frame": frame_index,
            }
            events.extend([occlusion_closed, recovery_record])
            state["occlusion_anchor"] = None
            state["occlusion_start_frame"] = None
            if best_response >= update_threshold:
                new_region = model_from_region(
                    frame,
                    best_box,
                    settings=settings,
                    sequence_id=sequence_id,
                    frame_index=frame_index,
                    label="recovered_target_box",
                    strict=True,
                )
                assert new_region is not None
                blended = blend_template_model(template, new_region["model"], alpha)
                state["template"] = {
                    "source_frame": frame_index,
                    "source_box_xywh_norm": best_box,
                    "model": blended,
                }
                template_updated = True
                template_after_digest = blended["model_digest"]
                events.append(
                    {
                        "type": "template_updated",
                        "frame_index": frame_index,
                        "alpha": stable_float(alpha),
                        "update_threshold": stable_float(update_threshold),
                        "response_score": stable_float(best_response),
                        "template_before_digest": template_before_digest,
                        "template_after_digest": template_after_digest,
                        "source_box_xywh_norm": best_box,
                        "after_recovery": True,
                    }
                )
        else:
            state_action = "hold_occluded"
            accepted_box = [stable_float(float(v)) for v in anchor["box_xywh_norm"]]
            state["velocity_per_frame"] = [0.0, 0.0]
            state["size_velocity_per_frame"] = [0.0, 0.0]

    state["box_xywh_norm"] = accepted_box
    state["last_response_score"] = stable_float(best_response)
    state["last_candidate_record_digest"] = candidate_record["candidate_record_digest"]
    frame_rate = float(sequence["frame_rate"])
    tracking_record: dict[str, Any] = {
        "schema_version": 1,
        "stage": "trajectory",
        "sequence_id": sequence_id,
        "frame_index": frame_index,
        "logical_time_ms": int(round(frame_index * 1000.0 / frame_rate)),
        "box_xywh_norm": accepted_box,
        "reference_truth_box_xywh_norm": reference_box,
        "iou": stable_float(iou(accepted_box, reference_box)),
        "center_error_normalized": stable_float(center_error_normalized(accepted_box, reference_box)),
        "response_score": stable_float(best_response),
        "state_action": state_action,
        "occluded": bool(state["occluded"]),
        "was_occluded": was_occluded,
        "template_updated": template_updated,
        "template_before_digest": template_before_digest,
        "template_after_digest": template_after_digest,
        "recovery": recovery_record,
        "events": events,
    }
    tracking_record["tracking_record_digest"] = digest(tracking_record)
    # The candidate stage never receives reference truth. Truth is consumed only in this
    # validation block after the trajectory state has been fixed.
    return state, candidate_record, tracking_record
