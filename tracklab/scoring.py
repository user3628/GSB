"""Stage 2: fixed-step candidate sampling and log-domain response scoring."""

from __future__ import annotations

import math
from typing import Any, Sequence

import numpy as np

from .appearance import model_from_region
from .canonical import digest, stable_float
from .config import Settings
from .errors import E_INVALID_REGION, E_NUMERIC, WorkbenchError, raise_error
from .geometry import box_center


def _finite(value: float) -> float:
    value = float(value)
    if not math.isfinite(value):
        raise_error(
            E_NUMERIC,
            "non-finite value produced while scoring candidates",
            context={"value": repr(value), "diagnostic": "non_finite_response_intermediate"},
        )
    return value


def bhattacharyya(left: Sequence[float], right: Sequence[float]) -> float:
    a = np.asarray(left, dtype=np.float64)
    b = np.asarray(right, dtype=np.float64)
    if a.shape != b.shape:
        return 0.0
    if not np.all(np.isfinite(a)) or not np.all(np.isfinite(b)):
        raise_error(
            E_NUMERIC,
            "appearance histogram contains NaN or Inf",
            context={"diagnostic": "non_finite_histogram"},
        )
    value = float(np.sqrt(np.maximum(0.0, a) * np.maximum(0.0, b)).sum())
    return _finite(value)


def build_candidate_grid(
    predicted_box: Sequence[float],
    sequence: dict[str, Any],
    settings: Settings,
    *,
    expansion_override: float | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    sequence_id = str(sequence["sequence_id"])
    frame_index = int(sequence.get("_current_frame_index", 0))
    height, width = (int(v) for v in sequence["image_shape"])
    expansion = float(
        sequence["search_window_expansion"] if expansion_override is None else expansion_override
    )
    step_ratio = float(settings.tracking["grid_step_ratio"])
    size_deltas = [int(v) for v in settings.tracking["size_deltas_pixels"]]
    x, y, w, h = (float(v) for v in predicted_box)
    if not all(math.isfinite(v) for v in (x, y, w, h)) or w <= 0.0 or h <= 0.0:
        raise_error(
            E_NUMERIC,
            "predicted box is invalid for candidate sampling",
            context={
                "sequence_id": sequence_id,
                "frame_index": frame_index,
                "predicted_box": predicted_box,
                "diagnostic": "invalid_predicted_box",
            },
        )
    cx = x + w / 2.0
    cy = y + h / 2.0
    half_window_x = w * (1.0 + expansion) / 2.0
    half_window_y = h * (1.0 + expansion) / 2.0
    search_window = [cx - half_window_x, cy - half_window_y, 2.0 * half_window_x, 2.0 * half_window_y]
    base_width_px = max(1, int(round(w * width)))
    base_height_px = max(1, int(round(h * height)))
    center_px_x = int(round(cx * width))
    center_px_y = int(round(cy * height))
    radius_px_x = max(1, int(math.floor(base_width_px * expansion / 2.0 + 1.0e-9)))
    radius_px_y = max(1, int(math.floor(base_height_px * expansion / 2.0 + 1.0e-9)))
    step_px_x = max(1, int(round(base_width_px * step_ratio)))
    step_px_y = max(1, int(round(base_height_px * step_ratio)))
    offsets_x = list(range(-radius_px_x, radius_px_x + 1, step_px_x))
    offsets_y = list(range(-radius_px_y, radius_px_y + 1, step_px_y))
    grid_info: dict[str, Any] = {
        "sequence_id": sequence_id,
        "frame_index": frame_index,
        "predicted_box": [stable_float(v) for v in predicted_box],
        "search_window_box_xywh_norm": [stable_float(v) for v in search_window],
        "search_window_expansion": stable_float(expansion),
        "grid_step_ratio": stable_float(step_ratio),
        "grid_step_pixels": [step_px_x, step_px_y],
        "grid_step_norm": [stable_float(step_px_x / width), stable_float(step_px_y / height)],
        "grid_radius_pixels": [radius_px_x, radius_px_y],
        "grid_offset_count": [len(offsets_x), len(offsets_y)],
        "grid_shape": [len(offsets_x), len(offsets_y), len(size_deltas) * len(size_deltas)],
        "size_deltas_pixels": size_deltas,
        "candidate_count_before_clip": len(offsets_x) * len(offsets_y) * len(size_deltas) * len(size_deltas),
        "image_shape": [height, width],
    }
    if (
        search_window[0] + search_window[2] <= 0.0
        or search_window[0] >= 1.0
        or search_window[1] + search_window[3] <= 0.0
        or search_window[1] >= 1.0
    ):
        raise_error(
            E_NUMERIC,
            "search window is completely outside the frame",
            context={**grid_info, "diagnostic": "search_window_outside_frame"},
        )
    candidates: list[dict[str, Any]] = []
    skipped = 0
    seen_pixel_boxes: set[tuple[int, int, int, int]] = set()
    shape_index = 0
    for delta_w in size_deltas:
        cand_width_px = base_width_px + delta_w
        if cand_width_px < 2:
            continue
        for delta_h in size_deltas:
            cand_height_px = base_height_px + delta_h
            if cand_height_px < 2:
                continue
            current_shape_index = shape_index
            shape_index += 1
            for iy in offsets_y:
                for ix in offsets_x:
                    cand_center_x = center_px_x + ix
                    cand_center_y = center_px_y + iy
                    x0 = cand_center_x - cand_width_px // 2
                    y0 = cand_center_y - cand_height_px // 2
                    x1 = x0 + cand_width_px
                    y1 = y0 + cand_height_px
                    if x0 < 0 or y0 < 0 or x1 > width or y1 > height:
                        skipped += 1
                        continue
                    pixel_box = (x0, y0, x1, y1)
                    if pixel_box in seen_pixel_boxes:
                        continue
                    seen_pixel_boxes.add(pixel_box)
                    box = [x0 / width, y0 / height, cand_width_px / width, cand_height_px / height]
                    candidate_id = f"c{len(candidates):05d}_d{delta_w}_{delta_h}"
                    candidates.append(
                        {
                            "candidate_id": candidate_id,
                            "box_xywh_norm": [stable_float(v) for v in box],
                            "box_pixels": [x0, y0, x1, y1],
                            "grid_ix": ix // step_px_x,
                            "grid_iy": iy // step_px_y,
                            "grid_offset_pixels": [ix, iy],
                            "size_delta_pixels": [delta_w, delta_h],
                            "shape_index": current_shape_index,
                            "scale_factor": stable_float(((cand_width_px / base_width_px) + (cand_height_px / base_height_px)) / 2.0),
                        }
                    )
    grid_info["candidate_count"] = len(candidates)
    grid_info["skipped_outside_frame"] = skipped
    if not candidates:
        raise_error(
            E_NUMERIC,
            "candidate grid produced zero in-frame samples",
            context={**grid_info, "diagnostic": "zero_candidates"},
        )
    return candidates, grid_info

def score_candidates(
    frame: np.ndarray,
    template_model: dict[str, Any],
    candidates: list[dict[str, Any]],
    predicted_box: Sequence[float],
    grid_info: dict[str, Any],
    sequence: dict[str, Any],
    settings: Settings,
    *,
    template_source_frame: int | None = None,
) -> dict[str, Any]:
    sequence_id = str(sequence["sequence_id"])
    frame_index = int(sequence.get("_current_frame_index", 0))
    tcfg = settings.tracking
    gray_weight = float(tcfg["gray_similarity_weight"])
    grad_weight = float(tcfg["gradient_similarity_weight"])
    spatial_weight = float(tcfg["spatial_similarity_weight"])
    prior_weight = float(tcfg["center_prior_weight"])
    scale_prior_weight = float(tcfg["scale_prior_weight"])
    sigma = float(tcfg["center_prior_sigma_ratio"])
    scale_sigma = max(float(tcfg["scale_prior_sigma"]), 1.0e-9)
    epsilon = float(tcfg["numeric_epsilon"])
    x, y, w, h = (float(v) for v in predicted_box)
    pred_cx = x + w / 2.0
    pred_cy = y + h / 2.0
    half_window_x = max(float(grid_info["search_window_box_xywh_norm"][2]) / 2.0, epsilon)
    half_window_y = max(float(grid_info["search_window_box_xywh_norm"][3]) / 2.0, epsilon)
    scored: list[dict[str, Any]] = []
    valid_count = 0
    for candidate in candidates:
        box = candidate["box_xywh_norm"]
        try:
            candidate_model = model_from_region(
                frame,
                box,
                settings=settings,
                sequence_id=sequence_id,
                frame_index=frame_index,
                label=f"candidate_{candidate['candidate_id']}",
                strict=False,
            )
        except WorkbenchError as exc:
            if exc.code != E_INVALID_REGION:
                raise
            candidate_model = None
        if candidate_model is None:
            gray_similarity = 0.0
            gradient_similarity = 0.0
            spatial_similarity = 0.0
            appearance_similarity = 0.0
            model_digest = None
        else:
            model = candidate_model["model"]
            try:
                gray_similarity = bhattacharyya(template_model["gray_histogram"], model["gray_histogram"])
                gradient_similarity = bhattacharyya(template_model["gradient_histogram"], model["gradient_histogram"])
                spatial_similarity = signature_similarity(
                    template_model.get("spatial_signature", []), model.get("spatial_signature", [])
                )
            except WorkbenchError as exc:
                if exc.code != E_NUMERIC:
                    raise
                raise_error(
                    E_NUMERIC,
                    exc.message,
                    context={
                        **grid_info,
                        "candidate_id": candidate["candidate_id"],
                        "diagnostic": exc.context.get("diagnostic", "non_finite_appearance_component"),
                    },
                )
            appearance_similarity = (
                gray_weight * gray_similarity
                + grad_weight * gradient_similarity
                + spatial_weight * spatial_similarity
            )
            model_digest = model["model_digest"]
            valid_count += 1
        cand_cx, cand_cy = box_center(box)
        dx = (cand_cx - pred_cx) / half_window_x
        dy = (cand_cy - pred_cy) / half_window_y
        spatial_prior = math.exp(-0.5 * ((dx / sigma) ** 2 + (dy / sigma) ** 2))
        scale_ratio = max(float(candidate["scale_factor"]), epsilon)
        scale_prior = math.exp(-0.5 * ((math.log(scale_ratio)) / scale_sigma) ** 2)
        center_prior = spatial_prior * scale_prior
        if not all(math.isfinite(v) for v in (gray_similarity, gradient_similarity, spatial_similarity, appearance_similarity, center_prior)):
            raise_error(
                E_NUMERIC,
                "response component is non-finite",
                context={
                    **grid_info,
                    "candidate_id": candidate["candidate_id"],
                    "diagnostic": "non_finite_response_component",
                },
            )
        log_appearance = math.log(max(appearance_similarity, epsilon))
        log_spatial_prior = math.log(max(spatial_prior, epsilon))
        log_scale_prior = math.log(max(scale_prior, epsilon))
        log_score = (
            log_appearance
            + prior_weight * log_spatial_prior
            + scale_prior_weight * log_scale_prior
        )
        response_score = math.exp(log_score)
        if not all(math.isfinite(v) for v in (log_score, response_score)):
            raise_error(
                E_NUMERIC,
                "response score is NaN or Inf",
                context={
                    **grid_info,
                    "candidate_id": candidate["candidate_id"],
                    "log_score": repr(log_score),
                    "response_score": repr(response_score),
                    "diagnostic": "non_finite_response_score",
                },
            )
        scored.append(
            {
                **candidate,
                "gray_similarity": stable_float(gray_similarity),
                "gradient_similarity": stable_float(gradient_similarity),
                "spatial_similarity": stable_float(spatial_similarity),
                "appearance_similarity": stable_float(appearance_similarity),
                "spatial_prior": stable_float(spatial_prior),
                "scale_prior": stable_float(scale_prior),
                "center_prior": stable_float(center_prior),
                "log_score": stable_float(log_score),
                "response_score": stable_float(response_score),
                "candidate_model_digest": model_digest,
            }
        )
    if not scored:
        raise_error(
            E_NUMERIC,
            "no candidate received a response score",
            context={**grid_info, "diagnostic": "zero_scored_candidates"},
        )
    if valid_count == 0:
        raise_error(
            E_NUMERIC,
            "all candidate regions were empty or all-zero",
            context={**grid_info, "diagnostic": "all_candidates_invalid_regions"},
        )
    scored.sort(key=lambda item: (-item["response_score"], item["candidate_id"]))
    top_k = max(1, int(tcfg["top_k"]))
    top = scored[:top_k]
    best = top[0]
    record = {
        "schema_version": 1,
        "stage": "candidates",
        "sequence_id": sequence_id,
        "frame_index": frame_index,
        "template_source_frame": template_source_frame,
        "template_model_digest": template_model.get("model_digest"),
        "predicted_box_xywh_norm": [stable_float(float(v)) for v in predicted_box],
        "sampling_grid": grid_info,
        "candidate_count": len(scored),
        "valid_candidate_count": valid_count,
        "top_candidates": top,
        "best_candidate": best,
        "response_score": best["response_score"],
        "log_score": best["log_score"],
        "score_components": {
            "gray_similarity": best["gray_similarity"],
            "gradient_similarity": best["gradient_similarity"],
            "spatial_similarity": best["spatial_similarity"],
            "appearance_similarity": best["appearance_similarity"],
            "center_prior": best["center_prior"],
            "log_score": best["log_score"],
        },
    }
    record["candidate_record_digest"] = digest(record)
    return record


def signature_similarity(left: Sequence[float], right: Sequence[float]) -> float:
    a = np.asarray(left, dtype=np.float64)
    b = np.asarray(right, dtype=np.float64)
    if a.shape != b.shape or a.size == 0:
        return 0.0
    if not np.all(np.isfinite(a)) or not np.all(np.isfinite(b)):
        raise_error(
            E_NUMERIC,
            "appearance spatial signature contains NaN or Inf",
            context={"diagnostic": "non_finite_spatial_signature"},
        )
    value = float(np.dot(a, b))
    return max(0.0, min(1.0, value))


def blend_template_model(
    old_model: dict[str, Any],
    new_model: dict[str, Any],
    alpha: float,
) -> dict[str, Any]:
    """Deterministic exponential template update for two normalized models."""
    alpha = float(alpha)
    result: dict[str, Any] = {}
    for key in ("gray_histogram", "gradient_histogram", "spatial_signature"):
        left = np.asarray(old_model[key], dtype=np.float64)
        right = np.asarray(new_model[key], dtype=np.float64)
        blended = (1.0 - alpha) * left + alpha * right
        if key != "spatial_signature":
            total = float(blended.sum())
            if total > 0.0:
                blended = blended / total
        else:
            norm = float(np.sqrt((blended * blended).sum()))
            if norm > 0.0:
                blended = blended / norm
        result[key] = [stable_float(float(v)) for v in blended]
    result.update(
        {
            "pixel_mean": stable_float(alpha * float(new_model["pixel_mean"]) + (1.0 - alpha) * float(old_model["pixel_mean"])),
            "pixel_std": stable_float(alpha * float(new_model["pixel_std"]) + (1.0 - alpha) * float(old_model["pixel_std"])),
            "pixel_sum": int(new_model["pixel_sum"]),
            "mean_gradient_magnitude": stable_float(
                alpha * float(new_model["mean_gradient_magnitude"])
                + (1.0 - alpha) * float(old_model["mean_gradient_magnitude"])
            ),
            "gray_bins": int(old_model["gray_bins"]),
            "gradient_bins": int(old_model["gradient_bins"]),
            "normalization": str(old_model["normalization"]),
            "spatial_grid_size": int(old_model["spatial_grid_size"]),
        }
    )
    result["model_digest"] = digest(result)
    return result
