"""Stage 1: deterministic grayscale and gradient-orientation appearance models."""

from __future__ import annotations

import math
from pathlib import Path
from typing import Any, Sequence

import numpy as np

from .canonical import atomic_write_json, digest, sha256_file, stable_float
from .config import Settings
from .errors import E_INVALID_REGION, raise_error
from .geometry import box_to_pixels, validate_box
from .manifest import ManifestView


def _normalize(vector: np.ndarray) -> list[float]:
    vector = np.asarray(vector, dtype=np.float64)
    total = float(vector.sum())
    if total <= 0.0 or not math.isfinite(total):
        return [0.0 for _ in vector]
    normalized = vector / total
    return [stable_float(float(v)) for v in normalized]


def gradient_orientation_histogram(crop: np.ndarray, bins: int, epsilon: float) -> tuple[list[float], float]:
    gray = np.asarray(crop, dtype=np.float64)
    height, width = gray.shape
    gx = np.zeros_like(gray)
    gy = np.zeros_like(gray)
    if width >= 3:
        gx[:, 1:-1] = (gray[:, 2:] - gray[:, :-2]) * 0.5
    if height >= 3:
        gy[1:-1, :] = (gray[2:, :] - gray[:-2, :]) * 0.5
    magnitude = np.hypot(gx, gy)
    total_magnitude = float(magnitude.sum())
    if total_magnitude <= epsilon:
        return [0.0 for _ in range(bins)], 0.0
    orientation = np.arctan2(gy, gx)
    bin_index = np.floor(((orientation + math.pi) / (2.0 * math.pi)) * bins).astype(np.int64)
    bin_index = np.clip(bin_index, 0, bins - 1)
    histogram = np.bincount(bin_index.ravel(), weights=magnitude.ravel(), minlength=bins)[:bins]
    return _normalize(histogram), stable_float(total_magnitude / max(1, height * width))


def spatial_signature(crop: np.ndarray, grid_size: int, epsilon: float) -> list[float]:
    gray = np.asarray(crop, dtype=np.float64)
    height, width = gray.shape
    grid_size = max(2, int(grid_size))
    cells: list[float] = []
    for gy in range(grid_size):
        y0 = (gy * height) // grid_size
        y1 = max(y0 + 1, ((gy + 1) * height) // grid_size)
        y1 = min(height, y1)
        for gx in range(grid_size):
            x0 = (gx * width) // grid_size
            x1 = max(x0 + 1, ((gx + 1) * width) // grid_size)
            x1 = min(width, x1)
            cells.append(float(gray[y0:y1, x0:x1].mean()))
    vector = np.asarray(cells, dtype=np.float64)
    vector = vector - float(vector.mean())
    norm = float(np.sqrt((vector * vector).sum()))
    if norm <= epsilon:
        return [0.0 for _ in vector]
    return [stable_float(float(v / norm)) for v in vector]


def model_from_region(
    frame: np.ndarray,
    box: Sequence[float],
    *,
    settings: Settings,
    sequence_id: str,
    frame_index: int,
    label: str,
    strict: bool = True,
) -> dict[str, Any] | None:
    validate_box(box, sequence_id=sequence_id, frame_index=frame_index, label=label)
    height, width = frame.shape
    x0, y0, x1, y1 = box_to_pixels(
        box,
        width,
        height,
        sequence_id=sequence_id,
        frame_index=frame_index,
        region=label,
    )
    crop = np.asarray(frame[y0:y1, x0:x1], dtype=np.uint8)
    pixel_sum = int(crop.sum(dtype=np.uint64))
    if crop.size == 0:
        if not strict:
            return None
        raise_error(
            E_INVALID_REGION,
            f"{label} crop is empty",
            context={
                "sequence_id": sequence_id,
                "frame_index": frame_index,
                "box": [float(v) for v in box],
                "box_pixels": [x0, y0, x1, y1],
                "diagnostic": "empty_crop",
            },
        )
    if pixel_sum == 0:
        if not strict:
            return None
        raise_error(
            E_INVALID_REGION,
            f"{label} crop contains only zero-valued pixels",
            context={
                "sequence_id": sequence_id,
                "frame_index": frame_index,
                "box": [float(v) for v in box],
                "box_pixels": [x0, y0, x1, y1],
                "pixel_sum": 0,
                "diagnostic": "all_zero_region",
            },
        )
    bins = int(settings.appearance["gray_histogram_bins"])
    grad_bins = int(settings.appearance["gradient_histogram_bins"])
    quantized = (crop.astype(np.int64) * bins) // 256
    gray_counts = np.bincount(quantized.ravel(), minlength=bins)[:bins]
    gray_histogram = _normalize(gray_counts)
    gradient_histogram, mean_gradient = gradient_orientation_histogram(
        crop,
        grad_bins,
        float(settings.appearance["gradient_epsilon"]),
    )
    signature = spatial_signature(
        crop,
        int(settings.appearance["spatial_grid_size"]),
        float(settings.appearance["spatial_epsilon"]),
    )
    gray_float = crop.astype(np.float64)
    model = {
        "gray_histogram": gray_histogram,
        "gradient_histogram": gradient_histogram,
        "spatial_signature": signature,
        "pixel_mean": stable_float(float(gray_float.mean())),
        "pixel_std": stable_float(float(gray_float.std())),
        "pixel_sum": pixel_sum,
        "mean_gradient_magnitude": mean_gradient,
        "gray_bins": bins,
        "gradient_bins": grad_bins,
        "normalization": str(settings.appearance["normalization"]),
        "spatial_grid_size": int(settings.appearance["spatial_grid_size"]),
    }
    model["model_digest"] = digest(model)
    return {
        "box_xywh_norm": [stable_float(float(v)) for v in box],
        "box_pixels": [x0, y0, x1, y1],
        "crop_shape": [int(y1 - y0), int(x1 - x0)],
        "model": model,
    }


def reference_and_initial_regions(
    frame: np.ndarray,
    *,
    sequence: dict[str, Any],
    frame_index: int,
    settings: Settings,
    strict: bool = True,
) -> dict[str, Any] | None:
    sequence_id = str(sequence["sequence_id"])
    reference_box = sequence["frames"][frame_index]["box_xywh_norm"]
    initial_box = sequence["initial_box"]
    reference = model_from_region(
        frame,
        reference_box,
        settings=settings,
        sequence_id=sequence_id,
        frame_index=frame_index,
        label="reference_truth_box",
        strict=strict,
    )
    initial = model_from_region(
        frame,
        initial_box,
        settings=settings,
        sequence_id=sequence_id,
        frame_index=frame_index,
        label="initial_anchor_box",
        strict=strict,
    )
    if reference is None or initial is None:
        return None
    return {"reference_truth": reference, "initial_anchor": initial}


def appearance_source_fingerprint(
    settings: Settings,
    sequence: dict[str, Any],
    frame_index: int,
    frame_sha256: str,
) -> str:
    entry = sequence["frames"][frame_index]
    return digest(
        {
            "kind": "appearance-source-v1",
            "sequence_id": sequence["sequence_id"],
            "frame_index": frame_index,
            "frame_sha256": frame_sha256,
            "initial_box": sequence["initial_box"],
            "reference_truth_box": entry["box_xywh_norm"],
            "appearance_config": settings.appearance,
        }
    )


def build_appearance_record(
    settings: Settings,
    sequence: dict[str, Any],
    frame_index: int,
    frame: np.ndarray,
    *,
    frame_sha256: str | None = None,
) -> dict[str, Any]:
    sequence_id = str(sequence["sequence_id"])
    if frame_sha256 is None:
        frame_sha256 = hashlib_sha256_array(frame)
    source_fingerprint = appearance_source_fingerprint(settings, sequence, frame_index, frame_sha256)
    regions = reference_and_initial_regions(
        frame,
        sequence=sequence,
        frame_index=frame_index,
        settings=settings,
        strict=True,
    )
    assert regions is not None
    model_digest = digest(
        {
            "reference_truth": regions["reference_truth"]["model"]["model_digest"],
            "initial_anchor": regions["initial_anchor"]["model"]["model_digest"],
        }
    )
    record = {
        "schema_version": 1,
        "stage": "appearance",
        "sequence_id": sequence_id,
        "frame_index": frame_index,
        "frame_sha256": frame_sha256,
        "source_fingerprint": source_fingerprint,
        "initial_box_xywh_norm": [stable_float(float(v)) for v in sequence["initial_box"]],
        "reference_truth_box_xywh_norm": [
            stable_float(float(v)) for v in sequence["frames"][frame_index]["box_xywh_norm"]
        ],
        "regions": regions,
        "model_digest": model_digest,
    }
    record["record_digest"] = digest(record)
    return record


def hashlib_sha256_array(array: np.ndarray) -> str:
    """Hash canonical .npy bytes for in-memory synthetic calls."""
    import hashlib
    import io

    buffer = io.BytesIO()
    np.save(buffer, np.ascontiguousarray(array), allow_pickle=False)
    return hashlib.sha256(buffer.getvalue()).hexdigest()


def appearance_artifact_path(settings: Settings, sequence_id: str) -> Path:
    return settings.sequence_artifact_dir(sequence_id) / "appearance.json"


def appearance_invalidation_reason(
    settings: Settings,
    sequence: dict[str, Any],
    frame_index: int,
    stored: dict[str, Any],
    current_frame_sha256: str,
) -> str | None:
    """Classify why a stored appearance source no longer matches current inputs."""
    entry = sequence["frames"][frame_index]
    if stored.get("frame_sha256") != current_frame_sha256:
        return "frame_content_changed"
    if stored.get("initial_box_xywh_norm") != sequence["initial_box"]:
        return "initial_box_changed"
    if stored.get("reference_truth_box_xywh_norm") != entry["box_xywh_norm"]:
        return "reference_truth_changed"
    if stored.get("source_fingerprint") != appearance_source_fingerprint(settings, sequence, frame_index, current_frame_sha256):
        return "appearance_config_changed"
    return None


def build_appearance_stage(
    settings: Settings,
    manifest: ManifestView,
    sequence: dict[str, Any],
) -> dict[str, Any]:
    sequence_id = str(sequence["sequence_id"])
    path = appearance_artifact_path(settings, sequence_id)
    existing: dict[str, Any] | None = None
    if path.exists():
        import json

        try:
            candidate = json.loads(path.read_text(encoding="utf-8"))
            if candidate.get("sequence_id") == sequence_id:
                existing = candidate
        except (OSError, ValueError):
            existing = None

    old_records: list[dict[str, Any]] = existing.get("records", []) if existing else []
    records: list[dict[str, Any]] = []
    reused_through = -1
    recomputed_from: int | None = None
    invalidation_reasons: list[dict[str, Any]] = []
    for frame_index, entry in enumerate(sequence["frames"]):
        frame_path = manifest.frame_path(sequence, frame_index)
        actual_sha = sha256_file(frame_path)
        expected_source = appearance_source_fingerprint(settings, sequence, frame_index, actual_sha)
        old = old_records[frame_index] if frame_index < len(old_records) else None
        if (
            old is not None
            and old.get("sequence_id") == sequence_id
            and old.get("frame_index") == frame_index
            and old.get("source_fingerprint") == expected_source
            and old.get("record_digest") == _record_digest_without_checksum(old)
        ):
            records.append(old)
            if recomputed_from is None:
                reused_through = frame_index
            continue
        if recomputed_from is None:
            recomputed_from = frame_index
            if old is not None:
                reason = appearance_invalidation_reason(settings, sequence, frame_index, old, actual_sha)
            else:
                reason = "appearance_artifact_missing"
            invalidation_reasons.append(
                {
                    "from_frame": frame_index,
                    "reason": reason,
                    "stored_fingerprint": old.get("source_fingerprint") if old else None,
                    "current_fingerprint": expected_source,
                }
            )
        frame = manifest.load_frame(sequence, frame_index)
        records.append(build_appearance_record(settings, sequence, frame_index, frame, frame_sha256=actual_sha))

    appearance_fingerprint = digest(
        {
            "kind": "appearance-stage-v1",
            "sequence_id": sequence_id,
            "records": [{"source": r["source_fingerprint"], "model": r["model_digest"]} for r in records],
        }
    )
    stage = {
        "schema_version": 1,
        "stage": "appearance",
        "sequence_id": sequence_id,
        "sequence_fingerprint": digest(
            {
                "sequence_id": sequence_id,
                "initial_box": sequence["initial_box"],
                "frames": [{"sha256": f["sha256"], "box": f["box_xywh_norm"]} for f in sequence["frames"]],
                "appearance_config": settings.appearance,
            }
        ),
        "appearance_fingerprint": appearance_fingerprint,
        "reused_through_frame": reused_through,
        "recomputed_from_frame": recomputed_from if recomputed_from is not None else len(records),
        "invalidation_reasons": invalidation_reasons,
        "records": records,
    }
    stage["stage_digest"] = digest(stage)
    atomic_write_json(path, stage)
    return {
        "ok": True,
        "sequence_id": sequence_id,
        "stage": "appearance",
        "appearance_fingerprint": appearance_fingerprint,
        "frame_count": len(records),
        "reused_through_frame": reused_through,
        "recomputed_from_frame": stage["recomputed_from_frame"],
        "invalidation_reasons": invalidation_reasons,
        "artifact_path": str(path),
        "stage_digest": stage["stage_digest"],
    }


def _record_digest_without_checksum(record: dict[str, Any]) -> str:
    payload = {k: v for k, v in record.items() if k != "record_digest"}
    return digest(payload)


def load_appearance_stage(settings: Settings, sequence_id: str) -> dict[str, Any] | None:
    path = appearance_artifact_path(settings, sequence_id)
    if not path.exists():
        return None
    import json

    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
