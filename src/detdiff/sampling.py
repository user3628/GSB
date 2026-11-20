"""Deterministic pseudo-noise sampling and reference-image fusion."""

from __future__ import annotations

import hashlib
import math
from pathlib import Path
from typing import Any

import numpy as np
from PIL import Image, UnidentifiedImageError

from .errors import BusinessError, ErrorCode
from .util import canonical_json_bytes, sha256_bytes


def generate_noise(seed: int, shape: tuple[int, int, int], *, sampler_version: int = 1) -> np.ndarray:
    count = int(np.prod(shape))
    material = canonical_json_bytes(
        {
            "domain": "detdiff.noise.v1",
            "seed": int(seed),
            "shape": [int(item) for item in shape],
            "sampler_version": sampler_version,
        }
    )
    raw = hashlib.shake_256(material).digest(count * 8)
    words = np.frombuffer(raw, dtype="<u8").astype(np.uint64, copy=False)
    values = ((words >> np.uint64(11)).astype(np.float64) * (2.0 / 9007199254740992.0)) - 1.0
    return np.ascontiguousarray(values.reshape(shape), dtype="<f8")


def load_reference_tensor(
    path: Path,
    shape: tuple[int, int, int],
    *,
    shard_id: str,
) -> tuple[np.ndarray, str]:
    try:
        raw = path.read_bytes()
    except FileNotFoundError as exc:
        raise BusinessError(
            ErrorCode.E_MISSING_REFERENCE,
            f"reference image is missing: {path}",
            shard=shard_id,
            field="reference",
            details={"path": str(path)},
        ) from exc
    try:
        from io import BytesIO

        with Image.open(BytesIO(raw)) as image:
            rgb = image.convert("RGB")
            actual = (rgb.width, rgb.height, 3)
            if actual != shape:
                raise BusinessError(
                    ErrorCode.E_MISSING_REFERENCE,
                    "reference image dimensions do not match configured output dimensions",
                    shard=shard_id,
                    field="reference",
                    details={
                        "path": str(path),
                        "expected": list(shape),
                        "actual": list(actual),
                    },
                )
            array = np.asarray(rgb, dtype=np.uint8)
    except BusinessError:
        raise
    except (OSError, UnidentifiedImageError, ValueError) as exc:
        raise BusinessError(
            ErrorCode.E_MISSING_REFERENCE,
            "reference image cannot be decoded",
            shard=shard_id,
            field="reference",
            details={"path": str(path)},
        ) from exc
    tensor = np.ascontiguousarray(array.astype(np.float64) / 127.5 - 1.0, dtype="<f8")
    return tensor, sha256_bytes(raw)


def conditional_field(embedding: np.ndarray, shape: tuple[int, int, int]) -> np.ndarray:
    height, width, channels = shape
    y, x = np.mgrid[0:height, 0:width]
    field = np.empty(shape, dtype=np.float64)
    for channel in range(channels):
        a = float(embedding[channel * 5])
        b = float(embedding[channel * 5 + 1])
        phase = float(embedding[channel * 5 + 2]) * math.pi
        scale = 0.55 + 0.35 * abs(float(embedding[channel * 5 + 3]))
        freq_x = 1.0 + 0.75 * abs(a)
        freq_y = 1.0 + 0.75 * abs(b)
        field[..., channel] = scale * np.sin(
            (x + 1.0) * math.pi * freq_x / max(width, 1) + phase
        ) * np.cos((y + 1.0) * math.pi * freq_y / max(height, 1) - phase * 0.5)
    return np.ascontiguousarray(np.clip(field, -1.0, 1.0), dtype="<f8")


def _smooth_field(noise: np.ndarray) -> np.ndarray:
    result = noise.copy()
    for axis in (0, 1):
        result = (result + np.roll(result, 1, axis=axis) + np.roll(result, -1, axis=axis)) / 3.0
    return np.ascontiguousarray(np.clip(result, -1.0, 1.0), dtype="<f8")


def _summary(tensor: np.ndarray, step: int) -> dict[str, Any]:
    flat = tensor.reshape(-1)
    count = int(flat.size)
    mean = math.fsum(float(value) for value in flat) / count
    variance = math.fsum((float(value) - mean) ** 2 for value in flat) / count
    return {
        "step": int(step),
        "mean": round(mean, 12),
        "variance": round(variance, 12),
    }


def run_sampling(
    embedding: np.ndarray,
    weights: np.ndarray,
    seed: int,
    shape: tuple[int, int, int],
    *,
    reference_tensor: np.ndarray | None = None,
    blend_strength: float = 0.0,
    shard_id: str | None = None,
) -> tuple[np.ndarray, list[dict[str, Any]]]:
    steps = int(weights.size)
    if steps < 1:
        raise BusinessError(
            ErrorCode.E_INVALID_CONFIG,
            "schedule weights must not be empty",
            shard=shard_id,
            field="steps",
        )
    noise = generate_noise(seed, shape)
    if reference_tensor is None:
        current = noise.copy()
    else:
        if reference_tensor.shape != shape:
            raise BusinessError(
                ErrorCode.E_MISSING_REFERENCE,
                "reference tensor dimensions do not match output dimensions",
                shard=shard_id,
                field="reference",
            )
        strength = float(blend_strength)
        if not math.isfinite(strength) or strength < 0.0 or strength > 1.0:
            raise BusinessError(
                ErrorCode.E_INVALID_CONFIG,
                "blend_strength must be a finite number in [0, 1]",
                shard=shard_id,
                field="blend_strength",
            )
        current = (1.0 - strength) * noise + strength * reference_tensor

    conditional = conditional_field(embedding, shape)
    unconditional = _smooth_field(noise)
    trajectory: list[dict[str, Any]] = []
    for index, raw_weight in enumerate(weights):
        weight = float(raw_weight)
        target = (1.0 - weight) * conditional + weight * unconditional
        alpha = 0.25 + 0.7 * (float(index + 1) / float(steps))
        current = (1.0 - alpha) * current + alpha * target
        trajectory.append(_summary(current, index))
    return np.ascontiguousarray(current, dtype="<f8"), trajectory

