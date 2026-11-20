from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np

from .errors import E_NUMERIC, E_VALIDATION, WorkbenchError
from .hashing import digest_array
from .numeric import ensure_finite, quantize_u8, validate_strength, validate_u8


@dataclass(frozen=True)
class AlgorithmStats:
    clipped_values: int = 0
    filled_pixels: int = 0
    changed_pixels: int = 0

    def to_dict(self) -> dict[str, int]:
        return {
            "clipped_values": self.clipped_values,
            "filled_pixels": self.filled_pixels,
            "changed_pixels": self.changed_pixels,
        }


@dataclass(frozen=True)
class AlgorithmResult:
    pixels: np.ndarray
    stats: AlgorithmStats

    @property
    def digest(self) -> str:
        return digest_array(self.pixels, role="rgb")


def repair_pixels(
    pixels: np.ndarray,
    mask: np.ndarray,
    iterations: int,
    *,
    fragment_id: str,
) -> AlgorithmResult:
    """Deterministic Jacobi diffusion restricted to the masked pixels.

    The masked area is seeded with the channel-wise mean of known pixels and then
    repeatedly replaced by the fixed 8-neighbour average. Unmasked pixels remain
    Dirichlet boundary values. NumPy operations run in float64 and never use a
    random source.
    """
    validate_u8(pixels, fragment_id=fragment_id)
    if mask.shape != pixels.shape[:2] or mask.dtype != np.bool_:
        raise WorkbenchError(E_VALIDATION, "Repair mask must be a boolean HxW array", fragment_id=fragment_id)
    if isinstance(iterations, bool) or not isinstance(iterations, int) or iterations < 0:
        raise WorkbenchError(E_VALIDATION, "Repair iterations must be a non-negative integer", fragment_id=fragment_id)
    source = pixels.astype(np.float64, copy=True)
    if not mask.any() or iterations == 0:
        return AlgorithmResult(pixels.copy(), AlgorithmStats())
    known = ~mask
    if not known.any():
        first_y, first_x = np.argwhere(mask)[0].tolist()
        raise WorkbenchError(
            E_NUMERIC,
            "Repair mask covers every pixel; no known neighbour can seed diffusion",
            fragment_id=fragment_id,
            pixel=[int(first_x), int(first_y)],
            details={"field": "mask", "reason": "no_known_pixels"},
        )
    work = source.copy()
    known_mean = source[known].mean(axis=0, dtype=np.float64)
    work[mask] = known_mean
    height, width = mask.shape
    padded = np.pad(work, ((1, 1), (1, 1), (0, 0)), mode="constant", constant_values=0.0)
    count_padded = np.pad(np.ones((height, width), dtype=np.float64), ((1, 1), (1, 1)), mode="constant", constant_values=0.0)
    neighbour_count = np.zeros((height, width), dtype=np.float64)
    for dy in range(3):
        for dx in range(3):
            if dx == 1 and dy == 1:
                continue
            neighbour_count += count_padded[dy : dy + height, dx : dx + width]
    for _ in range(iterations):
        total = np.zeros_like(work)
        for dy in range(3):
            for dx in range(3):
                if dx == 1 and dy == 1:
                    continue
                total += padded[dy : dy + height, dx : dx + width, :]
        average = total / neighbour_count[:, :, None]
        ensure_finite(average, fragment_id=fragment_id, field="repair_diffusion")
        work[mask] = average[mask]
        padded = np.pad(work, ((1, 1), (1, 1), (0, 0)), mode="constant", constant_values=0.0)
    out = quantize_u8(work, fragment_id=fragment_id, field="repair_output")
    changed = int(np.count_nonzero(np.any(out != pixels, axis=2)))
    return AlgorithmResult(
        out,
        AlgorithmStats(
            clipped_values=int(np.count_nonzero((work < 0.0) | (work > 255.0))),
            filled_pixels=int(mask.sum()),
            changed_pixels=changed,
        ),
    )


def style_transfer_pixels(
    pixels: np.ndarray,
    reference: np.ndarray,
    strength: float,
    *,
    fragment_id: str,
) -> AlgorithmResult:
    """Apply deterministic reference mean/std colour transfer in float64."""
    validate_u8(pixels, fragment_id=fragment_id)
    validate_u8(reference, fragment_id=fragment_id)
    strength = validate_strength(strength, fragment_id=fragment_id)
    if strength == 0.0:
        return AlgorithmResult(pixels.copy(), AlgorithmStats())
    target = pixels.astype(np.float64, copy=False)
    ref = reference.astype(np.float64, copy=False)
    target_mean = target.mean(axis=(0, 1), dtype=np.float64)
    target_std = target.std(axis=(0, 1), dtype=np.float64, ddof=0)
    ref_mean = ref.mean(axis=(0, 1), dtype=np.float64)
    ref_std = ref.std(axis=(0, 1), dtype=np.float64, ddof=0)
    safe_target_std = np.where(target_std < 1e-12, 1.0, target_std)
    transferred = (target - target_mean) / safe_target_std * ref_std + ref_mean
    mixed = target * (1.0 - strength) + transferred * strength
    ensure_finite(mixed, fragment_id=fragment_id, field="style_transfer")
    out = quantize_u8(mixed, fragment_id=fragment_id, field="style_output")
    changed = int(np.count_nonzero(np.any(out != pixels, axis=2)))
    return AlgorithmResult(
        out,
        AlgorithmStats(
            clipped_values=int(np.count_nonzero((mixed < 0.0) | (mixed > 255.0))),
            changed_pixels=changed,
        ),
    )


def algorithm_config_dict(*, iterations: int, strength: float) -> dict[str, Any]:
    return {
        "repair": {"algorithm": "jacobi-8-neighbour-diffusion-v1", "iterations": iterations},
        "style": {"algorithm": "reference-mean-std-v1", "strength": strength, "epsilon": 1e-12},
    }

