from __future__ import annotations

import math
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

import numpy as np

from .errors import E_NUMERIC, E_VALIDATION, WorkbenchError
from .hashing import digest_bytes


def _round_half_up(value: float) -> int:
    return math.floor(value + 0.5)


@dataclass(frozen=True)
class MaskRect:
    """A normalized, half-open pixel rectangle: [x0, x1) x [y0, y1)."""

    x0: float
    y0: float
    x1: float
    y1: float

    def __post_init__(self) -> None:
        values = (self.x0, self.y0, self.x1, self.y1)
        if not all(math.isfinite(value) for value in values):
            raise WorkbenchError(
                E_NUMERIC,
                "Mask coordinates must be finite numbers",
                pixel=[0, 0],
                details={"field": "mask_rects", "values": list(values)},
            )
        if not (0.0 <= self.x0 < self.x1 <= 1.0 and 0.0 <= self.y0 < self.y1 <= 1.0):
            raise WorkbenchError(
                E_VALIDATION,
                "Mask rectangle must satisfy 0 <= x0 < x1 <= 1 and 0 <= y0 < y1 <= 1",
                details={"rect": self.as_list()},
            )

    def pixel_bounds(self, width: int, height: int) -> tuple[int, int, int, int]:
        x0 = min(width, max(0, _round_half_up(self.x0 * width)))
        y0 = min(height, max(0, _round_half_up(self.y0 * height)))
        x1 = min(width, max(0, _round_half_up(self.x1 * width)))
        y1 = min(height, max(0, _round_half_up(self.y1 * height)))
        if x0 == x1 or y0 == y1:
            # Very small normalized rectangles can round to an empty pixel span.
            # Anchor them to the nearest legal pixel so the semantic mask is not lost.
            x1 = min(width, max(x0 + 1, x1))
            y1 = min(height, max(y0 + 1, y1))
            x0 = min(x1 - 1, x0)
            y0 = min(y1 - 1, y0)
        return x0, y0, x1, y1

    def as_list(self) -> list[float]:
        return [self.x0, self.y0, self.x1, self.y1]

    @classmethod
    def from_list(cls, value: Iterable[float]) -> MaskRect:
        values = list(value)
        if len(values) != 4:
            raise WorkbenchError(
                E_VALIDATION,
                "Mask rectangle must contain exactly four normalized coordinates",
                details={"value": values},
            )
        try:
            return cls(*(float(item) for item in values))
        except (TypeError, ValueError) as exc:
            raise WorkbenchError(
                E_NUMERIC,
                "Mask coordinates must be numeric",
                pixel=[0, 0],
                details={"value": values},
            ) from exc

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> MaskRect:
        if isinstance(value, dict) and {"x0", "y0", "x1", "y1"} <= value.keys():
            return cls.from_list([value["x0"], value["y0"], value["x1"], value["y1"]])
        if isinstance(value, dict) and {"x", "y", "width", "height"} <= value.keys():
            values = [value["x"], value["y"], float(value["x"]) + float(value["width"]), float(value["y"]) + float(value["height"])]
            return cls.from_list(values)
        if isinstance(value, (list, tuple)):
            return cls.from_list(value)
        raise WorkbenchError(E_VALIDATION, "Unsupported mask rectangle representation", details={"value": value})


@dataclass(frozen=True)
class MaskRegion:
    rects: tuple[MaskRect, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "rects", tuple(self.rects))

    @classmethod
    def from_dict(cls, value: Any) -> MaskRegion:
        if value is None:
            return cls()
        if isinstance(value, dict):
            value = value.get("rects", [])
        if not isinstance(value, (list, tuple)):
            raise WorkbenchError(E_VALIDATION, "mask_rects must be a list", details={"value": value})
        return cls(tuple(MaskRect.from_dict(item) for item in value))

    @classmethod
    def from_alpha(cls, alpha: np.ndarray) -> MaskRegion:
        """Convert an alpha mask into exact normalized row-run rectangles."""
        if alpha.ndim != 2:
            raise WorkbenchError(E_VALIDATION, "Alpha mask must be two-dimensional")
        height, width = alpha.shape
        rects: list[MaskRect] = []
        masked = alpha == 0
        for y in range(height):
            x = 0
            row = masked[y]
            while x < width:
                if not row[x]:
                    x += 1
                    continue
                start = x
                while x < width and row[x]:
                    x += 1
                rects.append(MaskRect(start / width, y / height, x / width, (y + 1) / height))
        return cls(tuple(rects))

    def rasterize(self, width: int, height: int) -> np.ndarray:
        mask = np.zeros((height, width), dtype=bool)
        for rect in self.rects:
            x0, y0, x1, y1 = rect.pixel_bounds(width, height)
            mask[y0:y1, x0:x1] = True
        return mask

    def to_lists(self) -> list[list[float]]:
        return [rect.as_list() for rect in self.rects]

    def digest(self, width: int, height: int) -> str:
        return digest_bytes(self.rasterize(width, height).tobytes())

    def pixel_count(self, width: int, height: int) -> int:
        return int(self.rasterize(width, height).sum())

    def union(self, other: MaskRegion) -> MaskRegion:
        return MaskRegion(self.rects + other.rects)

