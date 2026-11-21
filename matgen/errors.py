"""Business errors shared by the library and command line interface."""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any


E_EMPTY_PROMPT = "E_EMPTY_PROMPT"
E_PALETTE_INVALID = "E_PALETTE_INVALID"
E_DIM_MISMATCH = "E_DIM_MISMATCH"
E_NUMERIC = "E_NUMERIC"
E_OCTAVE_RANGE = "E_OCTAVE_RANGE"
E_INVALID_RECIPE = "E_INVALID_RECIPE"
E_INVALID_CONFIG = "E_INVALID_CONFIG"
E_INVALID_STAGE = "E_INVALID_STAGE"
E_STALE_CHECKPOINT = "E_STALE_CHECKPOINT"
E_IO = "E_IO"


@dataclass
class MatgenError(Exception):
    """A stable, machine-readable failure from the material pipeline.

    Every error carries the material id when one exists.  Numeric failures also
    carry the zero-based octave index so callers can locate the offending layer.
    """

    code: str
    message: str
    material_id: str | None = None
    stage: str | None = None
    octave_index: int | None = None
    details: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        super().__init__(self.message)

    def to_dict(self) -> dict[str, Any]:
        return {
            "error": {
                "code": self.code,
                "message": self.message,
                "material_id": self.material_id,
                "stage": self.stage,
                "octave_index": self.octave_index,
                "details": self.details,
            }
        }


def ensure_finite(
    value: float,
    *,
    code: str = E_NUMERIC,
    material_id: str | None,
    stage: str,
    octave_index: int | None = None,
    label: str = "value",
) -> float:
    """Return ``value`` or raise a finite-number business error."""

    if not isinstance(value, (int, float)) or not math.isfinite(float(value)):
        raise MatgenError(
            code=code,
            message=f"non-finite {label}",
            material_id=material_id,
            stage=stage,
            octave_index=octave_index,
            details={"label": label, "value": repr(value)},
        )
    return float(value)

