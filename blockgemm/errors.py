"""Stable business errors exposed by the blockgemm library."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


E_STALE_CHECKPOINT = "E_STALE_CHECKPOINT"
E_INVALID_SHAPE = "E_INVALID_SHAPE"
E_NUMERIC_MISMATCH = "E_NUMERIC_MISMATCH"
E_INVALID_CONFIG = "E_INVALID_CONFIG"
E_WORKSPACE = "E_WORKSPACE"


@dataclass
class BlockGemmError(Exception):
    """A serializable, stable operator error.

    ``block_coord`` and ``shard_id`` are deliberately present even for errors
    raised before a concrete block exists, so callers can use one error schema.
    """

    code: str
    message: str
    shard_id: str | None = None
    block_coord: list[int] | None = None
    reason: str = ""
    details: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        Exception.__init__(self, self.message)
        if self.block_coord is None:
            self.block_coord = [0, 0]

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "shard_id": self.shard_id,
            "block_coord": list(self.block_coord or [0, 0]),
            "reason": self.reason,
            "details": self.details,
        }


def stale_checkpoint(
    message: str,
    *,
    shard_id: str | None,
    block_coord: list[int] | None,
    reason: str,
    details: dict[str, Any] | None = None,
) -> BlockGemmError:
    return BlockGemmError(
        E_STALE_CHECKPOINT,
        message,
        shard_id=shard_id,
        block_coord=block_coord,
        reason=reason,
        details=details or {},
    )


def invalid_shape(
    message: str,
    *,
    shard_id: str | None = None,
    block_coord: list[int] | None = None,
    reason: str = "invalid_shape",
    details: dict[str, Any] | None = None,
) -> BlockGemmError:
    return BlockGemmError(
        E_INVALID_SHAPE,
        message,
        shard_id=shard_id,
        block_coord=block_coord,
        reason=reason,
        details=details or {},
    )


def numeric_mismatch(
    message: str,
    *,
    shard_id: str,
    block_coord: list[int],
    row_range: list[int],
    col_range: list[int],
    max_abs_error: float,
    max_rel_error: float,
    tolerances: dict[str, float],
    sequence: int | None = None,
) -> BlockGemmError:
    return BlockGemmError(
        E_NUMERIC_MISMATCH,
        message,
        shard_id=shard_id,
        block_coord=block_coord,
        reason="sampled_block_exceeds_tolerance",
        details={
            "sequence": sequence,
            "row_range": row_range,
            "col_range": col_range,
            "max_abs_error": max_abs_error,
            "max_rel_error": max_rel_error,
            "tolerances": tolerances,
        },
    )
