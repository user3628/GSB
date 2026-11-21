"""Input finiteness checks and aggregate online-softmax statistics."""

from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any

import numpy as np

from .config import AttentionConfig
from .errors import fail


@dataclass
class NumericStats:
    clamped_values: int = 0
    extreme_values: int = 0
    max_abs_raw_logit: float = 0.0
    rows_with_extremes: int = 0
    first_clamped: dict[str, int] | None = None

    def merge(self, other: "NumericStats") -> "NumericStats":
        first = self.first_clamped
        if first is None:
            first = other.first_clamped
        return NumericStats(
            clamped_values=self.clamped_values + other.clamped_values,
            extreme_values=self.extreme_values + other.extreme_values,
            max_abs_raw_logit=max(self.max_abs_raw_logit, other.max_abs_raw_logit),
            rows_with_extremes=self.rows_with_extremes + other.rows_with_extremes,
            first_clamped=dict(first) if first is not None else None,
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, value: dict[str, Any] | None) -> "NumericStats":
        value = value or {}
        return cls(
            clamped_values=int(value.get("clamped_values", 0)),
            extreme_values=int(value.get("extreme_values", 0)),
            max_abs_raw_logit=float(value.get("max_abs_raw_logit", 0.0)),
            rows_with_extremes=int(value.get("rows_with_extremes", 0)),
            first_clamped=value.get("first_clamped"),
        )


def validate_attention_inputs(
    q: np.ndarray,
    k: np.ndarray,
    v: np.ndarray,
    config: AttentionConfig,
    shard_id: str,
) -> None:
    for name, array in (("q", q), ("k", k), ("v", v)):
        if not np.issubdtype(array.dtype, np.floating):
            fail(
                "E_NUMERIC",
                f"{name} must have a floating dtype",
                shard_id=shard_id,
                array=name,
                dtype=str(array.dtype),
            )
        bad = np.argwhere(~np.isfinite(array))
        if bad.size == 0:
            continue
        batch, head, seq, dim = (int(item) for item in bad[0])
        row_block = seq // config.row_block_size
        col_block = seq // config.col_block_size
        value = array[batch, head, seq, dim]
        fail(
            "E_NUMERIC",
            f"Non-finite value in {name}",
            shard_id=shard_id,
            array=name,
            batch=batch,
            head=head,
            sequence_index=seq,
            head_dim=dim,
            row_block=row_block,
            col_block=col_block,
            value=float(value) if np.isfinite(value) else str(value),
        )


def ensure_finite(
    array: np.ndarray,
    *,
    shard_id: str,
    row_block: int,
    col_block: int,
    stage: str,
) -> None:
    if np.isfinite(array).all():
        return
    finite = array[np.isfinite(array)]
    max_abs = float(np.max(np.abs(finite))) if finite.size else None
    fail(
        "E_NUMERIC",
        f"Non-finite numerical state during {stage}",
        shard_id=shard_id,
        row_block=row_block,
        col_block=col_block,
        stage=stage,
        max_abs_finite=max_abs,
    )

