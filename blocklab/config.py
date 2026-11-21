"""YAML operator configuration loading and normalized fingerprints."""

from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from .errors import E_CONFIG, BusinessError
from .util import digest_json

ACTIVATIONS = ("gelu", "silu", "sigmoid")
REORDER_MODES = ("swap_head_seq", "tile")
SUM_STRATEGIES = ("sum", "mean", "l2")
KERNEL_VERSION = "blocklab-kernels-v1"

ACTIVATION_ALIASES = {
    "gelu": "gelu",
    "silu": "silu",
    "sigmoid": "sigmoid",
    "swish": "silu",
}
REORDER_ALIASES = {
    "swap_head_seq": "swap_head_seq",
    "swap_head_sequence": "swap_head_seq",
    "head_seq_swap": "swap_head_seq",
    "swap_axes_1_2": "swap_head_seq",
    "swap": "swap_head_seq",
    "tile": "tile",
    "tile_reorder": "tile",
    "block_tile": "tile",
}
SUM_ALIASES = {"sum": "sum", "stable_sum": "sum", "mean": "mean", "avg": "mean", "average": "mean", "l2": "l2", "norm": "l2"}


def _positive_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise BusinessError(E_CONFIG, f"{name} must be a positive integer", field=name)
    return value


def _tolerance(value: Any, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise BusinessError(E_CONFIG, f"{name} must be numeric", field=name)
    result = float(value)
    if not math.isfinite(result) or result < 0:
        raise BusinessError(E_CONFIG, f"{name} must be finite and non-negative", field=name)
    return result


def _shape(value: Any, name: str) -> tuple[int, ...] | None:
    if value is None:
        return None
    if not isinstance(value, (list, tuple)) or len(value) not in (2, 4):
        raise BusinessError(E_CONFIG, f"{name} must contain 2 or 4 dimensions", field=name)
    return tuple(_positive_int(item, name) for item in value)


@dataclass(frozen=True)
class OperatorConfig:
    activation: str
    reorder_mode: str
    row_block: int
    col_block: int
    sum_strategy: str
    abs_tol: float
    rel_tol: float
    target_shape: tuple[int, ...] | None = None
    source_version: int = 1

    def normalized(self) -> dict[str, Any]:
        return {
            "schema": "blocklab.operator-config.v1",
            "version": self.source_version,
            "activation": self.activation,
            "reorder_mode": self.reorder_mode,
            "row_block": self.row_block,
            "col_block": self.col_block,
            "sum_strategy": self.sum_strategy,
            "abs_tol": self.abs_tol,
            "rel_tol": self.rel_tol,
            "target_shape": list(self.target_shape) if self.target_shape else None,
            "kernel_version": KERNEL_VERSION,
        }

    @property
    def digest(self) -> str:
        return digest_json(self.normalized())

    @property
    def version(self) -> str:
        return f"sha256:{self.digest}"

    def stage_config_version(self, stage: str) -> str:
        if stage == "activation":
            value = {
                "activation": self.activation,
                "row_block": self.row_block,
                "col_block": self.col_block,
                "kernel_version": KERNEL_VERSION,
            }
        elif stage == "reorder":
            value = {
                "reorder_mode": self.reorder_mode,
                "target_shape": list(self.target_shape) if self.target_shape else None,
                "row_block": self.row_block,
                "col_block": self.col_block,
                "kernel_version": KERNEL_VERSION,
            }
        elif stage == "reduce":
            value = {
                "sum_strategy": self.sum_strategy,
                "abs_tol": self.abs_tol,
                "rel_tol": self.rel_tol,
                "kernel_version": KERNEL_VERSION,
            }
        else:
            raise ValueError(stage)
        return f"sha256:{digest_json(value)}"


def parse_config(raw: Any, *, source_version: int = 1) -> OperatorConfig:
    if not isinstance(raw, dict):
        raise BusinessError(E_CONFIG, "YAML root must be a mapping")

    activation_block = raw.get("activation", raw.get("activation_type"))
    if isinstance(activation_block, dict):
        activation = activation_block.get("type")
    else:
        activation = activation_block

    reorder_block = raw.get("reorder", raw.get("reorder_mode"))
    if isinstance(reorder_block, dict):
        reorder_mode = reorder_block.get("mode")
        target_shape_raw = reorder_block.get("target_shape")
    else:
        reorder_mode = reorder_block
        target_shape_raw = raw.get("target_shape")

    blocking = raw.get("blocking", {})
    if not isinstance(blocking, dict):
        raise BusinessError(E_CONFIG, "blocking must be a mapping")
    row_block = blocking.get(
        "row_block", blocking.get("row_block_size", raw.get("row_block", raw.get("row_block_size")))
    )
    col_block = blocking.get(
        "col_block", blocking.get("col_block_size", raw.get("col_block", raw.get("col_block_size")))
    )

    reduction = raw.get("reduction", {})
    if not isinstance(reduction, dict):
        raise BusinessError(E_CONFIG, "reduction must be a mapping")
    sum_strategy = reduction.get(
        "strategy", reduction.get("sum_strategy", raw.get("sum_strategy"))
    )
    tolerance_block = reduction.get("tolerance", raw.get("tolerance", raw.get("tolerances", {})))
    if not isinstance(tolerance_block, dict):
        tolerance_block = {}
    abs_tol = reduction.get(
        "abs_tol",
        reduction.get("atol", tolerance_block.get("abs", tolerance_block.get("abs_tol", raw.get("abs_tol", raw.get("atol", 1e-12))))),
    )
    rel_tol = reduction.get(
        "rel_tol",
        reduction.get("rtol", tolerance_block.get("rel", tolerance_block.get("rel_tol", raw.get("rel_tol", raw.get("rtol", 1e-9))))),
    )

    activation_key = str(activation).strip().lower() if activation is not None else None
    activation = ACTIVATION_ALIASES.get(activation_key or "", activation_key)
    reorder_key = str(reorder_mode).strip().lower() if reorder_mode is not None else None
    reorder_mode = REORDER_ALIASES.get(reorder_key or "", reorder_key)
    sum_key = str(sum_strategy).strip().lower() if sum_strategy is not None else None
    sum_strategy = SUM_ALIASES.get(sum_key or "", sum_key)

    if activation not in ACTIVATIONS:
        raise BusinessError(
            E_CONFIG,
            f"unsupported activation: {activation}",
            allowed=list(ACTIVATIONS),
        )
    if reorder_mode not in REORDER_MODES:
        raise BusinessError(
            E_CONFIG,
            f"unsupported reorder mode: {reorder_mode}",
            allowed=list(REORDER_MODES),
        )
    if sum_strategy not in SUM_STRATEGIES:
        raise BusinessError(
            E_CONFIG,
            f"unsupported sum strategy: {sum_strategy}",
            allowed=list(SUM_STRATEGIES),
        )

    raw_version = raw.get("version", source_version)
    if isinstance(raw_version, bool) or not isinstance(raw_version, int):
        raise BusinessError(E_CONFIG, "version must be an integer", field="version")

    config = OperatorConfig(
        activation=str(activation),
        reorder_mode=str(reorder_mode),
        row_block=_positive_int(row_block, "row_block"),
        col_block=_positive_int(col_block, "col_block"),
        sum_strategy=str(sum_strategy),
        abs_tol=_tolerance(abs_tol, "abs_tol"),
        rel_tol=_tolerance(rel_tol, "rel_tol"),
        target_shape=_shape(target_shape_raw, "target_shape"),
        source_version=int(raw_version),
    )
    return config


def load_config(path: Path) -> OperatorConfig:
    try:
        with path.open("r", encoding="utf-8") as handle:
            raw = yaml.safe_load(handle)
    except FileNotFoundError as exc:
        raise BusinessError(E_CONFIG, "config file not found", path=str(path)) from exc
    except yaml.YAMLError as exc:
        raise BusinessError(E_CONFIG, f"invalid YAML: {exc}", path=str(path)) from exc
    return parse_config(raw, source_version=1)

