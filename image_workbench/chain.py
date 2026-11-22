"""Operator-chain parsing and reachability validation."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

from .config import Config
from .errors import E_CHAIN_INVALID, WorkbenchError
from .models import TaskSpec
from .operators import OPERATOR_NAMES


@dataclass(frozen=True)
class ParsedChain:
    task_id: str
    operations: tuple[dict[str, Any], ...]
    input_size: tuple[int, int]
    output_size: tuple[int, int]

    def public_operations(self) -> list[dict[str, Any]]:
        return [dict(operation) for operation in self.operations]

    def to_public_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "operations": self.public_operations(),
            "input_size": list(self.input_size),
            "output_size": list(self.output_size),
            "chain_length": len(self.operations),
        }


def _chain_error(
    task_id: str,
    position: int | None,
    reason: str,
    message: str,
    details: dict[str, Any] | None = None,
) -> WorkbenchError:
    return WorkbenchError(
        E_CHAIN_INVALID,
        message,
        task_id=task_id,
        stage="parsed",
        position=position,
        reason=reason,
        details=details or {},
    )


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _require_int(
    task_id: str, position: int, params: dict[str, Any], key: str
) -> int:
    value = params.get(key)
    if not _is_int(value):
        raise _chain_error(
            task_id,
            position,
            "PARAM_NOT_INTEGER",
            f"operator at position {position} requires integer parameter {key!r}",
            {"parameter": key, "value": value},
        )
    return int(value)


def _require_number(
    task_id: str, position: int, params: dict[str, Any], key: str
) -> float:
    value = params.get(key)
    if not _is_number(value) or not math.isfinite(float(value)):
        raise _chain_error(
            task_id,
            position,
            "PARAM_NOT_FINITE_NUMBER",
            f"operator at position {position} requires finite numeric parameter {key!r}",
            {"parameter": key, "value": value},
        )
    return float(value)


def _extract_operation(
    task: TaskSpec, raw_operation: Any, position: int
) -> tuple[str, dict[str, Any]]:
    if isinstance(raw_operation, str):
        name = raw_operation
        params: dict[str, Any] = {}
        parameter_sets = task.raw.get("operator_params")
        if isinstance(parameter_sets, list) and position < len(parameter_sets):
            extra = parameter_sets[position]
            if extra is not None:
                if not isinstance(extra, dict):
                    raise _chain_error(
                        task.task_id,
                        position,
                        "OPERATOR_PARAMS_INVALID",
                        "operator_params entries must be mappings",
                    )
                params.update(extra)
        return name, params
    if not isinstance(raw_operation, dict):
        raise _chain_error(
            task.task_id,
            position,
            "OPERATOR_ENTRY_INVALID",
            "operator chain entries must be names or mappings",
            {"value_type": type(raw_operation).__name__},
        )
    name = raw_operation.get("op", raw_operation.get("operator"))
    if name is None and len(raw_operation) == 1:
        only_key = next(iter(raw_operation))
        if only_key in OPERATOR_NAMES:
            name = only_key
            value = raw_operation[only_key]
            params = value if isinstance(value, dict) else {}
            return str(name), dict(params)
    if not isinstance(name, str) or not name:
        raise _chain_error(
            task.task_id,
            position,
            "OPERATOR_NAME_MISSING",
            "operator mapping must contain an 'op' string",
        )
    params = raw_operation.get("params")
    if params is None:
        params = {
            key: value
            for key, value in raw_operation.items()
            if key not in {"op", "operator", "params"}
        }
    if not isinstance(params, dict):
        raise _chain_error(
            task.task_id,
            position,
            "OPERATOR_PARAMS_INVALID",
            "operator params must be a mapping",
        )
    return name, dict(params)


def _normalize_operation(
    task: TaskSpec,
    name: str,
    params: dict[str, Any],
    position: int,
    current_size: tuple[int, int],
    config: Config,
) -> tuple[dict[str, Any], tuple[int, int]]:
    if name not in OPERATORS_TABLE:
        raise _chain_error(
            task.task_id,
            position,
            "OPERATOR_UNKNOWN",
            f"unknown operator {name!r} at position {position}",
            {"operator": name, "known": list(OPERATOR_NAMES)},
        )
    if task.allowed_operators and name not in task.allowed_operators:
        raise _chain_error(
            task.task_id,
            position,
            "OPERATOR_NOT_ALLOWED",
            f"operator {name!r} at position {position} is outside allowed_operators",
            {"operator": name, "allowed": list(task.allowed_operators)},
        )
    width, height = current_size
    if name == "crop":
        x = _require_int(task.task_id, position, params, "x")
        y = _require_int(task.task_id, position, params, "y")
        crop_width = _require_int(task.task_id, position, params, "width")
        crop_height = _require_int(task.task_id, position, params, "height")
        if crop_width <= 0 or crop_height <= 0:
            raise _chain_error(
                task.task_id,
                position,
                "CROP_NON_POSITIVE_SIZE",
                f"crop at position {position} must have positive width and height",
                {"width": crop_width, "height": crop_height},
            )
        x0 = max(0, x)
        y0 = max(0, y)
        x1 = min(width, x + crop_width)
        y1 = min(height, y + crop_height)
        if x1 <= x0 or y1 <= y0:
            raise _chain_error(
                task.task_id,
                position,
                "CROP_EMPTY_INTERSECTION",
                f"crop at position {position} has an empty intersection with {width}x{height}",
                {
                    "requested": {
                        "x": x,
                        "y": y,
                        "width": crop_width,
                        "height": crop_height,
                    },
                    "current_size": [width, height],
                },
            )
        normalized = {
            "op": "crop",
            "params": {
                "x": x0,
                "y": y0,
                "width": x1 - x0,
                "height": y1 - y0,
            },
        }
        return normalized, (x1 - x0, y1 - y0)
    if name == "resize":
        target_width = _require_int(task.task_id, position, params, "width")
        target_height = _require_int(task.task_id, position, params, "height")
        mode = params.get("mode", "bilinear")
        if target_width <= 0 or target_height <= 0:
            raise _chain_error(
                task.task_id,
                position,
                "RESIZE_NON_POSITIVE_SIZE",
                f"resize at position {position} requires positive target dimensions",
                {"width": target_width, "height": target_height},
            )
        if target_width > config.max_dimension or target_height > config.max_dimension:
            raise _chain_error(
                task.task_id,
                position,
                "RESIZE_EXCEEDS_MAX_DIMENSION",
                f"resize at position {position} exceeds max_dimension={config.max_dimension}",
                {
                    "width": target_width,
                    "height": target_height,
                    "max_dimension": config.max_dimension,
                },
            )
        if mode not in {"nearest", "bilinear"}:
            raise _chain_error(
                task.task_id,
                position,
                "RESIZE_MODE_INVALID",
                f"resize at position {position} has unsupported mode {mode!r}",
                {"mode": mode},
            )
        return {
            "op": "resize",
            "params": {
                "width": target_width,
                "height": target_height,
                "mode": mode,
            },
        }, (target_width, target_height)
    if name == "rotate":
        angle = _require_number(task.task_id, position, params, "angle")
        if angle < config.rotate_angle_min or angle > config.rotate_angle_max:
            raise _chain_error(
                task.task_id,
                position,
                "ROTATE_ANGLE_OUT_OF_RANGE",
                f"rotate at position {position} angle {angle} is outside the legal range",
                {
                    "angle": angle,
                    "minimum": config.rotate_angle_min,
                    "maximum": config.rotate_angle_max,
                },
            )
        return {"op": "rotate", "params": {"angle": angle}}, current_size
    red = params.get("red", params.get("r", 0.0))
    green = params.get("green", params.get("g", 0.0))
    blue = params.get("blue", params.get("b", 0.0))
    deltas: list[float] = []
    for channel, value in (("red", red), ("green", green), ("blue", blue)):
        if not _is_number(value) or not math.isfinite(float(value)):
            raise _chain_error(
                task.task_id,
                position,
                "COLOR_SHIFT_PARAM_INVALID",
                f"color_shift at position {position} requires finite {channel} delta",
                {"channel": channel, "value": value},
            )
        deltas.append(float(value))
    return {
        "op": "color_shift",
        "params": {"red": deltas[0], "green": deltas[1], "blue": deltas[2]},
    }, current_size


OPERATORS_TABLE = frozenset(OPERATOR_NAMES)


def parse_chain(
    task: TaskSpec,
    input_size: tuple[int, int],
    config: Config,
) -> ParsedChain:
    raw_operations = list(task.operations)
    if len(raw_operations) > config.max_chain_length:
        raise _chain_error(
            task.task_id,
            config.max_chain_length,
            "CHAIN_TOO_LONG",
            f"operator chain length {len(raw_operations)} exceeds max_chain_length={config.max_chain_length}",
            {
                "chain_length": len(raw_operations),
                "max_chain_length": config.max_chain_length,
            },
        )
    width, height = input_size
    if not _is_int(width) or not _is_int(height) or width <= 0 or height <= 0:
        raise _chain_error(
            task.task_id,
            None,
            "INPUT_SIZE_INVALID",
            "input image dimensions must be positive",
            {"input_size": list(input_size)},
        )
    normalized_operations: list[dict[str, Any]] = []
    current_size = (int(width), int(height))
    for position, raw_operation in enumerate(raw_operations):
        name, params = _extract_operation(task, raw_operation, position)
        normalized, current_size = _normalize_operation(
            task, name, params, position, current_size, config
        )
        normalized_operations.append(normalized)
    if current_size[0] <= 0 or current_size[1] <= 0:
        raise _chain_error(
            task.task_id,
            len(normalized_operations),
            "CHAIN_OUTPUT_INVALID",
            "operator chain produced non-positive output dimensions",
            {"output_size": list(current_size)},
        )
    return ParsedChain(
        task_id=task.task_id,
        operations=tuple(normalized_operations),
        input_size=(int(width), int(height)),
        output_size=current_size,
    )
