"""Scheduling configuration loading and validation."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .errors import E_CONFIG_INVALID, WorkbenchError
from .util import canonical_json_bytes, sha256_bytes
from . import yaml_lite


def _as_int(data: dict[str, Any], key: str, default: int, *, minimum: int = 0) -> int:
    value = data.get(key, default)
    if isinstance(value, bool) or not isinstance(value, int):
        raise WorkbenchError(
            E_CONFIG_INVALID,
            f"config field {key!r} must be an integer",
            reason="CONFIG_TYPE",
            details={"field": key, "value": value},
        )
    if value < minimum:
        raise WorkbenchError(
            E_CONFIG_INVALID,
            f"config field {key!r} must be >= {minimum}",
            reason="CONFIG_RANGE",
            details={"field": key, "value": value, "minimum": minimum},
        )
    return value


def _as_float(
    data: dict[str, Any], key: str, default: float, *, minimum: float | None = None
) -> float:
    value = data.get(key, default)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise WorkbenchError(
            E_CONFIG_INVALID,
            f"config field {key!r} must be numeric",
            reason="CONFIG_TYPE",
            details={"field": key, "value": value},
        )
    value = float(value)
    if minimum is not None and value < minimum:
        raise WorkbenchError(
            E_CONFIG_INVALID,
            f"config field {key!r} must be >= {minimum}",
            reason="CONFIG_RANGE",
            details={"field": key, "value": value, "minimum": minimum},
        )
    return value


@dataclass(frozen=True)
class Config:
    raw: dict[str, Any]
    version: str
    config_version: str
    max_workers: int
    global_step_budget: int
    worker_step_quota: int
    task_timeout_seconds: float
    max_chain_length: int
    rotate_angle_min: float
    rotate_angle_max: float
    max_dimension: int
    steps_per_operator: int
    pixels_per_step: int
    min_task_steps: int
    export_format: str

    @classmethod
    def from_mapping(cls, data: dict[str, Any]) -> "Config":
        if not isinstance(data, dict):
            raise WorkbenchError(
                E_CONFIG_INVALID,
                "scheduler config root must be a mapping",
                reason="CONFIG_ROOT",
            )
        version = str(data.get("version", "1"))
        max_workers = _as_int(data, "max_workers", 1, minimum=1)
        global_step_budget = _as_int(data, "global_step_budget", 100, minimum=0)
        worker_step_quota = _as_int(data, "worker_step_quota", global_step_budget, minimum=1)
        task_timeout_seconds = _as_float(
            data, "task_timeout_seconds", 60.0, minimum=0.001
        )
        max_chain_length = _as_int(data, "max_chain_length", 8, minimum=0)

        reachability = data.get("reachability", {}) or {}
        if not isinstance(reachability, dict):
            raise WorkbenchError(
                E_CONFIG_INVALID,
                "reachability must be a mapping",
                reason="CONFIG_TYPE",
                details={"field": "reachability"},
            )
        rotate_angle_min = _as_float(
            reachability, "rotate_angle_min", -180.0, minimum=-360.0
        )
        rotate_angle_max = _as_float(
            reachability, "rotate_angle_max", 180.0, minimum=-360.0
        )
        if rotate_angle_min > rotate_angle_max:
            raise WorkbenchError(
                E_CONFIG_INVALID,
                "rotate_angle_min must not exceed rotate_angle_max",
                reason="CONFIG_RANGE",
            )
        max_dimension = _as_int(reachability, "max_dimension", 8192, minimum=1)

        cost_model = data.get("cost_model", {}) or {}
        if not isinstance(cost_model, dict):
            raise WorkbenchError(
                E_CONFIG_INVALID,
                "cost_model must be a mapping",
                reason="CONFIG_TYPE",
                details={"field": "cost_model"},
            )
        steps_per_operator = _as_int(
            cost_model, "steps_per_operator", 2, minimum=0
        )
        pixels_per_step = _as_int(cost_model, "pixels_per_step", 1024, minimum=1)
        min_task_steps = _as_int(cost_model, "min_task_steps", 1, minimum=0)

        export = data.get("export", {}) or {}
        if not isinstance(export, dict):
            raise WorkbenchError(
                E_CONFIG_INVALID,
                "export must be a mapping",
                reason="CONFIG_TYPE",
                details={"field": "export"},
            )
        export_format = str(export.get("format", "png-rgb-v1"))
        if export_format != "png-rgb-v1":
            raise WorkbenchError(
                E_CONFIG_INVALID,
                "only export.format=png-rgb-v1 is supported",
                reason="CONFIG_UNSUPPORTED",
                details={"format": export_format},
            )

        return cls(
            raw=data,
            version=version,
            config_version=sha256_bytes(canonical_json_bytes(data)),
            max_workers=max_workers,
            global_step_budget=global_step_budget,
            worker_step_quota=worker_step_quota,
            task_timeout_seconds=task_timeout_seconds,
            max_chain_length=max_chain_length,
            rotate_angle_min=rotate_angle_min,
            rotate_angle_max=rotate_angle_max,
            max_dimension=max_dimension,
            steps_per_operator=steps_per_operator,
            pixels_per_step=pixels_per_step,
            min_task_steps=min_task_steps,
            export_format=export_format,
        )

    @classmethod
    def load(cls, path: Path) -> "Config":
        path = Path(path)
        try:
            data = yaml_lite.load(path)
        except Exception as exc:
            raise WorkbenchError(
                E_CONFIG_INVALID,
                f"cannot parse YAML config: {exc}",
                reason="CONFIG_PARSE",
                details={"path": str(path)},
            ) from exc
        try:
            return cls.from_mapping(data)
        except WorkbenchError:
            raise
        except Exception as exc:
            raise WorkbenchError(
                E_CONFIG_INVALID,
                f"invalid scheduler config: {exc}",
                reason="CONFIG_PARSE",
            ) from exc

    def scheduling_components(self) -> dict[str, Any]:
        return {
            "max_workers": self.max_workers,
            "global_step_budget": self.global_step_budget,
            "worker_step_quota": self.worker_step_quota,
            "task_timeout_seconds": self.task_timeout_seconds,
            "max_chain_length": self.max_chain_length,
            "rotate_angle_min": self.rotate_angle_min,
            "rotate_angle_max": self.rotate_angle_max,
            "max_dimension": self.max_dimension,
            "cost_model": {
                "steps_per_operator": self.steps_per_operator,
                "pixels_per_step": self.pixels_per_step,
                "min_task_steps": self.min_task_steps,
            },
            "export_format": self.export_format,
        }

    def estimate_steps(self, input_size: tuple[int, int], chain_length: int) -> int:
        width, height = input_size
        pixel_steps = (width * height + self.pixels_per_step - 1) // self.pixels_per_step
        return max(
            self.min_task_steps,
            chain_length * self.steps_per_operator + pixel_steps,
        )

    def to_public_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "config_version": self.config_version,
            **self.scheduling_components(),
        }
