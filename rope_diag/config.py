"""Validated, versioned RoPE configuration."""

from __future__ import annotations

from dataclasses import asdict, dataclass, fields, replace
from pathlib import Path
from typing import Any, Mapping

import yaml

from .digest import atomic_write_bytes, canonical_json_bytes, sha256_bytes
from .errors import RopeError


SCALING_MODES = ("none", "linear", "ntk_piecewise")


@dataclass(frozen=True)
class RopeConfig:
    base_frequency: float = 10000.0
    scaling_mode: str = "none"
    scaling_factor: float = 1.0
    rotation_block_size: int = 2
    numeric_tolerance: float = 1e-6
    causal_mask: bool = True
    latency_budget_ms: float = 100.0
    memory_budget_bytes: int = 4 * 1024 * 1024
    row_chunk_size: int = 16

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "RopeConfig":
        if not isinstance(value, Mapping):
            raise RopeError(
                "E_INVALID_CONFIG", "配置必须是 YAML/JSON 对象", {"type": type(value).__name__}
            )
        allowed = {field.name for field in fields(cls)}
        unknown = sorted(set(value) - allowed)
        if unknown:
            raise RopeError(
                "E_INVALID_CONFIG", "配置包含未知字段", {"unknown_fields": unknown}
            )
        try:
            cfg = cls(**dict(value))
        except (TypeError, ValueError) as exc:
            raise RopeError(
                "E_INVALID_CONFIG", f"配置类型错误: {exc}", {"value": dict(value)}
            ) from exc
        cfg.validate()
        return cfg

    @classmethod
    def from_yaml(cls, path: str | Path) -> "RopeConfig":
        target = Path(path)
        if not target.exists():
            raise RopeError("E_NOT_FOUND", "配置文件不存在", {"path": str(target)})
        try:
            with target.open("r", encoding="utf-8") as handle:
                value = yaml.safe_load(handle)
        except yaml.YAMLError as exc:
            raise RopeError(
                "E_INVALID_CONFIG", f"YAML 解析失败: {exc}", {"path": str(target)}
            ) from exc
        if value is None:
            value = {}
        cfg = cls.from_mapping(value)
        return cfg

    def validate(self) -> None:
        if isinstance(self.base_frequency, bool) or not isinstance(
            self.base_frequency, (int, float)
        ):
            self._invalid("base_frequency", "基础频率必须是数值")
        if not float(self.base_frequency) > 1.0:
            self._invalid("base_frequency", "基础频率必须大于 1")
        if self.scaling_mode not in SCALING_MODES:
            self._invalid(
                "scaling_mode",
                f"缩放方式必须是 {', '.join(SCALING_MODES)} 之一",
            )
        if isinstance(self.scaling_factor, bool) or not isinstance(
            self.scaling_factor, (int, float)
        ):
            self._invalid("scaling_factor", "缩放系数必须是数值")
        if not float(self.scaling_factor) > 0.0:
            self._invalid("scaling_factor", "缩放系数必须大于 0")
        if isinstance(self.rotation_block_size, bool) or not isinstance(
            self.rotation_block_size, int
        ):
            self._invalid("rotation_block_size", "旋转角度分块大小必须是整数")
        if self.rotation_block_size < 2 or self.rotation_block_size % 2:
            self._invalid(
                "rotation_block_size", "旋转角度分块大小必须为大于等于 2 的偶数"
            )
        if isinstance(self.numeric_tolerance, bool) or not isinstance(
            self.numeric_tolerance, (int, float)
        ):
            self._invalid("numeric_tolerance", "数值容差必须是数值")
        if float(self.numeric_tolerance) < 0:
            self._invalid("numeric_tolerance", "数值容差不能为负")
        if not isinstance(self.causal_mask, bool):
            self._invalid("causal_mask", "因果掩码开关必须是布尔值")
        if isinstance(self.latency_budget_ms, bool) or not isinstance(
            self.latency_budget_ms, (int, float)
        ):
            self._invalid("latency_budget_ms", "延迟预算必须是数值")
        if float(self.latency_budget_ms) < 0:
            self._invalid("latency_budget_ms", "延迟预算不能为负")
        if isinstance(self.memory_budget_bytes, bool) or not isinstance(
            self.memory_budget_bytes, int
        ):
            self._invalid("memory_budget_bytes", "内存预算必须是整数")
        if self.memory_budget_bytes <= 0:
            self._invalid("memory_budget_bytes", "内存预算必须大于 0")
        if isinstance(self.row_chunk_size, bool) or not isinstance(
            self.row_chunk_size, int
        ):
            self._invalid("row_chunk_size", "行块大小必须是整数")
        if self.row_chunk_size <= 0:
            self._invalid("row_chunk_size", "行块大小必须大于 0")

    def _invalid(self, field: str, reason: str) -> None:
        raise RopeError(
            "E_INVALID_CONFIG",
            f"非法配置 {field}: {reason}",
            {"field": field, "reason": reason, "value": getattr(self, field)},
        )

    def to_mapping(self) -> dict[str, Any]:
        return asdict(self)

    @property
    def version(self) -> str:
        return sha256_bytes(canonical_json_bytes(self.to_mapping()))

    def with_patch(self, patch: Mapping[str, Any]) -> "RopeConfig":
        if not isinstance(patch, Mapping):
            raise RopeError("E_INVALID_CONFIG", "配置补丁必须是对象")
        allowed = {field.name for field in fields(self)}
        unknown = sorted(set(patch) - allowed)
        if unknown:
            raise RopeError(
                "E_INVALID_CONFIG", "配置补丁包含未知字段", {"unknown_fields": unknown}
            )
        cfg = replace(self, **dict(patch))
        cfg.validate()
        return cfg

    def changed_fields(self, other: "RopeConfig") -> list[str]:
        return sorted(
            field.name
            for field in fields(self)
            if getattr(self, field.name) != getattr(other, field.name)
        )

    def to_yaml_bytes(self) -> bytes:
        return yaml.safe_dump(
            self.to_mapping(), allow_unicode=True, sort_keys=False
        ).encode("utf-8")

    def write_yaml(self, path: str | Path) -> None:
        atomic_write_bytes(path, self.to_yaml_bytes())


COMPUTE_STAGE_BY_CONFIG_FIELD = {
    "base_frequency": "frequency_generation",
    "scaling_mode": "frequency_generation",
    "scaling_factor": "frequency_generation",
    "rotation_block_size": "block_rotation",
    "causal_mask": "scoring_normalization",
    "numeric_tolerance": "scoring_normalization",
}

STAGE_ORDER = ("frequency_generation", "block_rotation", "scoring_normalization")


def earliest_affected_stage(changed_fields: list[str]) -> str | None:
    indexes = [
        STAGE_ORDER.index(COMPUTE_STAGE_BY_CONFIG_FIELD[field])
        for field in changed_fields
        if field in COMPUTE_STAGE_BY_CONFIG_FIELD
    ]
    return STAGE_ORDER[min(indexes)] if indexes else None
