"""Operator configuration and semantic version fingerprints."""

from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Mapping

from .digest import sha256_json
from .errors import fail
from .yaml_lite import load as load_yaml


SUPPORTED_STRATEGIES = {"stable_running_max", "logsumexp_block_merge"}


@dataclass(frozen=True)
class AttentionConfig:
    row_block_size: int = 64
    col_block_size: int = 64
    causal: bool = True
    online_strategy: str = "stable_running_max"
    score_clamp: float = 80.0
    extreme_threshold: float = 60.0
    atol: float = 1.0e-10
    rtol: float = 1.0e-9
    memory_budget_bytes: int | None = 64 * 1024 * 1024
    workspace_factor: float = 1.25
    verify_incremental: bool = True
    name: str = "flashblock"

    def __post_init__(self) -> None:
        if not isinstance(self.row_block_size, int) or self.row_block_size <= 0:
            fail("E_CONFIG", "row_block_size must be a positive integer")
        if not isinstance(self.col_block_size, int) or self.col_block_size <= 0:
            fail("E_CONFIG", "col_block_size must be a positive integer")
        if self.online_strategy not in SUPPORTED_STRATEGIES:
            fail(
                "E_CONFIG",
                f"Unsupported online_strategy {self.online_strategy!r}",
                supported=sorted(SUPPORTED_STRATEGIES),
            )
        if not (self.score_clamp > 0.0):
            fail("E_CONFIG", "score_clamp must be positive")
        if not (self.extreme_threshold >= 0.0):
            fail("E_CONFIG", "extreme_threshold must be non-negative")
        if self.atol < 0.0 or self.rtol < 0.0:
            fail("E_CONFIG", "Numerical tolerances must be non-negative")
        if self.memory_budget_bytes is not None and self.memory_budget_bytes <= 0:
            fail("E_CONFIG", "memory_budget_bytes must be positive or null")
        if not (self.workspace_factor >= 1.0):
            fail("E_CONFIG", "workspace_factor must be >= 1.0")

    @classmethod
    def from_mapping(cls, mapping: Mapping[str, Any]) -> "AttentionConfig":
        allowed = set(cls.__dataclass_fields__)  # type: ignore[attr-defined]
        unknown = sorted(set(mapping) - allowed)
        if unknown:
            fail("E_CONFIG", f"Unknown operator config keys: {unknown}")
        try:
            return cls(**dict(mapping))
        except TypeError as exc:
            fail("E_CONFIG", f"Invalid operator configuration: {exc}")

    @classmethod
    def from_yaml(cls, path: str | Path) -> "AttentionConfig":
        document = load_yaml(path)
        operator = document.get("operator", document)
        if not isinstance(operator, dict):
            fail("E_CONFIG", "YAML operator section must be a mapping")
        return cls.from_mapping(operator)

    def with_overrides(self, **changes: Any) -> "AttentionConfig":
        try:
            return replace(self, **changes)
        except TypeError as exc:
            fail("E_CONFIG", f"Invalid config override: {exc}")

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "row_block_size": self.row_block_size,
            "col_block_size": self.col_block_size,
            "causal": self.causal,
            "online_strategy": self.online_strategy,
            "score_clamp": self.score_clamp,
            "extreme_threshold": self.extreme_threshold,
            "atol": self.atol,
            "rtol": self.rtol,
            "memory_budget_bytes": self.memory_budget_bytes,
            "workspace_factor": self.workspace_factor,
            "verify_incremental": self.verify_incremental,
        }

    def semantic_dict(self) -> dict[str, Any]:
        return {
            "row_block_size": self.row_block_size,
            "col_block_size": self.col_block_size,
            "causal": self.causal,
            "online_strategy": self.online_strategy,
            "score_clamp": self.score_clamp,
            "extreme_threshold": self.extreme_threshold,
        }

    @property
    def semantic_fingerprint(self) -> str:
        return sha256_json(self.semantic_dict())

    @property
    def version_fingerprint(self) -> str:
        # Scheduling and verification knobs do not change attention semantics.
        return self.semantic_fingerprint

