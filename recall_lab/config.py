from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import yaml

from .errors import BusinessError, ErrorCode
from .util import canonical_json_bytes, digest_bytes


SOURCE_ORDER = ("lexical", "category", "vector")
STRATEGIES = ("weighted_sum", "rrf")
DEDUP_RULES = ("merge_by_item_id",)


@dataclass(frozen=True)
class SourceConfig:
    name: str
    enabled: bool
    candidate_depth: int
    quota: int


@dataclass(frozen=True)
class FusionConfig:
    strategy: str
    weights: dict[str, float]
    rrf_k: int
    dedup_rule: str
    top_k: int


@dataclass(frozen=True)
class BudgetConfig:
    global_candidate_evaluations: int
    minimum_depth: int
    priority: tuple[str, ...]


@dataclass(frozen=True)
class LabConfig:
    schema_version: int
    sources: dict[str, SourceConfig]
    fusion: FusionConfig
    budget: BudgetConfig
    fingerprint: str
    canonical: dict[str, Any]
    source_path: str | None = None

    @property
    def recall_payload(self) -> dict[str, Any]:
        return {
            "format": "recall-config-v1",
            "sources": {
                name: {
                    "enabled": source.enabled,
                    "candidate_depth": source.candidate_depth,
                    "quota": source.quota,
                }
                for name, source in self.sources.items()
            },
        }

    @property
    def fusion_payload(self) -> dict[str, Any]:
        return {
            "format": "fusion-config-v1",
            "strategy": self.fusion.strategy,
            "weights": dict(self.fusion.weights),
            "rrf_k": self.fusion.rrf_k,
            "dedup_rule": self.fusion.dedup_rule,
            "top_k": self.fusion.top_k,
        }

    @property
    def budget_payload(self) -> dict[str, Any]:
        return {
            "format": "budget-config-v1",
            "global_candidate_evaluations": self.budget.global_candidate_evaluations,
            "minimum_depth": self.budget.minimum_depth,
            "priority": list(self.budget.priority),
        }


def _require_mapping(value: Any, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise BusinessError(ErrorCode.INVALID_INPUT, f"{name} must be an object")
    return value


def _positive_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise BusinessError(ErrorCode.INVALID_INPUT, f"{name} must be a positive integer", {"value": value})
    return value


def _nonnegative_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise BusinessError(
            ErrorCode.INVALID_INPUT,
            f"{name} must be a non-negative integer",
            {"value": value},
        )
    return value


def parse_config(raw: Any, *, source_path: str | None = None) -> LabConfig:
    root = _require_mapping(raw, "config")
    schema_version = _positive_int(root.get("schema_version", 1), "schema_version")
    if schema_version != 1:
        raise BusinessError(ErrorCode.INVALID_INPUT, "unsupported config schema_version", {"value": schema_version})

    recall = _require_mapping(root.get("recall"), "recall")
    sources: dict[str, SourceConfig] = {}
    for name in SOURCE_ORDER:
        entry = _require_mapping(recall.get(name), f"recall.{name}")
        enabled = entry.get("enabled", True)
        if not isinstance(enabled, bool):
            raise BusinessError(ErrorCode.INVALID_INPUT, f"recall.{name}.enabled must be boolean")
        sources[name] = SourceConfig(
            name=name,
            enabled=enabled,
            candidate_depth=_positive_int(entry.get("candidate_depth"), f"recall.{name}.candidate_depth"),
            quota=_positive_int(entry.get("quota"), f"recall.{name}.quota"),
        )
    extra_recall = sorted(set(recall) - set(SOURCE_ORDER))
    if extra_recall:
        raise BusinessError(ErrorCode.INVALID_INPUT, "unknown recall sources", {"sources": extra_recall})

    fusion_raw = _require_mapping(root.get("fusion"), "fusion")
    strategy = str(fusion_raw.get("strategy", "")).lower()
    if strategy not in STRATEGIES:
        raise BusinessError(
            ErrorCode.INVALID_INPUT,
            "fusion.strategy must be weighted_sum or rrf",
            {"value": strategy},
        )
    weights_raw = _require_mapping(fusion_raw.get("weights"), "fusion.weights")
    weights: dict[str, float] = {}
    for name in SOURCE_ORDER:
        value = weights_raw.get(name)
        if isinstance(value, bool) or not isinstance(value, (int, float)) or float(value) < 0:
            raise BusinessError(
                ErrorCode.INVALID_INPUT,
                f"fusion.weights.{name} must be a non-negative number",
                {"value": value},
            )
        weights[name] = float(value)
    if not any(weight > 0 for weight in weights.values()):
        raise BusinessError(ErrorCode.INVALID_INPUT, "at least one fusion weight must be positive")

    rrf_k = _positive_int(fusion_raw.get("rrf_k", 60), "fusion.rrf_k")
    dedup_rule = str(fusion_raw.get("dedup_rule", "merge_by_item_id"))
    if dedup_rule not in DEDUP_RULES:
        raise BusinessError(
            ErrorCode.INVALID_INPUT,
            "unsupported fusion.dedup_rule",
            {"value": dedup_rule, "allowed": list(DEDUP_RULES)},
        )
    top_k = _positive_int(fusion_raw.get("top_k"), "fusion.top_k")
    fusion = FusionConfig(strategy=strategy, weights=weights, rrf_k=rrf_k, dedup_rule=dedup_rule, top_k=top_k)

    budget_raw = _require_mapping(root.get("budget"), "budget")
    global_budget = _positive_int(
        budget_raw.get("global_candidate_evaluations"),
        "budget.global_candidate_evaluations",
    )
    minimum_depth = _positive_int(budget_raw.get("minimum_depth", 1), "budget.minimum_depth")
    priority_raw = budget_raw.get("priority", list(SOURCE_ORDER))
    if not isinstance(priority_raw, list) or sorted(priority_raw) != sorted(SOURCE_ORDER):
        raise BusinessError(
            ErrorCode.INVALID_INPUT,
            "budget.priority must contain each source exactly once",
            {"value": priority_raw},
        )
    priority = tuple(str(name) for name in priority_raw)
    budget = BudgetConfig(
        global_candidate_evaluations=global_budget,
        minimum_depth=minimum_depth,
        priority=priority,
    )

    canonical = {
        "schema_version": schema_version,
        "recall": {
            name: {
                "enabled": source.enabled,
                "candidate_depth": source.candidate_depth,
                "quota": source.quota,
            }
            for name, source in sources.items()
        },
        "fusion": {
            "strategy": fusion.strategy,
            "weights": fusion.weights,
            "rrf_k": fusion.rrf_k,
            "dedup_rule": fusion.dedup_rule,
            "top_k": fusion.top_k,
        },
        "budget": {
            "global_candidate_evaluations": budget.global_candidate_evaluations,
            "minimum_depth": budget.minimum_depth,
            "priority": list(budget.priority),
        },
    }
    fingerprint = digest_bytes(canonical_json_bytes(canonical))
    return LabConfig(
        schema_version=schema_version,
        sources=sources,
        fusion=fusion,
        budget=budget,
        fingerprint=fingerprint,
        canonical=canonical,
        source_path=source_path,
    )


def load_config(path: str | Path) -> LabConfig:
    config_path = Path(path)
    try:
        text = config_path.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise BusinessError(
            ErrorCode.INVALID_INPUT,
            "config file not found",
            {"path": str(config_path)},
        ) from exc
    try:
        raw = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise BusinessError(
            ErrorCode.INVALID_INPUT,
            "config YAML is invalid",
            {"path": str(config_path), "reason": str(exc)},
        ) from exc
    return parse_config(raw, source_path=str(config_path))
