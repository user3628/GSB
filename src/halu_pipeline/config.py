from __future__ import annotations

import copy
import json
import numbers
from typing import Any

from .canonical import PipelineError, deep_merge, sha256_obj


DEFAULT_CONFIG: dict[str, Any] = {
    "normalization_version": "nfkc-whitespace-lower-1",
    "scoring_version": "log-geometric-rules-1",
    "sentence_support_threshold": 0.82,
    "hallucination_ratio_threshold": 0.40,
    "scoring_weights": {
        "term": 0.30,
        "ngram": 0.45,
        "hash": 0.15,
        "prior": 0.10,
    },
    "evidence_weights": {"__default__": 1.0},
    "ngram_size": 3,
    "hash_buckets": 64,
    "consistency_min": 0.70,
    "min_annotation_coverage": 0.60,
    "latency_budget_ms": 50.0,
    "cache_max_entries": 256,
    "parallel_workers": 1,
    "declared_strata": [],
}


STAGE1_CONFIG_FIELDS = ("normalization_version",)
STAGE2_CONFIG_FIELDS = (
    "scoring_version",
    "sentence_support_threshold",
    "hallucination_ratio_threshold",
    "scoring_weights",
    "evidence_weights",
    "ngram_size",
    "hash_buckets",
)
STAGE3_CONFIG_FIELDS = ("declared_strata",)
STAGE4_CONFIG_FIELDS = ("consistency_min", "min_annotation_coverage")


def _is_number(value: Any) -> bool:
    return isinstance(value, numbers.Real) and not isinstance(value, bool)


def _require_number(config: dict[str, Any], key: str, low: float, high: float) -> None:
    value = config.get(key)
    if not _is_number(value) or not (low <= float(value) <= high):
        raise PipelineError(
            "E_INVALID_CONFIG",
            f"配置项 {key} 必须是 [{low}, {high}] 内的数值。",
            {"field": key, "value": value, "min": low, "max": high},
        )


def _require_int(config: dict[str, Any], key: str, low: int, high: int) -> None:
    value = config.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or not (low <= value <= high):
        raise PipelineError(
            "E_INVALID_CONFIG",
            f"配置项 {key} 必须是 [{low}, {high}] 内的整数。",
            {"field": key, "value": value, "min": low, "max": high},
        )


class Config:
    """Validated immutable-by-convention configuration snapshot."""

    def __init__(self, values: dict[str, Any]) -> None:
        self.values = Config._normalize(values)
        self.validate(self.values)

    @classmethod
    def defaults(cls) -> "Config":
        return cls(copy.deepcopy(DEFAULT_CONFIG))

    @classmethod
    def with_overrides(cls, overrides: dict[str, Any] | None = None) -> "Config":
        return cls(deep_merge(DEFAULT_CONFIG, overrides or {}))

    @staticmethod
    def _normalize(values: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(values, dict):
            raise PipelineError("E_INVALID_CONFIG", "配置必须是 JSON 对象。")
        result = deep_merge(DEFAULT_CONFIG, values)

        weights = result.get("evidence_weights", {})
        if isinstance(weights, dict) and "__default__" not in weights:
            weights["__default__"] = 1.0

        strata = result.get("declared_strata", [])
        if isinstance(strata, list):
            pairs: dict[tuple[str, str], dict[str, str]] = {}
            for item in strata:
                if isinstance(item, dict):
                    source = item.get("source")
                    lang = item.get("lang")
                    if isinstance(source, str) and isinstance(lang, str):
                        pairs[(source, lang)] = {"source": source, "lang": lang}
            result["declared_strata"] = [pairs[key] for key in sorted(pairs)]
        return result

    @staticmethod
    def validate(values: dict[str, Any]) -> None:
        known = set(DEFAULT_CONFIG)
        unknown = sorted(set(values) - known)
        if unknown:
            raise PipelineError(
                "E_INVALID_CONFIG", "存在未知配置项。", {"unknown_fields": unknown}
            )

        _require_number(values, "sentence_support_threshold", 0.0, 1.0)
        _require_number(values, "hallucination_ratio_threshold", 0.0, 1.0)
        _require_number(values, "consistency_min", 0.0, 1.0)
        _require_number(values, "min_annotation_coverage", 0.0, 1.0)
        _require_number(values, "latency_budget_ms", 0.0, 60_000.0)
        _require_int(values, "ngram_size", 1, 8)
        _require_int(values, "hash_buckets", 8, 4096)
        _require_int(values, "cache_max_entries", 1, 100_000)
        _require_int(values, "parallel_workers", 1, 32)

        for key in ("normalization_version", "scoring_version"):
            if not isinstance(values.get(key), str) or not values[key]:
                raise PipelineError(
                    "E_INVALID_CONFIG", f"配置项 {key} 必须是非空字符串。", {"field": key}
                )

        scoring_weights = values.get("scoring_weights")
        required_weight_keys = {"term", "ngram", "hash", "prior"}
        if not isinstance(scoring_weights, dict) or set(scoring_weights) != required_weight_keys:
            raise PipelineError(
                "E_INVALID_CONFIG",
                "scoring_weights 必须且只能包含 term、ngram、hash、prior。",
                {"field": "scoring_weights", "value": scoring_weights},
            )
        for key, value in scoring_weights.items():
            if not _is_number(value) or not (0.0 <= float(value) <= 1.0):
                raise PipelineError(
                    "E_INVALID_CONFIG",
                    "scoring_weights 中每个权重必须在 [0, 1]。",
                    {"field": f"scoring_weights.{key}", "value": value},
                )
        if sum(float(value) for value in scoring_weights.values()) <= 0.0:
            raise PipelineError("E_INVALID_CONFIG", "scoring_weights 权重和必须大于 0。")

        evidence_weights = values.get("evidence_weights")
        if not isinstance(evidence_weights, dict) or not evidence_weights:
            raise PipelineError(
                "E_INVALID_CONFIG", "evidence_weights 必须是非空 JSON 对象。"
            )
        for key, value in evidence_weights.items():
            if not isinstance(key, str) or not key:
                raise PipelineError(
                    "E_INVALID_CONFIG", "evidence_weights 的键必须是非空字符串。"
                )
            if not _is_number(value) or not (0.0 <= float(value) <= 1.0):
                raise PipelineError(
                    "E_INVALID_CONFIG",
                    "evidence_weights 中每个权重必须在 [0, 1]。",
                    {"field": f"evidence_weights.{key}", "value": value},
                )

        strata = values.get("declared_strata")
        if not isinstance(strata, list):
            raise PipelineError("E_INVALID_CONFIG", "declared_strata 必须是数组。")
        for item in strata:
            if (
                not isinstance(item, dict)
                or set(item) != {"source", "lang"}
                or not isinstance(item.get("source"), str)
                or not item["source"]
                or not isinstance(item.get("lang"), str)
                or not item["lang"]
            ):
                raise PipelineError(
                    "E_INVALID_CONFIG",
                    "declared_strata 每项必须形如 {\"source\":\"...\",\"lang\":\"...\"}。",
                    {"item": item},
                )

    def to_dict(self) -> dict[str, Any]:
        return copy.deepcopy(self.values)

    @property
    def version(self) -> str:
        return sha256_obj(self.values)

    def stage_subset(self, stage: int) -> dict[str, Any]:
        fields = {
            1: STAGE1_CONFIG_FIELDS,
            2: STAGE2_CONFIG_FIELDS,
            3: STAGE3_CONFIG_FIELDS,
            4: STAGE4_CONFIG_FIELDS,
        }[stage]
        return {field: copy.deepcopy(self.values[field]) for field in fields}

    def stage_config_fingerprint(self, stage: int) -> str:
        return sha256_obj(self.stage_subset(stage))

    def changed_stage(self, previous: "Config | None") -> int:
        """Return the earliest stage whose relevant configuration changed."""
        if previous is None:
            return 1
        for stage in (1, 2, 3, 4):
            if self.stage_config_fingerprint(stage) != previous.stage_config_fingerprint(stage):
                return stage
        return 0

    @property
    def sentence_support_threshold(self) -> float:
        return float(self.values["sentence_support_threshold"])

    @property
    def hallucination_ratio_threshold(self) -> float:
        return float(self.values["hallucination_ratio_threshold"])

    @property
    def consistency_min(self) -> float:
        return float(self.values["consistency_min"])

    @property
    def min_annotation_coverage(self) -> float:
        return float(self.values["min_annotation_coverage"])

    @property
    def evidence_weights(self) -> dict[str, float]:
        return {key: float(value) for key, value in self.values["evidence_weights"].items()}

    def __repr__(self) -> str:
        return f"Config(version={self.version[:12]})"


def apply_assignments(config_values: dict[str, Any], assignments: list[str]) -> dict[str, Any]:
    """Apply dotted key=value assignments, parsing values as JSON when possible."""
    result = copy.deepcopy(config_values)
    for assignment in assignments:
        if "=" not in assignment:
            raise PipelineError(
                "E_INVALID_CONFIG", "配置编辑必须形如 key=value。", {"assignment": assignment}
            )
        dotted, raw_value = assignment.split("=", 1)
        parts = dotted.split(".")
        target: Any = result
        for part in parts[:-1]:
            if not isinstance(target, dict) or part not in target:
                raise PipelineError(
                    "E_INVALID_CONFIG", "未知配置路径。", {"path": dotted}
                )
            target = target[part]
        if not isinstance(target, dict):
            raise PipelineError("E_INVALID_CONFIG", "未知配置路径。", {"path": dotted})
        try:
            value = json.loads(raw_value)
        except json.JSONDecodeError:
            value = raw_value
        target[parts[-1]] = value
    Config.validate(Config._normalize(result))
    return result
