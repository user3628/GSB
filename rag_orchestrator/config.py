"""YAML configuration loading and strict local validation."""

from __future__ import annotations

import copy
import math
from pathlib import Path
from typing import Any

import yaml

from .errors import E_CONFIG_INVALID, EngineError
from .util import sha256_json


def default_config() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "normalizer": {
            "max_subqueries": 8,
            "base_priority": 0,
            "rule_priority": 20,
            "clause_priority": 30,
            "synonym_priority": 40,
            "synonyms": {
                "并行": ["并发", "parallel"],
                "退款": ["退费", "refund"],
                "检查点": ["断点", "checkpoint"],
                "引用": ["引证", "citation"],
                "指纹": ["fingerprint"],
            },
            "decomposition_rules": [
                {
                    "id": "parallel-budget",
                    "triggers": ["并行", "parallel", "budget"],
                    "priority": 20,
                    "outputs": ["并行 检索 预算", "parallel scheduling degradation"],
                },
                {
                    "id": "fingerprint-invalidation",
                    "triggers": ["版本指纹", "fingerprint", "invalidation"],
                    "priority": 20,
                    "outputs": ["版本指纹 增量失效", "fingerprint invalidation 版本 失效"],
                },
                {
                    "id": "refund-policy",
                    "triggers": ["退款", "refund"],
                    "priority": 20,
                    "outputs": ["退款 时限", "refund timeline"],
                },
            ],
        },
        "retrieval": {
            "lexical_weight": 2.0,
            "title_weight": 1.8,
            "source_weight": 0.7,
            "length_weight": 0.035,
            "ideal_tokens": 34,
            "source_prior": {
                "official": 1.0,
                "faq": 0.6,
                "community": 0.25,
            },
        },
        "budget": {
            "global_retrieval_quota": 900,
            "per_channel_quota": 320,
            "max_concurrent_channels": 3,
            "latency_budget_ms": 5000.0,
            "cost_per_subquery": 20,
            "cost_per_doc_shard": 8,
            "cost_per_term": 2,
            "latency_estimate_ms_per_credit": 0.08,
        },
        "rerank": {
            "max_evidence": 4,
            "max_total_chars": 1200,
            "min_fragment_chars": 24,
            "ideal_fragment_chars": 150,
            "coverage_weight": 1.25,
            "length_fit_weight": 0.6,
            "mmr_similarity_threshold": 0.82,
        },
        "assembly": {
            "template_version": "grounded-local-v1",
            "answer_max_chars": 20000,
        },
    }


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = copy.deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = copy.deepcopy(value)
    return merged


def load_config(path: str | Path | None = None) -> dict[str, Any]:
    if path is None:
        value = default_config()
    else:
        config_path = Path(path)
        with config_path.open("r", encoding="utf-8") as handle:
            loaded = yaml.safe_load(handle)
        if not isinstance(loaded, dict):
            raise EngineError(E_CONFIG_INVALID, "config root must be a mapping")
        value = _deep_merge(default_config(), loaded)
    return validate_config(value)


def dump_config(config: dict[str, Any]) -> str:
    validated = validate_config(config)
    return yaml.safe_dump(validated, allow_unicode=True, sort_keys=False)


def _fail(message: str, details: dict[str, Any] | None = None) -> None:
    raise EngineError(E_CONFIG_INVALID, message, details=details)


def _number(
    value: Any,
    name: str,
    *,
    minimum: float | None = None,
    maximum: float | None = None,
    integer: bool = False,
) -> float | int:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        _fail(f"{name} must be numeric", {"value": value})
    if integer and not isinstance(value, int):
        _fail(f"{name} must be an integer", {"value": value})
    if not math.isfinite(float(value)):
        _fail(f"{name} must be finite", {"value": value})
    if minimum is not None and value < minimum:
        _fail(f"{name} must be >= {minimum}", {"value": value})
    if maximum is not None and value > maximum:
        _fail(f"{name} must be <= {maximum}", {"value": value})
    return value


def validate_config(config: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(config, dict):
        _fail("config must be a mapping")
    value = copy.deepcopy(config)
    if value.get("schema_version") != 1:
        _fail("schema_version must be 1", {"schema_version": value.get("schema_version")})

    normalizer = value.get("normalizer")
    if not isinstance(normalizer, dict):
        _fail("normalizer must be a mapping")
    normalizer["max_subqueries"] = int(
        _number(normalizer.get("max_subqueries"), "normalizer.max_subqueries", minimum=1, maximum=64, integer=True)
    )
    for key in ("base_priority", "rule_priority", "clause_priority", "synonym_priority"):
        normalizer[key] = int(
            _number(normalizer.get(key), f"normalizer.{key}", minimum=0, maximum=10000, integer=True)
        )
    synonyms = normalizer.get("synonyms")
    if not isinstance(synonyms, dict):
        _fail("normalizer.synonyms must be a mapping")
    clean_synonyms: dict[str, list[str]] = {}
    for key in sorted(synonyms):
        if not isinstance(key, str) or not key.strip():
            _fail("normalizer.synonyms keys must be non-empty strings")
        values = synonyms[key]
        if not isinstance(values, list) or not all(isinstance(item, str) and item.strip() for item in values):
            _fail("normalizer.synonyms values must be non-empty string lists", {"key": key})
        clean_synonyms[key] = list(values)
    normalizer["synonyms"] = clean_synonyms

    rules = normalizer.get("decomposition_rules")
    if not isinstance(rules, list):
        _fail("normalizer.decomposition_rules must be a list")
    clean_rules = []
    for index, rule in enumerate(rules):
        if not isinstance(rule, dict):
            _fail("decomposition rule must be a mapping", {"index": index})
        rule_id = rule.get("id")
        triggers = rule.get("triggers")
        outputs = rule.get("outputs")
        if not isinstance(rule_id, str) or not rule_id:
            _fail("decomposition rule id is required", {"index": index})
        if not isinstance(triggers, list) or not triggers or not all(isinstance(x, str) and x for x in triggers):
            _fail("decomposition triggers must be a non-empty string list", {"rule": rule_id})
        if not isinstance(outputs, list) or not outputs or not all(isinstance(x, str) and x for x in outputs):
            _fail("decomposition outputs must be a non-empty string list", {"rule": rule_id})
        priority = int(
            _number(rule.get("priority"), f"decomposition_rules[{index}].priority", minimum=0, maximum=10000, integer=True)
        )
        clean_rules.append(
            {
                "id": rule_id,
                "triggers": list(triggers),
                "outputs": list(outputs),
                "priority": priority,
            }
        )
    normalizer["decomposition_rules"] = clean_rules

    retrieval = value.get("retrieval")
    if not isinstance(retrieval, dict):
        _fail("retrieval must be a mapping")
    for key in ("lexical_weight", "title_weight", "source_weight", "length_weight", "ideal_tokens"):
        retrieval[key] = float(_number(retrieval.get(key), f"retrieval.{key}"))
    if retrieval["ideal_tokens"] <= 0:
        _fail("retrieval.ideal_tokens must be > 0")
    source_prior = retrieval.get("source_prior")
    if not isinstance(source_prior, dict):
        _fail("retrieval.source_prior must be a mapping")
    clean_prior: dict[str, float] = {}
    for key in sorted(source_prior):
        clean_prior[str(key)] = float(
            _number(source_prior[key], f"retrieval.source_prior.{key}", minimum=0)
        )
    retrieval["source_prior"] = clean_prior

    budget = value.get("budget")
    if not isinstance(budget, dict):
        _fail("budget must be a mapping")
    budget["global_retrieval_quota"] = int(
        _number(budget.get("global_retrieval_quota"), "budget.global_retrieval_quota", minimum=1, maximum=1_000_000_000, integer=True)
    )
    budget["per_channel_quota"] = int(
        _number(budget.get("per_channel_quota"), "budget.per_channel_quota", minimum=1, maximum=1_000_000_000, integer=True)
    )
    budget["max_concurrent_channels"] = int(
        _number(budget.get("max_concurrent_channels"), "budget.max_concurrent_channels", minimum=1, maximum=64, integer=True)
    )
    budget["latency_budget_ms"] = float(
        _number(budget.get("latency_budget_ms"), "budget.latency_budget_ms", minimum=0.0, maximum=3_600_000.0)
    )
    for key in ("cost_per_subquery", "cost_per_doc_shard", "cost_per_term"):
        budget[key] = int(
            _number(budget.get(key), f"budget.{key}", minimum=0, maximum=1_000_000, integer=True)
        )
    budget["latency_estimate_ms_per_credit"] = float(
        _number(budget.get("latency_estimate_ms_per_credit"), "budget.latency_estimate_ms_per_credit", minimum=0.0)
    )

    rerank = value.get("rerank")
    if not isinstance(rerank, dict):
        _fail("rerank must be a mapping")
    rerank["max_evidence"] = int(
        _number(rerank.get("max_evidence"), "rerank.max_evidence", minimum=0, maximum=50, integer=True)
    )
    rerank["max_total_chars"] = int(
        _number(rerank.get("max_total_chars"), "rerank.max_total_chars", minimum=1, maximum=1_000_000, integer=True)
    )
    rerank["min_fragment_chars"] = int(
        _number(rerank.get("min_fragment_chars"), "rerank.min_fragment_chars", minimum=0, maximum=1_000_000, integer=True)
    )
    rerank["ideal_fragment_chars"] = int(
        _number(rerank.get("ideal_fragment_chars"), "rerank.ideal_fragment_chars", minimum=1, maximum=1_000_000, integer=True)
    )
    for key in ("coverage_weight", "length_fit_weight"):
        rerank[key] = float(_number(rerank.get(key), f"rerank.{key}"))
    rerank["mmr_similarity_threshold"] = float(
        _number(rerank.get("mmr_similarity_threshold"), "rerank.mmr_similarity_threshold", minimum=0.0, maximum=1.0)
    )

    assembly = value.get("assembly")
    if not isinstance(assembly, dict):
        _fail("assembly must be a mapping")
    if not isinstance(assembly.get("template_version"), str) or not assembly["template_version"]:
        _fail("assembly.template_version is required")
    assembly["answer_max_chars"] = int(
        _number(assembly.get("answer_max_chars"), "assembly.answer_max_chars", minimum=1, maximum=1_000_000, integer=True)
    )
    return value


def config_section_hashes(config: dict[str, Any]) -> dict[str, str]:
    normalizer = config["normalizer"]
    return {
        "config": sha256_json(config),
        "synonyms": sha256_json(normalizer["synonyms"]),
        "decomposition": sha256_json(
            {
                "max_subqueries": normalizer["max_subqueries"],
                "base_priority": normalizer["base_priority"],
                "rule_priority": normalizer["rule_priority"],
                "clause_priority": normalizer["clause_priority"],
                "synonym_priority": normalizer["synonym_priority"],
                "decomposition_rules": normalizer["decomposition_rules"],
            }
        ),
        "retrieval": sha256_json(config["retrieval"]),
        "budget": sha256_json(config["budget"]),
        "rerank": sha256_json(
            {
                "ideal_fragment_chars": config["rerank"]["ideal_fragment_chars"],
                "coverage_weight": config["rerank"]["coverage_weight"],
                "length_fit_weight": config["rerank"]["length_fit_weight"],
                "mmr_similarity_threshold": config["rerank"]["mmr_similarity_threshold"],
            }
        ),
        "evidence": sha256_json(
            {
                "max_evidence": config["rerank"]["max_evidence"],
                "max_total_chars": config["rerank"]["max_total_chars"],
                "min_fragment_chars": config["rerank"]["min_fragment_chars"],
            }
        ),
        "assembly": sha256_json(config["assembly"]),
    }
