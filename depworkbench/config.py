"""YAML configuration loading, validation and version fingerprints."""
from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from .errors import E_INVALID_CONFIG, WorkbenchError
from .hashing import canonical_fingerprint

ACTIONS = ("shift", "left", "right")
N_FEATURES = 6
MAX_DISTANCE_HARD_LIMIT = 1000
MAX_WORKER_THREADS = 64


@dataclass(frozen=True)
class ModelConfig:
    weights: dict[str, tuple[float, ...]]
    max_dependency_distance: int
    unique_root: bool

    def fingerprint(self) -> str:
        payload = {
            "weights": {k: list(v) for k, v in sorted(self.weights.items())},
            "max_dependency_distance": self.max_dependency_distance,
            "unique_root": self.unique_root,
        }
        return canonical_fingerprint(
            json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")
        )


@dataclass(frozen=True)
class ParallelConfig:
    worker_threads: int


@dataclass(frozen=True)
class BudgetConfig:
    per_sentence_max_tokens: int
    global_total_tokens: int
    worker_token_quota: int

    def fingerprint(self) -> str:
        payload = {
            "per_sentence_max_tokens": self.per_sentence_max_tokens,
            "global_total_tokens": self.global_total_tokens,
            "worker_token_quota": self.worker_token_quota,
        }
        return canonical_fingerprint(json.dumps(payload, sort_keys=True).encode("utf-8"))


@dataclass(frozen=True)
class WorkbenchConfig:
    model: ModelConfig
    parallel: ParallelConfig
    budget: BudgetConfig
    raw: dict[str, Any]

    @property
    def model_fingerprint(self) -> str:
        return self.model.fingerprint()

    @property
    def budget_fingerprint(self) -> str:
        return self.budget.fingerprint()

    @property
    def config_fingerprint(self) -> str:
        payload = {
            "model": self.model_fingerprint,
            "budget": self.budget_fingerprint,
            "worker_threads": self.parallel.worker_threads,
        }
        return canonical_fingerprint(json.dumps(payload, sort_keys=True).encode("utf-8"))


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _require_int(value: Any, field: str, *, minimum: int | None = None,
                 maximum: int | None = None) -> int:
    if not _is_int(value):
        raise WorkbenchError(
            E_INVALID_CONFIG, f"field '{field}' must be an integer",
            {"field": field, "value": value},
        )
    if minimum is not None and value < minimum:
        raise WorkbenchError(
            E_INVALID_CONFIG,
            f"field '{field}'={value} below minimum {minimum}",
            {"field": field, "value": value, "minimum": minimum},
        )
    if maximum is not None and value > maximum:
        raise WorkbenchError(
            E_INVALID_CONFIG,
            f"field '{field}'={value} above maximum {maximum}",
            {"field": field, "value": value, "maximum": maximum},
        )
    return value


def _parse_weights(raw: Any) -> dict[str, tuple[float, ...]]:
    if not isinstance(raw, dict):
        raise WorkbenchError(
            E_INVALID_CONFIG, "model.weights must be a mapping of action vectors",
            {"field": "model.weights"},
        )
    missing = [a for a in ACTIONS if a not in raw]
    if missing:
        raise WorkbenchError(
            E_INVALID_CONFIG, f"missing action weight vectors: {','.join(missing)}",
            {"field": "model.weights", "missing": missing},
        )
    extra = [k for k in raw if k not in ACTIONS]
    if extra:
        raise WorkbenchError(
            E_INVALID_CONFIG, f"unknown action weight vectors: {','.join(extra)}",
            {"field": "model.weights", "extra": extra},
        )
    weights: dict[str, tuple[float, ...]] = {}
    for action in ACTIONS:
        vector = raw[action]
        if not isinstance(vector, list) or len(vector) != N_FEATURES:
            raise WorkbenchError(
                E_INVALID_CONFIG,
                f"weight vector '{action}' must be a list of {N_FEATURES} numbers",
                {"field": f"model.weights.{action}", "length": (
                    len(vector) if isinstance(vector, list) else None)},
            )
        parsed: list[float] = []
        for index, item in enumerate(vector):
            if isinstance(item, bool) or not isinstance(item, (int, float)):
                raise WorkbenchError(
                    E_INVALID_CONFIG,
                    f"weight '{action}[{index}]' is not numeric",
                    {"field": f"model.weights.{action}[{index}]", "value": item},
                )
            value = float(item)
            if not math.isfinite(value):
                raise WorkbenchError(
                    E_INVALID_CONFIG,
                    f"weight '{action}[{index}]' must be finite (got {value})",
                    {"field": f"model.weights.{action}[{index}]", "value": value},
                )
            parsed.append(value)
        weights[action] = tuple(parsed)
    return weights


def parse_config(raw: dict[str, Any]) -> WorkbenchConfig:
    if not isinstance(raw, dict):
        raise WorkbenchError(E_INVALID_CONFIG, "top-level YAML document must be a mapping")
    model_raw = raw.get("model")
    if not isinstance(model_raw, dict):
        raise WorkbenchError(E_INVALID_CONFIG, "section 'model' is required",
                             {"field": "model"})
    weights = _parse_weights(model_raw.get("weights"))
    distance = _require_int(
        model_raw.get("max_dependency_distance"),
        "model.max_dependency_distance",
        minimum=1, maximum=MAX_DISTANCE_HARD_LIMIT,
    )
    unique_root = model_raw.get("unique_root", True)
    if not isinstance(unique_root, bool):
        raise WorkbenchError(
            E_INVALID_CONFIG, "model.unique_root must be boolean",
            {"field": "model.unique_root", "value": unique_root},
        )

    parallel_raw = raw.get("parallel", {})
    if not isinstance(parallel_raw, dict):
        raise WorkbenchError(E_INVALID_CONFIG, "section 'parallel' must be a mapping",
                             {"field": "parallel"})
    worker_threads = _require_int(
        parallel_raw.get("worker_threads", 1),
        "parallel.worker_threads", minimum=1, maximum=MAX_WORKER_THREADS,
    )

    budget_raw = raw.get("budget")
    if not isinstance(budget_raw, dict):
        raise WorkbenchError(E_INVALID_CONFIG, "section 'budget' is required",
                             {"field": "budget"})
    per_sentence = _require_int(
        budget_raw.get("per_sentence_max_tokens"),
        "budget.per_sentence_max_tokens", minimum=1,
    )
    global_total = _require_int(
        budget_raw.get("global_total_tokens"),
        "budget.global_total_tokens", minimum=1,
    )
    worker_quota = _require_int(
        budget_raw.get("worker_token_quota"),
        "budget.worker_token_quota", minimum=1,
    )
    if worker_quota > global_total:
        raise WorkbenchError(
            E_INVALID_CONFIG,
            "budget.worker_token_quota cannot exceed global_total_tokens",
            {"field": "budget.worker_token_quota",
             "worker_token_quota": worker_quota,
             "global_total_tokens": global_total},
        )

    return WorkbenchConfig(
        model=ModelConfig(weights=weights, max_dependency_distance=distance,
                          unique_root=unique_root),
        parallel=ParallelConfig(worker_threads=worker_threads),
        budget=BudgetConfig(per_sentence_max_tokens=per_sentence,
                            global_total_tokens=global_total,
                            worker_token_quota=worker_quota),
        raw=raw,
    )


def load_config(path: str | Path) -> WorkbenchConfig:
    path = Path(path)
    if not path.exists():
        raise WorkbenchError(E_INVALID_CONFIG, f"config file not found: {path}",
                             {"path": str(path)})
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise WorkbenchError(E_INVALID_CONFIG, f"YAML parse error: {exc}",
                             {"path": str(path)}) from exc
    return parse_config(raw)
