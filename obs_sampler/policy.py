"""Versioned sampling policy snapshot parsing and integrity checks."""

from __future__ import annotations

import copy
import hashlib
import json
import math
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping

from .errors import PolicyError

SUPPORTED_DEDUP_SCOPES = frozenset({"trace_per_kind", "trace_global"})


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def policy_digest(document: Mapping[str, Any]) -> str:
    body = copy.deepcopy(dict(document))
    body.pop("digest", None)
    return hashlib.sha256(canonical_json(body).encode("utf-8")).hexdigest()


def _as_number(value: Any, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise PolicyError(f"{name} must be a number")
    number = float(value)
    if not math.isfinite(number):
        raise PolicyError(f"{name} must be finite")
    return number


def _as_int(value: Any, name: str, *, minimum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise PolicyError(f"{name} must be an integer")
    if minimum is not None and value < minimum:
        raise PolicyError(f"{name} must be >= {minimum}")
    return value


def _rate(value: Any, name: str) -> float:
    number = _as_number(value, name)
    if number < 0.0 or number > 1.0:
        raise PolicyError(f"{name} must be in [0, 1]")
    return number


@dataclass(frozen=True)
class TailRule:
    latency_threshold_us: int
    base_rate: float
    error_multiplier: float

    def probability(self, *, error: bool) -> float:
        probability = self.base_rate * (self.error_multiplier if error else 1.0)
        return min(1.0, max(0.0, probability))


@dataclass(frozen=True)
class Policy:
    version: int
    generated_at: str
    seed: int
    dedup_window_ms: int
    dedup_scope: str
    min_samples: int
    error_bound: float
    bias_correction: bool
    strata: dict[str, dict[str, float]]
    tail: TailRule
    digest: str
    document: dict[str, Any]

    @classmethod
    def from_mapping(cls, document: Mapping[str, Any]) -> "Policy":
        raw = copy.deepcopy(dict(document))

        version = _as_int(raw.get("version"), "version", minimum=1)
        generated_at = raw.get("generated_at")
        if not isinstance(generated_at, str) or not generated_at.strip():
            raise PolicyError("generated_at must be a non-empty ISO-8601 timestamp")
        try:
            datetime.fromisoformat(generated_at.replace("Z", "+00:00"))
        except ValueError as exc:
            raise PolicyError("generated_at must be a valid ISO-8601 timestamp") from exc

        seed = _as_int(raw.get("seed"), "seed")
        dedup_window_ms = _as_int(raw.get("dedup_window_ms"), "dedup_window_ms", minimum=0)
        dedup_scope = raw.get("dedup_scope", "trace_per_kind")
        if dedup_scope not in SUPPORTED_DEDUP_SCOPES:
            raise PolicyError(f"dedup_scope must be one of {sorted(SUPPORTED_DEDUP_SCOPES)}")
        min_samples = _as_int(raw.get("min_samples"), "min_samples", minimum=1)
        error_bound = _rate(raw.get("error_bound"), "error_bound")
        bias_correction = raw.get("bias_correction")
        if not isinstance(bias_correction, bool):
            raise PolicyError("bias_correction must be a boolean")

        strata_raw = raw.get("strata")
        if not isinstance(strata_raw, Mapping) or not strata_raw:
            raise PolicyError("strata must be a non-empty service -> level -> rate object")
        strata: dict[str, dict[str, float]] = {}
        for service, levels in strata_raw.items():
            if not isinstance(service, str) or not service.strip() or not isinstance(levels, Mapping):
                raise PolicyError("each strata entry must map a service name to level rates")
            if not levels:
                raise PolicyError(f"strata[{service}] must not be empty")
            normalized_levels: dict[str, float] = {}
            for level, rate in levels.items():
                if not isinstance(level, str) or not level.strip():
                    raise PolicyError(f"strata[{service}] contains an invalid level")
                normalized_levels[level.upper()] = _rate(rate, f"strata[{service}][{level}]")
            strata[service] = normalized_levels

        tail_raw = raw.get("tail_sampling")
        if not isinstance(tail_raw, Mapping):
            raise PolicyError("tail_sampling must be an object")
        tail = TailRule(
            latency_threshold_us=_as_int(
                tail_raw.get("latency_threshold_us"),
                "tail_sampling.latency_threshold_us",
                minimum=0,
            ),
            base_rate=_rate(tail_raw.get("base_rate"), "tail_sampling.base_rate"),
            error_multiplier=_as_number(
                tail_raw.get("error_multiplier"), "tail_sampling.error_multiplier"
            ),
        )
        if tail.error_multiplier < 1.0:
            raise PolicyError("tail_sampling.error_multiplier must be >= 1")

        supplied_digest = raw.get("digest")
        if not isinstance(supplied_digest, str) or len(supplied_digest) != 64:
            raise PolicyError("digest must be a 64-character SHA-256 hex string")
        actual_digest = policy_digest(raw)
        if supplied_digest.lower() != actual_digest:
            raise PolicyError("digest does not match the canonical policy snapshot")

        return cls(
            version=version,
            generated_at=generated_at,
            seed=seed,
            dedup_window_ms=dedup_window_ms,
            dedup_scope=dedup_scope,
            min_samples=min_samples,
            error_bound=error_bound,
            bias_correction=bias_correction,
            strata=strata,
            tail=tail,
            digest=actual_digest,
            document=raw,
        )

    def head_rate(self, service: str, level: str) -> float:
        return self.strata.get(service, {}).get(level.upper(), 0.0)

    def has_stratum(self, service: str, level: str) -> bool:
        return service in self.strata and level.upper() in self.strata[service]

    def as_summary(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "generated_at": self.generated_at,
            "seed": self.seed,
            "dedup_window_ms": self.dedup_window_ms,
            "dedup_scope": self.dedup_scope,
            "min_samples": self.min_samples,
            "error_bound": self.error_bound,
            "bias_correction": self.bias_correction,
            "digest": self.digest,
            "strata": self.strata,
            "tail_sampling": {
                "latency_threshold_us": self.tail.latency_threshold_us,
                "base_rate": self.tail.base_rate,
                "error_multiplier": self.tail.error_multiplier,
            },
        }
