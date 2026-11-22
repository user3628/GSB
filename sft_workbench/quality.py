"""Multi-factor quality scoring in the natural-log domain.

Factors per sample: length penalty, instruction/output vocabulary overlap,
template diversity and a source prior. The score is
``exp(sum(w_i * ln f_i) / sum(w_i))`` with configurable weights. When any
factor is exactly zero the score falls back to a configurable floor and the
fallback reason is recorded. Scores are quantized to 6 decimals so that
identical inputs and configs produce byte-identical outputs.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any

import numpy as np

from .models import InternalSample
from .textnorm import normalize_text, word_tokens
from .util import quantize

DEFAULT_WEIGHTS = {"length": 0.25, "overlap": 0.25, "template": 0.2, "source": 0.3}


@dataclass
class ScoringConfig:
    weights: dict[str, float] = field(default_factory=lambda: dict(DEFAULT_WEIGHTS))
    source_priors: dict[str, float] = field(default_factory=dict)
    default_source_prior: float = 0.8
    ideal_output_tokens: int = 120
    floor: float = 0.001

    def to_dict(self) -> dict[str, Any]:
        return {
            "weights": dict(self.weights),
            "source_priors": dict(self.source_priors),
            "default_source_prior": self.default_source_prior,
            "ideal_output_tokens": self.ideal_output_tokens,
            "floor": self.floor,
        }


def template_key(instruction: str) -> str:
    """Template identity: first 4 tokens of the normalized instruction."""
    tokens = word_tokens(normalize_text(instruction))
    return " ".join(tokens[:4])


def length_factor(output: str, ideal_tokens: int) -> float:
    count = len(word_tokens(normalize_text(output)))
    return 1.0 / (1.0 + abs(math.log((count + 1.0) / (ideal_tokens + 1.0))))


def overlap_factor(instruction: str, output: str) -> float:
    instr_words = set(word_tokens(normalize_text(instruction)))
    out_words = set(word_tokens(normalize_text(output)))
    return (1.0 + len(instr_words & out_words)) / (1.0 + len(instr_words))


def score_samples(
    kept: list[InternalSample],
    config: ScoringConfig,
    should_stop=None,
) -> list[dict[str, Any]]:
    template_counts: dict[str, int] = {}
    for sample in kept:
        key = template_key(sample.instruction)
        template_counts[key] = template_counts.get(key, 0) + 1

    factor_names = ("length", "overlap", "template", "source")
    weights = np.array(
        [float(config.weights.get(name, 0.0)) for name in factor_names], dtype=np.float64
    )
    weight_sum = float(weights.sum())
    records: list[dict[str, Any]] = []
    for sample in kept:
        factors = {
            "length": length_factor(sample.output, config.ideal_output_tokens),
            "overlap": overlap_factor(sample.instruction, sample.output),
            "template": 1.0 / template_counts[template_key(sample.instruction)],
            "source": float(config.source_priors.get(sample.source, config.default_source_prior)),
        }
        fallback_reason = ""
        if weight_sum <= 0.0:
            score = config.floor
            fallback_reason = "all factor weights are zero"
        else:
            zero_factors = [name for name in factor_names if factors[name] == 0.0]
            if zero_factors:
                score = config.floor
                fallback_reason = "zero factor(s): " + ",".join(zero_factors)
            else:
                logs = np.log(np.array([factors[name] for name in factor_names], dtype=np.float64))
                score = float(np.exp(np.dot(weights, logs) / weight_sum))
        records.append(
            {
                "sample_id": sample.sample_id,
                "score": quantize(score),
                "factors": {name: quantize(factors[name]) for name in factor_names},
                "fallback_reason": fallback_reason,
            }
        )
        if should_stop is not None and should_stop():
            from .pipeline import GracefulStop

            raise GracefulStop("stop requested during score stage")
    return records
