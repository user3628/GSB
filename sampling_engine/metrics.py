"""Float64 statistics with a local Decimal high-precision audit."""

from __future__ import annotations

import math
from decimal import Decimal, localcontext
from typing import Any, Iterable

import numpy as np


DECIMAL_PRECISION = 60


def _decimal_reference(
    post_logits: np.ndarray,
    candidate_indices: np.ndarray,
    token_ids: list[int],
) -> tuple[list[str], list[str]]:
    """Compute NLL and entropy references at 60 decimal digits.

    The source weights are the stored float64 post-processed logits. Decimal is
    used for log-sum-exp and aggregation, so the comparison isolates the
    numerical error introduced by the float64 normalization/summary path.
    """

    nll_values: list[str] = []
    entropy_values: list[str] = []
    with localcontext() as context:
        context.prec = DECIMAL_PRECISION
        for step in range(post_logits.shape[0]):
            valid = candidate_indices[step][candidate_indices[step] >= 0]
            if valid.size == 0:
                raise ValueError("candidate set is empty")
            weights = [Decimal.from_float(float(post_logits[step, int(index)])) for index in valid]
            maximum = max(weights)
            scaled_sum = sum((weight - maximum).exp() for weight in weights)
            log_z = maximum + scaled_sum.ln()
            token_id = token_ids[step]
            try:
                selected_position = int(np.flatnonzero(valid == token_id)[0])
            except IndexError as exc:
                raise ValueError("sampled token is not in the truncated candidate set") from exc
            selected_weight = weights[selected_position]
            nll = log_z - selected_weight
            entropy = log_z - sum(
                (weight - log_z).exp() * weight for weight in weights
            )
            nll_values.append(str(nll))
            entropy_values.append(str(entropy))
    return nll_values, entropy_values


def _digits(relative_error: float) -> int:
    if relative_error <= 0.0:
        return 15
    digits = int(math.floor(-math.log10(relative_error)))
    return max(0, min(15, digits))


def _compare(
    value: float, reference: Decimal, *, unit: str
) -> dict[str, Any]:
    ref_float = float(reference)
    absolute = abs(value - ref_float)
    scale = abs(ref_float)
    relative = absolute / scale if scale else absolute
    digits = _digits(relative)
    return {
        "value": value,
        "reference": str(reference),
        "unit": unit,
        "absolute_error": absolute,
        "relative_error": relative,
        "effective_digits": digits,
        "note": (
            "Digits are measured against a local Decimal(precision=60) "
            "log-sum-exp of the stored float64 post-processed logits; "
            "float64 typically matches 14-15 decimal digits on these samples."
        ),
    }


def build_precision_report(
    post_logits: np.ndarray,
    candidate_indices: np.ndarray,
    token_ids: list[int],
    *,
    float_nll: Iterable[float],
    float_entropy: Iterable[float],
) -> tuple[dict[str, Any], dict[str, Any]]:
    nll_reference_strings, entropy_reference_strings = _decimal_reference(
        post_logits, candidate_indices, token_ids
    )
    nll_float = [float(value) for value in float_nll]
    entropy_float = [float(value) for value in float_entropy]
    if len(nll_float) != len(nll_reference_strings):
        raise ValueError("float and reference metric lengths disagree")
    with localcontext() as context:
        context.prec = DECIMAL_PRECISION
        nll_references = [Decimal(value) for value in nll_reference_strings]
        entropy_references = [Decimal(value) for value in entropy_reference_strings]
        count = Decimal(len(nll_references))
        nll_mean_ref = sum(nll_references) / count
        entropy_mean_ref = sum(entropy_references) / count
        perplexity_ref = nll_mean_ref.exp()
    nll_mean_float = sum(nll_float) / len(nll_float)
    entropy_mean_float = sum(entropy_float) / len(entropy_float)
    try:
        perplexity_float = math.exp(nll_mean_float)
    except OverflowError:
        perplexity_float = float("inf")
    report = {
        "method": (
            "float64 engine metrics compared with Decimal(precision=60) "
            "log-sum-exp over the stored float64 post-processed logits"
        ),
        "nll": _compare(nll_mean_float, nll_mean_ref, unit="nats/token"),
        "perplexity": _compare(perplexity_float, perplexity_ref, unit="perplexity"),
        "entropy": _compare(entropy_mean_float, entropy_mean_ref, unit="nats/token"),
    }
    components = {
        "nll_float": nll_float,
        "nll_reference": nll_reference_strings,
        "entropy_float": entropy_float,
        "entropy_reference": entropy_reference_strings,
    }
    return report, components


def corpus_metrics(
    successful_summaries: Iterable[dict[str, Any]],
) -> dict[str, Any]:
    """Aggregate token-level NLL/entropy and audit the aggregate in Decimal."""

    summaries = list(successful_summaries)
    nll_float: list[float] = []
    entropy_float: list[float] = []
    nll_ref: list[Decimal] = []
    entropy_ref: list[Decimal] = []
    tokens = 0
    for summary in summaries:
        components = summary.get("precision_components", {})
        nll_float.extend(float(value) for value in components.get("nll_float", []))
        entropy_float.extend(
            float(value) for value in components.get("entropy_float", [])
        )
        nll_ref.extend(Decimal(value) for value in components.get("nll_reference", []))
        entropy_ref.extend(
            Decimal(value) for value in components.get("entropy_reference", [])
        )
        tokens += int(summary.get("metrics", {}).get("steps", 0))
    if not nll_float:
        return {
            "request_count": len(summaries),
            "token_count": 0,
            "mean_nll": None,
            "perplexity": None,
            "mean_entropy": None,
            "precision": None,
        }
    with localcontext() as context:
        context.prec = DECIMAL_PRECISION
        count = Decimal(len(nll_float))
        nll_mean_ref = sum(nll_ref) / count
        entropy_mean_ref = sum(entropy_ref) / count
        perplexity_ref = nll_mean_ref.exp()
    nll_mean_float = sum(nll_float) / len(nll_float)
    entropy_mean_float = sum(entropy_float) / len(entropy_float)
    try:
        perplexity_float = math.exp(nll_mean_float)
    except OverflowError:
        perplexity_float = float("inf")
    return {
        "request_count": len(summaries),
        "token_count": tokens,
        "mean_nll": nll_mean_float,
        "perplexity": perplexity_float,
        "mean_entropy": entropy_mean_float,
        "precision": {
            "nll": _compare(nll_mean_float, nll_mean_ref, unit="nats/token"),
            "perplexity": _compare(perplexity_float, perplexity_ref, unit="perplexity"),
            "entropy": _compare(entropy_mean_float, entropy_mean_ref, unit="nats/token"),
            "method": (
                "corpus aggregate first sums token-level float64 metrics, then "
                "compares with the sum of Decimal(precision=60) references"
            ),
        },
    }
