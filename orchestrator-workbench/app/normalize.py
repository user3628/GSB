"""Normalize decoded records into the internal fixed-point representation.

Weights are stored internally as permille (1/1000) integers:

  binary   - already fixed-point permille, only bounds checks apply
  jsonl    - float in the 0..1 range, multiplied by 1000 and quantized with
             the caller-declared rounding rule
  yaml     - absent, degraded to 0

For every record we report the discarded fraction (in permille units) and the
running cumulative rounding error over the batch, so callers can audit the
precision loss deterministically.
"""
from __future__ import annotations

from decimal import ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_EVEN, ROUND_HALF_UP, ROUND_DOWN, Decimal

from app.models import (
    NormalizedResource,
    RawRecord,
    RejectedEntry,
    RoundingInfo,
    RoundingRule,
)

MAX_BUDGET = 0xFFFFFFFF
MIN_WEIGHT_PERMILLE = -1000
MAX_WEIGHT_PERMILLE = 1_000_000

_ROUNDING_MAP = {
    RoundingRule.HALF_UP: ROUND_HALF_UP,
    RoundingRule.HALF_EVEN: ROUND_HALF_EVEN,
    RoundingRule.FLOOR: ROUND_FLOOR,
    RoundingRule.CEIL: ROUND_CEILING,
    RoundingRule.TRUNCATE: ROUND_DOWN,
}


class NormalizationResult:
    def __init__(
        self,
        resources: list[NormalizedResource],
        rounding: list[RoundingInfo],
        rejected: list[RejectedEntry],
        cumulative_error: float,
    ):
        self.resources = resources
        self.rounding = rounding
        self.rejected = rejected
        self.cumulative_error = cumulative_error


def _quantize(value: Decimal, rule: RoundingRule) -> tuple[int, Decimal]:
    quantized = value.quantize(Decimal("1"), rounding=_ROUNDING_MAP[rule])
    discarded = value - quantized
    return int(quantized), discarded


def normalize_records(
    records: list[RawRecord], rule: RoundingRule
) -> NormalizationResult:
    resources: list[NormalizedResource] = []
    rounding_infos: list[RoundingInfo] = []
    rejected: list[RejectedEntry] = []
    cumulative = Decimal("0")
    seen: set[str] = set()

    for record in records:
        resource_id = record.resource_id
        if not resource_id:
            rejected.append(RejectedEntry(resource_id="<blank>", reason="empty resource_id"))
            continue
        if resource_id in seen:
            rejected.append(RejectedEntry(resource_id=resource_id, reason="duplicate resource_id"))
            continue
        seen.add(resource_id)

        try:
            weight_permille, input_weight, discarded = _normalize_weight(record, rule)
        except ValueError as exc:
            rejected.append(RejectedEntry(resource_id=resource_id, reason=str(exc)))
            continue

        try:
            budget = _normalize_budget(record)
        except ValueError as exc:
            rejected.append(RejectedEntry(resource_id=resource_id, reason=str(exc)))
            continue

        expected_version = record.expected_version if record.expected_version is not None else 1
        if not isinstance(expected_version, int) or expected_version < 1:
            rejected.append(
                RejectedEntry(resource_id=resource_id, reason="expected_version must be a positive integer")
            )
            continue

        cumulative += discarded
        rounding_infos.append(
            RoundingInfo(
                resource_id=resource_id,
                input_weight=input_weight,
                rounding_rule=rule,
                discarded_fraction=float(discarded) / 1000.0,
                rounded_permille=weight_permille,
                cumulative_error=float(cumulative) / 1000.0,
            )
        )
        resources.append(
            NormalizedResource(
                resource_id=resource_id,
                budget=budget,
                weight_permille=weight_permille,
                parents=list(record.parents),
                expected_version=expected_version,
            )
        )

    return NormalizationResult(resources, rounding_infos, rejected, float(cumulative) / 1000.0)


def _normalize_weight(record: RawRecord, rule: RoundingRule) -> tuple[int, float, Decimal]:
    if record.weight_permille is not None:
        weight = record.weight_permille
        if not (MIN_WEIGHT_PERMILLE <= weight <= MAX_WEIGHT_PERMILLE):
            raise ValueError(f"weight_permille {weight} outside [{MIN_WEIGHT_PERMILLE},{MAX_WEIGHT_PERMILLE}]")
        return weight, weight / 1000.0, Decimal("0")

    if record.weight_float is not None:
        weight = record.weight_float
        if weight != weight or weight in (float("inf"), float("-inf")):
            raise ValueError("weight must be finite")
        if not (0.0 <= weight <= 1000.0):
            raise ValueError(f"float weight {weight} outside [0,1000]")
        target = Decimal(str(weight)) * Decimal("1000")
        quantized, discarded = _quantize(target, rule)
        if not (MIN_WEIGHT_PERMILLE <= quantized <= MAX_WEIGHT_PERMILLE):
            raise ValueError(f"quantized weight {quantized} outside permille bounds")
        return quantized, weight, discarded

    return 0, 0.0, Decimal("0")


def _normalize_budget(record: RawRecord) -> int:
    budget = record.budget if record.budget is not None else 0
    if isinstance(budget, bool) or not isinstance(budget, int):
        raise ValueError("budget must be an integer")
    if budget < 0:
        raise ValueError(f"budget must be non-negative, got {budget}")
    if budget > MAX_BUDGET:
        raise ValueError(f"budget {budget} overflows uint32 (max {MAX_BUDGET})")
    return budget
