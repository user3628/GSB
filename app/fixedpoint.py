from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, localcontext
from fractions import Fraction
from typing import Any

from app.models import HealthStatus


SCALE = 1_000_000
ROUNDING_MODE = "round_half_up_to_q6"

CONNECT_RATIO_NUM = 853
CONNECT_RATIO_DEN = 997
TOTAL_WAIT_RATIO_NUM = 1099
TOTAL_WAIT_RATIO_DEN = 1000

HEALTH_FACTORS: dict[HealthStatus, tuple[int, int]] = {
    HealthStatus.healthy: (1, 1),
    HealthStatus.degraded: (5, 4),
    HealthStatus.unhealthy: (7, 4),
}


def round_half_up(value: Fraction) -> int:
    """Round a non-negative rational to the nearest integer, halves upward."""
    if value < 0:
        raise ValueError("fixed-point budgets must be non-negative")
    return (value.numerator * 2 + value.denominator) // (2 * value.denominator)


def format_q6(value: int) -> str:
    sign = "-" if value < 0 else ""
    absolute = abs(value)
    whole, fraction = divmod(absolute, SCALE)
    return f"{sign}{whole}.{fraction:06d}"


def fraction_decimal(value: Fraction, places: int = 12) -> str:
    with localcontext() as context:
        context.prec = max(40, places + 20)
        quantizer = Decimal(1).scaleb(-places)
        dec = (Decimal(value.numerator) / Decimal(value.denominator)).quantize(quantizer)
    return format(dec, "f")


def fraction_payload(value: Fraction) -> dict[str, Any]:
    return {
        "ratio": f"{value.numerator}/{value.denominator}",
        "decimal": fraction_decimal(value),
    }


@dataclass(frozen=True)
class DerivationStep:
    name: str
    unit: str
    rounding_mode: str
    ideal: Fraction
    rounded_q6: int
    reason: str

    @property
    def error(self) -> Fraction:
        return Fraction(self.rounded_q6, 1) - self.ideal

    def payload(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "unit": self.unit,
            "rounding_mode": self.rounding_mode,
            "ideal_q6": fraction_payload(self.ideal),
            "rounded_q6": self.rounded_q6,
            "rounded_display": format_q6(self.rounded_q6),
            "rounding_error_q6": fraction_payload(self.error),
            "reason": self.reason,
        }


@dataclass(frozen=True)
class BudgetDerivation:
    timeout_q6: int
    connect_budget_q6: int
    total_wait_q6: int
    health_penalty_q6: int
    attempts: int
    steps: tuple[DerivationStep, ...]
    cumulative_error: Fraction
    cumulative_abs_rounding: Fraction

    def payload(self) -> dict[str, Any]:
        return {
            "contract": {
                "internal_format": "signed Q6 integer",
                "scale": SCALE,
                "display_unit": "millisecond-equivalent",
                "rounding": ROUNDING_MODE,
            },
            "attempts": self.attempts,
            "timeout_q6": self.timeout_q6,
            "connect_budget_q6": self.connect_budget_q6,
            "total_wait_q6": self.total_wait_q6,
            "health_penalty_q6": self.health_penalty_q6,
            "display": {
                "timeout_ms": format_q6(self.timeout_q6),
                "connect_budget_ms": format_q6(self.connect_budget_q6),
                "total_wait_ms": format_q6(self.total_wait_q6),
                "health_penalty_weight": format_q6(self.health_penalty_q6),
            },
            "steps": [step.payload() for step in self.steps],
            "cumulative_rounding_error_q6": fraction_payload(self.cumulative_error),
            "cumulative_abs_rounding_q6": fraction_payload(self.cumulative_abs_rounding),
        }


def derive_budget(timeout_ms: int, retries: int, health_status: HealthStatus) -> BudgetDerivation:
    """Derive all route budgets with a deterministic half-up fixed-point pipeline.

    Each actual stage consumes the previously rounded Q6 value. A parallel ideal
    rational chain is retained so per-stage and cumulative rounding error are explicit.
    """
    attempts = retries + 1
    factor_num, factor_den = HEALTH_FACTORS[health_status]

    ideal_timeout = Fraction(timeout_ms * SCALE, 1)
    actual_timeout = timeout_ms * SCALE

    ideal_connect = ideal_timeout * Fraction(CONNECT_RATIO_NUM, CONNECT_RATIO_DEN)
    actual_connect = round_half_up(
        Fraction(actual_timeout * CONNECT_RATIO_NUM, CONNECT_RATIO_DEN)
    )

    ideal_total = ideal_connect * attempts * Fraction(
        TOTAL_WAIT_RATIO_NUM, TOTAL_WAIT_RATIO_DEN
    )
    actual_total = round_half_up(
        Fraction(
            actual_connect * attempts * TOTAL_WAIT_RATIO_NUM,
            TOTAL_WAIT_RATIO_DEN,
        )
    )

    ideal_penalty = ideal_total * Fraction(factor_num, factor_den)
    actual_penalty = round_half_up(
        Fraction(actual_total * factor_num, factor_den)
    )

    steps = (
        DerivationStep(
            name="normalize_timeout",
            unit="ms",
            rounding_mode="exact_q6",
            ideal=ideal_timeout,
            rounded_q6=actual_timeout,
            reason="integer milliseconds multiplied by Q6 scale are exact",
        ),
        DerivationStep(
            name="connection_budget",
            unit="ms",
            rounding_mode=ROUNDING_MODE,
            ideal=ideal_connect,
            rounded_q6=actual_connect,
            reason="timeout * 853/997 connection ratio scaling",
        ),
        DerivationStep(
            name="total_wait_budget",
            unit="ms",
            rounding_mode=ROUNDING_MODE,
            ideal=ideal_total,
            rounded_q6=actual_total,
            reason="(retries + 1) attempts with 1099/1000 overhead ratio",
        ),
        DerivationStep(
            name="health_penalty_weight",
            unit="ms-equivalent",
            rounding_mode=ROUNDING_MODE,
            ideal=ideal_penalty,
            rounded_q6=actual_penalty,
            reason=f"health multiplier {factor_num}/{factor_den}",
        ),
    )

    cumulative = Fraction(actual_penalty, 1) - ideal_penalty
    cumulative_abs = sum((abs(step.error) for step in steps), Fraction(0, 1))
    return BudgetDerivation(
        timeout_q6=actual_timeout,
        connect_budget_q6=actual_connect,
        total_wait_q6=actual_total,
        health_penalty_q6=actual_penalty,
        attempts=attempts,
        steps=steps,
        cumulative_error=cumulative,
        cumulative_abs_rounding=cumulative_abs,
    )
