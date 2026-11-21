"""Verification-time rules: clock offset injection, sub-second truncation and
millisecond-integer validity margins.
"""

from __future__ import annotations

from dataclasses import dataclass

from .errors import Codes, WorkbenchError

# Cert validity fields are DER GeneralizedTime/UTCTime, whole seconds only.
CERT_BOUNDARY_GRANULARITY_MS = 1000
# Client verification instants are tracked at millisecond granularity.
VERIFICATION_GRANULARITY_MS = 1


@dataclass(frozen=True)
class VerificationInstant:
    nominal_wall_ms: int
    clock_offset_ms: int
    truncation_granularity_ms: int
    effective_verify_ms: int
    rule: str


def build_verification_instant(
    nominal_wall_ms: int,
    clock_offset_ms: int,
    truncation_granularity_ms: int = VERIFICATION_GRANULARITY_MS,
) -> VerificationInstant:
    """Inject the system clock offset, then apply sub-second truncation.

    Injection rule (explicit, documented):
        effective = TRUNCATE(nominal_wall_ms + clock_offset_ms)
    where TRUNCATE rounds toward negative infinity (floor) to a multiple of
    ``truncation_granularity_ms``. The offset is added *before* truncation so
    a positive offset that crosses a truncation boundary is observable.
    """
    if truncation_granularity_ms <= 0:
        raise ValueError("truncation granularity must be a positive integer")
    shifted = int(nominal_wall_ms) + int(clock_offset_ms)
    truncated = (
        shifted // truncation_granularity_ms
    ) * truncation_granularity_ms
    return VerificationInstant(
        nominal_wall_ms=int(nominal_wall_ms),
        clock_offset_ms=int(clock_offset_ms),
        truncation_granularity_ms=int(truncation_granularity_ms),
        effective_verify_ms=truncated,
        rule=(
            "effective_verify_ms = floor((nominal_wall_ms + clock_offset_ms) "
            "/ truncation_granularity_ms) * truncation_granularity_ms; "
            "certificate DER boundaries are whole seconds"
        ),
    )


@dataclass(frozen=True)
class MarginResult:
    not_before_ms: int
    not_after_ms: int
    effective_verify_ms: int
    margin_before_ms: int
    margin_after_ms: int
    is_valid: bool
    status: str
    boundary_rule: str


def evaluate_validity(
    not_before_ms: int,
    not_after_ms: int,
    instant: VerificationInstant,
) -> MarginResult:
    """Compute integer millisecond margins against a verification instant.

    Boundary contract (deterministic, including zero margins):
      margin_before == 0 -> exactly at not_before, treated as valid.
      margin_after  == 0 -> exactly at not_after, treated as valid.
      margin_before  < 0 -> E_CERT_NOT_YET_VALID.
      margin_after   < 0 -> E_CERT_EXPIRED.
    Both intervals are closed: [not_before_ms, not_after_ms].
    """
    verify_ms = instant.effective_verify_ms
    margin_before = not_before_ms - verify_ms
    margin_after = not_after_ms - verify_ms
    if margin_before > 0:
        status = Codes.CERT_NOT_YET_VALID
        valid = False
    elif margin_after < 0:
        status = Codes.CERT_EXPIRED
        valid = False
    else:
        status = "VALID"
        valid = True
    return MarginResult(
        not_before_ms=not_before_ms,
        not_after_ms=not_after_ms,
        effective_verify_ms=verify_ms,
        margin_before_ms=margin_before,
        margin_after_ms=margin_after,
        is_valid=valid,
        status=status,
        boundary_rule=(
            "closed interval [not_before_ms, not_after_ms]; zero margin at "
            "either boundary is VALID; negative margin is invalid"
        ),
    )


def assert_valid_margin(result: MarginResult) -> None:
    if not result.is_valid:
        raise WorkbenchError(
            result.status,
            f"certificate validity check failed: {result.status}",
            {
                "margin_before_ms": result.margin_before_ms,
                "margin_after_ms": result.margin_after_ms,
                "effective_verify_ms": result.effective_verify_ms,
            },
        )
