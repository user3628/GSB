"""Fixed-point signature strength scoring with a fully traceable numeric
contract.

Every intermediate value is a fixed-point integer (see
:mod:`tls_workbench.fixedpoint`). Each arithmetic step records its operands,
the exact rational ideal where rounding is needed, the chosen value and the
absolute rounding error in fixed-point units, so cumulative rounding error
is auditable and repeated scoring is bit-for-bit identical.
"""

from __future__ import annotations

from dataclasses import dataclass

from . import fixedpoint as fp
from .errors import Codes, WorkbenchError

# Base strength normalisation: bits / 256 -> unitless base.
RSA_BASE_DIVISOR = 256
EC_BASE_LEVEL = {
    "P-256": fp.from_int(8),       # ~128-bit security, 2048-RSA equivalent
    "P-384": fp.from_int(12),      # ~192-bit security, 3072-RSA equivalent
}

# Signature algorithm weight, applied as a multiplier on the base strength.
SIGNATURE_WEIGHTS = {
    "SHA256_WITH_RSA": fp.from_fraction(100, 100),
    "SHA384_WITH_RSA": fp.from_fraction(110, 100),
    "ECDSA_WITH_SHA256": fp.from_fraction(105, 100),
    "ECDSA_WITH_SHA384": fp.from_fraction(115, 100),
}

# Chain-level role weights, summing to exactly 1.0 in fixed point.
ROLE_WEIGHTS = {
    "leaf": fp.from_fraction(50, 100),
    "intermediate": fp.from_fraction(30, 100),
    "root": fp.from_fraction(20, 100),
}

DEFAULT_REL_TOL = fp.from_fraction(1, 1_000_000)  # 1e-6


@dataclass
class StepTrace:
    steps: list
    cumulative_rounding_error: int

    def record_exact(self, operation: str, operands: dict, result: int) -> None:
        self.steps.append(
            {
                "operation": operation,
                "operands": operands,
                "result": result,
                "rounding": "EXACT",
                "rounding_error": 0,
            }
        )

    def record_rounded(
        self, operation: str, operands: dict, ideal_numerator: int,
        ideal_denominator: int, result: int,
    ) -> None:
        ideal_fixed = fp.from_fraction(ideal_numerator, ideal_denominator)
        error = result - ideal_fixed
        self.cumulative_rounding_error += error
        self.steps.append(
            {
                "operation": operation,
                "operands": operands,
                "ideal_fixed_point": ideal_fixed,
                "result": result,
                "rounding": fp.ROUNDING,
                "rounding_error": error,
            }
        )


def _base_strength(info: dict, trace: StepTrace) -> int:
    algorithm = info["public_key_algorithm"]
    if algorithm == "RSA":
        bits = int(info["rsa_modulus_bits"])
        result = fp.from_fraction(bits, RSA_BASE_DIVISOR)
        trace.record_rounded(
            "rsa_base_strength",
            {"rsa_modulus_bits": bits, "divisor": RSA_BASE_DIVISOR},
            bits,
            RSA_BASE_DIVISOR,
            result,
        )
        return result
    if algorithm == "EC":
        curve = info["ec_curve"]
        if curve not in EC_BASE_LEVEL:
            raise WorkbenchError(
                Codes.CHAIN_INVALID, f"unsupported EC curve {curve}"
            )
        result = EC_BASE_LEVEL[curve]
        trace.record_exact(
            "ec_base_strength", {"ec_curve": curve}, result
        )
        return result
    raise WorkbenchError(
        Codes.CHAIN_INVALID, f"unsupported public key algorithm {algorithm}"
    )


def _margin_factor(margin_after_ms: int, trace: StepTrace) -> int:
    """Map remaining validity to a factor in [0.5, 1.2].

    factor = clamp(0.5 + margin_days / 3650, 0.5, 1.2), where margin_days is
    the integer millisecond margin divided by 86_400_000. Expired
    certificates (negative margin) fall through to the 0.5 floor rather than
    producing a negative factor.
    """
    floor = fp.from_fraction(5, 10)
    ceiling = fp.from_fraction(12, 10)
    margin_days = fp.from_fraction(max(margin_after_ms, 0), 86_400_000)
    trace.record_rounded(
        "margin_days",
        {"margin_after_ms": margin_after_ms, "ms_per_day": 86_400_000},
        max(margin_after_ms, 0),
        86_400_000,
        margin_days,
    )
    raw = fp.add(floor, fp.div(margin_days, fp.from_int(3650)))
    trace.record_rounded(
        "margin_factor_raw",
        {"floor": floor, "margin_days": margin_days, "horizon_days": 3650},
        floor + fp.from_fraction(max(margin_after_ms, 0), 86_400_000 * 3650),
        fp.SCALE,
        raw,
    )
    result = fp.clamp(raw, floor, ceiling)
    trace.record_exact(
        "margin_factor_clamp",
        {"raw": raw, "floor": floor, "ceiling": ceiling},
        result,
    )
    return result


def score_certificate(info: dict, margin_after_ms: int) -> dict:
    trace = StepTrace([], 0)
    base = _base_strength(info, trace)
    algorithm = info["signature_algorithm"]
    if algorithm not in SIGNATURE_WEIGHTS:
        raise WorkbenchError(
            Codes.CHAIN_INVALID, f"unsupported signature algorithm {algorithm}"
        )
    weight = SIGNATURE_WEIGHTS[algorithm]
    weighted = fp.mul(base, weight)
    trace.record_rounded(
        "algorithm_weighted_base",
        {"base": base, "signature_algorithm": algorithm, "weight": weight},
        base * weight,
        fp.SCALE,
        weighted,
    )
    factor = _margin_factor(margin_after_ms, trace)
    score = fp.mul(weighted, factor)
    trace.record_rounded(
        "certificate_score",
        {"weighted_base": weighted, "margin_factor": factor},
        weighted * factor,
        fp.SCALE,
        score,
    )
    return {
        "role": info.get("role"),
        "serial_number": info["serial_number"],
        "score_fixed": score,
        "score": fp.to_str(score),
        "base_strength_fixed": base,
        "algorithm_weight_fixed": weight,
        "margin_factor_fixed": factor,
        "cumulative_rounding_error": trace.cumulative_rounding_error,
        "steps": trace.steps,
    }


def score_chain(chain: dict, path) -> dict:
    margin_by_role = {entry["role"]: entry["margin_after_ms"] for entry in path.margins}
    certificate_scores: list[dict] = []
    trace = StepTrace([], 0)
    for role in path.ordered_roles:
        info = dict(chain["certificates"][role])
        info["role"] = role
        certificate_scores.append(
            score_certificate(info, margin_by_role[role])
        )
    total = 0
    weight_terms: list[int] = []
    for certificate_score in certificate_scores:
        role = certificate_score["role"]
        weight = ROLE_WEIGHTS[role]
        contribution = fp.mul(certificate_score["score_fixed"], weight)
        trace.record_rounded(
            "weighted_chain_contribution",
            {
                "role": role,
                "certificate_score": certificate_score["score_fixed"],
                "role_weight": weight,
            },
            certificate_score["score_fixed"] * weight,
            fp.SCALE,
            contribution,
        )
        weight_terms.append(contribution)
        total = fp.add(total, contribution)
        trace.record_exact(
            "chain_running_total",
            {"role": role, "running_total": total},
            total,
        )
    return {
        "chain_id": chain["chain_id"],
        "total_score_fixed": total,
        "total_score": fp.to_str(total),
        "scale": fp.SCALE,
        "rounding": fp.ROUNDING,
        "cumulative_rounding_error": trace.cumulative_rounding_error
        + sum(
            item["cumulative_rounding_error"] for item in certificate_scores
        ),
        "role_weights": {
            role: fp.to_str(weight) for role, weight in ROLE_WEIGHTS.items()
        },
        "certificate_scores": certificate_scores,
        "aggregation_steps": trace.steps,
    }


def compare_chains(
    left: dict, right: dict, rel_tol: int = DEFAULT_REL_TOL
) -> dict:
    """Compare two chain scores with a fixed-point relative tolerance.

    Within tolerance the scores are declared tied and broken by stable
    lexicographic ordering of the chain ids, which never flips.
    """
    left_value = left["total_score_fixed"]
    right_value = right["total_score_fixed"]
    tied = fp.within_relative_tolerance(left_value, right_value, rel_tol)
    if tied:
        stronger = left["chain_id"] if left["chain_id"] <= right["chain_id"] else right["chain_id"]
        relation = "TOLERANCE_TIE"
    elif left_value > right_value:
        stronger = left["chain_id"]
        relation = "LEFT_STRONGER"
    else:
        stronger = right["chain_id"]
        relation = "RIGHT_STRONGER"
    return {
        "left_chain_id": left["chain_id"],
        "right_chain_id": right["chain_id"],
        "left_score": fp.to_str(left_value),
        "right_score": fp.to_str(right_value),
        "relative_tolerance": fp.to_str(rel_tol),
        "absolute_difference_fixed": abs(left_value - right_value),
        "relation": relation,
        "tie_breaker": "lexicographic chain_id ascending" if tied else None,
        "stronger_chain_id": stronger,
    }
