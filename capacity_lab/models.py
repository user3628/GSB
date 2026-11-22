"""Plan normalisation and validation."""

from __future__ import annotations

import copy
import hashlib
import ipaddress
import json
import urllib.parse
from fractions import Fraction
from typing import Any

from .errors import PlanInvalid
from .fixedpoint import (
    SUPPORTED_ROUNDING,
    fixed_value,
    parse_decimal,
    scale_for,
)
from .quantiles import METHODS


DECISION_MODES = {"STOP", "RETURN_LAST_PASS", "STEP_DOWN_RETEST"}


def normalize_plan(payload: dict[str, Any], plan_id: str) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise PlanInvalid("plan body must be a JSON object")
    if not plan_id or not isinstance(plan_id, str):
        raise PlanInvalid("plan_id must be a non-empty string")
    plan = copy.deepcopy(payload)
    plan.pop("plan_id", None)
    plan.pop("version", None)
    plan["name"] = _optional_string(plan.get("name"), "plan.name", default=plan_id)
    plan["target"] = _validate_target(plan.get("target"))
    plan["seed"] = _positive_or_zero_int(plan.get("seed"), "plan.seed")

    contract = plan.get("numeric_contract")
    if not isinstance(contract, dict):
        raise PlanInvalid("numeric_contract must be an object")
    ratio_decimals = _decimal_places(contract.get("ratio_decimals"), "numeric_contract.ratio_decimals")
    latency_decimals = _decimal_places(
        contract.get("latency_decimals", 3), "numeric_contract.latency_decimals"
    )
    throughput_decimals = _decimal_places(
        contract.get("throughput_decimals", 3), "numeric_contract.throughput_decimals"
    )
    rounding = contract.get("rounding", "ROUND_HALF_EVEN")
    if rounding not in SUPPORTED_ROUNDING:
        raise PlanInvalid(
            "numeric_contract.rounding must be one of " + ", ".join(sorted(SUPPORTED_ROUNDING)),
            details={"field": "numeric_contract.rounding"},
        )
    method = contract.get("quantile_method")
    if method not in METHODS:
        raise PlanInvalid(
            "numeric_contract.quantile_method must explicitly be nearest_rank or linear",
            details={"field": "numeric_contract.quantile_method"},
        )
    min_samples = _positive_int(contract.get("min_samples", 100), "numeric_contract.min_samples")
    tolerance_ppm = _positive_or_zero_int(
        contract.get("relative_tolerance_ppm", 1000),
        "numeric_contract.relative_tolerance_ppm",
    )

    thresholds = plan.get("thresholds")
    if not isinstance(thresholds, dict):
        raise PlanInvalid("thresholds must be an object")
    p99_threshold = _non_negative_fraction(
        thresholds.get("p99_latency_ms_max"), "thresholds.p99_latency_ms_max"
    )
    error_threshold = _non_negative_fraction(
        thresholds.get("error_rate_max"), "thresholds.error_rate_max"
    )
    throughput_threshold = _non_negative_fraction(
        thresholds.get("min_throughput_rps"), "thresholds.min_throughput_rps"
    )
    if error_threshold > 1:
        raise PlanInvalid("thresholds.error_rate_max must be in [0,1]")

    ratio_scale = scale_for(ratio_decimals)
    latency_scale = scale_for(latency_decimals)
    throughput_scale = scale_for(throughput_decimals)
    stored_thresholds = {
        "p99_latency_ms_max": {
            **fixed_value(_quantize_plan_value(p99_threshold, latency_scale, rounding), latency_decimals),
            "scale": latency_scale,
        },
        "error_rate_max": {
            **fixed_value(_quantize_plan_value(error_threshold, ratio_scale, rounding), ratio_decimals),
            "scale": ratio_scale,
        },
        "min_throughput_rps": {
            **fixed_value(
                _quantize_plan_value(throughput_threshold, throughput_scale, rounding),
                throughput_decimals,
            ),
            "scale": throughput_scale,
        },
    }

    raw_stages = plan.get("stages")
    if not isinstance(raw_stages, list) or not raw_stages:
        raise PlanInvalid("stages must be a non-empty array")
    stages: list[dict[str, Any]] = []
    previous_concurrency = 0
    for stage_index, raw_stage in enumerate(raw_stages):
        stage = _normalize_stage(
            raw_stage,
            stage_index,
            previous_concurrency,
            ratio_decimals=ratio_decimals,
            rounding=rounding,
            min_samples=min_samples,
        )
        stages.append(stage)
        previous_concurrency = stage["concurrency"]
    plan["stages"] = stages

    rules = plan.get("decision_rules", {})
    if not isinstance(rules, dict):
        raise PlanInvalid("decision_rules must be an object")
    on_reject = rules.get("on_reject", "STOP")
    if on_reject not in DECISION_MODES:
        raise PlanInvalid(
            "decision_rules.on_reject must be STOP, RETURN_LAST_PASS or STEP_DOWN_RETEST",
            details={"field": "decision_rules.on_reject"},
        )
    max_retests = _positive_or_zero_int(
        rules.get("max_downshift_retests", 1), "decision_rules.max_downshift_retests"
    )
    fraction_ppm = _positive_or_zero_int(
        rules.get("downshift_fraction_ppm", 500000),
        "decision_rules.downshift_fraction_ppm",
    )
    if fraction_ppm > 1_000_000:
        raise PlanInvalid("decision_rules.downshift_fraction_ppm must be <= 1000000")
    plan["decision_rules"] = {
        "on_reject": on_reject,
        "max_downshift_retests": max_retests,
        "downshift_fraction_ppm": fraction_ppm,
    }

    plan["numeric_contract"] = {
        "ratio_decimals": ratio_decimals,
        "latency_decimals": latency_decimals,
        "throughput_decimals": throughput_decimals,
        "rounding": rounding,
        "quantile_method": method,
        "min_samples": min_samples,
        "relative_tolerance_ppm": tolerance_ppm,
        "throughput_definition": "closed_loop_successes_per_effective_second",
        "ratio_storage": "fixed_integer",
    }
    plan["stored_thresholds"] = stored_thresholds
    plan["plan_id"] = plan_id
    return plan


def stage_for_retest(
    original: dict[str, Any], *, concurrency: int, stage_index: int, name: str
) -> dict[str, Any]:
    stage = copy.deepcopy(original)
    stage["concurrency"] = concurrency
    stage["stage_index"] = stage_index
    stage["name"] = name
    stage["retest_of_stage_index"] = original["stage_index"]
    return stage


def downshift_concurrency(previous: int, rejected: int, fraction_ppm: int) -> int:
    delta = rejected - previous
    if delta <= 1:
        return 0
    candidate = previous + max(1, round(delta * fraction_ppm / 1_000_000))
    if candidate >= rejected:
        candidate = rejected - 1
    if candidate <= previous:
        return 0
    return candidate


def plan_fingerprint(plan: dict[str, Any]) -> str:
    material = {key: value for key, value in plan.items() if key != "stored_thresholds"}
    encoded = json.dumps(
        material,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _normalize_stage(
    raw: Any,
    index: int,
    previous_concurrency: int,
    *,
    ratio_decimals: int,
    rounding: str,
    min_samples: int,
) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise PlanInvalid(f"stages[{index}] must be an object")
    stage = copy.deepcopy(raw)
    concurrency = _positive_int(stage.get("concurrency"), f"stages[{index}].concurrency")
    if concurrency <= previous_concurrency:
        raise PlanInvalid(
            "stage concurrency values must be strictly increasing",
            details={
                "field": f"stages[{index}].concurrency",
                "previous_concurrency": previous_concurrency,
                "concurrency": concurrency,
            },
        )
    duration_seconds = _positive_int(
        stage.get("duration_seconds"), f"stages[{index}].duration_seconds"
    )
    default_samples = max(min_samples, concurrency * duration_seconds * 20)
    sample_size = _positive_int(
        stage.get("sample_size", default_samples), f"stages[{index}].sample_size"
    )
    timeout_ms = _positive_int(
        stage.get("request_timeout_ms", 1000), f"stages[{index}].request_timeout_ms"
    )
    error_rate = _non_negative_fraction(
        stage.get("error_rate", 0), f"stages[{index}].error_rate"
    )
    timeout_rate = _non_negative_fraction(
        stage.get("timeout_rate", 0), f"stages[{index}].timeout_rate"
    )
    if error_rate > 1 or timeout_rate > 1:
        raise PlanInvalid(f"stages[{index}] error_rate and timeout_rate must be in [0,1]")
    hang_ms = _positive_int(stage.get("hang_ms", timeout_ms + 1000), f"stages[{index}].hang_ms")
    service_sleep_ms = _positive_or_zero_int(
        stage.get("service_sleep_ms", 0), f"stages[{index}].service_sleep_ms"
    )
    distribution = _normalize_distribution(stage.get("distribution", {}), index)
    stage.update(
        {
            "name": _optional_string(stage.get("name"), f"stages[{index}].name", default=f"stage-{index}"),
            "stage_index": index,
            "concurrency": concurrency,
            "duration_seconds": duration_seconds,
            "sample_size": sample_size,
            "request_timeout_ms": timeout_ms,
            "hang_ms": hang_ms,
            "service_sleep_ms": service_sleep_ms,
            "distribution": distribution,
            "error_rate": _fraction_text(error_rate),
            "timeout_rate": _fraction_text(timeout_rate),
            "stored_ratios": {
                "error_rate": {
                    **fixed_value(
                        _quantize_plan_value(error_rate, scale_for(ratio_decimals), rounding),
                        ratio_decimals,
                    ),
                    "scale": scale_for(ratio_decimals),
                },
                "timeout_rate": {
                    **fixed_value(
                        _quantize_plan_value(timeout_rate, scale_for(ratio_decimals), rounding),
                        ratio_decimals,
                    ),
                    "scale": scale_for(ratio_decimals),
                },
            },
        }
    )
    return stage


def _normalize_distribution(raw: Any, stage_index: int) -> dict[str, Any]:
    if raw is None:
        raw = {}
    if not isinstance(raw, dict):
        raise PlanInvalid(f"stages[{stage_index}].distribution must be an object")
    kind = raw.get("kind", "lognormal")
    if kind not in {"lognormal", "normal"}:
        raise PlanInvalid(
            f"stages[{stage_index}].distribution.kind must be lognormal or normal"
        )
    median_ms = _non_negative_fraction(
        raw.get("median_ms", 10), f"stages[{stage_index}].distribution.median_ms"
    )
    if median_ms <= 0:
        raise PlanInvalid(f"stages[{stage_index}].distribution.median_ms must be > 0")
    sigma = _non_negative_fraction(
        raw.get("sigma", "0.25"), f"stages[{stage_index}].distribution.sigma"
    )
    overload_ref = _positive_int(
        raw.get("overload_ref_concurrency", 1),
        f"stages[{stage_index}].distribution.overload_ref_concurrency",
    )
    overload_power = _non_negative_fraction(
        raw.get("overload_power", "1.0"),
        f"stages[{stage_index}].distribution.overload_power",
    )
    overload_slope = _non_negative_fraction(
        raw.get("overload_slope", "0"),
        f"stages[{stage_index}].distribution.overload_slope",
    )
    return {
        "kind": kind,
        "median_ms": _fraction_text(median_ms),
        "sigma": _fraction_text(sigma),
        "overload_ref_concurrency": overload_ref,
        "overload_power": _fraction_text(overload_power),
        "overload_slope": _fraction_text(overload_slope),
    }


def _quantize_plan_value(value: Fraction, scale: int, rounding: str) -> int:
    from .fixedpoint import quantize

    return quantize(value, scale, rounding)


def _fraction_text(value: Fraction) -> str:
    if value.denominator == 1:
        return str(value.numerator)
    from decimal import Decimal, localcontext

    with localcontext() as context:
        context.prec = max(40, len(str(abs(value.numerator))) + len(str(value.denominator)) + 10)
        dec = Decimal(value.numerator) / Decimal(value.denominator)
        text = format(dec, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text


def _validate_target(value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise PlanInvalid("plan.target must be a non-empty URL")
    parsed = urllib.parse.urlparse(value)
    if parsed.scheme not in {"http", "https"}:
        raise PlanInvalid("plan.target must use http or https")
    if not parsed.hostname:
        raise PlanInvalid("plan.target must include a host")
    try:
        port = parsed.port
    except ValueError as exc:
        raise PlanInvalid("plan.target has an invalid port") from exc
    if port is None:
        port = 443 if parsed.scheme == "https" else 80
    host = parsed.hostname
    is_loopback = host == "localhost"
    if not is_loopback:
        try:
            is_loopback = ipaddress.ip_address(host).is_loopback
        except ValueError:
            is_loopback = False
    if not is_loopback:
        raise PlanInvalid("plan.target must point to a loopback/local test endpoint")
    netloc_host = f"[{host}]" if ":" in host and not host.startswith("[") else host
    if parsed.port is not None:
        netloc_host = f"{netloc_host}:{port}"
    return urllib.parse.urlunparse(
        (
            parsed.scheme,
            netloc_host,
            parsed.path or "/load",
            parsed.params,
            parsed.query,
            "",
        )
    )


def _optional_string(value: Any, field: str, *, default: str) -> str:
    if value is None:
        return default
    if not isinstance(value, str) or not value.strip():
        raise PlanInvalid(f"{field} must be a non-empty string")
    return value.strip()


def _positive_int(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise PlanInvalid(f"{field} must be a positive integer")
    return value


def _positive_or_zero_int(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise PlanInvalid(f"{field} must be a non-negative integer")
    return value


def _decimal_places(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0 or value > 12:
        raise PlanInvalid(f"{field} must be an integer in [0,12]")
    return value


def _non_negative_fraction(value: Any, field: str) -> Fraction:
    if value is None:
        raise PlanInvalid(f"{field} is required")
    try:
        parsed = parse_decimal(value, field)
    except ValueError as exc:
        raise PlanInvalid(str(exc)) from exc
    if parsed < 0:
        raise PlanInvalid(f"{field} must be non-negative")
    return parsed

