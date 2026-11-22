"""Stage 3: fixed-point normalization of duration, penalty and effective month."""
from __future__ import annotations

import re
from decimal import ROUND_HALF_UP, Decimal
from fractions import Fraction

from .codec import quantize_fraction, stable_sorted
from .config import NormalizeConfig
from .errors import UnitUnresolvedError
from .parse import parse_chinese_numeral

_OPEN_BRACKETS = {"(", "（", "[", "【"}
_CLOSE_BRACKETS = {")", "）", "]", "】"}
_INTERVAL_RE = re.compile(
    r"^\s*(?P<lbr>[(（\[【])?\s*"
    r"(?P<low>[0-9]+(?:\.[0-9]+)?)\s*[,，]\s*"
    r"(?P<high>[0-9]+(?:\.[0-9]+)?)\s*"
    r"(?P<rbr>[)）\]】])?\s*(?P<unit>.+?)\s*$"
)
_SCALAR_RE = re.compile(r"^\s*(?P<value>[0-9]+(?:\.[0-9]+)?)\s*(?P<unit>.+?)\s*$")
_MONTH_RE = re.compile(r"^\s*([0-9]{4})\s*[-/年.]\s*([0-9]{1,2})\s*月?\s*$")
_PERCENT_ZH_RE = re.compile(r"^\s*百分之\s*([0-9]+(?:\.[0-9]+)?|[零一二两三四五六七八九十百千]+)\s*$")


def _unit_error(ref_id, term_name, raw, reason):
    return UnitUnresolvedError(
        reason,
        {"ref_id": ref_id, "term": term_name, "raw": raw, "reason": reason},
    ).to_dict()


def _match_unit(text, factors):
    text = text.strip()
    for unit in sorted(factors, key=len, reverse=True):
        if text == unit or text.endswith(unit):
            head = text[: -len(unit)].strip()
            if not head:
                return unit
    return None


def _scaled(value: Fraction, factor: Fraction, precision: int):
    scaled = quantize_fraction(value * factor, precision)
    return {
        "scaled": scaled,
        "scale": 10 ** precision,
        "precision": precision,
        "factor": str(factor),
    }


def normalize_duration(raw: str, ref_id: str, config: NormalizeConfig):
    text = raw.replace(" ", "")
    interval = _INTERVAL_RE.match(text)
    if interval:
        low = Fraction(interval.group("low"))
        high = Fraction(interval.group("high"))
        lbr = interval.group("lbr")
        rbr = interval.group("rbr")
        unit_text = interval.group("unit")
        if (lbr and lbr in _CLOSE_BRACKETS) or (rbr and rbr in _OPEN_BRACKETS):
            return None, _unit_error(ref_id, "duration", raw, "interval bracket semantics conflict")
        if low >= high:
            return None, _unit_error(ref_id, "duration", raw, "interval endpoints conflict")
        unit = _match_unit(unit_text, config.duration_factors)
        if unit is None:
            return None, _unit_error(ref_id, "duration", raw, "unknown or missing duration unit")
        factor = config.duration_factor(unit)
        lower = _scaled(low, factor, config.duration_precision)
        upper = _scaled(high, factor, config.duration_precision)
        lower_open = bool(lbr) and lbr in {"(", "（"}
        upper_open = bool(rbr) and rbr in {")", "）"}
        return (
            {
                "kind": "duration_interval",
                "raw": raw,
                "unit": unit,
                "lower": lower,
                "upper": upper,
                "lower_open": lower_open,
                "upper_open": upper_open,
            },
            None,
        )

    scalar = _SCALAR_RE.match(text)
    if scalar:
        unit = _match_unit(scalar.group("unit"), config.duration_factors)
        if unit is None:
            return None, _unit_error(ref_id, "duration", raw, "unknown or missing duration unit")
        value = Fraction(scalar.group("value"))
        point = _scaled(value, config.duration_factor(unit), config.duration_precision)
        return (
            {
                "kind": "duration_point",
                "raw": raw,
                "unit": unit,
                "point": point,
                "lower": point,
                "upper": point,
                "lower_open": False,
                "upper_open": False,
            },
            None,
        )
    return None, _unit_error(ref_id, "duration", raw, "unresolvable duration expression")


def normalize_penalty(raw: str, ref_id: str, config: NormalizeConfig):
    text = raw.strip()
    zh_percent = _PERCENT_ZH_RE.match(text)
    if zh_percent:
        token = zh_percent.group(1)
        value = Fraction(int(token) if token.isdigit() else parse_chinese_numeral(token))
        point = _scaled(value, Fraction(100), config.penalty_precision)
        return {"kind": "penalty", "raw": raw, "unit": "百分之", "point": point}, None
    scalar = _SCALAR_RE.match(text)
    if not scalar:
        return None, _unit_error(ref_id, "penalty", raw, "unresolvable penalty expression")
    unit = _match_unit(scalar.group("unit"), config.penalty_factors)
    if unit is None:
        return None, _unit_error(ref_id, "penalty", raw, "missing or unknown penalty unit")
    value = Fraction(scalar.group("value"))
    point = _scaled(value, config.penalty_factor(unit), config.penalty_precision)
    return {"kind": "penalty", "raw": raw, "unit": unit, "point": point}, None


def normalize_month(raw: str, ref_id: str, config: NormalizeConfig):
    match = _MONTH_RE.match(raw.strip())
    if not match:
        return None, _unit_error(ref_id, "effective_month", raw, "unresolvable month expression")
    year, month = int(match.group(1)), int(match.group(2))
    if not 1 <= month <= 12:
        return None, _unit_error(ref_id, "effective_month", raw, "month out of range")
    index = year * 12 + month
    return (
        {
            "kind": "effective_month",
            "raw": raw,
            "point": {
                "scaled": index,
                "scale": 10 ** config.month_precision,
                "precision": config.month_precision,
                "factor": "year*12+month",
            },
        },
        None,
    )


def comparison_key(scaled: int, stored_precision: int, target_precision: int) -> int:
    """Re-quantize a stored fixed-point integer to the configured compare precision."""
    if target_precision >= stored_precision:
        return scaled * 10 ** (target_precision - stored_precision)
    quotient = Decimal(scaled) / Decimal(10 ** (stored_precision - target_precision))
    return int(quotient.to_integral_value(rounding=ROUND_HALF_UP))


def run_normalize(parse_product: dict, config: NormalizeConfig, stop_check=None) -> dict:
    """Stage-3 product: fixed-point term values, conversion metadata and term errors."""
    results = []
    errors = []
    term_handlers = {
        "duration": normalize_duration,
        "penalty": normalize_penalty,
        "effective_month": normalize_month,
    }
    for entry in parse_product["entries"]:
        normalized_terms = {}
        ref_errors = []
        for term_name in sorted(entry["raw_terms"]):
            raw = entry["raw_terms"][term_name]
            value, error = term_handlers[term_name](raw, entry["ref_id"], config)
            if error is not None:
                ref_errors.append(error)
                errors.append(error)
            else:
                normalized_terms[term_name] = value
        result = {
            "ref_id": entry["ref_id"],
            "contract_id": entry["contract_id"],
            "preferred_location": entry["preferred_location"],
            "normalized_terms": normalized_terms,
            "term_errors": ref_errors,
            "region_binding": entry["region_binding"],
        }
        results.append(result)
        if stop_check is not None:
            stop_check(entry["ref_id"])
    results = stable_sorted(results, key=lambda item: item["ref_id"])
    errors = stable_sorted(errors, key=lambda item: (item["details"]["ref_id"], item["details"]["term"]))
    return {
        "stage": "normalize",
        "results": results,
        "errors": errors,
        "stats": {"result_count": len(results), "term_error_count": len(errors)},
    }
