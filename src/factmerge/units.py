"""Stage 2: deterministic decimal unit conversion and month-calendar normalization."""

from __future__ import annotations

from decimal import Decimal, localcontext
from typing import Any, Iterable

from .util import (
    DECIMAL_CONTEXT_PRECISION,
    decimal_text,
    dec,
    digest_without,
    quantize,
)


REVIEW_CODE_TO_FALLBACK = {
    "E_UNIT_MISSING": "missing_unit",
    "E_UNIT_UNKNOWN": "unknown_unit",
    "E_UNIT_CATEGORY_MISMATCH": "unknown_unit",
    "E_NEGATIVE_VALUE": "negative_value",
    "E_INTERVAL_REVERSED": "interval_reversed",
    "E_INTERVAL_OPEN_CONFLICT": "interval_open_conflict",
}


def ordinal_to_iso(ordinal: int) -> str:
    year, month = divmod(int(ordinal), 12)
    return f"{year:04d}-{month + 1:02d}"


def _review_issue(code: str, message: str, config: dict[str, Any]) -> dict[str, Any]:
    key = REVIEW_CODE_TO_FALLBACK.get(code, "unknown_unit")
    strategy = config["fallback"].get(key, "review")
    return {
        "code": code,
        "message": message,
        "strategy": strategy,
        "disposition": "review",
    }


def _scale_contained(unit_text: str, scale_text: str) -> bool:
    def compact(text: str) -> str:
        return "".join(str(text).casefold().split())

    return compact(scale_text) in compact(unit_text)


def _factor_trace(
    parsed: dict[str, Any], unit_spec: dict[str, Any] | None, embedded_factor: Decimal
) -> tuple[Decimal, int, list[dict[str, Any]], bool]:
    factor = Decimal(1)
    precision = 0
    trace: list[dict[str, Any]] = []
    embedded_scale = parsed.get("embedded_scale")
    dedupe_embedded = False
    if embedded_scale and parsed.get("unit") and _scale_contained(parsed["unit"]["text"], embedded_scale["token"]):
        dedupe_embedded = True
    if unit_spec is not None:
        unit_factor = dec(unit_spec["factor"])
        factor *= unit_factor
        precision = max(precision, int(unit_spec.get("factor_precision", 0)))
        trace.append(
            {
                "component": "unit",
                "unit_id": unit_spec["id"],
                "text": parsed["unit"]["text"] if parsed.get("unit") else None,
                "factor": decimal_text(unit_factor),
                "factor_precision": int(unit_spec.get("factor_precision", 0)),
            }
        )
    if embedded_scale:
        effective = Decimal(1) if dedupe_embedded else embedded_factor
        factor *= effective
        precision = max(precision, int(embedded_scale.get("factor_precision", 0)))
        trace.append(
            {
                "component": "embedded_scale",
                "token": embedded_scale["token"],
                "factor": decimal_text(effective),
                "declared_factor": embedded_scale["factor"],
                "factor_precision": int(embedded_scale.get("factor_precision", 0)),
                "deduplicated_with_unit": dedupe_embedded,
            }
        )
    return factor, precision, trace, dedupe_embedded


def _normalize_scalar(parsed: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
    category = parsed["category"]
    issues: list[dict[str, Any]] = []
    unit = parsed.get("unit")
    unit_spec = unit.get("spec") if unit else None
    if not unit or not unit.get("text"):
        issues.append(_review_issue("E_UNIT_MISSING", "unit is missing; value cannot be safely converted", config))
    elif not unit_spec:
        issues.append(
            _review_issue("E_UNIT_UNKNOWN", f"unknown unit {unit.get('text')!r}; mapped conversion is unavailable", config)
        )
    elif unit_spec["category"] != category:
        issues.append(
            _review_issue(
                "E_UNIT_CATEGORY_MISMATCH",
                f"unit category {unit_spec['category']} does not match claim category {category}",
                config,
            )
        )

    coefficient = dec(parsed["numeric"]["coefficient"])
    embedded_factor = Decimal(1)
    if parsed.get("embedded_scale"):
        embedded_factor = dec(parsed["embedded_scale"]["factor"])
    factor, factor_precision, trace, dedupe = _factor_trace(parsed, unit_spec, embedded_factor)
    qspec = config["quantization"][category]
    quantum = qspec["quantum"]
    precision_digits = int(qspec["precision_digits"])
    rounding = qspec.get("rounding", "ROUND_HALF_EVEN")
    canonical_unit = unit_spec["base_unit"] if unit_spec else config["unit_dictionary"][category]["base_unit"]

    with localcontext() as ctx:
        ctx.prec = DECIMAL_CONTEXT_PRECISION
        unrounded = coefficient * factor
        normalized = quantize(unrounded, quantum, rounding)

    if coefficient < 0:
        issues.append(_review_issue("E_NEGATIVE_VALUE", "negative scalar value requires review", config))

    value_text = decimal_text(normalized)
    equality_key = f"{category}|{canonical_unit}|{value_text}"
    conversion = {
        "operation": "quantize(coefficient * unit_factor * embedded_scale_factor, quantum)",
        "coefficient": decimal_text(coefficient),
        "factor": decimal_text(factor),
        "factor_precision": factor_precision,
        "factor_trace": trace,
        "embedded_scale_deduplicated": dedupe,
        "quantum": quantum,
        "precision_digits": precision_digits,
        "rounding": rounding,
        "unrounded": decimal_text(unrounded),
        "result": value_text,
        "recompute": {
            "expression": f"quantize({decimal_text(coefficient)} * {decimal_text(factor)}, {quantum})",
            "expected_result": value_text,
            "exact_decimal": True,
        },
    }
    status = "review" if issues else "ok"
    return {
        "claim_id": parsed["claim_id"],
        "entity_id": parsed["entity_id"],
        "lang": parsed["lang"],
        "category": category,
        "kind": "scalar",
        "status": status,
        "review_issues": issues,
        "unit": {
            "text": unit.get("text") if unit else None,
            "span": unit.get("span") if unit else None,
            "unit_id": unit_spec["id"] if unit_spec else None,
            "canonical_unit": canonical_unit,
            "factor": decimal_text(dec(unit_spec["factor"])) if unit_spec else None,
            "factor_precision": int(unit_spec.get("factor_precision", 0)) if unit_spec else None,
        },
        "conversion": conversion,
        "canonical_value": {
            "type": "decimal",
            "value": value_text,
            "canonical_unit": canonical_unit,
            "quantum": quantum,
            "precision_digits": precision_digits,
            "comparison": "decimal_exact",
            "equality_key": equality_key,
        },
        "parse_source": parsed.get("parse_source"),
        "parse_warnings": parsed.get("warnings", []),
    }


def _normalize_interval(parsed: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
    interval = parsed["interval"]
    issues: list[dict[str, Any]] = []
    unit = parsed.get("unit")
    unit_spec = unit.get("spec") if unit else None
    if not unit or not unit.get("text"):
        issues.append(_review_issue("E_UNIT_MISSING", "unit is missing for month interval", config))
    elif not unit_spec:
        issues.append(
            _review_issue("E_UNIT_UNKNOWN", f"unknown interval unit {unit.get('text')!r}", config)
        )
    if interval.get("closure_conflict"):
        issues.append(
            _review_issue(
                "E_INTERVAL_OPEN_CONFLICT",
                "interval closure markers or open-ended syntax are inconsistent",
                config,
            )
        )

    start = interval.get("start")
    end = interval.get("end")
    start_ordinal = start["ordinal"] if start else None
    end_ordinal = end["ordinal"] if end else None
    if start_ordinal is not None and end_ordinal is not None and start_ordinal > end_ordinal:
        issues.append(
            _review_issue(
                "E_INTERVAL_REVERSED",
                f"interval start {ordinal_to_iso(start_ordinal)} is later than end {ordinal_to_iso(end_ordinal)}",
                config,
            )
        )

    def endpoint(item: dict[str, Any] | None) -> dict[str, Any] | None:
        if item is None:
            return None
        return {
            "year": item["year"],
            "month": item["month"],
            "ordinal": item["ordinal"],
            "iso": ordinal_to_iso(item["ordinal"]),
            "span": item["span"],
        }

    start_payload = endpoint(start)
    end_payload = endpoint(end)
    value_text = (
        f"{(start_payload or {}).get('iso', 'open')}..{(end_payload or {}).get('iso', 'open')}"
        f"[{'closed' if interval['start_closed'] else 'open'},"
        f"{'closed' if interval['end_closed'] else 'open'}]"
    )
    equality_key = (
        "tenure|month_interval|"
        f"{start_ordinal if start_ordinal is not None else 'open'}|"
        f"{end_ordinal if end_ordinal is not None else 'open'}|"
        f"{'closed' if interval['start_closed'] else 'open'}|"
        f"{'closed' if interval['end_closed'] else 'open'}"
    )
    status = "review" if issues else "ok"
    conversion = {
        "operation": "calendar_month_ordinal",
        "formula": config["calendar"].get("month_ordinal", "year*12+(month-1)"),
        "factor": "1",
        "factor_precision": 0,
        "factor_trace": [
            {
                "component": "calendar",
                "start": (start_payload or {}).get("iso"),
                "start_ordinal": start_ordinal,
                "end": (end_payload or {}).get("iso"),
                "end_ordinal": end_ordinal,
            }
        ],
        "quantum": "1",
        "precision_digits": 0,
        "rounding": "NONE",
        "unrounded": value_text,
        "result": value_text,
        "recompute": {
            "start_expression": (
                f"{start['year']}*12+({start['month']}-1)" if start is not None else "open"
            ),
            "end_expression": (f"{end['year']}*12+({end['month']}-1)" if end is not None else "open"),
            "expected_start_ordinal": start_ordinal,
            "expected_end_ordinal": end_ordinal,
            "exact_integer_months": True,
        },
    }
    return {
        "claim_id": parsed["claim_id"],
        "entity_id": parsed["entity_id"],
        "lang": parsed["lang"],
        "category": "tenure",
        "kind": "month_interval",
        "status": status,
        "review_issues": issues,
        "unit": {
            "text": unit.get("text") if unit else None,
            "span": unit.get("span") if unit else None,
            "unit_id": unit_spec["id"] if unit_spec else None,
            "canonical_unit": "month_index",
            "factor": "1" if unit_spec else None,
            "factor_precision": 0 if unit_spec else None,
        },
        "conversion": conversion,
        "canonical_value": {
            "type": "month_interval",
            "value": value_text,
            "start": start_payload,
            "end": end_payload,
            "start_closed": bool(interval["start_closed"]),
            "end_closed": bool(interval["end_closed"]),
            "interval_kind": interval["interval_kind"],
            "quantum": "1",
            "precision_digits": 0,
            "comparison": "month_ordinal_exact",
            "equality_key": equality_key,
        },
        "parse_source": parsed.get("parse_source"),
        "parse_warnings": parsed.get("warnings", []),
    }


def normalize_claim(parsed: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
    if parsed.get("kind") == "month_interval" or parsed.get("category") == "tenure":
        result = _normalize_interval(parsed, config)
    else:
        result = _normalize_scalar(parsed, config)
    result["stage1_digest"] = parsed.get("digest")
    result["digest"] = digest_without(result, "digest")
    return result


def normalize_entity_claims(parsed_claims: Iterable[dict[str, Any]], config: dict[str, Any]) -> list[dict[str, Any]]:
    return [normalize_claim(claim, config) for claim in parsed_claims]
