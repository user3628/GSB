"""Stage 2: element normalisation, validation and conflict grouping.

Elements are normalised to canonical, exactly-comparable forms:

* duration interval -> whole-month integer interval
* penalty           -> fixed-point percent (configurable decimal places)
* effective month   -> ISO ``YYYY-MM`` string

Every conversion records its factor so the evidence chain can explain the
normalisation.  Invalid elements raise ``E_ELEMENT_INVALID`` naming both the
citation id and the offending element; such citations are never dropped
silently.
"""

from decimal import ROUND_HALF_UP, Decimal, localcontext

from .errors import E_ELEMENT_INVALID, LegalCiteError

_MONTH_RE = __import__("re").compile(r"^\d{4}-(0[1-9]|1[0-2])$")


def _invalid(citation_id, element, reason, value):
    return LegalCiteError(
        E_ELEMENT_INVALID, reason,
        {"citation_id": citation_id, "element": element, "value": value},
    )


def _round_decimal(value, places):
    quantum = Decimal(1).scaleb(-places)
    return value.quantize(quantum, rounding=ROUND_HALF_UP)


def normalize_penalty(raw, cfg, citation_id):
    """Normalize a penalty to a fixed-point percent float."""
    precision = cfg["precision"]
    places = precision["penalty_decimal_places"]
    if isinstance(raw, dict):
        value = raw.get("value")
        unit = str(raw.get("unit", "percent")).lower()
    else:
        value, unit = raw, "percent"
    try:
        with localcontext() as ctx:
            ctx.prec = 50
            dec = Decimal(str(value)).copy_abs() if not _is_negative(value) \
                else -Decimal(str(abs(value)))
    except Exception:
        raise _invalid(citation_id, "penalty", "non-numeric penalty value", value)
    factor = cfg["unit_conversions"]["penalty_scale"].get(unit)
    if factor is None:
        raise _invalid(citation_id, "penalty", f"unknown penalty unit {unit!r}",
                       value)
    converted = dec * Decimal(str(factor))
    rounded = _round_decimal(converted, places)
    percent = float(rounded)
    if percent < precision["min_penalty_percent"] \
            or percent > precision["max_penalty_percent"]:
        raise _invalid(
            citation_id, "penalty",
            "penalty percent outside representable configured range",
            percent,
        )
    return {
        "value": percent,
        "unit": "percent",
        "conversion": {"from_unit": unit, "factor": factor,
                       "decimal_places": places, "raw_value": str(value)},
    }


def _is_negative(value):
    return str(value).strip().startswith("-")


def normalize_duration(raw, cfg, citation_id):
    """Normalize an interval to whole-month integer bounds."""
    places = cfg["precision"]["duration_decimal_places"]
    table = cfg["unit_conversions"]["duration"]
    result = {}
    for bound in ("start_months", "end_months"):
        item = raw[bound]
        if isinstance(item, dict):
            num, unit = item.get("value"), str(item.get("unit", "month")).lower()
        else:
            num, unit = item, "month"
        factor = table.get(unit)
        if factor is None:
            raise _invalid(citation_id, "duration",
                           f"unknown duration unit {unit!r}", item)
        with localcontext() as ctx:
            ctx.prec = 50
            months = Decimal(str(num)) * Decimal(str(factor))
            months = _round_decimal(months, places)
        result[bound] = int(months)
        result.setdefault("conversion", {})[bound] = {
            "from_unit": unit, "factor": factor, "raw_value": str(num)}
    if result["end_months"] < result["start_months"]:
        raise _invalid(
            citation_id, "duration",
            "duration interval ends before it starts",
            [result["start_months"], result["end_months"]],
        )
    return result


def normalize_effective_month(raw, cfg, citation_id):
    month = str(raw)
    if not _MONTH_RE.match(month):
        raise _invalid(citation_id, "effective_month",
                       "effective month must be ISO YYYY-MM", raw)
    return {"value": month, "conversion": {"from_unit": "iso_month",
                                           "factor": 1, "raw_value": raw}}


def normalize_elements(parsed, cfg):
    """Normalize all elements of a stage-1 parsed citation."""
    citation_id = parsed["citation_id"]
    raw_elements = parsed["elements_raw"]
    normalized = {}
    if "duration" in raw_elements:
        normalized["duration"] = normalize_duration(
            raw_elements["duration"], cfg, citation_id)
    if "penalty" in raw_elements:
        normalized["penalty"] = normalize_penalty(
            raw_elements["penalty"], cfg, citation_id)
    if "effective_month" in raw_elements:
        normalized["effective_month"] = normalize_effective_month(
            raw_elements["effective_month"], cfg, citation_id)
    out = dict(parsed)
    out["elements_normalized"] = normalized
    return out


def comparable_tuple(elements_normalized):
    """Hashable exact-comparison tuple of normalized elements."""
    keys = ("duration", "penalty", "effective_month")
    parts = []
    for key in keys:
        if key not in elements_normalized:
            parts.append(None)
        elif key == "duration":
            parts.append((elements_normalized[key]["start_months"],
                          elements_normalized[key]["end_months"]))
        else:
            parts.append(elements_normalized[key]["value"])
    return tuple(parts)


def group_citations(valid_records, invalid_diagnostics):
    """Group valid citations by (contract, locator) and flag conflicts.

    Returns a list of group dicts.  Invalid citations are carried alongside
    as diagnostics instead of being dropped.
    """
    buckets = {}
    for rec in valid_records:
        key = (rec["contract_id"], rec["location"]["locator"])
        buckets.setdefault(key, []).append(rec)
    groups = []
    for key in sorted(buckets):
        contract_id, locator = key
        members = sorted(buckets[key], key=lambda r: r["citation_id"])
        signatures = {comparable_tuple(r["elements_normalized"]) for r in members}
        groups.append({
            "contract_id": contract_id,
            "locator": locator,
            "article": members[0]["location"]["article"],
            "section": members[0]["location"]["section"],
            "citation_ids": [r["citation_id"] for r in members],
            "distinct_value_signatures": len(signatures),
            "has_conflict": len(signatures) > 1,
            "members": members,
        })
    return groups
