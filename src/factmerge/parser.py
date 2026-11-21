"""Stage 1: statement normalization and numeric/calendar parsing."""

from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable

from .errors import FactMergeError
from .util import (
    NormalizedText,
    compact_key,
    decimal_text,
    dec,
    normalize_key,
    normalize_text,
)


_ARABIC_PATTERN = re.compile(
    r"(?<![\w])(?P<number>[+\-]?(?:\d[\d.,]*\d|\d))"
    r"(?:\s*(?P<scale>万|萬|亿|億|兆|千|百|十|thousand|million|billion|trillion|mn|bn|[kKmMbB])(?![A-Za-z]))?",
    re.IGNORECASE,
)
_CHINESE_PATTERN = re.compile(r"(?P<number>[负負+\-]?[零〇一二两兩三四五六七八九十百千万萬亿億兆点點]+)")
_RANGE_MARKER = re.compile(
    r"至|到|—|–|~|～|,|，|;|；|-|through|until|\bto\b",
    re.IGNORECASE,
)
_OPEN_END_MARKER = re.compile(r"至今|迄今|present|now|ongoing|today", re.IGNORECASE)
_OPEN_START_MARKER = re.compile(r"since|自|从|從", re.IGNORECASE)

_EN_MONTHS = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}
_MONTH_WORD = "|".join(sorted(_EN_MONTHS, key=len, reverse=True))
_NUMERIC_MONTH_PATTERN = re.compile(
    r"(?<!\d)(?P<year>(?:18|19|20|21)\d{2})\s*(?:[-/.年]\s*)(?P<month>0?[1-9]|1[0-2])(?:\s*月)?(?!\d)"
)
_MONTH_FIRST_PATTERN = re.compile(
    rf"(?<![A-Za-z])(?P<month_name>{_MONTH_WORD})\s+(?P<year>(?:18|19|20|21)\d{{2}})(?![A-Za-z0-9])",
    re.IGNORECASE,
)
_YEAR_FIRST_PATTERN = re.compile(
    rf"(?<!\d)(?P<year>(?:18|19|20|21)\d{{2}})\s+(?P<month_name>{_MONTH_WORD})(?![A-Za-z])",
    re.IGNORECASE,
)


def _unit_index(config: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    index: dict[str, list[dict[str, Any]]] = {}
    for category, group in config["unit_dictionary"].items():
        base_unit = group.get("base_unit", category)
        for spec in group["units"]:
            entry = {
                "category": category,
                "base_unit": base_unit,
                "id": spec["id"],
                "factor": spec["factor"],
                "factor_precision": spec.get("factor_precision", 0),
                "aliases": list(spec.get("aliases", [])),
            }
            for alias in spec.get("aliases", []):
                keys = {normalize_key(alias), compact_key(alias)}
                for key in keys:
                    if key:
                        index.setdefault(key, []).append(entry)
    return index


def _dedupe_units(entries: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[str, str]] = set()
    result: list[dict[str, Any]] = []
    for entry in entries:
        key = (entry["category"], entry["id"])
        if key not in seen:
            seen.add(key)
            result.append(entry)
    return result


def lookup_unit(
    text: str,
    config: dict[str, Any],
    category: str | None = None,
) -> list[dict[str, Any]]:
    if not text:
        return []
    index = _unit_index(config)
    candidates = list(index.get(normalize_key(text), []))
    candidates.extend(index.get(compact_key(text), []))
    candidates = _dedupe_units(candidates)
    if category is not None:
        candidates = [item for item in candidates if item["category"] == category]
    return candidates


def infer_category(claim: dict[str, Any], normalized_parts: list[str], config: dict[str, Any]) -> str | None:
    explicit = claim.get("category")
    if explicit:
        if explicit not in config["unit_dictionary"]:
            raise FactMergeError(
                "E_PARSE_AMBIGUOUS",
                f"unsupported category: {explicit}",
                claim_id=str(claim.get("claim_id", "")),
                entity_id=str(claim.get("entity_id", "")),
                offsets={"field": "category", "start": 0, "end": len(str(explicit))},
            )
        return str(explicit)

    unit_text = str(claim.get("unit") or "")
    unit_categories = {item["category"] for item in lookup_unit(unit_text, config)}
    if len(unit_categories) == 1:
        return next(iter(unit_categories))

    haystack = " ".join(part.casefold() for part in normalized_parts if part)
    hits: list[tuple[int, str]] = []
    for category, hints in config["category_hints"].items():
        for hint in hints:
            if hint.casefold() in haystack:
                hits.append((haystack.index(hint.casefold()), category))
    if hits:
        hits.sort()
        return hits[0][1]
    if "年月区间" in unit_text or "year-month" in unit_text.casefold():
        return "tenure"
    return None


def _find_embedded_unit(
    normalized: NormalizedText,
    start: int,
    config: dict[str, Any],
    category: str | None,
) -> tuple[str, dict[str, Any], dict[str, Any] | None] | None:
    text = normalized.normalized
    tail = text[start : start + 64]
    if not tail:
        return None
    stripped_tail = tail.lstrip(" :：,，;；-")
    skipped = len(tail) - len(stripped_tail)
    tail_start = start + skipped

    aliases: list[tuple[str, dict[str, Any]]] = []
    for category_name, group in config["unit_dictionary"].items():
        if category is not None and category_name != category:
            continue
        base_unit = group.get("base_unit", category_name)
        for spec in group["units"]:
            entry = {
                "category": category_name,
                "base_unit": base_unit,
                "id": spec["id"],
                "factor": spec["factor"],
                "factor_precision": spec.get("factor_precision", 0),
                "aliases": list(spec.get("aliases", [])),
            }
            for alias in spec.get("aliases", []):
                normalized_alias = normalize_key(alias)
                if normalized_alias:
                    aliases.append((normalized_alias, entry))
    aliases.sort(key=lambda item: len(item[0]), reverse=True)
    folded_tail = tail_start < len(text) and text[tail_start].isascii() and text[tail_start].isalpha()
    compare = tail.casefold() if folded_tail else tail
    for alias, entry in aliases:
        if not compare.startswith(alias):
            continue
        end = tail_start + len(alias)
        if alias[-1:].isascii() and alias[-1:].isalpha() and end < len(text) and text[end].isascii() and text[end].isalpha():
            continue
        return text[tail_start:end], normalized.span_dict(tail_start, end, "raw_value"), entry
    return None


def _parse_arabic_number(token: str, ambiguous_policy: str = "strict_mixed") -> Decimal:
    sign = 1
    body = token
    if body.startswith(("+", "-")):
        if body[0] == "-":
            sign = -1
        body = body[1:]
    if not body:
        raise ValueError("empty number")

    commas = body.count(",")
    dots = body.count(".")
    if commas and dots:
        last_comma = body.rfind(",")
        last_dot = body.rfind(".")
        if last_comma > last_dot:
            integer, fraction = body.rsplit(",", 1)
            groups = integer.split(".")
            if not groups or any(len(group) != 3 for group in groups[1:]):
                raise ValueError("ambiguous mixed separators")
            if not 1 <= len(fraction) <= 2:
                raise ValueError("ambiguous mixed separators")
            integer = "".join(groups)
            normalized = f"{integer}.{fraction}"
        else:
            integer, fraction = body.rsplit(".", 1)
            groups = integer.split(",")
            if not groups or any(len(group) != 3 for group in groups[1:]):
                raise ValueError("ambiguous mixed separators")
            if not 1 <= len(fraction) <= 3:
                raise ValueError("ambiguous mixed separators")
            integer = "".join(groups)
            normalized = f"{integer}.{fraction}"
        return dec(normalized) * sign

    if commas:
        groups = body.split(",")
        if len(groups) == 1 or any(not group.isdigit() for group in groups):
            raise ValueError("ambiguous comma separators")
        if len(groups) == 2 and len(groups[1]) != 3:
            if len(groups[1]) not in (1, 2):
                raise ValueError("ambiguous comma separator")
            return dec(groups[0] + "." + groups[1]) * sign
        if len(groups) == 2 and len(groups[1]) == 3 and ambiguous_policy == "strict_locale":
            raise ValueError("single comma is ambiguous between grouping and decimal separator")
        if len(groups[1]) != 3 or any(len(group) != 3 for group in groups[1:]):
            raise ValueError("ambiguous comma grouping")
        return dec("".join(groups)) * sign

    if dots:
        if dots > 1:
            raise ValueError("ambiguous multiple decimal separators")
        integer, fraction = body.split(".", 1)
        if not integer.isdigit() or not fraction.isdigit():
            raise ValueError("invalid decimal")
        return dec(f"{integer}.{fraction}") * sign

    if not body.isdigit():
        raise ValueError("invalid integer")
    return dec(body) * sign


_CN_DIGITS = {
    "零": 0,
    "〇": 0,
    "一": 1,
    "二": 2,
    "两": 2,
    "兩": 2,
    "三": 3,
    "四": 4,
    "五": 5,
    "六": 6,
    "七": 7,
    "八": 8,
    "九": 9,
}
_CN_SMALL_UNITS = {"十": 10, "百": 100, "千": 1000}
_CN_LARGE_UNITS = {"万": 10_000, "萬": 10_000, "亿": 100_000_000, "億": 100_000_000, "兆": 1_000_000_000_000}


def _parse_chinese_integer(text: str) -> int:
    if not text:
        return 0
    if not any(char in _CN_SMALL_UNITS or char in _CN_LARGE_UNITS for char in text):
        if any(char not in _CN_DIGITS for char in text):
            raise ValueError("invalid Chinese numeral")
        digits = "".join(str(_CN_DIGITS[char]) for char in text)
        return int(digits or "0")

    total = 0
    section = 0
    number: int | None = None
    for char in text:
        if char in _CN_DIGITS:
            digit = _CN_DIGITS[char]
            number = digit if number is None else number * 10 + digit
        elif char in _CN_SMALL_UNITS:
            unit = _CN_SMALL_UNITS[char]
            number = 1 if number is None else number
            section += number * unit
            number = None
        elif char in _CN_LARGE_UNITS:
            unit = _CN_LARGE_UNITS[char]
            number = 0 if number is None else number
            section += number
            number = None
            if unit <= 10_000:
                total += section * unit
                section = 0
            else:
                total = (total + section) * unit
                section = 0
        else:
            raise ValueError("invalid Chinese numeral")
    return total + section + (number or 0)


def parse_chinese_number(token: str) -> Decimal:
    sign = 1
    body = token
    if body.startswith(("+", "-", "负", "負")):
        if body[0] in {"-", "负", "負"}:
            sign = -1
        body = body[1:]
    body = body.replace("點", "点")
    if not body:
        raise ValueError("empty Chinese numeral")

    if "点" in body:
        integer_part, fraction_part = body.split("点", 1)
        large_factor = Decimal(1)
        if fraction_part and fraction_part[-1] in "万萬亿億兆":
            large_factor = dec(_CN_LARGE_UNITS[fraction_part[-1]])
            fraction_part = fraction_part[:-1]
        if not fraction_part or any(char not in _CN_DIGITS for char in fraction_part):
            raise ValueError("invalid Chinese fractional numeral")
        fraction_digits = "".join(str(_CN_DIGITS[char]) for char in fraction_part)
        integer_value = _parse_chinese_integer(integer_part) if integer_part else 0
        value = dec(integer_value) + dec("0." + fraction_digits)
        return value * large_factor * sign

    return dec(_parse_chinese_integer(body)) * sign


def _scale_spec(token: str, config: dict[str, Any]) -> dict[str, Any] | None:
    scales = config["numeric_scales"]
    if token in scales:
        spec = scales[token]
    else:
        lowered = token.casefold()
        spec = scales.get(lowered)
    if spec is None:
        return None
    return {"token": token, "factor": spec["factor"], "factor_precision": spec.get("precision", 0)}


def _find_number(normalized: NormalizedText) -> dict[str, Any] | None:
    text = normalized.normalized
    arabic = _ARABIC_PATTERN.search(text)
    chinese = _CHINESE_PATTERN.search(text)
    candidates: list[dict[str, Any]] = []
    if arabic:
        candidates.append(
            {
                "kind": "arabic",
                "start": arabic.start("number"),
                "end": arabic.end("number"),
                "token": arabic.group("number"),
                "scale_start": arabic.start("scale") if arabic.group("scale") else None,
                "scale_end": arabic.end("scale") if arabic.group("scale") else None,
                "scale_token": arabic.group("scale"),
                "after": arabic.end(),
            }
        )
    if chinese:
        chinese_token = chinese.group("number")
        if len(chinese_token) > 1 or chinese_token not in "零〇一二两兩三四五六七八九":
            candidates.append(
                {
                    "kind": "chinese",
                    "start": chinese.start("number"),
                    "end": chinese.end("number"),
                    "token": chinese_token,
                    "scale_start": None,
                    "scale_end": None,
                    "scale_token": None,
                    "after": chinese.end(),
                }
            )
    if not candidates:
        return None
    candidates.sort(key=lambda item: (item["start"], -(item["end"] - item["start"])))
    return candidates[0]


def _date_matches(text: str) -> list[dict[str, Any]]:
    matches: list[dict[str, Any]] = []
    for match in _NUMERIC_MONTH_PATTERN.finditer(text):
        matches.append(
            {
                "start": match.start(),
                "end": match.end(),
                "year": int(match.group("year")),
                "month": int(match.group("month")),
            }
        )
    for pattern in (_MONTH_FIRST_PATTERN, _YEAR_FIRST_PATTERN):
        for match in pattern.finditer(text):
            matches.append(
                {
                    "start": match.start(),
                    "end": match.end(),
                    "year": int(match.group("year")),
                    "month": _EN_MONTHS[match.group("month_name").casefold()],
                }
            )
    matches.sort(key=lambda item: (item["start"], -(item["end"] - item["start"])))
    deduped: list[dict[str, Any]] = []
    for item in matches:
        if deduped and item["start"] < deduped[-1]["end"]:
            if item["end"] - item["start"] > deduped[-1]["end"] - deduped[-1]["start"]:
                deduped[-1] = item
            continue
        deduped.append(item)
    return deduped


def _closure_state(text: str, first_start: int, last_end: int) -> dict[str, Any]:
    prefix = text[:first_start].rstrip()
    suffix = text[last_end:].lstrip()
    start_bracket = prefix[-1] if prefix and prefix[-1] in "([)" else None
    end_bracket = suffix[0] if suffix and suffix[0] in ")](" else None
    conflict = False
    start_closed = True
    end_closed = True
    if start_bracket in {"(", ")"}:
        start_closed = False
    if start_bracket == ")":
        conflict = True
    if end_bracket in {")", "("}:
        end_closed = False
    if end_bracket == "(":
        conflict = True
    if (start_bracket is None) != (end_bracket is None):
        conflict = True
    return {
        "start_bracket": start_bracket,
        "end_bracket": end_bracket,
        "start_closed": start_closed,
        "end_closed": end_closed,
        "conflict": conflict,
    }


def _parse_month_interval(
    normalized: NormalizedText,
    config: dict[str, Any],
    claim: dict[str, Any],
    unit_info: dict[str, Any] | None,
) -> dict[str, Any] | None:
    text = normalized.normalized
    dates = _date_matches(text)
    if not dates:
        return None
    if len(dates) > 2:
        raise FactMergeError(
            "E_PARSE_AMBIGUOUS",
            "more than two month values form an ambiguous interval",
            claim_id=str(claim.get("claim_id", "")),
            entity_id=str(claim.get("entity_id", "")),
            offsets=normalized.span_dict(dates[0]["start"], dates[-1]["end"], "raw_value"),
        )

    if len(dates) == 2:
        between = text[dates[0]["end"] : dates[1]["start"]]
        if not _RANGE_MARKER.search(between):
            raise FactMergeError(
                "E_PARSE_AMBIGUOUS",
                "two month values without a recognized range separator",
                claim_id=str(claim.get("claim_id", "")),
                entity_id=str(claim.get("entity_id", "")),
                offsets=normalized.span_dict(dates[0]["start"], dates[1]["end"], "raw_value"),
            )
        closure = _closure_state(text, dates[0]["start"], dates[1]["end"])
        after = text[dates[1]["end"] :]
        open_conflict = bool(_OPEN_END_MARKER.search(after) or _OPEN_START_MARKER.search(between))
        kind = "range"
        start = dates[0]
        end = dates[1]
    else:
        date = dates[0]
        after = text[date["end"] :]
        before = text[: date["start"]]
        open_end = bool(_OPEN_END_MARKER.search(after))
        open_start = bool(_OPEN_START_MARKER.search(before))
        closure = _closure_state(text, date["start"], date["end"])
        open_conflict = bool(open_end and open_start)
        if open_end:
            kind = "open_end"
            start = date
            end = None
            closure["end_closed"] = False
        elif open_start:
            kind = "open_start"
            start = None
            end = date
            closure["start_closed"] = False
        else:
            kind = "point"
            start = date
            end = date

    def date_payload(item: dict[str, Any] | None) -> dict[str, Any] | None:
        if item is None:
            return None
        span = normalized.span_dict(item["start"], item["end"], "raw_value")
        return {
            "year": item["year"],
            "month": item["month"],
            "ordinal": item["year"] * 12 + (item["month"] - 1),
            "span": span,
        }

    embedded_unit = None
    if unit_info is None and dates:
        tail_start = dates[-1]["end"]
        found = _find_embedded_unit(normalized, tail_start, config, "tenure")
        if found:
            embedded_unit = {
                "text": found[0],
                "span": found[1],
                "spec": found[2],
            }

    return {
        "kind": "month_interval",
        "interval_kind": kind,
        "start": date_payload(start),
        "end": date_payload(end),
        "start_closed": closure["start_closed"],
        "end_closed": closure["end_closed"],
        "closure_conflict": bool(closure["conflict"] or open_conflict),
        "closure_syntax": closure,
        "range_marker": text[dates[0]["end"] : dates[1]["start"]] if len(dates) == 2 else None,
        "embedded_unit": embedded_unit,
        "parse_source": "raw_value.month_interval",
    }


def _unit_fragment_from_field(claim: dict[str, Any]) -> tuple[str, dict[str, Any]] | None:
    raw_unit = claim.get("unit")
    if raw_unit is None or str(raw_unit) == "":
        return None
    normalized = normalize_text(str(raw_unit))
    if not normalized.normalized.strip():
        return None
    start = 0
    end = len(normalized.normalized)
    while start < end and normalized.normalized[start].isspace():
        start += 1
    while end > start and normalized.normalized[end - 1].isspace():
        end -= 1
    return normalized.normalized[start:end], normalized.span_dict(start, end, "unit")


def _error_offsets(normalized: NormalizedText, field: str, start: int = 0, end: int | None = None) -> dict[str, Any]:
    if end is None:
        end = len(normalized.normalized)
    return normalized.span_dict(start, end, field)


def parse_claim(claim: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(claim, dict):
        raise FactMergeError("E_INVALID_FORMAT", "claim must be a JSON object")
    claim_id = str(claim.get("claim_id") or "")
    entity_id = str(claim.get("entity_id") or "")
    if not claim_id or not entity_id:
        raise FactMergeError(
            "E_INVALID_FORMAT",
            "claim_id and entity_id are required",
            claim_id=claim_id or None,
            entity_id=entity_id or None,
        )

    sentence = str(claim.get("sentence", claim.get("statement", claim.get("sentence_template", ""))) or "")
    field_order = list(config["parse"].get("field_order", ["raw_value", "sentence"]))
    if not config["parse"].get("allow_sentence_fallback", True):
        field_order = ["raw_value"]
    field_texts: list[tuple[str, str]] = []
    for field in field_order:
        if field == "raw_value":
            value = "" if claim.get("raw_value") is None else str(claim.get("raw_value"))
        elif field == "sentence":
            value = sentence
        else:
            value = str(claim.get(field, "") or "")
        field_texts.append((field, value))

    normalized_fields: list[tuple[str, NormalizedText]] = [
        (field, normalize_text(text, fold_whitespace=bool(config["normalization"].get("fold_whitespace", True))))
        for field, text in field_texts
    ]
    unit_field = _unit_fragment_from_field(claim)
    parts = [normalized.normalized for _, normalized in normalized_fields]
    if unit_field:
        parts.append(unit_field[0])
    category = infer_category(claim, parts, config)

    chosen_field: str | None = None
    chosen: NormalizedText | None = None
    interval: dict[str, Any] | None = None
    if category in (None, "tenure"):
        for field, normalized in normalized_fields:
            parsed_interval = _parse_month_interval(normalized, config, claim, unit_field)
            if parsed_interval is not None:
                chosen_field, chosen, interval = field, normalized, parsed_interval
                category = "tenure"
                break

    number_data: dict[str, Any] | None = None
    if interval is None:
        for field, normalized in normalized_fields:
            number_data = _find_number(normalized)
            if number_data is not None:
                chosen_field, chosen = field, normalized
                break
        if number_data is None or chosen is None or chosen_field is None:
            reason = "empty numeric fragment" if not any(text.strip() for _, text in field_texts) else "unparseable numeric fragment"
            first = normalized_fields[0][1]
            raise FactMergeError(
                "E_PARSE_AMBIGUOUS",
                reason,
                claim_id=claim_id,
                entity_id=entity_id,
                offsets=_error_offsets(first, normalized_fields[0][0]),
            )

    if interval is not None:
        unit = None
        if unit_field:
            unit = {"text": unit_field[0], "span": unit_field[1], "spec": None}
            candidates = lookup_unit(unit_field[0], config, "tenure")
            if candidates:
                unit["spec"] = candidates[0]
        elif interval.get("embedded_unit"):
            unit = interval["embedded_unit"]
        result = {
            "claim_id": claim_id,
            "entity_id": entity_id,
            "lang": str(claim.get("lang") or "und"),
            "category": "tenure",
            "kind": "month_interval",
            "field": chosen_field,
            "normalized_text": chosen.normalized if chosen else "",
            "source_text": chosen.original if chosen else "",
            "unit": unit,
            "interval": interval,
            "parse_source": interval["parse_source"],
            "status": "ok",
            "warnings": (["E_INTERVAL_OPEN_CONFLICT"] if interval.get("closure_conflict") else []),
        }
        return result

    assert chosen is not None and chosen_field is not None and number_data is not None
    try:
        if number_data["kind"] == "arabic":
            coefficient = _parse_arabic_number(
                number_data["token"],
                str(config["parse"].get("ambiguous_separator_policy", "strict_mixed")),
            )
        else:
            coefficient = parse_chinese_number(number_data["token"])
    except (ValueError, InvalidOperation) as exc:
        raise FactMergeError(
            "E_PARSE_AMBIGUOUS",
            f"ambiguous or invalid numeric separator: {exc}",
            claim_id=claim_id,
            entity_id=entity_id,
            offsets=chosen.span_dict(number_data["start"], number_data["end"], chosen_field),
        ) from exc

    embedded_scale = None
    if number_data.get("scale_token"):
        spec = _scale_spec(number_data["scale_token"], config)
        if spec is not None:
            embedded_scale = {
                **spec,
                "span": chosen.span_dict(number_data["scale_start"], number_data["scale_end"], chosen_field),
            }

    embedded_unit = _find_embedded_unit(chosen, int(number_data["after"]), config, category)
    unit: dict[str, Any] | None = None
    if unit_field:
        unit = {"text": unit_field[0], "span": unit_field[1], "spec": None}
        candidates = lookup_unit(unit_field[0], config, category)
        if not candidates:
            all_candidates = lookup_unit(unit_field[0], config)
            candidates = all_candidates
        if candidates:
            unit["spec"] = candidates[0]
    elif embedded_unit:
        unit = {"text": embedded_unit[0], "span": embedded_unit[1], "spec": embedded_unit[2]}

    if category is None and embedded_unit:
        category = embedded_unit[2]["category"]
    if category is None and unit and unit.get("spec"):
        category = unit["spec"]["category"]
    if category is None:
        offset_norm = chosen or normalized_fields[0][1]
        offset_field = chosen_field or normalized_fields[0][0]
        raise FactMergeError(
            "E_PARSE_AMBIGUOUS",
            "unable to determine the element category",
            claim_id=claim_id,
            entity_id=entity_id,
            offsets=_error_offsets(offset_norm, offset_field),
        )

    numeric_fragment = chosen.span_dict(number_data["start"], number_data["end"], chosen_field)
    result = {
        "claim_id": claim_id,
        "entity_id": entity_id,
        "lang": str(claim.get("lang") or "und"),
        "category": category,
        "kind": "scalar",
        "field": chosen_field,
        "normalized_text": chosen.normalized,
        "source_text": chosen.original,
        "numeric": {
            "coefficient": decimal_text(coefficient),
            "kind": number_data["kind"],
            "span": numeric_fragment,
        },
        "embedded_scale": embedded_scale,
        "embedded_unit": (
            {"text": embedded_unit[0], "span": embedded_unit[1], "spec": embedded_unit[2]}
            if embedded_unit
            else None
        ),
        "unit": unit,
        "parse_source": f"{chosen_field}.{number_data['kind']}",
        "status": "ok",
        "warnings": [],
        "unicode": {
            "form": "NFKC",
            "original_length": len(chosen.original),
            "normalized_length": len(chosen.normalized),
        },
    }
    return result


def parse_entity_claims(claims: Iterable[dict[str, Any]], config: dict[str, Any]) -> dict[str, Any]:
    parsed: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    for claim in sorted(claims, key=lambda row: str(row.get("claim_id", ""))):
        try:
            parsed.append(parse_claim(claim, config))
        except FactMergeError as exc:
            errors.append(exc.as_dict())
    return {"claims": parsed, "errors": errors}
