from __future__ import annotations

import re
from decimal import Decimal, ROUND_HALF_UP
from typing import Any

from .errors import E_DURATION_PARSE, error_record
from .text import normalize_text


_ARABIC_RE = re.compile(
    r"""
    (?P<approx>约|大约|大概|约莫|将近|about|approx\.?|~|≈)?
    \s*
    (?P<num>[+-]?(?:\d+(?:\.\d+)?|\.\d+))
    \s*
    (?P<unit>毫秒|milliseconds?|ms|秒钟|seconds?|secs?|秒|s|分钟|minutes?|mins?|分|m|帧|frames?|f)
    (?![A-Za-z])
    \s*
    (?P<half>半)?
    """,
    re.IGNORECASE | re.VERBOSE,
)

_CN_DIGITS = {
    "零": 0,
    "〇": 0,
    "一": 1,
    "二": 2,
    "两": 2,
    "三": 3,
    "四": 4,
    "五": 5,
    "六": 6,
    "七": 7,
    "八": 8,
    "九": 9,
}
_CN_SMALL_UNITS = {"十": 10, "百": 100, "千": 1000}
_CN_LARGE_UNITS = {"万": 10000}
_CN_NUM_CHARS = "零〇一二两三四五六七八九十百千万点"

_CN_TOKEN_RE = re.compile(
    rf"""
    (?P<num>半|[{_CN_NUM_CHARS}]+)
    \s*
    (?P<unit>毫秒|秒钟|秒|分钟|分|帧)
    (?P<half>半)?
    """,
    re.VERBOSE,
)

_CN_APPROX_RE = re.compile(r"(?:约|大约|大概|约莫|将近)\s*$")
_GENERIC_NUMBER_RE = re.compile(
    r"[+-]?(?:\d+(?:\.\d+)?|\.\d+)|[零〇一二两三四五六七八九十百千万点]+"
)


def _round_int(value: Decimal) -> int:
    return int(value.quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def _decimal_places(value: str) -> int:
    if "." in value:
        return len(value.split(".", 1)[1])
    return 0


def _parse_cn_integer(value: str) -> Decimal:
    if not value:
        raise ValueError("empty Chinese number")
    if value in _CN_DIGITS:
        return Decimal(_CN_DIGITS[value])
    if value == "半":
        return Decimal("0.5")

    if "点" in value:
        whole, fraction = value.split("点", 1)
        whole_value = _parse_cn_integer(whole) if whole else Decimal(0)
        if not fraction or any(ch not in _CN_DIGITS for ch in fraction):
            raise ValueError(f"invalid Chinese decimal: {value}")
        fraction_text = "".join(str(_CN_DIGITS[ch]) for ch in fraction)
        return whole_value + Decimal(f"0.{fraction_text}")

    total = Decimal(0)
    section = Decimal(0)
    number = Decimal(0)
    saw_digit = False
    for character in value:
        if character in _CN_DIGITS:
            number = Decimal(_CN_DIGITS[character])
            saw_digit = True
        elif character in _CN_SMALL_UNITS:
            if number == 0:
                number = Decimal(1)
            section += number * _CN_SMALL_UNITS[character]
            number = Decimal(0)
            saw_digit = True
        elif character in _CN_LARGE_UNITS:
            section += number
            total += section * _CN_LARGE_UNITS[character]
            section = Decimal(0)
            number = Decimal(0)
            saw_digit = True
        else:
            raise ValueError(f"invalid Chinese number: {value}")
    if not saw_digit:
        raise ValueError(f"invalid Chinese number: {value}")
    return total + section + number


def _unit_factor_ms(unit: str) -> tuple[Decimal, str]:
    lowered = unit.casefold()
    if lowered in {"毫秒", "millisecond", "milliseconds", "ms"}:
        return Decimal(1), "ms"
    if lowered in {"秒", "秒钟", "second", "seconds", "sec", "secs", "s"}:
        return Decimal(1000), "seconds"
    if lowered in {"分", "分钟", "minute", "minutes", "min", "mins", "m"}:
        return Decimal(60000), "minutes"
    if lowered in {"帧", "frame", "frames", "f"}:
        return Decimal(0), "frames"
    raise ValueError(f"unknown unit: {unit}")


def _precision_for_span(text: str, numeric_text: str, half: bool) -> int:
    precision = _decimal_places(numeric_text)
    if "点" in numeric_text:
        precision = max(precision, len(numeric_text.split("点", 1)[1]))
    if half:
        precision = max(precision, 1)
    return precision


def _arabic_phrase(match: re.Match[str], fps: Decimal) -> dict[str, Any] | None:
    unit = match.group("unit")
    try:
        factor, unit_kind = _unit_factor_ms(unit)
    except ValueError:
        return None
    number = Decimal(match.group("num"))
    half = bool(match.group("half"))
    if half:
        number += Decimal("0.5")
    if unit_kind == "frames":
        value = _round_int(number * Decimal(1000) / fps)
        source = "frames"
        precision = _decimal_places(match.group("num"))
    else:
        value = _round_int(number * factor)
        source = "arabic_unit"
        precision = _decimal_places(match.group("num"))
    if half:
        precision = max(precision, 1)
    return {
        "text": match.group(0),
        "start": match.start(),
        "end": match.end(),
        "value_ms": value,
        "source": source,
        "precision_digits": precision,
        "approximate": bool(match.group("approx")),
        "numeric_text": match.group("num"),
        "unit": unit,
    }


def _cn_phrase(
    match: re.Match[str],
    *,
    text: str,
    fps: Decimal,
) -> dict[str, Any] | None:
    number_text = match.group("num")
    unit = match.group("unit")
    half = bool(match.group("half"))
    try:
        factor, unit_kind = _unit_factor_ms(unit)
        number = _parse_cn_integer(number_text)
    except ValueError:
        return None
    if half:
        number += Decimal("0.5")
    if unit_kind == "frames":
        value = _round_int(number * Decimal(1000) / fps)
        source = "frames"
    else:
        value = _round_int(number * factor)
        source = "cn_quantifier"
    preceding = text[: match.start()]
    return {
        "text": match.group(0),
        "start": match.start(),
        "end": match.end(),
        "value_ms": value,
        "source": source,
        "precision_digits": _precision_for_span(match.group(0), number_text, half),
        "approximate": bool(_CN_APPROX_RE.search(preceding)),
        "numeric_text": number_text,
        "unit": unit,
    }


def _find_phrases(text: str, fps: Decimal) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for match in _ARABIC_RE.finditer(text):
        parsed = _arabic_phrase(match, fps)
        if parsed is not None:
            candidates.append(parsed)
    occupied = [(item["start"], item["end"]) for item in candidates]
    for match in _CN_TOKEN_RE.finditer(text):
        if any(match.start() < end and start < match.end() for start, end in occupied):
            continue
        parsed = _cn_phrase(match, text=text, fps=fps)
        if parsed is not None:
            candidates.append(parsed)
            occupied.append((parsed["start"], parsed["end"]))
    candidates.sort(key=lambda item: (item["start"], item["end"]))
    return candidates


def _uncovered_numeric_tokens(
    text: str, phrases: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    uncovered: list[dict[str, Any]] = []
    for match in _GENERIC_NUMBER_RE.finditer(text):
        if any(
            match.start() < phrase["end"] and phrase["start"] < match.end()
            for phrase in phrases
        ):
            continue
        uncovered.append(
            {
                "text": match.group(0),
                "start": match.start(),
                "end": match.end(),
            }
        )
    return uncovered


def parse_duration_phrase(
    shot_id: str,
    phrase: str,
    *,
    fps: int | float = 25,
) -> dict[str, Any]:
    """Parse one duration phrase into integer milliseconds."""
    normalized = normalize_text(phrase or "")
    fps_decimal = Decimal(str(fps))
    if fps_decimal <= 0:
        raise ValueError("fps must be positive")

    phrases = _find_phrases(normalized, fps_decimal)
    uncovered = _uncovered_numeric_tokens(normalized, phrases)

    if not normalized:
        return {
            "ok": False,
            "error": error_record(
                E_DURATION_PARSE,
                "duration",
                "duration phrase is empty",
                shot_id=shot_id,
                phrase_offset=0,
                phrase_offsets=[],
                reason="empty_phrase",
                normalized_phrase=normalized,
            ),
        }

    if uncovered:
        token = uncovered[0]
        return {
            "ok": False,
            "error": error_record(
                E_DURATION_PARSE,
                "duration",
                "duration phrase contains an unknown or unsupported unit",
                shot_id=shot_id,
                phrase_offset=token["start"],
                phrase_offsets=[item["start"] for item in uncovered],
                reason="unknown_unit",
                offending_text=token["text"],
                normalized_phrase=normalized,
            ),
        }

    if not phrases:
        return {
            "ok": False,
            "error": error_record(
                E_DURATION_PARSE,
                "duration",
                "no supported duration expression was found",
                shot_id=shot_id,
                phrase_offset=0,
                phrase_offsets=[],
                reason="no_duration_expression",
                normalized_phrase=normalized,
            ),
        }

    non_positive = [item for item in phrases if item["value_ms"] <= 0]
    if non_positive:
        return {
            "ok": False,
            "error": error_record(
                E_DURATION_PARSE,
                "duration",
                "duration must be greater than zero",
                shot_id=shot_id,
                phrase_offset=non_positive[0]["start"],
                phrase_offsets=[item["start"] for item in non_positive],
                reason="non_positive_duration",
                parsed_values=[item["value_ms"] for item in phrases],
                normalized_phrase=normalized,
            ),
        }

    unique_values = sorted({item["value_ms"] for item in phrases})
    if len(unique_values) > 1:
        return {
            "ok": False,
            "error": error_record(
                E_DURATION_PARSE,
                "duration",
                "conflicting duration phrases were supplied",
                shot_id=shot_id,
                phrase_offset=phrases[0]["start"],
                phrase_offsets=[item["start"] for item in phrases],
                reason="conflicting_durations",
                parsed_values=[item["value_ms"] for item in phrases],
                normalized_phrase=normalized,
            ),
        }

    chosen = phrases[0]
    return {
        "ok": True,
        "result": {
            "duration_ms": chosen["value_ms"],
            "source": chosen["source"],
            "precision_digits": max(item["precision_digits"] for item in phrases),
            "phrase_offset": chosen["start"],
            "phrase_offsets": [item["start"] for item in phrases],
            "phrases": phrases,
            "normalized_phrase": normalized,
            "fps": int(fps) if float(fps).is_integer() else fps,
        },
    }
