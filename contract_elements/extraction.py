from __future__ import annotations

import datetime as dt
import re
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Callable, Iterable

import numpy as np

from .canonical import fixed_decimal, short_hash
from .errors import BusinessError
from .normalization import ClauseNormalizer, _chinese_to_int


_AMOUNT_NUMBER = r"(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?"
_MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11,
    "december": 12, "jan": 1, "feb": 2, "mar": 3, "apr": 4, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "sept": 9, "oct": 10, "nov": 11, "dec": 12,
}
_MONTH_PATTERN = "|".join(sorted(_MONTHS, key=lambda item: (-len(item), item)))


@dataclass(frozen=True)
class RegexRule:
    rule_id: str
    element_type: str
    pattern: re.Pattern[str]
    parser: Callable[[re.Match[str], dict[str, Any]], str]


def _decimal_to_string(value: Decimal, scale: int, max_abs: str) -> str:
    return fixed_decimal(value, scale, max_abs=max_abs)


def _parse_amount(match: re.Match[str], config: dict[str, Any]) -> str:
    text = match.group(0)
    numeric = re.search(_AMOUNT_NUMBER, text)
    if numeric is None:
        raise ValueError("amount pattern did not contain a number")
    upper = text.upper()
    currency = str(config.get("default_currency", "CNY"))
    if "USD" in upper or "美元" in text:
        currency = "USD"
    elif "HKD" in upper or "港元" in text:
        currency = "HKD"
    elif any(token in text for token in ("人民币", "CNY", "RMB", "￥", "¥", "元")):
        currency = "CNY"
    factor = Decimal(1)
    if "亿元" in text:
        factor = Decimal("100000000")
    elif "万元" in text:
        factor = Decimal("10000")
    try:
        value = Decimal(numeric.group(0).replace(",", "")) * factor
    except Exception as exc:
        raise ValueError("invalid amount literal") from exc
    quantized = _decimal_to_string(value, int(config["amount_scale"]), str(config["max_abs"]))
    return f"{currency}:{quantized}"


def _parse_date(match: re.Match[str], config: dict[str, Any]) -> str:
    text = match.group(0)
    chinese = re.fullmatch(r"(\d{4})年(\d{1,2})月(\d{1,2})日", text)
    if chinese:
        year, month, day = (int(item) for item in chinese.groups())
        return dt.date(year, month, day).isoformat()
    iso = re.fullmatch(r"(\d{4})[-/](\d{1,2})[-/](\d{1,2})", text)
    if iso:
        year, month, day = (int(item) for item in iso.groups())
        return dt.date(year, month, day).isoformat()
    month_first = re.fullmatch(r"([A-Za-z]+)\s+(\d{1,2}),\s*(\d{4})", text)
    if month_first:
        month_name, day, year = month_first.groups()
        month = _MONTHS.get(month_name.lower())
        if month is None:
            raise ValueError("unknown English month")
        return dt.date(int(year), month, int(day)).isoformat()
    day_first = re.fullmatch(r"(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})", text)
    if day_first:
        day, month_name, year = day_first.groups()
        month = _MONTHS.get(month_name.lower())
        if month is None:
            raise ValueError("unknown English month")
        return dt.date(int(year), month, int(day)).isoformat()
    raise ValueError("unsupported date format")


def _number_literal(value: str) -> int:
    if value.isdigit():
        return int(value)
    return _chinese_to_int(value)


def _parse_term(match: re.Match[str], config: dict[str, Any]) -> str:
    text = match.group(0).strip()
    numeric = re.search(r"\d+|[一二三四五六七八九十百千零〇两]+", text)
    if numeric is None:
        raise ValueError("term pattern did not contain a number or numeral")
    value = _number_literal(numeric.group(0))
    lowered = text.lower()
    if "工作日" in text or "business" in lowered or "workday" in lowered:
        unit = "business_day"
    elif re.search(r"(?:个月|月|months?|month)", lowered):
        unit = "month"
    elif re.search(r"(?:年|years?|year)", lowered):
        unit = "year"
    elif re.search(r"(?:周|weeks?|week)", lowered):
        unit = "week"
    elif re.search(r"(?:日|天|days?|day)", lowered):
        unit = "day"
    else:
        raise ValueError("unsupported term unit")
    return f"{unit}:{value}"


def _parse_ratio(match: re.Match[str], config: dict[str, Any]) -> str:
    text = match.group(0)
    value: Decimal
    if "万分之" in text:
        number = re.search(r"[零〇一二三四五六七八九十百\d.]+", text.split("万分之", 1)[1])
        if number is None:
            raise ValueError("missing ratio number")
        value = Decimal(_number_literal(number.group(0))) / Decimal(10000)
    elif "千分之" in text:
        number = re.search(r"[零〇一二三四五六七八九十百\d.]+", text.split("千分之", 1)[1])
        if number is None:
            raise ValueError("missing ratio number")
        value = Decimal(_number_literal(number.group(0))) / Decimal(1000)
    elif "百分之" in text:
        number = re.search(r"[零〇一二三四五六七八九十百\d.]+", text.split("百分之", 1)[1])
        if number is None:
            raise ValueError("missing ratio number")
        value = Decimal(_number_literal(number.group(0))) / Decimal(100)
    elif "‰" in text:
        number = re.search(r"\d+(?:\.\d+)?", text)
        if number is None:
            raise ValueError("missing ratio number")
        value = Decimal(number.group(0)) / Decimal(1000)
    elif "%" in text:
        number = re.search(r"\d+(?:\.\d+)?", text)
        if number is None:
            raise ValueError("missing ratio number")
        value = Decimal(number.group(0)) / Decimal(100)
    else:
        number = re.search(r"\d+(?:\.\d+)?", text)
        if number is None:
            raise ValueError("missing ratio number")
        value = Decimal(number.group(0))
        if "percent" in text.lower() or "per cent" in text.lower():
            value = value / Decimal(100)
    return _decimal_to_string(value, int(config["ratio_scale"]), str(config["max_abs"]))


def _compile_rule(rule_id: str, element_type: str, pattern: str, parser: Callable[..., str]) -> RegexRule:
    return RegexRule(rule_id, element_type, re.compile(pattern, re.IGNORECASE), parser)


REGEX_RULES: tuple[RegexRule, ...] = (
    _compile_rule("AMT_CNY_PREFIX", "amount", rf"(?:人民币|CNY|RMB)\s*[￥¥]?\s*{_AMOUNT_NUMBER}(?:\s*(?:亿元|万元|元|美元|港元|USD|HKD))?", _parse_amount),
    _compile_rule("AMT_UNIT_SUFFIX", "amount", rf"(?<![\d.,]){_AMOUNT_NUMBER}\s*(?:亿元|万元|元)", _parse_amount),
    _compile_rule("AMT_SYMBOL", "amount", rf"[￥¥]\s*{_AMOUNT_NUMBER}(?:\s*(?:亿元|万元|元))?", _parse_amount),
    _compile_rule("AMT_USD_HKD", "amount", rf"(?:USD|HKD)\s*{_AMOUNT_NUMBER}(?:\s*(?:美元|港元))?", _parse_amount),
    _compile_rule("AMT_NONFINITE", "amount", r"(?:人民币|CNY|RMB|USD|HKD)\s*(?:NaN|nan|Infinity|inf)(?:\s*(?:元|美元|港元))?", _parse_amount),
    _compile_rule("DATE_CN", "date", r"\d{4}年\d{1,2}月\d{1,2}日", _parse_date),
    _compile_rule("DATE_ISO", "date", r"(?<!\d)\d{4}[-/]\d{1,2}[-/]\d{1,2}(?!\d)", _parse_date),
    _compile_rule("DATE_EN_MDY", "date", rf"(?:{_MONTH_PATTERN})\s+\d{{1,2}},\s*\d{{4}}", _parse_date),
    _compile_rule("DATE_EN_DMY", "date", rf"\d{{1,2}}\s+(?:{_MONTH_PATTERN})\s+\d{{4}}", _parse_date),
    _compile_rule("TERM_NUMERIC_CN", "term", r"(?<![\d年月])\d+\s*(?:个工作日|工作日|日|天|周|个月|月|年)(?!\s*\d{1,2}\s*[月日])", _parse_term),
    _compile_rule("TERM_CHINESE_CN", "term", r"(?<![\d年月])[一二三四五六七八九十百零〇两]+\s*(?:个工作日|工作日|日|天|周|个月|月|年)(?!\s*\d{1,2}\s*[月日])", _parse_term),
    _compile_rule("TERM_EN", "term", r"(?<![\d-])\d+\s*(?:business\s+days?|calendar\s+days?|workdays?|days?|weeks?|months?|years?)", _parse_term),
    _compile_rule("RATIO_PERCENT", "penalty_ratio", r"(?<![\d.])\d+(?:\.\d+)?\s*%", _parse_ratio),
    _compile_rule("RATIO_PERMILLE", "penalty_ratio", r"(?<![\d.])\d+(?:\.\d+)?\s*‰", _parse_ratio),
    _compile_rule("RATIO_CHINESE_PHRASE", "penalty_ratio", r"(?:万分之|千分之|百分之)[零〇一二三四五六七八九十百\d.]+", _parse_ratio),
    _compile_rule("RATIO_ENGLISH", "penalty_ratio", r"(?<![\d.])\d+(?:\.\d+)?\s*(?:percent|per\s+cent)\b", _parse_ratio),
    _compile_rule("RATIO_NONFINITE", "penalty_ratio", r"(?<![\d.])(?:NaN|nan|Infinity|inf)\s*%", _parse_ratio),
)


class DictionaryStore:
    def __init__(self, entries: Iterable[dict[str, Any]]) -> None:
        self.entries = sorted(
            (dict(entry) for entry in entries),
            key=lambda entry: (entry["entry_id"], entry["element_type"], entry["surface"]),
        )

    def candidate_entries(self) -> list[dict[str, Any]]:
        return [entry for entry in self.entries if entry.get("mode") == "candidate"]

    def evidence_for(self, element_type: str, text: str, start: int, end: int) -> list[str]:
        span = text[start:end].lower()
        result: list[str] = []
        for entry in self.entries:
            if entry["element_type"] == element_type and entry["surface"].lower() in span:
                result.append(entry["entry_id"])
        return sorted(set(result))


class RuleEngine:
    def __init__(self, dictionary: DictionaryStore, config: dict[str, Any]) -> None:
        self.dictionary = dictionary
        self.config = config

    def _parse_candidate(self, unit: dict[str, Any], element_type: str, start: int, end: int,
                         parser: Callable[[re.Match[str], dict[str, Any]], str], match: re.Match[str]):
        try:
            return parser(match, self.config), None
        except (ValueError, ArithmeticError, OverflowError) as exc:
            code = "E_NUMERIC" if element_type in {"amount", "penalty_ratio"} else "E_INVALID_DATE"
            error = BusinessError(code, unit["contract_id"], "extract", f"failed to normalize {element_type} candidate", {
                "element": element_type, "element_type": element_type, "text": unit["text"][start:end],
                "start": start, "end": end, "reason": str(exc),
            }).to_dict()
            return None, error

    def raw_candidates(self, unit: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        text = unit["text"]
        candidates: dict[tuple[Any, ...], dict[str, Any]] = {}
        errors: list[dict[str, Any]] = []

        def add_candidate(element_type: str, start: int, end: int, normalized_value: str | None,
                          regex_id: str | None = None, dictionary_id: str | None = None,
                          error: dict[str, Any] | None = None) -> None:
            key = (element_type, start, end, normalized_value, error is not None)
            candidate = candidates.get(key)
            if candidate is None:
                candidate = {"element_type": element_type, "start": start, "end": end,
                             "text": text[start:end], "normalized_value": normalized_value,
                             "valid": error is None, "regex_ids": [], "dictionary_ids": [], "errors": []}
                candidates[key] = candidate
            if regex_id is not None:
                candidate["regex_ids"].append(regex_id)
            if dictionary_id is not None:
                candidate["dictionary_ids"].append(dictionary_id)
            if error is not None:
                candidate["errors"].append(error)
                if error not in errors:
                    errors.append(error)

        for rule in REGEX_RULES:
            for match in rule.pattern.finditer(text):
                normalized, error = self._parse_candidate(unit, rule.element_type, match.start(), match.end(), rule.parser, match)
                add_candidate(rule.element_type, match.start(), match.end(), normalized, regex_id=rule.rule_id, error=error)

        for entry in self.dictionary.candidate_entries():
            surface = entry["surface"]
            lowered = text.lower()
            start = 0
            while True:
                index = lowered.find(surface.lower(), start)
                if index < 0:
                    break
                end = index + len(surface)
                add_candidate(entry["element_type"], index, end, str(entry["normalized_value"]), dictionary_id=entry["entry_id"])
                start = end

        date_spans = [(candidate["start"], candidate["end"]) for candidate in candidates.values() if candidate["element_type"] == "date"]
        filtered: list[dict[str, Any]] = []
        for candidate in candidates.values():
            if candidate["element_type"] == "term" and any(candidate["start"] < end and candidate["end"] > start for start, end in date_spans):
                continue
            evidence_ids = self.dictionary.evidence_for(candidate["element_type"], text, candidate["start"], candidate["end"])
            candidate["dictionary_ids"] = sorted(set(candidate["dictionary_ids"]).union(evidence_ids))
            candidate["regex_ids"] = sorted(set(candidate["regex_ids"]))
            filtered.append(candidate)
        filtered.sort(key=lambda item: (item["start"], item["element_type"], item["end"], "" if item["normalized_value"] is None else item["normalized_value"]))
        return filtered, errors


class SeedEstimator:
    def __init__(self, normalizer: ClauseNormalizer, dictionary: DictionaryStore, extraction_config: dict[str, Any]) -> None:
        self.normalizer = normalizer
        self.dictionary = dictionary
        self.config = extraction_config
        self.engine = RuleEngine(dictionary, extraction_config)

    def _gold_value(self, annotation: dict[str, Any]) -> str:
        if annotation.get("normalized_value") is not None:
            return str(annotation["normalized_value"])
        element_type = annotation["element_type"]
        text = annotation.get("text", "")
        rule = {
            "amount": _compile_rule("ANNOTATION_AMOUNT", "amount", _AMOUNT_NUMBER, _parse_amount),
            "date": _compile_rule("ANNOTATION_DATE", "date", r".+", _parse_date),
            "term": _compile_rule("ANNOTATION_TERM", "term", r".+", _parse_term),
            "penalty_ratio": _compile_rule("ANNOTATION_RATIO", "penalty_ratio", r".+", _parse_ratio),
        }[element_type]
        match = rule.pattern.search(text)
        if match is None:
            raise BusinessError("E_INTEROP", annotation.get("contract_id", ""), "extract", "seed annotation value cannot be normalized", {"element_type": element_type, "text": text})
        return rule.parser(match, self.config)

    def estimate(self, seed_records: list[dict[str, Any]]) -> dict[str, Any]:
        stats: dict[str, dict[str, dict[str, int]]] = {"regex": {}, "dictionary": {}}
        seed_units = 0
        for index, record in enumerate(seed_records):
            normalized = self.normalizer.normalize_record(record, index)
            seed_units += len(normalized["units"])
            for unit in normalized["units"]:
                candidates, _ = self.engine.raw_candidates(unit)
                labeled = [(annotation["element_type"], annotation["start"], annotation["end"], self._gold_value(annotation)) for annotation in unit.get("annotations", [])]
                for candidate in candidates:
                    true_positive = any(candidate["start"] < end and candidate["end"] > start and candidate["element_type"] == element_type and candidate["normalized_value"] == value for element_type, start, end, value in labeled)
                    for rule_id in candidate["regex_ids"]:
                        bucket = stats["regex"].setdefault(rule_id, {"tp": 0, "total": 0})
                        bucket["total"] += 1
                        bucket["tp"] += int(true_positive)
                    for dictionary_id in candidate["dictionary_ids"]:
                        bucket = stats["dictionary"].setdefault(dictionary_id, {"tp": 0, "total": 0})
                        bucket["total"] += 1
                        bucket["tp"] += int(true_positive)

        def finalize(bucket_map: dict[str, dict[str, int]], kind: str) -> dict[str, dict[str, Any]]:
            result: dict[str, dict[str, Any]] = {}
            for identifier in sorted(bucket_map):
                counts = bucket_map[identifier]
                total, tp = counts["total"], counts["tp"]
                precision = float(np.divide(np.asarray([tp], dtype=np.float64), np.asarray([total], dtype=np.float64))[0]) if total else 0.65
                weight = 0.5 + 0.5 * float(np.clip(precision, 0.0, 1.0))
                override_key = "regex_weight_overrides" if kind == "regex" else "dictionary_weight_overrides"
                override = self.config.get(override_key, {}).get(identifier)
                if override is not None:
                    weight = float(override)
                result[identifier] = {"tp": tp, "total": total, "precision": f"{precision:.6f}", "weight": f"{max(0.0, min(1.0, weight)):.6f}"}
            return result

        return {"schema_version": 1, "seed_records": len(seed_records), "seed_units": seed_units,
                "regex_rules": finalize(stats["regex"], "regex"),
                "dictionary_entries": finalize(stats["dictionary"], "dictionary")}


def _confidence(regex_weights: list[float], dictionary_weights: list[float]) -> tuple[str, dict[str, str]]:
    regex_score = float(np.mean(np.asarray(regex_weights, dtype=np.float64))) if regex_weights else 0.0
    dictionary_score = float(np.mean(np.asarray(dictionary_weights, dtype=np.float64))) if dictionary_weights else 0.0
    coefficients = np.asarray([0.45, 0.35, 0.20], dtype=np.float64)
    features = np.asarray([1.0, regex_score, dictionary_score], dtype=np.float64)
    confidence = float(np.clip(np.dot(coefficients, features), 0.0, 1.0))
    return f"{confidence:.6f}", {"regex_score": f"{regex_score:.6f}", "dictionary_score": f"{dictionary_score:.6f}"}


class ElementExtractor:
    def __init__(self, model: dict[str, Any], dictionary: DictionaryStore, config: dict[str, Any]) -> None:
        self.model = model
        self.dictionary = dictionary
        self.config = config
        self.engine = RuleEngine(dictionary, config)
        self.threshold = Decimal(str(config.get("confidence_threshold", "0.5")))

    def _normalize_annotation(self, annotation: dict[str, Any], unit: dict[str, Any]) -> dict[str, Any]:
        result = dict(annotation)
        if result.get("normalized_value") is not None:
            return result
        element_type = result["element_type"]
        text = result.get("text", "")
        raw_value = result.get("value")
        parser_rule = {
            "amount": _compile_rule("ANNOTATION_AMOUNT", "amount", _AMOUNT_NUMBER, _parse_amount),
            "date": _compile_rule("ANNOTATION_DATE", "date", r".+", _parse_date),
            "term": _compile_rule("ANNOTATION_TERM", "term", r".+", _parse_term),
            "penalty_ratio": _compile_rule("ANNOTATION_RATIO", "penalty_ratio", r".+", _parse_ratio),
        }.get(element_type)
        if parser_rule is None:
            raise BusinessError("E_UNKNOWN_ELEMENT", unit["contract_id"], "extract", "annotation element type is not in the element table", {"element": element_type, "element_type": element_type})
        source = text if text else str(raw_value if raw_value is not None else "")
        match = parser_rule.pattern.search(source)
        if match is None:
            raise BusinessError("E_INTEROP", unit["contract_id"], "extract", "annotation value cannot be normalized", {"element_type": element_type, "value": raw_value, "text": text})
        result["normalized_value"] = parser_rule.parser(match, self.config)
        return result

    def extract_unit(self, unit: dict[str, Any], effective_date: str | None = None) -> dict[str, Any]:
        raw_candidates, raw_errors = self.engine.raw_candidates(unit)
        regex_model = self.model.get("regex_rules", {})
        dictionary_model = self.model.get("dictionary_entries", {})
        candidates: list[dict[str, Any]] = []
        errors = list(raw_errors)
        for raw in raw_candidates:
            regex_evidence = sorted(set(raw["regex_ids"]))
            dictionary_evidence = sorted(set(raw["dictionary_ids"]))
            regex_weights = [float(regex_model.get(identifier, {}).get("weight", "0.650000")) for identifier in regex_evidence]
            dictionary_weights = [float(dictionary_model.get(identifier, {}).get("weight", "0.650000")) for identifier in dictionary_evidence]
            confidence, scores = _confidence(regex_weights, dictionary_weights)
            evidence = [{"kind": "regex", "id": identifier, "weight": f"{weight:.6f}"} for identifier, weight in zip(regex_evidence, regex_weights)]
            evidence += [{"kind": "dictionary", "id": identifier, "weight": f"{weight:.6f}"} for identifier, weight in zip(dictionary_evidence, dictionary_weights)]
            identity = {"clause_id": unit["clause_id"], "element_type": raw["element_type"], "start": raw["start"], "end": raw["end"], "normalized_value": raw["normalized_value"], "evidence": evidence}
            candidate = {
                "candidate_id": f"{unit['clause_id']}:{raw['element_type']}:{short_hash(identity, 12)}",
                "contract_id": unit["contract_id"], "clause_id": unit["clause_id"],
                "clause_no": unit["clause_no"], "unit_index": unit["unit_index"],
                "template_id": unit["template_id"],
                "element_type": raw["element_type"], "start": raw["start"], "end": raw["end"],
                "source_start": raw["start"], "source_end": raw["end"],
                "offset_basis": "normalized_clause_codepoint", "text": raw["text"],
                "normalized_value": raw["normalized_value"], "confidence": confidence,
                "score_components": scores, "evidence": evidence, "valid": raw["valid"],
                "candidate_errors": raw["errors"],
            }
            candidates.append(candidate)
            if not raw["valid"]:
                for error in raw["errors"]:
                    if error not in errors:
                        errors.append(error)

        annotations: list[dict[str, Any]] = []
        for annotation in unit.get("annotations", []):
            try:
                annotations.append(self._normalize_annotation(annotation, unit))
            except BusinessError as error:
                errors.append(error.to_dict())
        candidates.sort(key=lambda item: (item["element_type"], item["start"], item["end"], item["normalized_value"] or "", item["candidate_id"]))
        return {"type": "extracted_clause", "contract_id": unit["contract_id"],
                "clause_id": unit["clause_id"], "clause_no": unit["clause_no"],
                "unit_index": unit["unit_index"], "template_id": unit["template_id"],
                "text": unit["text"], "candidates": candidates,
                "annotations": annotations, "errors": errors}
