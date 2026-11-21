from __future__ import annotations

import datetime as dt
import re
from decimal import Decimal
from typing import Any

from .canonical import canonical_json, fixed_decimal, read_jsonl, sha256_bytes
from .errors import BusinessError
from .normalization import ClauseNormalizer, utf8_byte_to_codepoint_offset


_NUMBER_RE = re.compile(r"(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?")
_TERM_CANONICAL_RE = re.compile(r"(business_day|day|week|month|year):(\d+)")


def _amount_from_v1(value: Any, config: dict[str, Any]) -> str:
    text = str(value).strip()
    number = _NUMBER_RE.search(text)
    if number is None:
        raise ValueError("v1 amount value has no numeric literal")
    currency = str(config.get("default_currency", "CNY"))
    upper = text.upper()
    if "USD" in upper or "美元" in text:
        currency = "USD"
    elif "HKD" in upper or "港元" in text:
        currency = "HKD"
    amount = Decimal(number.group(0).replace(",", ""))
    scale = int(config.get("amount_scale", 2))
    return f"{currency}:{fixed_decimal(amount, scale)}"


def _ratio_from_v1(value: Any, config: dict[str, Any]) -> str:
    text = str(value).strip()
    number = re.search(r"\d+(?:\.\d+)?", text)
    if number is None:
        raise ValueError("v1 ratio value has no numeric literal")
    ratio = Decimal(number.group(0))
    if "%" in text:
        ratio /= Decimal(100)
    elif "‰" in text:
        ratio /= Decimal(1000)
    elif "万分之" in text:
        ratio /= Decimal(10000)
    elif "千分之" in text:
        ratio /= Decimal(1000)
    scale = int(config.get("ratio_scale", 6))
    return fixed_decimal(ratio, scale)


def _term_from_v1(value: Any, raw_text: str) -> str:
    text = str(value).strip()
    canonical = _TERM_CANONICAL_RE.fullmatch(text)
    if canonical:
        return f"{canonical.group(1)}:{int(canonical.group(2))}"
    source = f"{text} {raw_text}".lower()
    number = re.search(r"\d+", source)
    if number is None:
        raise ValueError("v1 term value has no numeric literal")
    if "business" in source or "workday" in source or "工作日" in source:
        unit = "business_day"
    elif "month" in source or "个月" in source:
        unit = "month"
    elif "year" in source or "年" in source:
        unit = "year"
    elif "week" in source or "周" in source:
        unit = "week"
    elif "day" in source or "日" in source or "天" in source:
        unit = "day"
    else:
        raise ValueError("v1 term value has no supported unit")
    return f"{unit}:{int(number.group(0))}"


def _date_from_v1(value: Any) -> str:
    text = str(value).strip()
    try:
        parsed = dt.date.fromisoformat(text)
    except ValueError as exc:
        raise ValueError("v1 date value is not ISO YYYY-MM-DD") from exc
    if parsed.isoformat() != text:
        raise ValueError("v1 date value is not canonical ISO")
    return text


def _normalize_v1_value(element_type: str, value: Any, raw_text: str, config: dict[str, Any]) -> str:
    if element_type == "amount":
        return _amount_from_v1(value, config)
    if element_type == "date":
        return _date_from_v1(value)
    if element_type == "term":
        return _term_from_v1(value, raw_text)
    if element_type == "penalty_ratio":
        return _ratio_from_v1(value, config)
    raise ValueError(f"unsupported element type: {element_type}")


def _format_amount_for_v1(normalized: str, default_currency: str) -> str:
    if ":" not in normalized:
        raise ValueError("invalid normalized amount")
    currency, number = normalized.split(":", 1)
    sign = ""
    if number.startswith("-"):
        sign, number = "-", number[1:]
    integer, dot, fraction = number.partition(".")
    groups: list[str] = []
    while integer:
        groups.append(integer[-3:])
        integer = integer[:-3]
    formatted = ",".join(reversed(groups))
    if dot:
        formatted += "." + fraction
    formatted = sign + formatted
    if currency != default_currency:
        return f"{currency} {formatted}"
    return formatted


def _format_value_for_v1(element_type: str, normalized: str, config: dict[str, Any]) -> str:
    if element_type == "amount":
        return _format_amount_for_v1(normalized, str(config.get("default_currency", "CNY")))
    return normalized


class V1Interop:
    def __init__(self, config: dict[str, Any], normalizer: ClauseNormalizer) -> None:
        self.config = config
        self.normalizer = normalizer

    def _offset(self, raw: str, value: Any) -> int:
        offset = int(value)
        basis = self.config.get("interop", {}).get("offset_basis", "codepoint")
        if basis == "utf8_byte":
            return utf8_byte_to_codepoint_offset(raw, offset)
        if basis != "codepoint":
            raise BusinessError("E_INTEROP", "", "interop_import", "unsupported offset basis", {"offset_basis": basis})
        return offset

    def import_rows(self, rows: list[dict[str, Any]]) -> dict[str, Any]:
        contracts: list[dict[str, Any]] = []
        for row_index, row in enumerate(rows):
            version = row.get("structure_version")
            if version != 1:
                raise BusinessError(
                    "E_INTEROP",
                    str(row.get("contract_id", "")),
                    "interop_import",
                    "annotation row is not structure_version 1",
                    {"structure_version": version, "row_index": row_index},
                )
            contract_id = str(row.get("contract_id", ""))
            template_id = str(row.get("template_id", ""))
            clauses_in = row.get("clauses")
            if not isinstance(clauses_in, list):
                raise BusinessError("E_INTEROP", contract_id, "interop_import", "v1 row must contain clauses", {"row_index": row_index})
            imported_clauses: list[dict[str, Any]] = []
            for clause_index, clause in enumerate(clauses_in):
                raw = clause.get("clause_text")
                if not isinstance(raw, str):
                    raise BusinessError("E_INTEROP", contract_id, "interop_import", "v1 clause_text must be a string", {"clause_index": clause_index})
                elements = clause.get("elements", clause.get("annotations", []))
                if not isinstance(elements, list):
                    raise BusinessError("E_INTEROP", contract_id, "interop_import", "v1 elements must be a list", {"clause_index": clause_index})
                synthetic_annotations: list[dict[str, Any]] = []
                for element in elements:
                    element_type = element.get("type", element.get("element_type"))
                    allowed = set(self.config.get("experiment", {}).get("element_types", []))
                    if element_type not in allowed:
                        raise BusinessError("E_UNKNOWN_ELEMENT", contract_id, "interop_import", "v1 element type is not in the element table", {"element": element_type, "element_type": element_type, "clause_index": clause_index})
                    start = self._offset(raw, element.get("start"))
                    end = self._offset(raw, element.get("end"))
                    raw_text = raw[start:end]
                    try:
                        normalized_value = _normalize_v1_value(
                            element_type,
                            element.get("value"),
                            raw_text,
                            self.config.get("interop", self.config),
                        )
                    except ValueError as exc:
                        raise BusinessError("E_INTEROP", contract_id, "interop_import", "v1 element value cannot be converted", {"element_type": element_type, "value": element.get("value"), "reason": str(exc)}) from exc
                    synthetic_annotations.append({
                        "element_type": element_type,
                        "start": start,
                        "end": end,
                        "value": element.get("value"),
                        "normalized_value": normalized_value,
                    })
                synthetic = {
                    "contract_id": contract_id,
                    "template_id": template_id,
                    "clause_text": raw,
                    "annotations": synthetic_annotations,
                    "effective_date": row.get("effective_date"),
                }
                if "clause_no" in clause and clause.get("clause_no") not in (None, ""):
                    synthetic["clause_no"] = clause["clause_no"]
                try:
                    normalized = self.normalizer.normalize_record(synthetic, clause_index)
                except BusinessError:
                    raise
                for unit in normalized["units"]:
                    source_text = raw[unit["source_start"]:unit["source_end"]]
                    unit_annotations = []
                    for annotation in unit.get("annotations", []):
                        local_start = annotation["source_start"] - unit["source_start"]
                        local_end = annotation["source_end"] - unit["source_start"]
                        unit_annotations.append({
                            "element_type": annotation["element_type"],
                            "start": local_start,
                            "end": local_end,
                            "text": annotation["text"],
                            "value": annotation["value"],
                            "normalized_value": annotation["normalized_value"],
                        })
                    imported_clauses.append({
                        "clause_no": unit["clause_no"],
                        "unit_index": unit["unit_index"],
                        "clause_text": source_text,
                        "annotations": sorted(unit_annotations, key=lambda item: (item["element_type"], item["start"], item["end"], item["normalized_value"])),
                    })
            contracts.append({
                "contract_id": contract_id,
                "template_id": template_id,
                "effective_date": row.get("effective_date"),
                "clauses": imported_clauses,
            })
        return {
            "schema_version": "contract-elements-interop/2",
            "structure_version": 2,
            "offset_basis": "codepoint",
            "contracts": contracts,
        }

    def export_rows(self, structure: dict[str, Any]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        default_currency = str(
            self.config.get("interop", self.config).get("default_currency", "CNY")
        )
        for contract in structure.get("contracts", []):
            clauses_out: list[dict[str, Any]] = []
            for clause in contract.get("clauses", []):
                elements_out = []
                seen: set[tuple[Any, ...]] = set()
                for annotation in clause.get("annotations", []):
                    element_type = annotation["element_type"]
                    value = _format_value_for_v1(
                        element_type,
                        str(annotation["normalized_value"]),
                        {"default_currency": default_currency},
                    )
                    key = (element_type, int(annotation["start"]), int(annotation["end"]), value)
                    if key in seen:
                        continue
                    seen.add(key)
                    elements_out.append({"type": element_type, "start": int(annotation["start"]), "end": int(annotation["end"]), "value": value})
                elements_out.sort(key=lambda item: (item["start"], item["type"], item["end"], item["value"]))
                clauses_out.append({
                    "clause_no": int(clause["clause_no"]),
                    "clause_text": clause["clause_text"],
                    "elements": elements_out,
                })
            row: dict[str, Any] = {
                "structure_version": 1,
                "contract_id": contract["contract_id"],
                "template_id": contract["template_id"],
                "clauses": clauses_out,
            }
            if contract.get("effective_date") not in (None, ""):
                row["effective_date"] = contract["effective_date"]
            rows.append(row)
        rows.sort(key=lambda item: item["contract_id"])
        return rows

    def canonical_rows_bytes(self, rows: list[dict[str, Any]]) -> bytes:
        return b"".join((canonical_json(row) + "\n").encode("utf-8") for row in rows)

    def compatibility(self, rows: list[dict[str, Any]]) -> dict[str, Any]:
        first_structure = self.import_rows(rows)
        first_export = self.export_rows(first_structure)
        first_bytes = self.canonical_rows_bytes(first_export)
        second_structure = self.import_rows(first_export)
        second_export = self.export_rows(second_structure)
        second_bytes = self.canonical_rows_bytes(second_export)
        internal_same = canonical_json(first_structure) == canonical_json(second_structure)
        success = first_bytes == second_bytes and internal_same
        successful_records = 0
        failures: list[dict[str, Any]] = []
        for row in rows:
            try:
                one_first = self.export_rows(self.import_rows([row]))
                one_first_bytes = self.canonical_rows_bytes(one_first)
                one_second_structure = self.import_rows(one_first)
                one_first_structure = self.import_rows([row])
                one_second = self.export_rows(one_second_structure)
                one_second_bytes = self.canonical_rows_bytes(one_second)
                if one_first_bytes == one_second_bytes and canonical_json(one_first_structure) == canonical_json(one_second_structure):
                    successful_records += 1
                else:
                    failures.append({"contract_id": row.get("contract_id"), "reason": "roundtrip_bytes_differ"})
            except Exception as exc:
                failures.append({"contract_id": row.get("contract_id"), "reason": str(exc)})
        return {
            "success": success,
            "input_records": len(rows),
            "successful_records": successful_records,
            "failures": failures,
            "converted_records": len(first_structure["contracts"]),
            "first_export_sha256": sha256_bytes(first_bytes),
            "second_export_sha256": sha256_bytes(second_bytes),
            "internal_structure_bytes_identical": internal_same,
        }


def load_v1(path: str) -> list[dict[str, Any]]:
    return read_jsonl(path)

