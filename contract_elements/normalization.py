from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from typing import Any, Iterable

from .canonical import sha256_value, short_hash
from .errors import BusinessError, error_dict


_SEQ_PREFIX_RE = re.compile(
    r"^(?:(?:第\s*[0-9一二三四五六七八九十百千零〇两]+\s*条)"
    r"|(?:(?:Article|Clause)\s+[0-9]+))\s*[:.、\-—]?\s*",
    re.IGNORECASE,
)
_CHINESE_NUMBER_RE = re.compile(r"^[0-9一二三四五六七八九十百千零〇两]+$")
_ELEMENT_TYPES = {"amount", "date", "term", "penalty_ratio"}


@dataclass
class MappedText:
    text: str
    source_starts: list[int]
    source_ends: list[int]


def _chinese_to_int(value: str) -> int:
    digits = {
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
    units = {"十": 10, "百": 100, "千": 1000}
    if value.isdigit():
        return int(value)
    total = 0
    current = 0
    for char in value:
        if char in digits:
            current = digits[char]
        elif char in units:
            unit = units[char]
            if current == 0:
                current = 1
            total += current * unit
            current = 0
        else:
            raise ValueError(f"not a Chinese number: {value}")
    return total + current


def parse_clause_number_text(value: str) -> int | None:
    text = unicodedata.normalize("NFKC", value).strip()
    match = _SEQ_PREFIX_RE.match(text)
    if match:
        if text[match.end():].strip():
            return None
        inner = match.group(0)
    else:
        inner = text
    inner = re.sub(r"^(?:第|Article|Clause)\s*", "", inner, flags=re.IGNORECASE)
    inner = re.sub(r"\s*条\s*$", "", inner)
    inner = inner.strip(" :.、-—")
    if not inner or not _CHINESE_NUMBER_RE.fullmatch(inner):
        return None
    try:
        number = _chinese_to_int(inner)
    except ValueError:
        return None
    if number < 1 or number > 9999:
        return None
    return number


def _nfkc_with_source(raw: str) -> MappedText:
    chars: list[str] = []
    starts: list[int] = []
    ends: list[int] = []
    for index, character in enumerate(raw):
        normalized = unicodedata.normalize("NFKC", character)
        for item in normalized:
            chars.append(item)
            starts.append(index)
            ends.append(index + 1)
    return MappedText("".join(chars), starts, ends)


def _fold_whitespace(mapped: MappedText) -> MappedText:
    chars: list[str] = []
    starts: list[int] = []
    ends: list[int] = []
    index = 0
    while index < len(mapped.text):
        character = mapped.text[index]
        if character.isspace():
            run_end = index + 1
            contains_newline = character in "\n\r"
            while run_end < len(mapped.text) and mapped.text[run_end].isspace():
                contains_newline = contains_newline or mapped.text[run_end] in "\n\r"
                run_end += 1
            chars.append("\n" if contains_newline else " ")
            starts.append(mapped.source_starts[index])
            ends.append(mapped.source_ends[run_end - 1])
            index = run_end
        else:
            chars.append(character)
            starts.append(mapped.source_starts[index])
            ends.append(mapped.source_ends[index])
            index += 1
    return MappedText("".join(chars), starts, ends)


def _slice_mapped(mapped: MappedText, start: int, end: int) -> MappedText:
    return MappedText(mapped.text[start:end], mapped.source_starts[start:end], mapped.source_ends[start:end])


def _trim_mapped(mapped: MappedText) -> tuple[MappedText, int, int]:
    start = 0
    end = len(mapped.text)
    while start < end and mapped.text[start].isspace():
        start += 1
    while end > start and mapped.text[end - 1].isspace():
        end -= 1
    return _slice_mapped(mapped, start, end), start, end


def _has_business_content(text: str) -> bool:
    return any(character.isalnum() or ("\u4e00" <= character <= "\u9fff") for character in text)


def _split_units(mapped: MappedText) -> list[tuple[int, int]]:
    boundaries = {"。", "！", "？", "；", "\n"}
    spans: list[tuple[int, int]] = []
    start = 0
    index = 0
    while index < len(mapped.text):
        if mapped.text[index] in boundaries:
            if mapped.text[index] == "\n":
                end = index
                next_start = index + 1
            else:
                end = index + 1
                next_start = index + 1
            if start < end:
                spans.append((start, end))
            start = next_start
        index += 1
    if start < len(mapped.text):
        spans.append((start, len(mapped.text)))

    result: list[tuple[int, int]] = []
    for raw_start, raw_end in spans:
        segment = _slice_mapped(mapped, raw_start, raw_end)
        trimmed, left, right = _trim_mapped(segment)
        if not trimmed.text or not _has_business_content(trimmed.text):
            continue
        result.append((raw_start + left, raw_start + right))
    return result


def utf8_byte_to_codepoint_offset(raw: str, byte_offset: int) -> int:
    if byte_offset < 0:
        return 0
    encoded = raw.encode("utf-8")
    if byte_offset >= len(encoded):
        return len(raw)
    consumed = 0
    for index, character in enumerate(raw):
        width = len(character.encode("utf-8"))
        if consumed + width > byte_offset:
            return index
        consumed += width
    return len(raw)


class ClauseNormalizer:
    def __init__(
        self,
        config: dict[str, Any],
        *,
        element_types: Iterable[str] = _ELEMENT_TYPES,
    ) -> None:
        self.config = config
        self.element_types = set(element_types)

    def _derive_clause_no(self, raw: str, fallback_index: int) -> int:
        policy = self.config.get("interop", {}).get("default_clause_no_policy", "infer_from_text_then_index")
        if policy == "index_only":
            return fallback_index
        normalized = unicodedata.normalize("NFKC", raw)
        match = _SEQ_PREFIX_RE.match(normalized)
        if match:
            parsed = parse_clause_number_text(match.group(0))
            if parsed is not None:
                return parsed
        return fallback_index

    def _parse_clause_no(self, record: dict[str, Any], raw: str, fallback_index: int) -> int:
        raw_value = record.get("clause_no")
        if raw_value is None or raw_value == "":
            return self._derive_clause_no(raw, fallback_index)
        if isinstance(raw_value, bool):
            parsed = None
        elif isinstance(raw_value, int):
            parsed = raw_value
        elif isinstance(raw_value, str):
            parsed = parse_clause_number_text(raw_value)
        else:
            parsed = None
        if parsed is None or parsed < 1 or parsed > 9999:
            raise BusinessError(
                "E_INVALID_CLAUSE_NO",
                str(record.get("contract_id", "")),
                "normalize",
                "invalid clause number",
                {"value": raw_value, "offset": [0, len(raw)]},
            )
        return parsed

    def _convert_annotation_span(
        self,
        unit: dict[str, Any],
        source_start: int,
        source_end: int,
    ) -> tuple[int, int] | None:
        starts: list[int] = unit["_source_starts"]
        ends: list[int] = unit["_source_ends"]
        indexes = [
            index
            for index, (char_start, char_end) in enumerate(zip(starts, ends))
            if char_start < source_end and char_end > source_start
        ]
        if not indexes:
            return None
        return indexes[0], indexes[-1] + 1

    def normalize_record(self, record: dict[str, Any], record_index: int) -> dict[str, Any]:
        contract_id = str(record.get("contract_id", ""))
        raw = record.get("clause_text", "")
        if not isinstance(raw, str):
            raw = str(raw)
        clause_no = self._parse_clause_no(record, raw, record_index + 1)
        max_chars = int(self.config["normalization"]["max_text_chars"])
        mapped = _fold_whitespace(_nfkc_with_source(raw))
        prefix = _SEQ_PREFIX_RE.match(mapped.text)
        prefix_end = prefix.end() if prefix else 0
        content = _slice_mapped(mapped, prefix_end, len(mapped.text))
        content, _, _ = _trim_mapped(content)

        if len(content.text) > max_chars:
            raise BusinessError(
                "E_TEXT_TOO_LONG",
                contract_id,
                "normalize",
                "clause text exceeds configured maximum",
                {
                    "length": len(content.text),
                    "max_text_chars": max_chars,
                    "offset": [0, len(raw)],
                },
            )
        if not content.text or not _has_business_content(content.text):
            raise BusinessError(
                "E_CLAUSE_EMPTY",
                contract_id,
                "normalize",
                "clause contains no business text after sequence stripping",
                {"offset": [0, len(raw)]},
            )

        annotations = record.get("annotations") or []
        if not isinstance(annotations, list):
            raise BusinessError(
                "E_INTEROP",
                contract_id,
                "normalize",
                "annotations must be a list",
                {"offset": [0, len(raw)]},
            )

        unit_spans = _split_units(content)
        if not unit_spans:
            raise BusinessError(
                "E_CLAUSE_EMPTY",
                contract_id,
                "normalize",
                "clause contains no splittable unit",
                {"offset": [0, len(raw)]},
            )

        template_id = str(record.get("template_id", ""))
        units: list[dict[str, Any]] = []
        for unit_index, (start, end) in enumerate(unit_spans):
            text = content.text[start:end]
            source_starts = content.source_starts[start:end]
            source_ends = content.source_ends[start:end]
            source_start = source_starts[0]
            source_end = source_ends[-1]
            identity = {
                "contract_id": contract_id,
                "clause_no": clause_no,
                "unit_index": unit_index,
                "text": text,
                "template_id": template_id,
                "source_start": source_start,
                "source_end": source_end,
            }
            clause_id = f"{contract_id}#{clause_no:04d}.{unit_index:02d}.{short_hash(identity, 10)}"
            unit = {
                "contract_id": contract_id,
                "clause_id": clause_id,
                "clause_no": clause_no,
                "unit_index": unit_index,
                "template_id": template_id,
                "text": text,
                "source_start": source_start,
                "source_end": source_end,
                "norm_start": prefix_end + start,
                "norm_end": prefix_end + end,
                "annotations": [],
                "_source_starts": source_starts,
                "_source_ends": source_ends,
            }
            units.append(unit)

        provisional: list[dict[str, Any]] = []
        for annotation in annotations:
            if not isinstance(annotation, dict):
                raise BusinessError(
                    "E_INTEROP",
                    contract_id,
                    "normalize",
                    "annotation must be an object",
                    {"offset": [0, len(raw)]},
                )
            element_type = annotation.get("element_type", annotation.get("type"))
            if element_type not in self.element_types:
                raise BusinessError(
                    "E_UNKNOWN_ELEMENT",
                    contract_id,
                    "extract",
                    "annotation element type is not in the configured element table",
                    {
                        "element": element_type,
                        "element_type": element_type,
                        "allowed": sorted(self.element_types),
                        "offset": [0, len(raw)],
                    },
                )
            start = annotation.get("start")
            end = annotation.get("end")
            if not isinstance(start, int) or not isinstance(end, int) or start < 0 or end <= start:
                raise BusinessError(
                    "E_INTEROP",
                    contract_id,
                    "normalize",
                    "annotation offsets are invalid",
                    {"element_type": element_type, "offset": [start, end]},
                )
            source_start = annotation.get("source_start", start)
            source_end = annotation.get("source_end", end)
            for unit in units:
                converted = self._convert_annotation_span(unit, int(source_start), int(source_end))
                if converted is None:
                    continue
                norm_start, norm_end = converted
                provisional.append(
                    {
                        "contract_id": contract_id,
                        "clause_id": unit["clause_id"],
                        "clause_no": clause_no,
                        "unit_index": unit["unit_index"],
                        "element_type": element_type,
                        "start": norm_start,
                        "end": norm_end,
                        "source_start": int(source_start),
                        "source_end": int(source_end),
                        "text": unit["text"][norm_start:norm_end],
                        "value": annotation.get("value"),
                        "normalized_value": annotation.get("normalized_value"),
                        "effective_date": annotation.get("effective_date"),
                    }
                )

        by_clause: dict[str, list[dict[str, Any]]] = {unit["clause_id"]: [] for unit in units}
        for annotation in provisional:
            by_clause[annotation["clause_id"]].append(annotation)
        for unit in units:
            unit["annotations"] = sorted(
                by_clause[unit["clause_id"]],
                key=lambda item: (item["element_type"], item["start"], item["end"]),
            )

        public_units: list[dict[str, Any]] = []
        for unit in units:
            public = {key: value for key, value in unit.items() if not key.startswith("_")}
            public_units.append(public)

        return {
            "type": "normalized_record",
            "contract_id": contract_id,
            "template_id": template_id,
            "source_index": record_index,
            "structure_version": record.get("structure_version", 2),
            "normalized_text": content.text,
            "units": public_units,
        }

    @staticmethod
    def error_payload(error: BusinessError) -> dict[str, Any]:
        return error.to_dict()

