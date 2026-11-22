from __future__ import annotations

import re
import unicodedata
from typing import Any

from .canonical import PipelineError, sha256_obj


_WHITESPACE_RE = re.compile(r"\s+", flags=re.UNICODE)
_BOUNDARY_CHARS = set(".!?。！？；;，,")


def normalize_text(value: str) -> str:
    """NFKC -> lowercase -> Unicode whitespace fold."""
    normalized = unicodedata.normalize("NFKC", value)
    normalized = normalized.lower()
    normalized = _WHITESPACE_RE.sub(" ", normalized)
    return normalized.strip()


def _is_cjk(char: str) -> bool:
    code = ord(char)
    return (
        0x3400 <= code <= 0x4DBF
        or 0x4E00 <= code <= 0x9FFF
        or 0xF900 <= code <= 0xFAFF
        or 0x20000 <= code <= 0x2FA1F
        or 0x3040 <= code <= 0x30FF
        or 0xAC00 <= code <= 0xD7AF
    )


def _is_word_char(char: str) -> bool:
    return char.isalnum() or char in "_'-"


def tokenize_with_offsets(text: str, base_offset: int = 0) -> list[dict[str, Any]]:
    """Split CJK characters individually and Latin/alphanumeric runs as words.

    Offsets always point into the normalized text supplied by the caller.
    """
    tokens: list[dict[str, Any]] = []
    index = 0
    length = len(text)
    while index < length:
        char = text[index]
        if _is_cjk(char):
            tokens.append({"text": char, "start": base_offset + index, "end": base_offset + index + 1})
            index += 1
            continue
        if _is_word_char(char):
            start = index
            index += 1
            while index < length:
                current = text[index]
                if _is_cjk(current):
                    break
                if _is_word_char(current):
                    index += 1
                    continue
                if (
                    current in "._'-"
                    and index + 1 < length
                    and _is_word_char(text[index + 1])
                    and not _is_cjk(text[index + 1])
                ):
                    index += 1
                    continue
                break
            tokens.append(
                {
                    "text": text[start:index],
                    "start": base_offset + start,
                    "end": base_offset + index,
                }
            )
            continue
        index += 1
    return tokens


def _is_boundary(text: str, index: int) -> bool:
    char = text[index]
    if char not in _BOUNDARY_CHARS:
        return False
    if char == ".":
        before_digit = index > 0 and text[index - 1].isdigit()
        after_digit = index + 1 < len(text) and text[index + 1].isdigit()
        if before_digit and after_digit:
            return False
        if index + 1 < len(text) and not text[index + 1].isspace():
            # Common abbreviations such as U.S. stay intact; decimal-like tokens
            # are protected above.
            return False
    return True


def split_sentences_with_offsets(text: str) -> list[dict[str, Any]]:
    """Deterministically split claims at sentence and clause punctuation."""
    if not text:
        return []
    result: list[dict[str, Any]] = []
    start = 0
    index = 0
    while index < len(text):
        if not _is_boundary(text, index):
            index += 1
            continue
        end = index + 1
        while end < len(text) and text[end] in _BOUNDARY_CHARS:
            end += 1
        sentence_start = start
        while sentence_start < end and text[sentence_start].isspace():
            sentence_start += 1
        sentence_end = end
        while sentence_end > sentence_start and text[sentence_end - 1].isspace():
            sentence_end -= 1
        if sentence_end > sentence_start:
            sentence_text = text[sentence_start:sentence_end]
            result.append(
                {
                    "text": sentence_text,
                    "start": sentence_start,
                    "end": sentence_end,
                    "tokens": tokenize_with_offsets(sentence_text, sentence_start),
                }
            )
        start = end
        index = end

    tail_start = start
    while tail_start < len(text) and text[tail_start].isspace():
        tail_start += 1
    tail_end = len(text)
    while tail_end > tail_start and text[tail_end - 1].isspace():
        tail_end -= 1
    if tail_end > tail_start:
        sentence_text = text[tail_start:tail_end]
        result.append(
            {
                "text": sentence_text,
                "start": tail_start,
                "end": tail_end,
                "tokens": tokenize_with_offsets(sentence_text, tail_start),
            }
        )
    for sentence_index, sentence in enumerate(result):
        sentence["sentence_index"] = sentence_index
    return result


def _parse_annotation(record: dict[str, Any], sample_id: str) -> dict[str, Any]:
    raw = record.get("annotation")
    if raw is None and "human_annotation" in record:
        raw = record.get("human_annotation")
    if raw is None and (
        "human_hallucination" in record or "human_needs_review" in record
    ):
        raw = {
            "is_hallucination": record.get("human_hallucination"),
            "needs_review": record.get("human_needs_review"),
        }
    if raw is None:
        return {
            "present": False,
            "is_hallucination": None,
            "needs_review": None,
        }
    if not isinstance(raw, dict):
        raise PipelineError(
            "E_INVALID_RECORD",
            "annotation 必须是 JSON 对象。",
            {"sample_id": sample_id, "field": "annotation"},
        )
    hallucination = raw.get("is_hallucination")
    needs_review = raw.get("needs_review")
    if hallucination is not None and not isinstance(hallucination, bool):
        raise PipelineError(
            "E_INVALID_RECORD",
            "annotation.is_hallucination 必须是布尔值或 null。",
            {"sample_id": sample_id, "field": "annotation.is_hallucination"},
        )
    if needs_review is not None and not isinstance(needs_review, bool):
        raise PipelineError(
            "E_INVALID_RECORD",
            "annotation.needs_review 必须是布尔值或 null。",
            {"sample_id": sample_id, "field": "annotation.needs_review"},
        )
    return {
        "present": True,
        "is_hallucination": hallucination,
        "needs_review": needs_review,
    }


def validate_and_normalize_record(record: Any) -> dict[str, Any]:
    if not isinstance(record, dict):
        raise PipelineError("E_INVALID_RECORD", "JSONL 每条记录必须是 JSON 对象。")

    sample_id = record.get("sample_id")
    if not isinstance(sample_id, str) or not sample_id:
        raise PipelineError(
            "E_INVALID_RECORD", "sample_id 必须是非空字符串。", {"record": record}
        )

    for field in ("prompt", "model_output", "reference_answer"):
        if not isinstance(record.get(field), str):
            raise PipelineError(
                "E_INVALID_RECORD",
                f"{field} 必须是字符串。",
                {"sample_id": sample_id, "field": field},
            )
    if not isinstance(record.get("lang"), str) or not record["lang"]:
        raise PipelineError(
            "E_INVALID_RECORD",
            "lang 必须是非空字符串。",
            {"sample_id": sample_id, "field": "lang"},
        )
    if not isinstance(record.get("source"), str) or not record["source"]:
        raise PipelineError(
            "E_INVALID_RECORD",
            "source 必须是非空字符串。",
            {"sample_id": sample_id, "field": "source"},
        )
    evidence = record.get("retrieved_evidence")
    if not isinstance(evidence, list) or any(not isinstance(item, str) for item in evidence):
        raise PipelineError(
            "E_INVALID_RECORD",
            "retrieved_evidence 必须是字符串数组。",
            {"sample_id": sample_id, "field": "retrieved_evidence"},
        )

    annotation = _parse_annotation(record, sample_id)
    prompt = normalize_text(record["prompt"])
    output = normalize_text(record["model_output"])
    normalized_evidence = [normalize_text(item) for item in evidence]

    errors: list[dict[str, Any]] = []
    if not output:
        errors.append(
            {
                "code": "E_EMPTY_FIELD",
                "sample_id": sample_id,
                "field": "model_output",
            }
        )
    if not evidence:
        errors.append(
            {
                "code": "E_EMPTY_FIELD",
                "sample_id": sample_id,
                "field": "retrieved_evidence",
                "evidence_index": None,
            }
        )
    else:
        for evidence_index, text in enumerate(normalized_evidence):
            if not text:
                errors.append(
                    {
                        "code": "E_EMPTY_FIELD",
                        "sample_id": sample_id,
                        "field": f"retrieved_evidence[{evidence_index}]",
                        "evidence_index": evidence_index,
                    }
                )

    output_sentences = split_sentences_with_offsets(output)
    evidence_sentences = [
        split_sentences_with_offsets(text) for text in normalized_evidence
    ]

    return {
        "sample_id": sample_id,
        "source": record["source"],
        "lang": record["lang"],
        "content_fingerprint": sha256_obj(record),
        "status": "ok" if not errors else "error",
        "errors": errors,
        "annotation": annotation,
        "normalized": {
            "prompt": prompt,
            "model_output": output,
            "retrieved_evidence": normalized_evidence,
        },
        "tokens": {
            "prompt": tokenize_with_offsets(prompt),
            "model_output": tokenize_with_offsets(output),
            "retrieved_evidence": [
                tokenize_with_offsets(text) for text in normalized_evidence
            ],
        },
        "sentences": {
            "model_output": output_sentences,
            "retrieved_evidence": evidence_sentences,
        },
    }


def normalize_records(records: list[Any]) -> list[dict[str, Any]]:
    normalized = [validate_and_normalize_record(record) for record in records]
    normalized.sort(key=lambda item: item["sample_id"])
    return normalized
