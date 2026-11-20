"""Stage 1: Unicode normalization, whitespace folding and sentence splitting."""
from __future__ import annotations

import re
import unicodedata

from .errors import E_EMPTY_TEXT
from .util import digest_obj

_WHITESPACE_RE = re.compile(r"\s+")
_SENT_SPLIT_RE = re.compile(r"[。！？；!?;]+")
_CJK_RE = re.compile(r"[\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff]")
_LATIN_RE = re.compile(r"[A-Za-z]")


def normalize_text(text: str) -> str:
    # NFKC unifies full-width/half-width and compatibility forms.
    text = unicodedata.normalize("NFKC", text)
    text = _WHITESPACE_RE.sub(" ", text)
    return text.strip()


def detect_lang(text: str) -> str:
    has_cjk = bool(_CJK_RE.search(text))
    has_latin = bool(_LATIN_RE.search(text))
    if has_cjk and has_latin:
        return "mixed"
    if has_cjk:
        return "zh"
    if has_latin:
        return "en"
        # No letters: inherit neutral marker; caller may fall back to input lang.
    return "neutral"


def is_empty_or_punct(text: str) -> bool:
    for ch in text:
        if ch.isspace():
            continue
        if unicodedata.category(ch).startswith("P") or unicodedata.category(ch).startswith("S"):
            continue
        return False
    return True


def split_sentences(text: str) -> list[dict]:
    segments: list[dict] = []
    start = 0
    index = 0
    for match in _SENT_SPLIT_RE.finditer(text):
        end = match.end()
        piece = text[start:end].strip()
        if piece:
            offset = text.index(piece, start)
            segments.append(
                {
                    "index": index,
                    "text": piece,
                    "start": offset,
                    "end": offset + len(piece),
                    "lang_norm": detect_lang(piece),
                }
            )
            index += 1
        start = end
    tail = text[start:].strip()
    if tail:
        offset = text.index(tail, start)
        segments.append(
            {
                "index": index,
                "text": tail,
                "start": offset,
                "end": offset + len(tail),
                "lang_norm": detect_lang(tail),
            }
        )
    return segments


def normalize_clause(clause: dict) -> dict:
    clause_id = clause["clause_id"]
    raw_text = clause.get("text", "")
    normalized = normalize_text(str(raw_text))
    diagnostics: list[dict] = []
    segments: list[dict] = []
    if not normalized or is_empty_or_punct(normalized):
        diagnostics.append(
            {
                "code": E_EMPTY_TEXT,
                "clause_id": clause_id,
                "message": "clause text is empty or punctuation only after normalization",
            }
        )
        status = "empty"
    else:
        segments = split_sentences(normalized)
        status = "ok"
    record = {
        "clause_id": clause_id,
        "contract_id": clause.get("contract_id", ""),
        "template_id": clause.get("template_id", ""),
        "lang": str(clause.get("lang", "zh")).lower(),
        "human_level": clause.get("human_level"),
        "normalized_text": normalized,
        "segments": segments,
        "status": status,
        "diagnostics": diagnostics,
        "input_digest": digest_obj(clause),
    }
    return record


def run(clauses: list[dict]) -> list[dict]:
    return [normalize_clause(c) for c in clauses]
