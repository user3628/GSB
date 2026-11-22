"""Stage 1: text normalisation and clause-locator parsing.

Normalisation performs Unicode NFKC (which also unifies full-width /
half-width forms) and whitespace folding while keeping a per-character map
back to the original ``raw_text`` indices, so every parsed location carries
original character offsets.
"""

import re
import unicodedata

from .errors import E_CITATION_UNPARSED, LegalCiteError

_CN_DIGITS = {"零": 0, "〇": 0, "一": 1, "二": 2, "两": 2, "三": 3, "四": 4,
              "五": 5, "六": 6, "七": 7, "八": 8, "九": 9}

_CN_NUM = r"[0-9]+|[" + "".join(_CN_DIGITS) + r"十百]+"
_EN_SECTION = re.compile(r"section\s+(-?\d+)(?:\.(-?\d+))?", re.IGNORECASE)
_EN_ARTICLE = re.compile(r"article\s+(-?\d+)(?:\.(-?\d+))?", re.IGNORECASE)
_CN_TIAO = re.compile(
    r"第\s*(" + _CN_NUM + r")\s*条"
    r"(?:\s*第\s*(" + _CN_NUM + r")\s*款)?"
)
_CN_KUAN_ONLY = re.compile(r"第\s*(" + _CN_NUM + r")\s*款")


def normalize_text(raw):
    """NFKC + whitespace fold.

    Returns ``(text, index_map)`` where ``index_map[i]`` is the index in
    ``raw`` of the character that produced normalized position ``i``.
    """
    out_chars = []
    index_map = []
    last_was_space = True  # collapse leading spaces
    for orig_idx, ch in enumerate(raw):
        for part in unicodedata.normalize("NFKC", ch):
            if part.isspace():
                if not last_was_space:
                    out_chars.append(" ")
                    index_map.append(orig_idx)
                    last_was_space = True
                continue
            out_chars.append(part)
            index_map.append(orig_idx)
            last_was_space = False
    if out_chars and out_chars[-1] == " ":
        out_chars.pop()
        index_map.pop()
    return "".join(out_chars), index_map


def cn_to_int(token):
    """Convert Arabic or a Chinese numeral (<= 9999) token to int."""
    if token.isdigit():
        return int(token)
    if token in _CN_DIGITS:
        return _CN_DIGITS[token]
    total = 0
    section = 0
    number = 0
    for ch in token:
        if ch in _CN_DIGITS:
            number = _CN_DIGITS[ch]
        elif ch == "十":
            section += number if number else 1
            section *= 10
            number = 0
        elif ch == "百":
            section += number if number else 1
            section *= 100
            number = 0
    return total + section + number


def canonical_locator(article, section):
    return f"A{int(article)}.S{int(section)}"


def _to_raw_span(index_map, start, end):
    if end <= start:
        return [index_map[start], index_map[start] + 1]
    return [index_map[start], index_map[end - 1] + 1]


def parse_locator(normalized, index_map):
    """Parse the first clause locator in normalized text.

    Returns a location dict, or raises ``E_CITATION_UNPARSED``.
    """
    candidates = []
    for match, pattern in ((_EN_SECTION, "en_section_dotted"),
                           (_EN_ARTICLE, "en_article_dotted"),
                           (_CN_TIAO, "cn_tiao_kuan"),
                           (_CN_KUAN_ONLY, "cn_kuan_only")):
        m = match.search(normalized)
        if m:
            candidates.append((m.start(), m, pattern))
    if not candidates:
        raise LegalCiteError(
            E_CITATION_UNPARSED,
            "no clause locator pattern matched normalized text",
            {"offset": None, "normalized_text": normalized},
        )
    _, m, pattern = min(candidates, key=lambda item: item[0])
    art_token = m.group(1)
    sec_token = m.group(2) if m.lastindex and m.lastindex >= 2 else None
    article = cn_to_int(art_token)
    if pattern == "cn_kuan_only":
        # A bare 第X款 has no article anchor and is not a full locator.
        raise LegalCiteError(
            E_CITATION_UNPARSED,
            "bare section reference without article anchor",
            {"offset": _to_raw_span(index_map, m.start(), m.end())[0]},
        )
    section = cn_to_int(sec_token) if sec_token else 0
    if article < 0 or section < 0:
        raise LegalCiteError(
            E_CITATION_UNPARSED,
            "negative clause number in locator",
            {"offset": _to_raw_span(index_map, m.start(), m.end())[0],
             "article": article, "section": section},
        )
    if article == 0:
        raise LegalCiteError(
            E_CITATION_UNPARSED,
            "article number must be >= 1",
            {"offset": _to_raw_span(index_map, m.start(), m.end())[0]},
        )
    raw_span = _to_raw_span(index_map, m.start(), m.end())
    return {
        "article": int(article),
        "section": int(section),
        "locator": canonical_locator(article, section),
        "pattern": pattern,
        "matched_text": normalized[m.start():m.end()],
        "normalized_span": [m.start(), m.end()],
        "raw_span": raw_span,
    }


def parse_citation(record):
    """Stage-1 entry point for one input record.

    Validates the citation fragment against ``raw_text`` and parses the
    clause locator.  Raises :class:`LegalCiteError` (``E_CITATION_UNPARSED``)
    with citation id and offsets on failure.
    """
    citation_id = record["citation_id"]
    raw = record["raw_text"]
    span = record.get("citation_span")
    if not (isinstance(span, list) and len(span) == 2
            and all(isinstance(v, int) for v in span)):
        raise LegalCiteError(
            E_CITATION_UNPARSED, "missing or malformed citation_span",
            {"citation_id": citation_id, "offset": None},
        )
    start, end = span
    if start < 0 or end < 0 or start > end or end > len(raw):
        raise LegalCiteError(
            E_CITATION_UNPARSED, "citation_span out of raw_text bounds",
            {"citation_id": citation_id, "offset": span},
        )
    if raw[start:end] != record.get("citation_text", ""):
        raise LegalCiteError(
            E_CITATION_UNPARSED,
            "citation_text does not match raw_text at citation_span",
            {"citation_id": citation_id, "offset": span,
             "raw_fragment": raw[start:end]},
        )
    normalized, index_map = normalize_text(raw)
    try:
        location = parse_locator(normalized, index_map)
    except LegalCiteError as exc:
        exc.details.setdefault("citation_id", citation_id)
        exc.details.setdefault("offset", span)
        raise
    return {
        "citation_id": citation_id,
        "contract_id": record["contract_id"],
        "lang": record["lang"],
        "channel": record["channel"],
        "issued_date": record.get("issued_date"),
        "shard_id": record.get("shard_id"),
        "raw_text": raw,
        "citation_span": span,
        "normalized_text": normalized,
        "location": location,
        "elements_raw": record.get("elements", {}),
    }
