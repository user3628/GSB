"""Stage 2: region binding, citation text normalization and clause-location parsing."""
from __future__ import annotations

import re
import unicodedata

from .codec import stable_sorted
from .config import ParseConfig
from .errors import RefParseError

_CN_NUMERAL = {"零": 0, "一": 1, "二": 2, "两": 2, "三": 3, "四": 4, "五": 5,
               "六": 6, "七": 7, "八": 8, "九": 9}

_CN_ARTICLE_RE = re.compile(r"第([0-9]+(?:\.[0-9]+)?|[零一二两三四五六七八九十百千]+)条")
_CN_CLAUSE_RE = re.compile(r"第([0-9]+|[零一二两三四五六七八九十百千]+)款")
_EN_SECTION_RE = re.compile(r"[Ss]ection\s+([0-9]+)(?:\.([0-9]+))?")
_EN_ARTICLE_RE = re.compile(r"[Aa]rticle\s+([0-9]+)(?:\.([0-9]+))?")
_WS_RE = re.compile(r"\s+")


def normalize_text(raw: str):
    """NFKC + full/half-width unification + whitespace collapse, with offset mapping."""
    normalized_chars = []
    index_map = []
    for raw_index, char in enumerate(unicodedata.normalize("NFKC", raw)):
        folded = _WS_RE.sub(" ", char)
        for piece in folded:
            normalized_chars.append(piece)
            index_map.append(raw_index)
    text = "".join(normalized_chars)
    # Collapse whitespace runs while keeping the mapping aligned to the first char.
    result_chars = []
    result_map = []
    prev_space = False
    for char, raw_index in zip(text, index_map):
        is_space = char == " "
        if is_space and prev_space:
            continue
        result_chars.append(char)
        result_map.append(raw_index)
        prev_space = is_space
    final = "".join(result_chars).strip()
    leading = len("".join(result_chars)) - len("".join(result_chars).lstrip())
    trailing = len(result_chars) - (len(final) + leading)
    result_map = result_map[leading: len(result_chars) - trailing] if trailing else result_map[leading:]
    return final, result_map


def parse_chinese_numeral(token: str) -> int:
    if token.isdigit():
        return int(token)
    if token in _CN_NUMERAL:
        return _CN_NUMERAL[token]
    total = 0
    section = 0
    number = 0
    units = {"十": 10, "百": 100, "千": 1000}
    for char in token:
        if char in _CN_NUMERAL:
            number = _CN_NUMERAL[char]
        elif char in units:
            section += (number or 1) * units[char]
            number = 0
        else:
            raise ValueError("unsupported chinese numeral: %s" % token)
    total = section + number
    if total == 0:
        raise ValueError("unsupported chinese numeral: %s" % token)
    return total


def _spans(match, mapping):
    start, end = match.start(), match.end()
    raw_start = mapping[start]
    raw_end = mapping[min(end - 1, len(mapping) - 1)] + 1
    return [start, end], [raw_start, raw_end]


def _dotted(token):
    if "." in token:
        article, clause = token.split(".", 1)
        return int(article), int(clause)
    return None


def extract_locations(text: str, mapping: list):
    """Return every parseable location candidate with spans and parse-source labels."""
    article_matches = []
    candidates = []

    for match in _CN_ARTICLE_RE.finditer(text):
        token = match.group(1)
        span_norm, span_raw = _spans(match, mapping)
        dotted = _dotted(token)
        if dotted is not None:
            article, clause = dotted
            source = "cn_dotted"
        else:
            article, clause, source = parse_chinese_numeral(token), 0, "cn_article"
        record = {
            "article": article,
            "clause": clause,
            "source": source,
            "span_norm": span_norm,
            "span_raw": span_raw,
            "order": match.start(),
        }
        article_matches.append(record)
        candidates.append(dict(record))

    for match in _CN_CLAUSE_RE.finditer(text):
        token = match.group(1)
        span_norm, span_raw = _spans(match, mapping)
        clause_no = parse_chinese_numeral(token)
        preceding = [item for item in article_matches if item["order"] < match.start()]
        article_no = preceding[-1]["article"] if preceding else 0
        candidates.append(
            {
                "article": article_no,
                "clause": clause_no,
                "source": "cn_clause",
                "span_norm": span_norm,
                "span_raw": span_raw,
                "order": match.start(),
            }
        )

    for regex, source in ((_EN_SECTION_RE, "en_section"), (_EN_ARTICLE_RE, "en_article")):
        for match in regex.finditer(text):
            span_norm, span_raw = _spans(match, mapping)
            article = int(match.group(1))
            clause = int(match.group(2)) if match.group(2) else 0
            candidates.append(
                {
                    "article": article,
                    "clause": clause,
                    "source": source,
                    "span_norm": span_norm,
                    "span_raw": span_raw,
                    "order": match.start(),
                }
            )

    # Deduplicate identical (article, clause, source) and order deterministically.
    unique = {}
    for candidate in candidates:
        key = (candidate["article"], candidate["clause"], candidate["source"])
        unique.setdefault(key, candidate)
    return list(unique.values())


def _rank(candidate, priority):
    try:
        return priority.index(candidate["source"])
    except ValueError:
        return len(priority)


def run_parse(sample: dict, bindings: dict, config: ParseConfig, stop_check=None) -> dict:
    """Stage-2 product: bindings, normalized text, offsets, multi-location candidates, errors."""
    entries = []
    errors = []
    for page in sample["pages"]:
        for region in page["regions"]:
            binding = bindings["%s|%s" % (page["page_id"], region["region_id"])]
            for ref in region["refs"]:
                normalized, mapping = normalize_text(ref["raw_text"])
                locations = extract_locations(normalized, mapping)
                locations = stable_sorted(
                    locations,
                    key=lambda item: (
                        _rank(item, config.location_priority),
                        item["order"],
                        item["article"],
                        item["clause"],
                    ),
                )
                preferred = None
                if locations:
                    preferred = {"article": locations[0]["article"], "clause": locations[0]["clause"]}
                    for index, candidate in enumerate(locations):
                        candidate["preferred"] = index == 0
                        candidate.pop("order", None)
                entry = {
                    "ref_id": ref["ref_id"],
                    "contract_id": ref["contract_id"],
                    "lang": ref["lang"],
                    "source": ref["source"],
                    "crawl_month": ref["crawl_month"],
                    "struct_version": ref["struct_version"],
                    "raw_text": ref["raw_text"],
                    "raw_terms": ref["raw_terms"],
                    "normalized_text": normalized,
                    "page_id": page["page_id"],
                    "region_id": region["region_id"],
                    "region_binding": {
                        "category": binding["category"],
                        "gt_xywh": binding["gt_xywh"],
                        "page_width": binding["page_width"],
                        "page_height": binding["page_height"],
                        "matched": binding["matched"],
                        "match": binding["match"],
                    },
                    "location_candidates": locations,
                    "preferred_location": preferred,
                }
                if not locations:
                    span_raw = [0, len(ref["raw_text"])]
                    error = RefParseError(
                        "no clause location could be parsed",
                        {
                            "ref_id": ref["ref_id"],
                            "offset_norm": [0, len(normalized)],
                            "offset_raw": span_raw,
                            "normalized_text": normalized,
                        },
                    )
                    entry["parse_error"] = error.to_dict()
                    errors.append(error.to_dict())
                entries.append(entry)
                if stop_check is not None:
                    stop_check(ref["ref_id"])
    entries = stable_sorted(entries, key=lambda item: item["ref_id"])
    errors = stable_sorted(errors, key=lambda item: item["details"]["ref_id"])
    return {
        "stage": "parse",
        "entries": entries,
        "errors": errors,
        "stats": {"entry_count": len(entries), "parse_error_count": len(errors)},
    }
