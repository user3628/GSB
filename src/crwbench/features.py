"""Stage 2: keyword hits, negation and degree-modifier feature extraction."""
from __future__ import annotations

import math

from .config import CATEGORY_KEYS, Config

UNKNOWN_KEYWORD_TABLE = "UNKNOWN_KEYWORD_TABLE"


def _langs_for_clause(clause: dict) -> list[str]:
    if clause["lang"] == "mixed":
        return ["zh", "en"]
    return [clause["lang"]] if clause["lang"] in ("zh", "en") else ["zh", "en"]


def _segment_index(clause: dict, pos: int) -> int:
    current = 0
    for seg in clause["segments"]:
        if seg["start"] <= pos < seg["end"]:
            return seg["index"]
        current = seg["index"]
    return current


def _find_hits(text: str, table: dict, lang: str, config: Config, clause: dict, category: str):
    occurrences = []
    for term, weight in table.items():
        start = 0
        while True:
            idx = text.find(term, start)
            if idx < 0:
                break
            occurrences.append((idx, idx + len(term), term, float(weight)))
            start = idx + max(1, len(term))
    # Longest-first, then earliest, then alphabetical => deterministic overlap resolution.
    occurrences.sort(key=lambda x: (x[0], -(x[1] - x[0]), x[2]))
    chosen: list[tuple[int, int, str, float]] = []
    for occ in occurrences:
        start, end, term, weight = occ
        if any(start < prev_end and end > prev_start for prev_start, prev_end, _, _ in chosen):
            continue
        chosen.append(occ)

    window = int(config.negation.get("window", 12))
    neg_terms = config.negation_terms(lang)
    degree_terms = config.degree_terms(lang)
    hits = []
    for start, end, term, weight in chosen:
        window_text = text[max(0, start - window):start]
        negator = ""
        negated = False
        best_neg_pos = -1
        for nterm in neg_terms:
            pos = window_text.rfind(nterm)
            if pos >= 0 and pos > best_neg_pos:
                best_neg_pos = pos
                negator = nterm
                negated = True
        modifier = ""
        multiplier = 1.0
        best_mod_pos = -1
        for dterm, dmult in degree_terms.items():
            pos = window_text.rfind(dterm)
            if pos >= 0 and pos > best_mod_pos:
                best_mod_pos = pos
                modifier = dterm
                multiplier = float(dmult)
        hits.append(
            {
                "category": category,
                "keyword": term,
                "source_lang": lang,
                "weight": round(weight, 6),
                "start": start,
                "end": end,
                "segment_index": _segment_index(clause, start),
                "negated": negated,
                "negator": negator,
                "modifier": modifier,
                "multiplier": round(multiplier, 6),
                "polarity": "negative" if negated else "positive",
            }
        )
    return hits


def extract_clause(clause: dict, config: Config) -> dict:
    hits: list[dict] = []
    unknown_categories: list[dict] = []
    if clause["status"] == "ok":
        langs = _langs_for_clause(clause)
        text = clause["normalized_text"]
        for category in CATEGORY_KEYS:
            category_hits: list[dict] = []
            available_tables = 0
            for lang in langs:
                table = config.keyword_table(category, lang)
                if table is None:
                    continue
                available_tables += 1
                category_hits.extend(_find_hits(text, table, lang, config, clause, category))
            if available_tables == 0:
                # Explicit unknown marker: the category judgment is never silently dropped.
                unknown_categories.append(
                    {
                        "category": category,
                        "marker": UNKNOWN_KEYWORD_TABLE,
                        "langs": langs,
                    }
                )
            category_hits.sort(key=lambda h: (h["start"], h["end"], h["keyword"]))
            hits.extend(category_hits)
    else:
        for category in CATEGORY_KEYS:
            unknown_categories.append(
                {
                    "category": category,
                    "marker": UNKNOWN_KEYWORD_TABLE,
                    "langs": _langs_for_clause(clause),
                    "skipped_reason": clause["status"],
                }
            )
    hits.sort(key=lambda h: (h["start"], h["end"], h["category"], h["keyword"]))
    modifier_strength = round(sum(math.log(h["multiplier"]) for h in hits), 6)
    return {
        "clause_id": clause["clause_id"],
        "contract_id": clause["contract_id"],
        "human_level": clause["human_level"],
        "status": clause["status"],
        "length": len(clause["normalized_text"]),
        "hit_count": len(hits),
        "negated_count": sum(1 for h in hits if h["negated"]),
        "modifier_strength": modifier_strength,
        "hits": hits,
        "unknown_categories": unknown_categories,
        "diagnostics": list(clause["diagnostics"]),
    }


def run(normalized: list[dict], config: Config) -> list[dict]:
    return [extract_clause(clause, config) for clause in normalized]
