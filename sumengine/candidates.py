"""Deterministic extraction, compression and template candidate generation."""

from __future__ import annotations

import math
import re
from collections import Counter
from typing import Any, Iterable

import numpy as np

from .text import (
    _PLACE_RE,
    _TIME_RE,
    coverage_ratio,
    key_points,
    language_consistency,
    language_profile,
    prepare_sentences,
    target_band,
    tokenize,
)


def _round(value: float, digits: int = 9) -> float:
    return round(float(value), digits)


def _join_sentences(sentences: list[dict[str, Any]], language: str) -> str:
    separator = " " if language == "en" else ""
    return separator.join(sentence["text"].strip() for sentence in sentences).strip()


def _sentence_scores(
    doc: dict[str, Any],
    sentences: list[dict[str, Any]],
    config: dict[str, Any],
    band: dict[str, int],
) -> list[dict[str, Any]]:
    counts: Counter[str] = Counter()
    for sentence in sentences:
        counts.update(sentence["tokens"])
    title_tokens = set(tokenize(doc.get("title", "")))
    max_count = max(counts.values(), default=1)
    weights = config["extractor"]["weights"]
    weight_sum = sum(float(value) for value in weights.values()) or 1.0
    decay = float(config["extractor"]["position_decay"])
    target_mid = (band["min_chars"] + band["max_chars"]) / 2
    target_half = max(1.0, (band["max_chars"] - band["min_chars"]) / 2)
    result: list[dict[str, Any]] = []
    for sentence in sentences:
        tokens = sentence["tokens"]
        unique = set(tokens)
        if unique:
            term_frequency = sum(counts[token] for token in unique) / (math.sqrt(max_count) * math.sqrt(len(unique)))
            term_component = math.log1p(term_frequency) / math.log1p(math.sqrt(max_count) * 12)
            term_component = min(1.0, max(0.0, term_component))
        else:
            term_component = 0.0
        position_component = 1.0 / (1.0 + decay * sentence["index"])
        if title_tokens:
            title_component = len(unique & title_tokens) / len(title_tokens)
        else:
            title_component = 0.0
        length_component = max(0.0, 1.0 - abs(len(sentence["text"]) - target_mid) / (target_half + 1.0))
        raw = (
            float(weights["term_frequency"]) * term_component
            + float(weights["position"]) * position_component
            + float(weights["title_overlap"]) * title_component
            + float(weights["length_fit"]) * length_component
        )
        score = raw / weight_sum
        result.append(
            {
                "index": sentence["index"],
                "start": sentence["start"],
                "end": sentence["end"],
                "text": sentence["text"],
                "score": _round(score),
                "components": {
                    "term_frequency": _round(term_component),
                    "position": _round(position_component),
                    "title_overlap": _round(title_component),
                    "length_fit": _round(length_component),
                },
                "tokens": tokens,
            }
        )
    return result


def _similarity(left: set[str], right: set[str]) -> float:
    if not left or not right:
        return 0.0
    return len(left & right) / math.sqrt(len(left) * len(right))


def _mmr_select(
    scored: list[dict[str, Any]],
    sentences: list[dict[str, Any]],
    config: dict[str, Any],
    band: dict[str, int],
    language: str,
) -> tuple[list[int], list[dict[str, Any]]]:
    max_chars = band["max_chars"]
    max_sentences = int(config["extractor"]["max_sentences"])
    lam = float(config["extractor"]["mmr_lambda"])
    eligible = [item for item in scored if len(item["text"]) <= max_chars]
    if not eligible:
        return [], []
    eligible.sort(key=lambda item: (-item["score"], item["index"]))
    n = len(eligible)
    token_sets = [set(item["tokens"]) for item in eligible]
    similarities = np.zeros((n, n), dtype=np.float64)
    for i in range(n):
        for j in range(i + 1, n):
            value = _similarity(token_sets[i], token_sets[j])
            similarities[i, j] = value
            similarities[j, i] = value
    selected_positions: list[int] = []
    selection_details: list[dict[str, Any]] = []
    selected_chars = 0
    separator_chars = 1 if language == "en" else 0
    # `max_sentences` is a normal cap, but it must not prevent reaching the
    # configured minimum character band when more eligible sentences fit.
    while len(selected_positions) < max_sentences or selected_chars < band["min_chars"]:
        best_position: int | None = None
        best_value = -1e30
        best_similarity = 0.0
        best_relevance = 0.0
        for position, item in enumerate(eligible):
            if position in selected_positions:
                continue
            added_chars = len(item["text"]) + (separator_chars if selected_positions else 0)
            if selected_chars + added_chars > max_chars:
                continue
            relevance = float(item["score"])
            max_similarity = (
                max(float(similarities[position, chosen]) for chosen in selected_positions)
                if selected_positions
                else 0.0
            )
            value = lam * relevance - (1.0 - lam) * max_similarity
            if (
                value > best_value + 1e-15
                or (
                    abs(value - best_value) <= 1e-15
                    and best_position is not None
                    and item["index"] < eligible[best_position]["index"]
                )
            ):
                best_position = position
                best_value = value
                best_similarity = max_similarity
                best_relevance = relevance
        if best_position is None:
            break
        selected_positions.append(best_position)
        item = eligible[best_position]
        selected_chars += len(item["text"]) + (separator_chars if len(selected_positions) > 1 else 0)
        selection_details.append(
            {
                "selection_order": len(selection_details),
                "sentence_index": item["index"],
                "mmr_marginal": _round(best_value),
                "relevance": _round(best_relevance),
                "max_similarity": _round(best_similarity),
            }
        )
        if selected_chars >= max_chars:
            break
    return [eligible[position]["index"] for position in selected_positions], selection_details


def _candidate_common(
    doc: dict[str, Any],
    route: str,
    text: str,
    band: dict[str, int],
    points: list[dict[str, Any]],
) -> dict[str, Any]:
    coverage, covered, missing = coverage_ratio(text, points)
    profile = language_profile(text)
    lang_score, lang_ok, lang_reason = language_consistency(str(doc["language"]), text)
    length = len(text)
    reasons: list[str] = []
    if not text.strip():
        reasons.append("EMPTY_CANDIDATE")
    if length < band["min_chars"] or length > band["max_chars"]:
        reasons.append("LENGTH_OUT_OF_BAND")
    if not lang_ok:
        reasons.append(lang_reason)
    return {
        "candidate_id": f"{doc['doc_id']}:{route}",
        "doc_id": doc["doc_id"],
        "route": route,
        "text": text,
        "length_chars": length,
        "target_length": doc["target_length"],
        "target_band": dict(band),
        "coverage": _round(coverage),
        "covered_key_points": covered,
        "missing_key_points": missing,
        "language": {
            **profile,
            "doc_label": doc["language"],
            "consistency_score": _round(lang_score),
            "consistent": lang_ok,
            "reason": lang_reason,
        },
        "structural_unavailable_reasons": reasons,
    }


def generate_extractive(
    doc: dict[str, Any],
    sentences: list[dict[str, Any]],
    points: list[dict[str, Any]],
    config: dict[str, Any],
) -> dict[str, Any]:
    band = target_band(config, doc["target_length"])
    scored = _sentence_scores(doc, sentences, config, band)
    selected_indices, selection_details = _mmr_select(scored, sentences, config, band, doc["language"])
    selected_set = set(selected_indices)
    order_by_index = {detail["sentence_index"]: detail for detail in selection_details}
    selected_sentences = [scored[index] for index in sorted(selected_set)]
    text = _join_sentences(selected_sentences, doc["language"])
    candidate = _candidate_common(doc, "extractive", text, band, points)
    candidate.update(
        {
            "selected_sentence_indices": sorted(selected_set),
            "selection_order": selected_indices,
            "selection_details": selection_details,
            "sentences": [
                {
                    "index": item["index"],
                    "start": item["start"],
                    "end": item["end"],
                    "text": item["text"],
                    "score": item["score"],
                    "components": item["components"],
                    "selected": item["index"] in selected_set,
                    "mmr_marginal": (
                        order_by_index[item["index"]]["mmr_marginal"] if item["index"] in order_by_index else None
                    ),
                }
                for item in scored
            ],
        }
    )
    return candidate


def _clean_removed_text(text: str) -> str:
    text = re.sub(r"[，,]\s*[，,]+", "，", text)
    text = re.sub(r"^[，,；;:\s]+", "", text)
    text = re.sub(r"[，,；;]\s*([。.!?！？])", r"\1", text)
    return text.strip()


def generate_compressed(
    doc: dict[str, Any],
    sentences: list[dict[str, Any]],
    points: list[dict[str, Any]],
    config: dict[str, Any],
    extractive: dict[str, Any],
) -> dict[str, Any]:
    band = target_band(config, doc["target_length"])
    compression = config["compression"]
    source_indices = list(extractive["selected_sentence_indices"])
    sentence_map = {sentence["index"]: sentence for sentence in sentences}
    parts: list[str] = [sentence_map[index]["text"] for index in source_indices if index in sentence_map]
    base_text = _join_sentences([sentence_map[index] for index in source_indices if index in sentence_map], doc["language"])
    min_coverage = float(compression["min_coverage"])
    max_removed_ratio = float(compression["max_removed_ratio"])
    removed_fragments: list[dict[str, Any]] = []
    skipped_removals: list[dict[str, Any]] = []
    removed_chars = 0
    base_chars = max(1, sum(len(part) for part in parts))
    separator = " " if doc["language"] == "en" else ""

    def current_text() -> str:
        return separator.join(part for part in parts if part).strip()

    for part_index, original_part in enumerate(list(parts)):
        for pattern in compression["removable_clause_patterns"]:
            compiled = re.compile(pattern)
            match = compiled.search(parts[part_index])
            if match is None:
                continue
            fragment = match.group(0)
            trial_part = _clean_removed_text(parts[part_index][: match.start()] + parts[part_index][match.end() :])
            if not trial_part:
                skipped_removals.append(
                    {
                        "sentence_index": source_indices[part_index],
                        "text": fragment,
                        "rule": pattern,
                        "reason": "WOULD_EMPTY_SENTENCE",
                    }
                )
                continue
            trial_parts = list(parts)
            trial_parts[part_index] = trial_part
            trial_text = separator.join(item for item in trial_parts if item).strip()
            trial_coverage = coverage_ratio(trial_text, points)[0]
            trial_removed = removed_chars + len(fragment)
            if trial_coverage + 1e-12 < min_coverage:
                skipped_removals.append(
                    {
                        "sentence_index": source_indices[part_index],
                        "text": fragment,
                        "rule": pattern,
                        "reason": "COVERAGE_GUARD",
                    }
                )
                continue
            if trial_removed / base_chars > max_removed_ratio:
                skipped_removals.append(
                    {
                        "sentence_index": source_indices[part_index],
                        "text": fragment,
                        "rule": pattern,
                        "reason": "MAX_REMOVED_RATIO_GUARD",
                    }
                )
                continue
            parts[part_index] = trial_part
            removed_chars = trial_removed
            removed_fragments.append(
                {
                    "sentence_index": source_indices[part_index],
                    "text": fragment,
                    "rule": pattern,
                }
            )

    replacements: list[dict[str, Any]] = []
    skipped_replacements: list[dict[str, Any]] = []
    text_before_replacements = current_text()
    replacement_items = sorted(
        compression["synonym_replacements"].items(),
        key=lambda item: (-len(str(item[0])), str(item[0])),
    )
    for source, target in replacement_items:
        source = str(source)
        target = str(target)
        flags = re.IGNORECASE if re.search(r"[A-Za-z]", source) else 0
        pattern = re.compile(re.escape(source), flags)
        count = len(pattern.findall(current_text()))
        if not count:
            continue
        trial_text = pattern.sub(target, current_text())
        trial_coverage = coverage_ratio(trial_text, points)[0]
        if trial_coverage + 1e-12 < min_coverage:
            skipped_replacements.append(
                {
                    "from": source,
                    "to": target,
                    "occurrences": count,
                    "reason": "COVERAGE_GUARD",
                }
            )
            continue
        parts = [pattern.sub(target, part) for part in parts]
        replacements.append({"from": source, "to": target, "occurrences": count, "reason": "SYNONYM_TABLE"})

    text = current_text()
    fallbacks: list[dict[str, Any]] = []
    fallback_count = 0
    for term in compression.get("required_replacement_terms", []):
        term = str(term)
        if term in compression["synonym_replacements"]:
            continue
        occurrences = len(re.findall(re.escape(term), text, re.IGNORECASE if re.search(r"[A-Za-z]", term) else 0))
        if occurrences:
            fallback_count += occurrences
            fallbacks.append(
                {
                    "term": term,
                    "occurrences": occurrences,
                    "action": "KEEP_ORIGINAL",
                    "reason": "NO_SYNONYM_ENTRY",
                }
            )
    coverage, covered, missing = coverage_ratio(text, points)
    candidate = _candidate_common(doc, "compressed", text, band, points)
    if coverage + 1e-12 < min_coverage:
        candidate["structural_unavailable_reasons"].append("COVERAGE_BELOW_MINIMUM")
    candidate.update(
        {
            "selected_sentence_indices": source_indices,
            "sentence_count_in": len(source_indices),
            "sentence_count_out": len(source_indices),
            "kept_sentences": [
                {
                    "sentence_index": index,
                    "original": sentence_map[index]["text"],
                    "rewritten": parts[position],
                }
                for position, index in enumerate(source_indices)
                if index in sentence_map
            ],
            "removed_fragments": removed_fragments,
            "skipped_removals": skipped_removals,
            "replacements": replacements,
            "skipped_replacements": skipped_replacements,
            "fallbacks": fallbacks,
            "fallback_count": fallback_count,
            "coverage": _round(coverage),
            "covered_key_points": covered,
            "missing_key_points": missing,
            "coverage_minimum": min_coverage,
            "coverage_ok": coverage + 1e-12 >= min_coverage,
            "silent_sentence_drop": False,
        }
    )
    return candidate


def _trim_title(title: str) -> str:
    title = title.strip()
    title = re.split(r"[：:|｜]", title, maxsplit=1)[0].strip()
    if len(title) > 42:
        title = title[:42]
        if " " in title:
            title = title.rsplit(" ", 1)[0]
    return title.rstrip("，,。.!?！？;； ")


def _time_place_phrase(sentence: str) -> str:
    matches: list[str] = []
    for regex in (_TIME_RE, _PLACE_RE):
        match = regex.search(sentence)
        if match:
            matches.append(match.group(0))
    return " ".join(dict.fromkeys(matches))


def generate_template(
    doc: dict[str, Any],
    sentences: list[dict[str, Any]],
    points: list[dict[str, Any]],
    config: dict[str, Any],
    extractive: dict[str, Any],
) -> dict[str, Any]:
    band = target_band(config, doc["target_length"])
    template_cfg = config["template"]
    templates = template_cfg["templates"]
    template_name = doc["language"] if doc["language"] in templates else "zh"
    if doc["language"] == "mixed" and "mixed" in templates:
        template_name = "mixed"
    template = str(templates.get(template_name) or templates["zh"])
    if "{who}" not in template or "{what}" not in template or "{when_where}" not in template:
        raise ValueError("template must contain {who}, {what} and {when_where}")
    sentence_map = {sentence["index"]: sentence for sentence in sentences}
    title = _trim_title(str(doc.get("title", "")))
    if not title:
        first = sentences[0]["text"] if sentences else str(doc.get("source", "news"))
        title = first[:42].rstrip("，,。.!?！？;；")
    ranked_indices = list(extractive["selection_order"]) or [sentence["index"] for sentence in sentences]
    if not ranked_indices:
        ranked_indices = [sentence["index"] for sentence in sentences]
    separator = " " if doc["language"] == "en" else ""
    chosen: list[int] = []
    rendered = ""
    coverage = 0.0
    minimum = float(template_cfg["min_coverage"])
    max_points = int(template_cfg["max_points"])
    time_index = None
    for sentence in sentences:
        if _TIME_RE.search(sentence["text"]) or _PLACE_RE.search(sentence["text"]):
            time_index = sentence["index"]
            break
    slot_cache: dict[str, Any] = {}
    for index in ranked_indices:
        sentence = sentence_map.get(index)
        if sentence is None:
            continue
        candidate_indices = chosen + [index]
        what_text = separator.join(sentence_map[item]["text"] for item in candidate_indices)
        if time_index is not None:
            when_where = _time_place_phrase(sentence_map[time_index]["text"])
            if not when_where:
                when_where = f"来源 / source: {doc.get('source', 'unknown')}"
        else:
            when_where = f"来源 / source: {doc.get('source', 'unknown')}"
        trial_slots = {
            "who": title,
            "what": what_text.rstrip("。.!?！？;； "),
            "when_where": when_where.rstrip("。.!?！？;； "),
        }
        trial_text = template.format(**trial_slots)
        trial_coverage = coverage_ratio(trial_text, points)[0]
        if len(trial_text) > band["max_chars"] and chosen:
            continue
        if len(trial_text) > band["max_chars"]:
            shortened = what_text[: max(0, band["max_chars"] - len(title) - len(when_where) - 20)]
            trial_slots["what"] = shortened
            trial_text = template.format(**trial_slots)
            trial_coverage = coverage_ratio(trial_text, points)[0]
        chosen = candidate_indices
        slot_cache = trial_slots
        rendered = trial_text
        coverage = trial_coverage
        if coverage + 1e-12 >= minimum and len(rendered) >= band["min_chars"]:
            break
        if len(chosen) >= max_points:
            break
    if not chosen and ranked_indices:
        chosen = [ranked_indices[0]]
        sentence = sentence_map[chosen[0]]
        slot_cache = {
            "who": title,
            "what": sentence["text"].rstrip("。.!?！？;； "),
            "when_where": f"来源 / source: {doc.get('source', 'unknown')}",
        }
        rendered = template.format(**slot_cache)
        coverage = coverage_ratio(rendered, points)[0]
    candidate = _candidate_common(doc, "template", rendered, band, points)
    if coverage + 1e-12 < minimum:
        candidate["structural_unavailable_reasons"].append("COVERAGE_BELOW_MINIMUM")
    candidate.update(
        {
            "template_name": template_name,
            "template": template,
            "slots": slot_cache,
            "selected_sentence_indices": sorted(set(chosen)),
            "key_point_slots": points,
            "coverage": _round(coverage),
            "coverage_minimum": minimum,
            "coverage_ok": coverage + 1e-12 >= minimum,
            "full_sentences_dropped": False,
        }
    )
    return candidate


def generate_candidates(doc: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
    sentences = prepare_sentences(doc, config)
    points = key_points(doc, sentences)
    extractive = generate_extractive(doc, sentences, points, config)
    compressed = generate_compressed(doc, sentences, points, config, extractive)
    template = generate_template(doc, sentences, points, config, extractive)
    return {
        "doc_id": doc["doc_id"],
        "input_fingerprint": None,
        "sentence_count": len(sentences),
        "evaluation_count": len(sentences) * 3 + int(config["fusion"]["fusion_candidate_evaluations"]),
        "key_points": points,
        "candidates": [extractive, compressed, template],
        "sentences": [
            {
                "index": sentence["index"],
                "start": sentence["start"],
                "end": sentence["end"],
                "text": sentence["text"],
                "tokens": sentence["tokens"],
            }
            for sentence in sentences
        ],
    }
