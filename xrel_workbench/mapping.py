from __future__ import annotations

import math
from collections import Counter
from typing import Any, Iterable

from .bridge import BridgeIndex
from .util import is_finite_number


def _finite_or_none(value: Any) -> float | int | None:
    return value if is_finite_number(value) else None


def _char_ngrams(term: str, sizes: Iterable[int]) -> Counter[str]:
    compact = term.replace(" ", "")
    grams: Counter[str] = Counter()
    for size in sizes:
        size = int(size)
        if size <= 0:
            continue
        if len(compact) < size:
            if compact:
                grams[compact] += 1
            continue
        for index in range(len(compact) - size + 1):
            grams[compact[index : index + size]] += 1
    return grams


def char_ngram_overlap(left: str, right: str, sizes: Iterable[int]) -> float:
    if left == right:
        return 1.0
    left_grams = _char_ngrams(left, sizes)
    right_grams = _char_ngrams(right, sizes)
    total = sum(left_grams.values()) + sum(right_grams.values())
    if total == 0:
        return 0.0
    intersection = sum((left_grams & right_grams).values())
    return (2.0 * intersection) / total


def _document_stats(stage1_records: list[dict[str, Any]]) -> tuple[dict[str, int], float, int]:
    document_frequency: Counter[str] = Counter()
    total_tokens = 0
    for record in stage1_records:
        terms = {token["term"] for token in record["doc_tokens"]}
        document_frequency.update(terms)
        total_tokens += len(record["doc_tokens"])
    count = len(stage1_records)
    average_length = (total_tokens / count) if count else 0.0
    return dict(document_frequency), average_length, count


def _quantization_scale(
    weights: list[float],
    bits: int,
) -> int:
    base = 1 << max(0, bits - 4)
    qmax = (1 << (bits - 1)) - 1
    positive = [abs(weight) for weight in weights if is_finite_number(weight)]
    largest = max(positive, default=0.0)
    if largest <= 0:
        return base
    required = largest / qmax
    scale = base
    while scale < required:
        scale *= 2
    return scale


def map_and_quantize_shard(
    stage1_records: list[dict[str, Any]],
    bridge: dict[str, Any],
    config: dict[str, Any],
) -> list[dict[str, Any]]:
    index = BridgeIndex(bridge)
    document_frequency, average_length, document_count = _document_stats(stage1_records)
    mapping_config = config["mapping"]
    bm25 = mapping_config["bm25"]
    k1 = float(bm25["k1"])
    b = float(bm25["b"])
    smooth = float(mapping_config["smooth_strength"])
    lexical_coefficient = float(mapping_config["lexical_weight"])
    bridge_coefficient = float(mapping_config["bridge_weight"])
    char_sizes = [int(size) for size in mapping_config["char_ngram_sizes"]]
    bits = int(mapping_config["quantization_bits"])
    qmax = (1 << (bits - 1)) - 1

    results: list[dict[str, Any]] = []
    for record in stage1_records:
        pair_id = str(record["pair_id"])
        query_tokens = record["query_tokens"]
        doc_tokens = record["doc_tokens"]
        doc_counts = Counter(token["term"] for token in doc_tokens)
        first_doc_token: dict[str, dict[str, Any]] = {}
        for token in doc_tokens:
            first_doc_token.setdefault(token["term"], token)
        query_counts = Counter(token["term"] for token in query_tokens)

        merged: dict[tuple[str, str], dict[str, Any]] = {}
        for query_token in query_tokens:
            query_term = query_token["term"]
            direct_match = query_term in doc_counts
            bridge_candidates = index.targets_for_query_term(query_term)
            grouped: dict[str, dict[str, Any]] = {}
            if direct_match:
                grouped[query_term] = {
                    "lexical_similarity": 1.0,
                    "entry_ids": set(),
                    "entry_weight": None,
                    "negative_entry_ids": set(),
                    "routes": {"direct"},
                }
            for target_term, entry_id, entry_weight in bridge_candidates:
                if target_term not in doc_counts:
                    continue
                candidate = grouped.setdefault(
                    target_term,
                    {
                        "lexical_similarity": char_ngram_overlap(query_term, target_term, char_sizes),
                        "entry_ids": set(),
                        "entry_weight": None,
                        "negative_entry_ids": set(),
                        "routes": set(),
                    },
                )
                candidate["entry_ids"].add(entry_id)
                numeric_weight = float(entry_weight)
                if candidate["entry_weight"] is None or numeric_weight > candidate["entry_weight"]:
                    candidate["entry_weight"] = numeric_weight
                if numeric_weight < 0:
                    candidate["negative_entry_ids"].add(entry_id)
                candidate["routes"].add("bridge")

            for target_term, candidate in grouped.items():
                key = (query_term, target_term)
                if key in merged:
                    existing = merged[key]
                    existing["lexical_similarity"] = max(
                        existing["lexical_similarity"], candidate["lexical_similarity"]
                    )
                    if candidate["entry_weight"] is not None and (
                        existing["entry_weight"] is None
                        or candidate["entry_weight"] > existing["entry_weight"]
                    ):
                        existing["entry_weight"] = candidate["entry_weight"]
                    existing["entry_ids"].update(candidate["entry_ids"])
                    existing["negative_entry_ids"].update(candidate["negative_entry_ids"])
                    existing["routes"].update(candidate["routes"])
                    continue
                merged[key] = {
                    "query_token": query_token,
                    "target_term": target_term,
                    "target_token": first_doc_token[target_term],
                    "lexical_similarity": candidate["lexical_similarity"],
                    "entry_weight": candidate["entry_weight"],
                    "entry_ids": set(candidate["entry_ids"]),
                    "negative_entry_ids": set(candidate["negative_entry_ids"]),
                    "routes": set(candidate["routes"]),
                    "query_tf": query_counts[query_term],
                }

        candidates: list[dict[str, Any]] = []
        for key in sorted(
            merged,
            key=lambda item: (
                merged[item]["query_token"]["start"],
                merged[item]["query_token"]["end"],
                item[0],
                item[1],
            ),
        ):
            item = merged[key]
            target = item["target_term"]
            tf = doc_counts[target]
            df = document_frequency.get(target, 0)
            idf = math.log(1.0 + ((document_count - df + 0.5) / (df + 0.5)))
            smoothed_idf = idf + smooth
            length_ratio = (len(doc_tokens) / average_length) if average_length else 0.0
            tf_norm = (tf * (k1 + 1.0)) / (tf + k1 * (1.0 - b + b * length_ratio))
            document_factor = smoothed_idf * tf_norm
            lexical_component = lexical_coefficient * item["lexical_similarity"]
            entry_weight = float(item["entry_weight"] or 0.0)
            bridge_component = bridge_coefficient * entry_weight
            mapping_weight = lexical_component + bridge_component
            weight_raw = mapping_weight * document_factor * item["query_tf"]
            lexical_weight = lexical_component * document_factor * item["query_tf"]
            bridge_weight = bridge_component * document_factor * item["query_tf"]
            negative_entry_ids = sorted(item["negative_entry_ids"])
            if negative_entry_ids:
                numeric_issue = "negative bridge entry weight"
            elif not is_finite_number(entry_weight):
                numeric_issue = "non-finite bridge entry weight"
            elif not all(
                is_finite_number(value)
                for value in (
                    idf,
                    smoothed_idf,
                    tf_norm,
                    document_factor,
                    lexical_component,
                    bridge_component,
                    mapping_weight,
                    lexical_weight,
                    bridge_weight,
                )
            ):
                numeric_issue = "non-finite intermediate weight"
            elif not is_finite_number(weight_raw):
                numeric_issue = "non-finite raw weight"
            else:
                numeric_issue = None
            candidates.append(
                {
                    "query_term": item["query_token"]["term"],
                    "query_start": item["query_token"]["start"],
                    "query_end": item["query_token"]["end"],
                    "query_token_id": item["query_token"]["term_id"],
                    "target_term": target,
                    "target_start": item["target_token"]["start"],
                    "target_end": item["target_token"]["end"],
                    "target_token_id": item["target_token"]["term_id"],
                    "target_lang": item["target_token"]["lang"],
                    "routes": sorted(item["routes"]),
                    "entry_ids": sorted(item["entry_ids"]),
                    "negative_entry_ids": negative_entry_ids,
                    "char_ngram_overlap": item["lexical_similarity"],
                    "entry_weight": _finite_or_none(entry_weight),
                    "mapping_lexical_component": _finite_or_none(lexical_component),
                    "mapping_bridge_component": _finite_or_none(bridge_component),
                    "mapping_weight": _finite_or_none(mapping_weight),
                    "query_tf": item["query_tf"],
                    "document_tf": tf,
                    "document_df": df,
                    "bm25_idf": _finite_or_none(idf),
                    "bm25_smoothed_idf": _finite_or_none(smoothed_idf),
                    "bm25_tf_normalized": _finite_or_none(tf_norm),
                    "document_factor": _finite_or_none(document_factor),
                    "lexical_weight": _finite_or_none(lexical_weight),
                    "bridge_weight": _finite_or_none(bridge_weight),
                    "weight_raw": weight_raw if numeric_issue is None else None,
                    "weight_raw_text": repr(weight_raw),
                    "numeric_issue": numeric_issue,
                }
            )

        finite_weights = [
            float(candidate["weight_raw"])
            for candidate in candidates
            if candidate["weight_raw"] is not None
        ]
        scale = _quantization_scale(finite_weights, bits)
        step = 1.0 / scale
        for term_index, candidate in enumerate(candidates, 1):
            candidate["term_id"] = f"m{term_index:06d}"
            weight = candidate["weight_raw"]
            if weight is None:
                candidate["quantization"] = {
                    "q": None,
                    "scale_factor": scale,
                    "step": step,
                    "rounding": "round_half_even",
                    "weight_quantized": None,
                    "absolute_error": None,
                    "absolute_error_bound": 0.5 / scale,
                    "numeric_issue": candidate["numeric_issue"],
                }
                continue
            scaled = weight * scale
            if not is_finite_number(scaled):
                candidate["quantization"] = {
                    "q": None,
                    "scale_factor": scale,
                    "step": step,
                    "rounding": "round_half_even",
                    "weight_quantized": None,
                    "absolute_error": None,
                    "absolute_error_bound": 0.5 / scale,
                    "numeric_issue": "non-finite scaled weight",
                }
                candidate["numeric_issue"] = "non-finite scaled weight"
                continue
            q = int(round(scaled))
            quantized = q / scale
            abs_error = abs(quantized - weight)
            overflow = abs(q) > qmax
            candidate["quantization"] = {
                "q": q,
                "scale_factor": scale,
                "step": step,
                "rounding": "round_half_even",
                "weight_quantized": quantized,
                "absolute_error": abs_error,
                "absolute_error_bound": 0.5 / scale,
                "overflow": overflow,
            }

        results.append(
            {
                "pair_id": pair_id,
                "shard": record["shard"],
                "query": record["query"],
                "doc": record["doc"],
                "lang_query": record["lang_query"],
                "lang_doc": record["lang_doc"],
                "document_stats": {
                    "document_count": document_count,
                    "average_document_length": _finite_or_none(average_length),
                    "document_length": len(doc_tokens),
                },
                "quantization": {
                    "bits": bits,
                    "scale_factor": scale,
                    "step": step,
                    "rounding": "round_half_even",
                    "qmax": qmax,
                },
                "candidates": candidates,
            }
        )
    return results
