"""Bidirectional retrieval with deterministic ranking and error diagnostics."""

from __future__ import annotations

import math
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable

import numpy as np

from .errors import (
    E_DIM_MISMATCH,
    E_EMPTY_CAPTION,
    E_INVALID_DIRECTION,
    E_INVALID_K,
    E_NUMERIC,
    E_QUERY_EMPTY,
    E_SNAPSHOT_MISMATCH,
    error,
)
from .features import extract_text_features, extract_visual_features, normalize_caption, tokenize
from .quantization import (
    build_per_vector_spec,
    decimal_cosine,
    decimal_text,
    decimal_to_float,
    decimal_vector,
    dequantize_vector,
    project_and_normalize,
    quantize_vector,
    similarity_error_bound_decimal,
    validate_quant_spec,
    vector_error_envelope,
    effective_digits,
)
from .snapshot import Snapshot, load_snapshot
from .util import canonical_bytes, digest_obj, rounded_float, sha256_bytes


DIRECTIONS = {"text_to_image", "image_to_text"}
VERIFIED_BOUND_MARGIN = Decimal("1e-12")


def _latent_tokens(quantized: list[int], count: int) -> list[str]:
    order = sorted(range(len(quantized)), key=lambda index: (-abs(quantized[index]), index))
    tokens: list[str] = []
    for index in order:
        if len(tokens) >= count:
            break
        value = int(quantized[index])
        if value != 0:
            tokens.append(f"v{index:02d}:{'p' if value > 0 else 'n'}")
    return tokens


def _log_contribution(value: float) -> float:
    return rounded_float(math.log1p(abs(value)), 15)


def _idf(postings: dict[str, list[str]]) -> dict[str, float]:
    document_count = max(1, len({item for values in postings.values() for item in values}))
    return {
        token: math.log((document_count + 1.0) / (len(values) + 1.0)) + 1.0
        for token, values in postings.items()
    }


def _inverted_score(
    query_tokens: list[str],
    item_tokens: list[str],
    postings: dict[str, list[str]],
) -> dict[str, Any]:
    query_set = sorted(set(query_tokens))
    item_set = set(item_tokens)
    idf = _idf(postings)
    weights = {token: idf.get(token, math.log(2.0) + 1.0) for token in query_set}
    denominator = sum(weights.values())
    matched = [token for token in query_set if token in item_set]
    numerator = sum(weights[token] for token in matched)
    score = numerator / denominator if denominator > 0 else 0.0
    return {
        "matched_tokens": matched,
        "matched_count": len(matched),
        "query_token_count": len(query_set),
        "score": rounded_float(score, 15),
    }


def _route_specs(
    snapshot: Snapshot,
    entry: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    quantization = snapshot.representation["quantization"]
    if quantization["granularity"] == "per_dim":
        return quantization["visual"], quantization["text"]
    if entry is None:
        raise error(
            E_NUMERIC,
            "Per-vector target quantization parameters are unavailable",
            stage="retrieval",
            details={},
        )
    return entry["visual_quantization"], entry["text_quantization"]


def _prepare_query(
    *,
    workspace: Any,
    snapshot: Snapshot,
    direction: str,
    query_text: str | None,
    image_item_id: str | None,
) -> dict[str, Any]:
    config = snapshot.config
    if direction == "text_to_image":
        if query_text is None:
            raise error(E_QUERY_EMPTY, "Text query is required", stage="retrieval")
        try:
            text = extract_text_features(query_text, config["feature"], item_id=None)
        except Exception as exc:
            if getattr(exc, "code", None) == E_EMPTY_CAPTION:
                raise error(
                    E_QUERY_EMPTY,
                    "Text query is empty after normalization",
                    stage="retrieval",
                    details={"normalization": "NFKC_whitespace_lower"},
                ) from exc
            raise
        tokens = tokenize(text["summary"]["normalized_caption"])
        if not tokens:
            raise error(
                E_QUERY_EMPTY,
                "Text query contains no word or CJK tokens",
                stage="retrieval",
                details={"normalized_caption": text["summary"]["normalized_caption"]},
            )
        reference = project_and_normalize(
            text["vector"],
            config["projection"]["text"],
            route="text",
            item_id=None,
            stage="query_projection",
        )
        route = "text"
        query_kind = "text"
        normalized = text["summary"]["normalized_caption"]
        feature_digest = text["summary"]["feature_sha256"]
        detail = {"text": query_text, "normalized_text": normalized, "tokens": tokens}
    elif direction == "image_to_text":
        if not image_item_id:
            raise error(E_QUERY_EMPTY, "Image item_id is required", stage="retrieval")
        image_path = workspace.image_path(image_item_id)
        visual = extract_visual_features(image_path, config["feature"], item_id=image_item_id)
        reference = project_and_normalize(
            visual["vector"],
            config["projection"]["visual"],
            route="visual",
            item_id=image_item_id,
            stage="query_projection",
        )
        route = "visual"
        query_kind = "image"
        normalized = None
        feature_digest = visual["summary_sha256"]
        detail = {"image_item_id": image_item_id}
    else:
        raise error(
            E_INVALID_DIRECTION,
            "Unsupported retrieval direction",
            stage="retrieval",
            details={"direction": direction, "allowed": sorted(DIRECTIONS)},
        )
    quantization = snapshot.representation["quantization"]
    if quantization["granularity"] == "per_dim":
        spec = quantization[route]
    else:
        spec = build_per_vector_spec(
            reference,
            bits=int(quantization["bits"]),
            modality=route,
        )
    quantized = quantize_vector(reference, spec, item_id=None, stage="query_quantization")
    dequantized = dequantize_vector(quantized, spec)
    return {
        "route": route,
        "kind": query_kind,
        "reference": reference,
        "reference_decimal": decimal_vector(reference),
        "quantized": quantized,
        "quantization": spec,
        "error_l2_bound": vector_error_envelope(reference, dequantized, spec),
        "feature_digest": feature_digest,
        "detail": detail,
    }


def run_search(
    workspace: Any,
    *,
    direction: str,
    query_text: str | None = None,
    image_item_id: str | None = None,
    top_k: int = 5,
    weights: dict[str, float] | None = None,
) -> dict[str, Any]:
    snapshot = load_snapshot(workspace, require_active_config=True)
    if direction not in DIRECTIONS:
        raise error(
            E_INVALID_DIRECTION,
            "Unsupported retrieval direction",
            stage="retrieval",
            details={"direction": direction, "allowed": sorted(DIRECTIONS)},
        )
    try:
        top_k = int(top_k)
    except (TypeError, ValueError) as exc:
        raise error(E_INVALID_K, "top_k must be an integer", stage="retrieval", details={"top_k": top_k}) from exc
    candidates = snapshot.index["dense"]
    if top_k <= 0 or top_k > len(candidates):
        raise error(
            E_INVALID_K,
            "top_k is outside the available index size",
            stage="retrieval",
            details={"top_k": top_k, "index_size": len(candidates)},
        )
    ranking = snapshot.index["ranking"]
    published_weights = {
        "dense_weight": float(ranking["dense_weight"]),
        "inverted_weight": float(ranking["inverted_weight"]),
    }
    if weights is not None:
        requested = {key: float(weights.get(key, published_weights[key])) for key in published_weights}
        if requested != published_weights:
            raise error(
                E_SNAPSHOT_MISMATCH,
                "Query-time ranking weights must match the published index configuration",
                stage="retrieval",
                details={"requested": requested, "published": published_weights},
            )
    dense_weight = published_weights["dense_weight"]
    inverted_weight = published_weights["inverted_weight"]
    query = _prepare_query(
        workspace=workspace,
        snapshot=snapshot,
        direction=direction,
        query_text=query_text,
        image_item_id=image_item_id,
    )
    query_spec = query["quantization"]
    validate_quant_spec(query_spec, item_id=None, stage="query_quantization")
    query_dequant = dequantize_vector(query["quantized"], query_spec)
    query_error_bound = float(query["error_l2_bound"])
    token_count = int(snapshot.config["index"]["visual_token_count"])
    if direction == "text_to_image":
        query_tokens = tokenize(query["detail"]["normalized_text"])
        inverted_postings = snapshot.index["text_inverted"]
    else:
        query_tokens = _latent_tokens(query["quantized"], token_count)
        inverted_postings = snapshot.index["text_latent_inverted"]

    results: list[dict[str, Any]] = []
    for entry in candidates:
        item_id = entry["item_id"]
        visual_spec, text_spec = _route_specs(snapshot, entry)
        target_spec = visual_spec if direction == "text_to_image" else text_spec
        validate_quant_spec(target_spec, item_id=item_id, stage="retrieval")
        target_q = entry["visual_q"] if direction == "text_to_image" else entry["text_q"]
        target_dequant = dequantize_vector(target_q, target_spec)
        norm_product = float(np.linalg.norm(query_dequant) * np.linalg.norm(target_dequant))
        if not math.isfinite(norm_product) or norm_product <= 0:
            raise error(
                E_NUMERIC,
                "Dequantized cosine has zero or non-finite norm product",
                stage="retrieval",
                item_id=item_id,
                details={"norm_product": str(norm_product)},
            )
        dense_cosine = float(np.dot(query_dequant, target_dequant) / norm_product)
        if not math.isfinite(dense_cosine):
            raise error(
                E_NUMERIC,
                "Dequantized cosine is non-finite",
                stage="retrieval",
                item_id=item_id,
                details={"cosine": str(dense_cosine)},
            )
        target_reference = snapshot.reference["vectors"][item_id][
            "visual" if direction == "text_to_image" else "text"
        ]
        reference_cosine = decimal_cosine(query["reference_decimal"], target_reference)
        analytic_bound = similarity_error_bound_decimal(
            query_error_bound,
            float(entry["visual_error_l2_bound" if direction == "text_to_image" else "text_error_l2_bound"]),
            reference_cosine,
        )
        dequantized_cosine = decimal_cosine(
            decimal_vector(query_dequant),
            decimal_vector(target_dequant),
        )
        actual_error = abs(dequantized_cosine - reference_cosine)
        verified_bound = actual_error + VERIFIED_BOUND_MARGIN
        if actual_error > analytic_bound:
            raise error(
                E_NUMERIC,
                "Verified error exceeds the analytic scale-only bound",
                stage="retrieval",
                item_id=item_id,
                details={
                    "actual_error": decimal_text(actual_error),
                    "analytic_bound": decimal_text(analytic_bound),
                },
            )
        error_bound = verified_bound
        within_bound = actual_error <= error_bound and actual_error <= analytic_bound
        if not within_bound:
            raise error(
                E_NUMERIC,
                "Observed retrieval similarity error exceeds the declared bound",
                stage="retrieval",
                item_id=item_id,
                details={
                    "reference_cosine": decimal_text(reference_cosine),
                    "dequantized_cosine": str(dense_cosine),
                    "actual_error": decimal_text(actual_error),
                    "declared_bound": decimal_text(error_bound),
                },
            )
        if direction == "text_to_image":
            item_tokens = snapshot.index["item_postings"][item_id]["caption_tokens"]
        else:
            item_tokens = snapshot.index["item_postings"][item_id]["text_latent_tokens"]
        inverted = _inverted_score(query_tokens, item_tokens, inverted_postings)
        dense_component = rounded_float(dense_weight * dense_cosine, 15)
        inverted_component = rounded_float(inverted_weight * float(inverted["score"]), 15)
        combined = rounded_float(dense_component + inverted_component, 15)
        if direction == "text_to_image":
            visual_component = dense_component
            text_component = inverted_component
        else:
            visual_component = inverted_component
            text_component = dense_component
        effective = effective_digits(reference_cosine, error_bound)
        results.append(
            {
                "item_id": item_id,
                "source_shard": entry["source"],
                "dense_cosine": rounded_float(dense_cosine, 15),
                "reference_cosine": rounded_float(decimal_to_float(reference_cosine), 15),
                "reference_cosine_decimal": decimal_text(reference_cosine),
                "cosine_error_bound": rounded_float(decimal_to_float(error_bound), 15),
                "cosine_error_bound_decimal": decimal_text(error_bound),
                "cosine_error_bound_analytic": rounded_float(decimal_to_float(analytic_bound), 15),
                "cosine_error_bound_analytic_decimal": decimal_text(analytic_bound),
                "error_bound_method": "verified_a_posteriori_plus_1e-12; analytic_scale_envelope_retained",
                "cosine_error_actual": rounded_float(decimal_to_float(actual_error), 15),
                "within_declared_bound": within_bound,
                "effective_digits": effective,
                "quantization_error": {
                    "query_l2_bound": rounded_float(query_error_bound, 15),
                    "target_l2_bound": rounded_float(
                        float(entry["visual_error_l2_bound" if direction == "text_to_image" else "text_error_l2_bound"]),
                        15,
                    ),
                    "bound_source": "explicit_half_step_scale_l2_decimal",
                },
                "inverted": inverted,
                "score_components": {
                    "dense": dense_component,
                    "inverted": inverted_component,
                    "visual": visual_component,
                    "text": text_component,
                },
                "log_contributions": {
                    "visual": _log_contribution(visual_component),
                    "text": _log_contribution(text_component),
                },
                "combined_score": combined,
            }
        )
    results.sort(key=lambda result: (-float(result["combined_score"]), result["item_id"]))
    selected = results[:top_k]
    for rank, result in enumerate(selected, start=1):
        result["rank"] = rank
    body = {
        "format": "shardseek-search-report-v1",
        "direction": direction,
        "query": {
            **query["detail"],
            "route": query["route"],
            "feature_digest": query["feature_digest"],
            "quantized_query_sha256": digest_obj(query["quantized"]),
            "quantization_parameter_digest": digest_obj(query_spec),
            "quantization_error_l2_bound": rounded_float(query_error_bound, 15),
            "reference_vector_sha256": digest_obj(query["reference_decimal"]),
        },
        "top_k": top_k,
        "index_size": len(candidates),
        "weights": published_weights,
        "config_fingerprint": snapshot.state["config_fingerprint"],
        "representation_fingerprint": snapshot.representation_fingerprint,
        "index_fingerprint": snapshot.index_fingerprint,
        "tie_break": "item_id_ascending",
        "sort_key": ["combined_score_desc", "item_id_asc"],
        "error_bound_model": {
            "reference": "Decimal precision-50 cosine over projected float64 vectors",
            "quantization": "per-dimension max(half-step, explicit clipping residual) L2 envelope",
            "similarity_bound": "(Eq+Et+Eq*Et)*(1+|cos_ref|)/((1-Eq)*(1-Et))+1e-15",
            "reported_bound": "exact high-precision dequant/ref difference + 1e-12 verifier margin",
        },
        "results": selected,
        "diagnostics_summary": {
            "max_cosine_error_bound": max((result["cosine_error_bound"] for result in selected), default=0.0),
            "min_effective_digits": min((result["effective_digits"] for result in selected), default=0),
            "all_within_declared_bound": all(result["within_declared_bound"] for result in selected),
            "result_count": len(selected),
        },
    }
    body["report_sha256"] = sha256_bytes(canonical_bytes(body))
    return body


def corpus_diagnostics(workspace: Any) -> dict[str, Any]:
    snapshot = load_snapshot(workspace, require_active_config=True)
    representation = snapshot.representation
    quantization = representation["quantization"]
    items = sorted(representation["items"], key=lambda entry: entry["item_id"])
    if quantization["granularity"] == "per_dim":
        visual_specs = {entry["item_id"]: quantization["visual"] for entry in items}
        text_specs = {entry["item_id"]: quantization["text"] for entry in items}
    else:
        visual_specs = {entry["item_id"]: entry["visual_quantization"] for entry in items}
        text_specs = {entry["item_id"]: entry["text_quantization"] for entry in items}
    series: list[dict[str, Any]] = []
    for entry in items:
        validate_quant_spec(visual_specs[entry["item_id"]], item_id=entry["item_id"], stage="diagnostics")
        validate_quant_spec(text_specs[entry["item_id"]], item_id=entry["item_id"], stage="diagnostics")
    for size in range(1, len(items) + 1):
        prefix = items[:size]
        bounds: list[Decimal] = []
        analytic_bounds: list[Decimal] = []
        effective_values: list[int] = []
        for query_entry in prefix:
            query_id = query_entry["item_id"]
            query_quant = dequantize_vector(query_entry["text_q"], text_specs[query_id])
            query_dequant_strings = decimal_vector(query_quant)
            query_reference = snapshot.reference["vectors"][query_id]["text"]
            for target_entry in prefix:
                target_id = target_entry["item_id"]
                target_quant = dequantize_vector(target_entry["visual_q"], visual_specs[target_id])
                reference_cosine = decimal_cosine(
                    query_reference,
                    snapshot.reference["vectors"][target_id]["visual"],
                )
                dequant_cosine = decimal_cosine(query_dequant_strings, decimal_vector(target_quant))
                analytic_bound = similarity_error_bound_decimal(
                    float(query_entry["text_error_l2_bound"]),
                    float(target_entry["visual_error_l2_bound"]),
                    reference_cosine,
                )
                actual = abs(dequant_cosine - reference_cosine)
                if actual > analytic_bound:
                    raise error(
                        E_NUMERIC,
                        "Corpus diagnostic observed error exceeds the declared bound",
                        stage="diagnostics",
                        item_id=query_id,
                        details={
                            "target_item_id": target_id,
                            "actual_error": decimal_text(actual),
                            "declared_bound": decimal_text(analytic_bound),
                        },
                    )
                bound = actual + VERIFIED_BOUND_MARGIN
                bounds.append(bound)
                analytic_bounds.append(analytic_bound)
                effective_values.append(effective_digits(reference_cosine, bound))
        series.append(
            {
                "shard_count": size,
                "item_count": size,
                "ordered_item_ids": [entry["item_id"] for entry in prefix],
                "pair_count": len(bounds),
                "max_similarity_error_bound": rounded_float(decimal_to_float(max(bounds)), 15),
                "max_similarity_error_bound_decimal": decimal_text(max(bounds)),
                "max_analytic_similarity_error_bound": rounded_float(
                    decimal_to_float(max(analytic_bounds)), 15
                ),
                "max_analytic_similarity_error_bound_decimal": decimal_text(max(analytic_bounds)),
                "mean_similarity_error_bound": rounded_float(
                    decimal_to_float(sum(bounds, Decimal(0)) / Decimal(len(bounds))), 15
                ),
                "min_effective_digits": min(effective_values),
                "max_effective_digits": max(effective_values),
            }
        )
    body = {
        "format": "shardseek-corpus-diagnostics-v1",
        "bits": int(snapshot.config["quantization"]["bits"]),
        "granularity": snapshot.config["quantization"]["granularity"],
        "config_fingerprint": snapshot.state["config_fingerprint"],
        "representation_fingerprint": snapshot.representation_fingerprint,
        "index_fingerprint": snapshot.index_fingerprint,
        "series": series,
    }
    body["diagnostics_sha256"] = sha256_bytes(canonical_bytes(body))
    return body

