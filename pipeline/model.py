from __future__ import annotations

import hashlib
import math
from collections import defaultdict
from typing import Any, Iterable

from .errors import E_EMPTY_CLASS, E_ZERO_SMOOTHING
from .util import canonical_digest, stable_float


MODEL_SCHEMA = "multinomial-nb-hashed-v1"


def is_holdout(doc_id: str, modulus: int, remainder: int) -> bool:
    digest = hashlib.sha256(doc_id.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") % modulus == remainder


def _usable(feature: dict[str, Any], label: str | None, labels: tuple[str, ...]) -> bool:
    return bool(feature.get("tokens")) and not feature.get("error_code") and label in labels


def train_model(
    batch_id: str,
    features: Iterable[dict[str, Any]],
    documents: dict[str, dict[str, Any]],
    labels: tuple[str, ...],
    smoothing: float,
    unknown_token: str,
    hash_dim: int,
    training_scope: str,
) -> dict[str, Any]:
    feature_list = list(features)
    doc_count_by_label = {label: 0 for label in labels}
    token_total_by_label = {label: 0 for label in labels}
    token_counts: dict[str, dict[int, int]] = {label: {} for label in labels}
    seen_terms: set[str] = set()
    bucket_terms_acc: dict[int, set[str]] = defaultdict(set)
    excluded: list[dict[str, str]] = []
    usable_count = 0
    for feature in feature_list:
        doc_id = feature["doc_id"]
        label = documents[doc_id].get("label")
        if not _usable(feature, label, labels):
            error = feature.get("error_code")
            if error:
                excluded.append({"doc_id": doc_id, "error_code": error})
            elif label not in labels:
                excluded.append({"doc_id": doc_id, "error_code": "E_INVALID_LABEL"})
            else:
                excluded.append({"doc_id": doc_id, "error_code": "E_NO_TOKENS"})
            continue
        usable_count += 1
        doc_count_by_label[label] += 1
        counts = token_counts[label]
        for token in feature["tokens"]:
            term = token["term"]
            bucket = int(token["bucket"])
            counts[bucket] = counts.get(bucket, 0) + 1
            token_total_by_label[label] += 1
            seen_terms.add(term)
            bucket_terms_acc[bucket].add(term)
    all_buckets = sorted(bucket_terms_acc)
    vocabulary_size = len(all_buckets)
    unknown_vocabulary_index = vocabulary_size
    effective_vocabulary_size = vocabulary_size + 1
    priors: dict[str, Any] = {}
    denominators: dict[str, int] = {}
    log_likelihoods: dict[str, dict[str, Any]] = {}
    unknown_likelihoods: dict[str, Any] = {}
    diagnostics: list[dict[str, Any]] = []
    for label in labels:
        count = doc_count_by_label[label]
        if usable_count == 0:
            priors[label] = {"doc_count": count, "log_prior": None, "status": E_EMPTY_CLASS}
        elif count == 0:
            priors[label] = {"doc_count": 0, "log_prior": None, "status": E_EMPTY_CLASS}
            diagnostics.append({"label": label, "error_code": E_EMPTY_CLASS})
        else:
            priors[label] = {
                "doc_count": count,
                "log_prior": math.log(count / usable_count),
                "status": "ok",
            }
        denominator = token_total_by_label[label] + smoothing * effective_vocabulary_size
        denominators[label] = denominator
        label_likelihoods: dict[str, Any] = {}
        class_counts = token_counts[label]
        for bucket in all_buckets:
            numerator = class_counts.get(bucket, 0) + smoothing
            if numerator <= 0.0 or denominator <= 0.0:
                label_likelihoods[str(bucket)] = None
                if smoothing == 0.0:
                    diagnostics.append(
                        {
                            "label": label,
                            "bucket": bucket,
                            "error_code": E_ZERO_SMOOTHING,
                        }
                    )
            else:
                label_likelihoods[str(bucket)] = math.log(numerator / denominator)
        unknown_numerator = smoothing
        if unknown_numerator <= 0.0 or denominator <= 0.0:
            unknown_likelihoods[label] = None
        else:
            unknown_likelihoods[label] = math.log(unknown_numerator / denominator)
        log_likelihoods[label] = label_likelihoods
    model = {
        "schema": MODEL_SCHEMA,
        "batch_id": batch_id,
        "training_scope": training_scope,
        "labels": list(labels),
        "hash_dim": hash_dim,
        "smoothing": smoothing,
        "unknown_token": unknown_token,
        "training": {
            "usable_doc_count": usable_count,
            "doc_count_by_label": doc_count_by_label,
            "token_total_by_label": token_total_by_label,
            "excluded_docs": excluded,
            "vocabulary_size": vocabulary_size,
            "effective_vocabulary_size": effective_vocabulary_size,
            "unknown_vocabulary_index": unknown_vocabulary_index,
        },
        "bucket_terms": {str(bucket): sorted(bucket_terms_acc[bucket]) for bucket in all_buckets},
        "seen_terms": sorted(seen_terms),
        "priors": priors,
        "token_counts": {label: {str(bucket): token_counts[label][bucket] for bucket in sorted(token_counts[label])} for label in labels},
        "denominators": denominators,
        "log_likelihoods": log_likelihoods,
        "unknown_log_likelihoods": unknown_likelihoods,
        "diagnostics": diagnostics,
    }
    model["model_digest"] = canonical_digest(model)
    return model


def _probabilities(log_scores: dict[str, float | None]) -> dict[str, float]:
    finite = {label: value for label, value in log_scores.items() if value is not None and math.isfinite(value)}
    if not finite:
        return {label: 0.0 for label in sorted(log_scores)}
    maximum = max(finite.values())
    exponentials = {label: math.exp(value - maximum) for label, value in finite.items()}
    total = math.fsum(exponentials.values())
    return {
        label: (exponentials[label] / total if label in exponentials else 0.0)
        for label in sorted(log_scores)
    }


def score_features(
    model: dict[str, Any],
    feature: dict[str, Any],
) -> dict[str, Any]:
    if feature.get("error_code"):
        error_code = feature["error_code"]
        return {
            "batch_id": model["batch_id"],
            "doc_id": feature["doc_id"],
            "model_digest": model["model_digest"],
            "training_scope": model["training_scope"],
            "tokens": [],
            "per_class": {},
            "log_scores": {label: None for label in sorted(model["labels"])},
            "probabilities": {label: 0.0 for label in sorted(model["labels"])},
            "score_error_codes": [error_code],
        }
    labels = tuple(sorted(model["labels"]))
    seen_terms = set(model["seen_terms"])
    unknown_token = model["unknown_token"]
    token_trace: list[dict[str, Any]] = []
    predicted_buckets: list[tuple[int | None, str, str]] = []
    for token in feature.get("tokens", []):
        term = token["term"]
        if term in seen_terms:
            bucket: int | None = int(token["bucket"])
            effective_term = term
        else:
            bucket = None
            effective_term = unknown_token
        predicted_buckets.append((bucket, term, effective_term))
        token_trace.append(
            {
                "position": token["position"],
                "term": term,
                "effective_term": effective_term,
                "bucket": bucket,
            }
        )
    log_scores: dict[str, float | None] = {}
    per_class: dict[str, dict[str, Any]] = {}
    score_errors: set[str] = set()
    for label in labels:
        prior_info = model["priors"][label]
        log_prior = prior_info["log_prior"]
        if log_prior is None:
            log_scores[label] = None
            per_class[label] = {
                "log_prior": None,
                "log_likelihood_sum": None,
                "status": E_EMPTY_CLASS,
            }
            score_errors.add(E_EMPTY_CLASS)
            continue
        contributions: list[float] = []
        status = "ok"
        for bucket, _term, _effective in predicted_buckets:
            if bucket is None:
                value = model["unknown_log_likelihoods"][label]
            else:
                value = model["log_likelihoods"][label].get(str(bucket))
            if value is None:
                status = E_ZERO_SMOOTHING
                score_errors.add(E_ZERO_SMOOTHING)
                contributions = []
                break
            contributions.append(float(value))
        if status != "ok":
            log_scores[label] = None
            per_class[label] = {
                "log_prior": stable_float(float(log_prior)),
                "log_likelihood_sum": None,
                "status": status,
            }
            continue
        likelihood_sum = math.fsum(contributions)
        log_scores[label] = float(log_prior) + likelihood_sum
        per_class[label] = {
            "log_prior": stable_float(float(log_prior)),
            "log_likelihood_sum": stable_float(likelihood_sum),
            "token_contribution_count": len(contributions),
            "status": "ok",
        }
    probabilities = _probabilities(log_scores)
    return {
        "batch_id": model["batch_id"],
        "doc_id": feature["doc_id"],
        "model_digest": model["model_digest"],
        "training_scope": model["training_scope"],
        "tokens": token_trace,
        "per_class": per_class,
        "log_scores": {label: (stable_float(value) if value is not None else None) for label, value in sorted(log_scores.items())},
        "probabilities": {label: probabilities[label] for label in labels},
        "score_error_codes": sorted(score_errors),
    }


def train_and_score(
    batch_id: str,
    features: list[dict[str, Any]],
    documents: dict[str, dict[str, Any]],
    labels: tuple[str, ...],
    smoothing: float,
    unknown_token: str,
    hash_dim: int,
    training_scope: str,
    training_doc_ids: set[str] | None = None,
    scoring_doc_ids: set[str] | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    training_features = [
        feature
        for feature in features
        if training_doc_ids is None or feature["doc_id"] in training_doc_ids
    ]
    model = train_model(
        batch_id=batch_id,
        features=training_features,
        documents=documents,
        labels=labels,
        smoothing=smoothing,
        unknown_token=unknown_token,
        hash_dim=hash_dim,
        training_scope=training_scope,
    )
    scoring_features = [
        feature
        for feature in features
        if scoring_doc_ids is None or feature["doc_id"] in scoring_doc_ids
    ]
    scored = [score_features(model, feature) for feature in scoring_features]
    scored.sort(key=lambda row: row["doc_id"])
    return model, scored


def model_parameter_summary(model: dict[str, Any]) -> dict[str, Any]:
    return {
        "batch_id": model["batch_id"],
        "training_scope": model["training_scope"],
        "model_digest": model["model_digest"],
        "labels": model["labels"],
        "smoothing": model["smoothing"],
        "unknown_token": model["unknown_token"],
        "hash_dim": model["hash_dim"],
        "usable_doc_count": model["training"]["usable_doc_count"],
        "vocabulary_size": model["training"]["vocabulary_size"],
        "doc_count_by_label": model["training"]["doc_count_by_label"],
        "diagnostic_count": len(model["diagnostics"]),
    }


def bucket_collision_term_map(feature_list: Iterable[dict[str, Any]]) -> dict[int, list[str]]:
    buckets: dict[int, set[str]] = defaultdict(set)
    for feature in feature_list:
        for token in feature.get("tokens", []):
            buckets[int(token["bucket"])].add(token["term"])
    return {bucket: sorted(terms) for bucket, terms in sorted(buckets.items()) if len(terms) > 1}
