from __future__ import annotations

import hashlib
import math
import re
import time
import unicodedata
from collections import Counter
from typing import Any, Callable

import numpy as np

from .canonical import digest_json, round_float, round_vector, sha256_bytes
from .config import Config
from .dataio import Dataset
from .errors import BuildStopped, business_error
from .injection import Injection, injection_map


STAGES = ("text_encoding", "projection_alignment", "fusion_scoring", "aggregation")
STAGE_INDEX = {name: index + 1 for index, name in enumerate(STAGES)}
TOKEN_PATTERN = re.compile(
    r"[A-Za-z0-9]+(?:['’][A-Za-z0-9]+)?|[\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff]|[\u3040-\u30ff\uac00-\ud7af]+"
)
FULLWIDTH_TRANSLATION = str.maketrans(
    "！＂＃＄％＆＇（）＊＋，－．／０１２３４５６７８９：；＜＝＞？＠［＼］＾＿｀｛｜｝～",
    "!\"#$%&'()*+,-./0123456789:;<=>?@[\\]^_`{|}~",
)


StopCallback = Callable[[], bool]


def _stop(stage: str, completed_stages: list[str]) -> None:
    raise BuildStopped(stage, completed_stages)


def _pause(config: Config) -> None:
    delay = config.unit_delay_seconds
    if delay:
        time.sleep(delay)


def normalize_text(text: str) -> str:
    folded = unicodedata.normalize("NFKC", text).translate(FULLWIDTH_TRANSLATION)
    collapsed = re.sub(r"\s+", " ", folded).strip()
    return collapsed.lower()


def tokenize_offsets(normalized: str) -> list[dict[str, Any]]:
    return [
        {"token": match.group(0), "start": match.start(), "end": match.end()}
        for match in TOKEN_PATTERN.finditer(normalized)
    ]


def _meaningful(normalized: str) -> bool:
    return any(unicodedata.category(character)[0] in {"L", "N"} for character in normalized)


def _token_hash_vector(token: str, salt: str) -> list[float]:
    values: list[float] = []
    prefix = f"{salt}|token|{token}".encode("utf-8")
    for dimension in range(64):
        digest = hashlib.sha256(prefix + b"|" + str(dimension).encode("ascii")).digest()
        integer = int.from_bytes(digest[:8], "big")
        values.append(integer / float(1 << 64) - 0.5)
    return values


def hash_embedding(tokens: list[dict[str, Any]], salt: str) -> list[float]:
    accumulated = [0.0] * 64
    for token in tokens:
        vector = _token_hash_vector(str(token["token"]), salt)
        for index, value in enumerate(vector):
            accumulated[index] += value
    norm = math.sqrt(math.fsum(value * value for value in accumulated))
    if not math.isfinite(norm) or norm <= 1e-15:
        return [0.0] * 64
    return round_vector([value / norm for value in accumulated])


def _target_id(sample_id: str | None, records: list[dict[str, Any]], id_field: str) -> str:
    if sample_id is not None and any(str(record.get(id_field)) == sample_id for record in records):
        return sample_id
    if not records:
        return "<projection>"
    return str(records[0][id_field])


def compute_text_encoding(
    dataset: Dataset,
    config: Config,
    *,
    injection: Injection | None = None,
    should_stop: StopCallback | None = None,
) -> dict[str, Any]:
    records: list[dict[str, Any]] = []
    seen_content: dict[str, list[str]] = {}
    injected_too_long = injection if injection and injection.kind == "text_too_long" else None
    target = _target_id(injected_too_long.sample_id if injected_too_long else None, list(dataset.texts), "text_id")

    for text_record in dataset.texts:
        if should_stop and should_stop():
            _stop("text_encoding", [])
        text_id = str(text_record["text_id"])
        text = str(text_record["text"])
        if injected_too_long and text_id == target:
            text = text + ("x" * (config.max_chars + 16))
        normalized = normalize_text(text)
        if len(normalized) > config.max_chars:
            raise business_error(
                "E_TEXT_TOO_LONG",
                "text_encoding",
                "normalized text exceeds the configured character limit",
                sample_id=text_id,
                field="text",
                diagnostics={
                    "length": len(normalized),
                    "max_chars": config.max_chars,
                    "injected": bool(injected_too_long and text_id == target),
                },
                http_status=422,
            )
        if not normalized or not _meaningful(normalized):
            raise business_error(
                "E_EMPTY_INPUT",
                "text_encoding",
                "text is empty or contains no letters/numbers after normalization",
                sample_id=text_id,
                field="text",
                diagnostics={
                    "original_length": len(text),
                    "normalized_length": len(normalized),
                    "normalized_text": normalized,
                },
                http_status=422,
            )
        tokens = tokenize_offsets(normalized)
        embedding = hash_embedding(tokens, config.hash_salt)
        if not any(value != 0.0 for value in embedding):
            raise business_error(
                "E_EMPTY_INPUT",
                "text_encoding",
                "fixed-hash embedding is a zero vector",
                sample_id=text_id,
                field="embedding",
                diagnostics={"token_count": len(tokens), "embedding_dim": config.embedding_dim},
                http_status=422,
            )
        if len(embedding) != config.embedding_dim:
            raise business_error(
                "E_EMPTY_INPUT",
                "text_encoding",
                "embedding dimension does not match configuration",
                sample_id=text_id,
                field="embedding",
                diagnostics={"embedding_dim": len(embedding), "configured_dim": config.embedding_dim},
                http_status=422,
            )
        record = {
            "text_id": text_id,
            "content_sha256": sha256_bytes(normalized.encode("utf-8")),
            "normalized_text": normalized,
            "original_char_length": len(text),
            "normalized_char_length": len(normalized),
            "tokens": tokens,
            "embedding": embedding,
        }
        records.append(record)
        seen_content.setdefault(record["content_sha256"], []).append(text_id)
        _pause(config)
        if should_stop and should_stop():
            _stop("text_encoding", [])

    duplicate_groups = [
        {"content_sha256": key, "text_ids": sorted(ids)}
        for key, ids in sorted(seen_content.items())
        if len(ids) > 1
    ]
    return {
        "schema_version": "mmcoop.artifact.v1",
        "stage": "text_encoding",
        "stage_index": 1,
        "structure_version": config.structure_version,
        "upstream": {},
        "records": sorted(records, key=lambda item: item["text_id"]),
        "summary": {
            "text_count": len(records),
            "token_count": sum(len(record["tokens"]) for record in records),
            "duplicate_input_records": dataset.text_duplicates,
            "duplicate_content_groups": duplicate_groups,
            "embedding_dim": config.embedding_dim,
        },
    }


def _matrix_error(
    image_id: str,
    raw_dim: int,
    expected_dim: int,
    matrix: list[list[float]],
    *,
    injected: bool = False,
    reason: str,
) -> None:
    row_lengths = sorted({len(row) for row in matrix})
    raise business_error(
        "E_PROJECTION_INVALID",
        "projection_alignment",
        reason,
        sample_id=image_id,
        field="feature" if raw_dim != expected_dim else "projection_matrix",
        diagnostics={
            "original_dim": raw_dim,
            "expected_dim": expected_dim,
            "matrix_shape": [len(matrix), row_lengths[0] if len(row_lengths) == 1 else row_lengths],
            "configured_input_dim": expected_dim,
            "injected": injected,
        },
        http_status=422,
    )


def compute_projection_alignment(
    text_artifact: dict[str, Any],
    dataset: Dataset,
    config: Config,
    *,
    injection: Injection | None = None,
    should_stop: StopCallback | None = None,
) -> dict[str, Any]:
    injections = injection_map((injection,)) if injection else {}
    dim_injection = injections.get("projection_dim")
    missing_injection = injections.get("missing_ref")
    nan_injection = injections.get("similarity_nan")
    images = list(dataset.images)
    dim_image = _target_id(dim_injection.sample_id if dim_injection else None, images, "image_id")
    missing_image = _target_id(missing_injection.sample_id if missing_injection else None, images, "image_id")
    nan_image = _target_id(nan_injection.sample_id if nan_injection else None, images, "image_id")

    text_map = {record["text_id"]: record for record in text_artifact["records"]}
    matrix = config.matrix
    expected_input = config.projection_input_dim
    expected_output = config.projection_output_dim
    text_dim = config.embedding_dim
    if dim_injection:
        expected_input = max(0, expected_input - 1)
    if images and (not matrix or any(len(row) != expected_input for row in matrix) or len(matrix) != expected_output):
        _matrix_error(
            dim_image,
            len(images[0].get("feature", [])),
            expected_input,
            matrix,
            injected=bool(dim_injection),
            reason="projection matrix shape does not match configured dimensions",
        )
    if expected_output != text_dim:
        _matrix_error(
            dim_image,
            len(images[0].get("feature", [])) if images else expected_input,
            expected_input,
            matrix,
            reason="projection output dimension does not match text embedding dimension",
        )

    matrix_array = np.asarray(matrix, dtype=np.float64)
    records: list[dict[str, Any]] = []
    similarities: list[float] = []
    for image in images:
        if should_stop and should_stop():
            _stop("projection_alignment", ["text_encoding"])
        image_id = str(image["image_id"])
        feature = image.get("feature", [])
        if dim_injection and image_id == dim_image:
            _matrix_error(
                image_id,
                len(feature),
                expected_input,
                matrix,
                injected=True,
                reason="injected illegal projection dimension",
            )
        if len(feature) != expected_input:
            _matrix_error(
                image_id,
                len(feature),
                expected_input,
                matrix,
                reason="visual feature dimension does not match projection input dimension",
            )
        try:
            feature_array = np.asarray([float(value) for value in feature], dtype=np.float64)
        except (TypeError, ValueError) as error:
            raise business_error(
                "E_PROJECTION_INVALID",
                "projection_alignment",
                "visual feature contains a non-numeric value",
                sample_id=image_id,
                field="feature",
                diagnostics={"original_dim": len(feature), "expected_dim": expected_input},
                http_status=422,
            ) from error
        if not np.isfinite(feature_array).all():
            raise business_error(
                "E_PROJECTION_INVALID",
                "projection_alignment",
                "visual feature contains NaN or Inf",
                sample_id=image_id,
                field="feature",
                diagnostics={"original_dim": len(feature), "expected_dim": expected_input},
                http_status=422,
            )

        text_id = image.get("text_id")
        if (missing_injection and image_id == missing_image) or text_id is None or str(text_id) not in text_map:
            injected_missing = bool(missing_injection and image_id == missing_image)
            raise business_error(
                "E_MISSING_REF",
                "projection_alignment",
                "image references a text_id that does not exist",
                sample_id=image_id,
                field="text_id",
                diagnostics={
                    "missing_text_id": "__injected_missing_text__" if injected_missing else text_id,
                    "available_text_ids": sorted(text_map),
                    "injected": injected_missing,
                },
                http_status=422,
            )
        text_record = text_map[str(text_id)]
        text_embedding = np.asarray(text_record["embedding"], dtype=np.float64)
        projected = matrix_array @ feature_array
        if not np.isfinite(projected).all():
            raise business_error(
                "E_PROJECTION_INVALID",
                "projection_alignment",
                "projected vector contains NaN or Inf",
                sample_id=image_id,
                field="feature",
                diagnostics={"original_dim": len(feature), "expected_dim": expected_input},
                http_status=422,
            )
        projected_norm = float(np.linalg.norm(projected))
        text_norm = float(np.linalg.norm(text_embedding))
        if not math.isfinite(projected_norm) or not math.isfinite(text_norm) or projected_norm <= 1e-15 or text_norm <= 1e-15:
            raise business_error(
                "E_PROJECTION_INVALID",
                "projection_alignment",
                "projection or text embedding has zero/non-finite norm",
                sample_id=image_id,
                field="feature",
                diagnostics={"projected_norm": projected_norm, "text_norm": text_norm},
                http_status=422,
            )
        projected_unit = projected / projected_norm
        similarity = float(np.dot(projected_unit, text_embedding / text_norm))
        if nan_injection and image_id == nan_image:
            raise business_error(
                "E_NUMERIC",
                "projection_alignment",
                "injected cosine similarity is NaN",
                sample_id=image_id,
                field="similarity",
                diagnostics={"similarity": "NaN", "injected": True},
                http_status=422,
            )
        if not math.isfinite(similarity):
            raise business_error(
                "E_PROJECTION_INVALID",
                "projection_alignment",
                "cosine similarity is NaN or Inf",
                sample_id=image_id,
                field="similarity",
                diagnostics={"similarity": str(similarity), "stage": "projection_alignment"},
                http_status=422,
            )
        similarity = max(-1.0, min(1.0, similarity))
        similarities.append(similarity)
        records.append(
            {
                "image_id": image_id,
                "text_id": str(text_id),
                "feature_dim": len(feature),
                "projection_output_dim": len(projected_unit),
                "projected_vector": round_vector(projected_unit),
                "cosine_similarity": round_float(similarity),
            }
        )
        _pause(config)
        if should_stop and should_stop():
            _stop("projection_alignment", ["text_encoding"])

    return {
        "schema_version": "mmcoop.artifact.v1",
        "stage": "projection_alignment",
        "stage_index": 2,
        "structure_version": config.structure_version,
        "upstream": {"text_encoding": digest_json(text_artifact)},
        "records": sorted(records, key=lambda item: item["image_id"]),
        "summary": {
            "image_count": len(records),
            "similarity_min": round_float(min(similarities)) if similarities else None,
            "similarity_max": round_float(max(similarities)) if similarities else None,
            "similarity_mean": round_float(float(np.mean(similarities))) if similarities else None,
            "matrix_shape": [len(matrix), len(matrix[0]) if matrix else 0],
        },
    }


def detect_language(normalized_text: str) -> str:
    has_han = any("\u3400" <= character <= "\u9fff" or "\uf900" <= character <= "\ufaff" for character in normalized_text)
    has_latin = any("a" <= character <= "z" for character in normalized_text)
    if has_han and has_latin:
        return "zh-en"
    if has_han:
        return "zh"
    if has_latin:
        return "en"
    return "unknown"


def language_consistency(text_record: dict[str, Any], config: Config) -> tuple[float | None, str, str]:
    declared = str(text_record.get("lang", "")).strip().lower().replace("_", "-")
    mixed_labels = {str(value).lower().replace("_", "-") for value in config.language_config.get("mixed_labels", ["zh-en", "mixed"])}
    detected = detect_language(normalize_text(str(text_record.get("text", ""))))
    if not declared or "unknown" in declared:
        return None, declared, detected
    if declared in mixed_labels or declared in {"zh-en", "mixed"}:
        if detected == "zh-en":
            return 1.0, declared, detected
        if detected in {"zh", "en"}:
            return 0.5, declared, detected
        return None, declared, detected
    if declared in {"zh", "en"}:
        if detected == declared:
            return 1.0, declared, detected
        if detected == "zh-en":
            return 0.5, declared, detected
        return 0.0, declared, detected
    return None, declared, detected


def _component(value: float | None, reason: str | None = None) -> dict[str, Any]:
    if value is None:
        return {"available": False, "value": None, "reason": reason or "unavailable"}
    if not math.isfinite(value):
        return {"available": False, "value": None, "reason": "non_finite"}
    return {"available": True, "value": round_float(value), "reason": None}


def compute_fusion_scoring(
    projection_artifact: dict[str, Any],
    dataset: Dataset,
    config: Config,
    *,
    injection: Injection | None = None,
    should_stop: StopCallback | None = None,
) -> dict[str, Any]:
    projections = {record["image_id"]: record for record in projection_artifact["records"]}
    text_map = dataset.text_map()
    image_map = dataset.image_map()
    operator_injection = injection if injection and injection.kind == "invalid_operator" else None
    operator_target = _target_id(operator_injection.sample_id if operator_injection else None, list(dataset.images), "image_id")
    weights = config.weights
    thresholds = config.thresholds
    confidence_config = config.operator_confidence
    records: list[dict[str, Any]] = []
    verdicts: Counter[str] = Counter()

    for image_id in sorted(projections):
        if should_stop and should_stop():
            _stop("fusion_scoring", ["text_encoding", "projection_alignment"])
        projection = projections[image_id]
        image = image_map[image_id]
        operator = str(image.get("source_operator", ""))
        if operator_injection and image_id == operator_target:
            operator = "__injected_invalid_operator__"
        if operator not in confidence_config:
            raise business_error(
                "E_INVALID_OPERATOR",
                "fusion_scoring",
                "source operator is not present in the confidence configuration",
                sample_id=image_id,
                field="source_operator",
                diagnostics={
                    "operator": operator,
                    "known_operators": sorted(confidence_config),
                    "injected": bool(operator_injection and image_id == operator_target),
                },
                http_status=422,
            )
        confidence = confidence_config[operator]
        if confidence is not None and (not math.isfinite(float(confidence)) or not 0.0 <= float(confidence) <= 1.0):
            raise business_error(
                "E_INVALID_OPERATOR",
                "fusion_scoring",
                "operator confidence must be null or a finite value in [0, 1]",
                sample_id=image_id,
                field="operator_confidence",
                diagnostics={"operator": operator, "confidence": confidence},
                http_status=422,
            )
        text = text_map[projection["text_id"]]
        language_value, declared_language, detected_language = language_consistency(text, config)
        similarity = float(projection["cosine_similarity"])
        similarity_value = (similarity + 1.0) / 2.0 if math.isfinite(similarity) else None
        components = {
            "similarity": _component(similarity_value, None if similarity_value is not None else "invalid_similarity"),
            "operator_confidence": _component(
                None if confidence is None else float(confidence),
                None if confidence is not None else "operator_confidence_unavailable",
            ),
            "language_consistency": _component(language_value, None if language_value is not None else "language_unknown"),
        }
        skipped: list[dict[str, str]] = []
        effective: dict[str, float] = {}
        active_weight_sum = 0.0
        for name, component in components.items():
            weight = float(weights.get(name, 0.0))
            if not math.isfinite(weight) or weight < 0:
                skipped.append({"component": name, "reason": "invalid_weight", "weight": str(weight)})
                continue
            if weight == 0.0:
                skipped.append({"component": name, "reason": "zero_weight", "weight": "0.0"})
                continue
            if not component["available"]:
                skipped.append({"component": name, "reason": str(component["reason"]), "weight": str(round_float(weight))})
                continue
            effective[name] = weight
            active_weight_sum += weight
        if active_weight_sum <= 1e-15:
            raise business_error(
                "E_WEIGHT_ZERO",
                "fusion_scoring",
                "all usable component weights sum to zero",
                sample_id=image_id,
                field="weights",
                diagnostics={
                    "weights": weights,
                    "components": components,
                    "skipped_components": skipped,
                    "active_weight_sum": 0.0,
                },
                http_status=422,
            )
        normalized_weights = {name: round_float(weight / active_weight_sum) for name, weight in effective.items()}
        score = math.fsum(float(components[name]["value"]) * normalized_weights[name] for name in effective)
        score = round_float(score)
        if score >= thresholds["consistent"]:
            verdict = "consistent"
        elif score >= thresholds["weak"]:
            verdict = "weak"
        else:
            verdict = "conflict"
        verdicts[verdict] += 1
        contributions: dict[str, Any] = {}
        for name, component in components.items():
            if name in normalized_weights:
                value = float(component["value"])
                contributions[name] = {
                    "available": True,
                    "value": round_float(value),
                    "original_weight": round_float(weights[name]),
                    "normalized_weight": normalized_weights[name],
                    "log_value": round_float(math.log(max(value, 1e-12))),
                    "log_contribution": round_float(math.log(max(value, 1e-12)) * normalized_weights[name]),
                    "linear_contribution": round_float(value * normalized_weights[name]),
                }
            else:
                contributions[name] = {
                    "available": False,
                    "value": component["value"],
                    "reason": component["reason"] or next(
                        (item["reason"] for item in skipped if item["component"] == name), "skipped"
                    ),
                }
        records.append(
            {
                "image_id": image_id,
                "text_id": projection["text_id"],
                "source_operator": operator,
                "language_declared": declared_language,
                "language_detected": detected_language,
                "components": components,
                "weights": {
                    "original": weights,
                    "active_sum": round_float(active_weight_sum),
                    "normalized": normalized_weights,
                    "skipped": skipped,
                    "renormalized": len(skipped) > 0,
                },
                "log_contributions": contributions,
                "fusion_score": score,
                "verdict": verdict,
            }
        )
        _pause(config)
        if should_stop and should_stop():
            _stop("fusion_scoring", ["text_encoding", "projection_alignment"])

    return {
        "schema_version": "mmcoop.artifact.v1",
        "stage": "fusion_scoring",
        "stage_index": 3,
        "structure_version": config.structure_version,
        "upstream": {"projection_alignment": digest_json(projection_artifact)},
        "records": records,
        "summary": {"image_count": len(records), "verdict_counts": dict(sorted(verdicts.items()))},
    }


def compute_aggregation(
    fusion_artifact: dict[str, Any],
    dataset: Dataset,
    config: Config,
    *,
    should_stop: StopCallback | None = None,
) -> dict[str, Any]:
    by_text: dict[str, list[dict[str, Any]]] = {}
    for record in fusion_artifact["records"]:
        by_text.setdefault(record["text_id"], []).append(record)
    records: list[dict[str, Any]] = []
    verdicts: Counter[str] = Counter()
    missing_link_ids: list[str] = []
    missing_verdict = str(config.aggregation.get("missing_link_verdict", "weak"))
    if missing_verdict not in {"consistent", "weak", "conflict"}:
        missing_verdict = "weak"

    for text in sorted(dataset.texts, key=lambda item: item["text_id"]):
        if should_stop and should_stop():
            _stop("aggregation", ["text_encoding", "projection_alignment", "fusion_scoring"])
        text_id = str(text["text_id"])
        linked = sorted(by_text.get(text_id, []), key=lambda item: item["image_id"])
        if not linked:
            verdict = missing_verdict
            missing_link_ids.append(text_id)
            diagnostics = [
                {
                    "code": "missing_image_link",
                    "image_id": None,
                    "verdict": verdict,
                    "fusion_score": None,
                }
            ]
            scores: list[float] = []
            operators: list[str] = []
        else:
            linked_verdicts = {record["verdict"] for record in linked}
            if "conflict" in linked_verdicts:
                verdict = "conflict"
            elif "weak" in linked_verdicts:
                verdict = "weak"
            else:
                verdict = "consistent"
            scores = [float(record["fusion_score"]) for record in linked]
            operators = sorted({str(record["source_operator"]) for record in linked})
            diagnostics = [
                {
                    "code": "linked_evaluation",
                    "image_id": record["image_id"],
                    "verdict": record["verdict"],
                    "fusion_score": record["fusion_score"],
                    "log_contributions": record["log_contributions"],
                }
                for record in linked
            ]
        verdicts[verdict] += 1
        records.append(
            {
                "text_id": text_id,
                "lang": text["lang"],
                "source_channel": text["source_channel"],
                "linked_image_ids": [record["image_id"] for record in linked],
                "source_operators": operators,
                "verdict": verdict,
                "fusion_score_min": round_float(min(scores)) if scores else None,
                "fusion_score_max": round_float(max(scores)) if scores else None,
                "fusion_score_mean": round_float(float(np.mean(scores))) if scores else None,
                "diagnostics": diagnostics,
            }
        )
        _pause(config)
        if should_stop and should_stop():
            _stop("aggregation", ["text_encoding", "projection_alignment", "fusion_scoring"])

    duplicate_groups: list[dict[str, Any]] = []
    content_groups: dict[str, list[str]] = {}
    for text in dataset.texts:
        content_groups.setdefault(sha256_bytes(str(text["text"]).encode("utf-8")), []).append(str(text["text_id"]))
    for key, ids in sorted(content_groups.items()):
        if len(ids) > 1:
            duplicate_groups.append({"content_sha256": key, "text_ids": sorted(ids)})
    return {
        "schema_version": "mmcoop.artifact.v1",
        "stage": "aggregation",
        "stage_index": 4,
        "structure_version": config.structure_version,
        "upstream": {"fusion_scoring": digest_json(fusion_artifact)},
        "records": records,
        "summary": {
            "text_count": len(records),
            "image_count": len(fusion_artifact["records"]),
            "linked_text_count": len(records) - len(missing_link_ids),
            "missing_link_text_ids": missing_link_ids,
            "verdict_counts": dict(sorted(verdicts.items())),
            "duplicate_content_groups": duplicate_groups,
        },
    }
