"""Feature normalization, explicit numeric buckets, hashing, and crossing."""

from __future__ import annotations

import bisect
import math
import re
import unicodedata
from collections import defaultdict
from typing import Any, Iterable

import numpy as np

from .config import Config
from .errors import (
    E_CROSS_LIMIT,
    E_HASH_DIM_ZERO,
    E_INVALID_FEATURE,
    E_INVALID_STAGE,
    E_VECTOR_LENGTH_MISMATCH,
    PipelineError,
)
from .util import stable_hash_int


WHITESPACE_RE = re.compile(r"\s+", flags=re.UNICODE)


def normalize_category_value(value: Any, *, sample_id: str, field: str) -> str:
    if not isinstance(value, str):
        raise PipelineError(
            E_INVALID_FEATURE,
            "categorical feature value must be a string",
            sample_id=sample_id,
            field=field,
            value=value,
        )
    normalized = unicodedata.normalize("NFKC", value)
    normalized = WHITESPACE_RE.sub(" ", normalized).strip().lower()
    if not normalized:
        raise PipelineError(
            E_INVALID_FEATURE,
            "categorical feature is empty after normalization",
            sample_id=sample_id,
            field=field,
            raw_value=value,
        )
    return normalized


def _split_field(field: str) -> tuple[str, str]:
    if "." not in field:
        raise ValueError(f"field must use region.name form: {field}")
    return tuple(field.split(".", 1))  # type: ignore[return-value]


def _get_region(record: dict[str, Any], region: str, sample_id: str) -> dict[str, Any]:
    value = record.get(region)
    if not isinstance(value, dict) or not value:
        raise PipelineError(
            E_INVALID_FEATURE,
            "feature region is missing or empty",
            sample_id=sample_id,
            field=region,
        )
    return value


def _validate_boundaries(
    boundaries: Any, *, sample_id: str, field: str
) -> list[float]:
    if not isinstance(boundaries, list):
        raise PipelineError(
            E_INVALID_FEATURE,
            "numeric bucket boundaries must be a list",
            sample_id=sample_id,
            field=field,
        )
    parsed: list[float] = []
    for value in boundaries:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise PipelineError(
                E_INVALID_FEATURE,
                "numeric bucket boundary must be a finite number",
                sample_id=sample_id,
                field=field,
                boundary=value,
            )
        numeric = float(value)
        if not math.isfinite(numeric):
            raise PipelineError(
                E_INVALID_FEATURE,
                "numeric bucket boundary must be finite",
                sample_id=sample_id,
                field=field,
                boundary=value,
            )
        parsed.append(numeric)
    for previous, current in zip(parsed, parsed[1:]):
        if current <= previous:
            raise PipelineError(
                E_INVALID_FEATURE,
                "numeric bucket boundaries must be strictly increasing",
                sample_id=sample_id,
                field=field,
                boundaries=boundaries,
            )
    return parsed


def _bucket_label(boundaries: list[float], bucket_index: int, missing_label: str) -> str:
    if bucket_index < 0:
        return missing_label
    if bucket_index == 0:
        if not boundaries:
            return "(-inf,inf)"
        return f"(-inf,{boundaries[0]:g})"
    if bucket_index >= len(boundaries):
        return f"[{boundaries[-1]:g},inf)"
    return f"[{boundaries[bucket_index - 1]:g},{boundaries[bucket_index]:g})"


def _normalize_numeric(
    region_value: dict[str, Any],
    *,
    region: str,
    field_name: str,
    spec: dict[str, Any],
    sample_id: str,
    field: str,
) -> dict[str, Any]:
    missing = field_name not in region_value or region_value[field_name] is None
    raw_value = region_value.get(field_name)
    if not missing and isinstance(raw_value, str) and not raw_value.strip():
        missing = True
    boundaries = _validate_boundaries(spec.get("boundaries", []), sample_id=sample_id, field=field)
    missing_label = str(spec.get("missing_bucket", "__MISSING__"))
    if missing:
        return {
            "kind": "numeric",
            "value": None,
            "missing": True,
            "bucket_index": -1,
            "bucket_label": missing_label,
            "slot_count": len(boundaries) + 2,
        }
    if isinstance(raw_value, bool) or not isinstance(raw_value, (int, float, str)):
        raise PipelineError(
            E_INVALID_FEATURE,
            "numeric feature is not a number",
            sample_id=sample_id,
            field=field,
            value=raw_value,
        )
    try:
        numeric = float(raw_value)
    except (TypeError, ValueError) as exc:
        raise PipelineError(
            E_INVALID_FEATURE,
            "numeric feature is not a number",
            sample_id=sample_id,
            field=field,
            value=raw_value,
        ) from exc
    if not math.isfinite(numeric):
        raise PipelineError(
            E_INVALID_FEATURE,
            "numeric feature must be finite",
            sample_id=sample_id,
            field=field,
            value=raw_value,
        )
    bucket_index = bisect.bisect_right(boundaries, numeric)
    return {
        "kind": "numeric",
        "value": numeric,
        "missing": False,
        "bucket_index": bucket_index,
        "bucket_label": _bucket_label(boundaries, bucket_index, missing_label),
        "slot_count": len(boundaries) + 2,
    }


def _normalize_metadata_value(value: Any, *, sample_id: str, field: str) -> str:
    if value is None or (isinstance(value, str) and not value.strip()):
        return "__missing__"
    return normalize_category_value(value, sample_id=sample_id, field=field)


def normalize_samples(
    raw_records: list[dict[str, Any]], config: Config, scale: str
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    regions = list(config.features.get("regions", []))
    if not regions:
        raise PipelineError(E_INVALID_STAGE, "config.features.regions must be non-empty", scale=scale)
    numeric_fields = sorted(config.numeric_fields)
    categorical_fields = list(config.categorical_fields)
    labels = config.labels
    normalized_records: list[dict[str, Any]] = []
    missing_counts = {field: 0 for field in numeric_fields}
    category_cardinality: dict[str, set[str]] = {field: set() for field in categorical_fields}
    first_sample_id = str(raw_records[0].get("sample_id")) if raw_records else "<no-samples>"

    for index, record in enumerate(raw_records):
        if not isinstance(record, dict):
            raise PipelineError(
                E_INVALID_FEATURE,
                "sample must be a JSON object",
                sample_id=None,
                field="<sample>",
                line=index + 1,
            )
        sample_id = record.get("sample_id")
        if not isinstance(sample_id, str) or not sample_id.strip():
            raise PipelineError(
                E_INVALID_FEATURE,
                "sample_id must be a non-empty string",
                sample_id=sample_id,
                field="sample_id",
                line=index + 1,
            )
        features: dict[str, Any] = {}
        configured_by_region: dict[str, set[str]] = defaultdict(set)
        for field in numeric_fields + categorical_fields:
            region, name = _split_field(field)
            configured_by_region[region].add(name)

        for region in regions:
            region_value = _get_region(record, region, sample_id)
            if not configured_by_region.get(region):
                raise PipelineError(
                    E_INVALID_FEATURE,
                    "feature region has no configured fields",
                    sample_id=sample_id,
                    field=region,
                )
            features[region] = {}

        for field in numeric_fields:
            region, name = _split_field(field)
            region_value = features[region]
            source_value = record[region]
            normalized = _normalize_numeric(
                source_value,
                region=region,
                field_name=name,
                spec=config.numeric_fields[field],
                sample_id=sample_id,
                field=field,
            )
            region_value[name] = normalized
            if normalized["missing"]:
                missing_counts[field] += 1

        for field in categorical_fields:
            region, name = _split_field(field)
            source_value = record[region]
            if name not in source_value or source_value[name] is None:
                raise PipelineError(
                    E_INVALID_FEATURE,
                    "categorical feature is missing",
                    sample_id=sample_id,
                    field=field,
                )
            normalized_value = normalize_category_value(
                source_value[name], sample_id=sample_id, field=field
            )
            features[region][name] = {
                "kind": "categorical",
                "value": normalized_value,
            }
            category_cardinality[field].add(normalized_value)

        normalized_labels: dict[str, Any] = {}
        for semantic_name in ("exposure", "click", "conversion"):
            label_field = labels[semantic_name]
            normalized_labels[semantic_name] = record.get(label_field)

        normalized_records.append(
            {
                "sample_id": sample_id,
                "features": features,
                "metadata": {
                    "source": _normalize_metadata_value(
                        record.get("source"), sample_id=sample_id, field="source"
                    ),
                    "lang": _normalize_metadata_value(
                        record.get("lang"), sample_id=sample_id, field="lang"
                    ),
                },
                "labels": normalized_labels,
            }
        )

    # Validate boundary definitions even if the input contains no records.
    if not raw_records:
        for field, spec in config.numeric_fields.items():
            _validate_boundaries(
                spec.get("boundaries", []), sample_id=first_sample_id, field=field
            )

    summary = {
        "scale": scale,
        "sample_count": len(normalized_records),
        "normalization_version": config.features.get("normalization_version"),
        "numeric_missing_counts": missing_counts,
        "categorical_cardinality": {
            field: len(values) for field, values in sorted(category_cardinality.items())
        },
        "regions": regions,
        "numeric_fields": numeric_fields,
        "categorical_fields": categorical_fields,
    }
    return normalized_records, summary


def numeric_layout(config: Config) -> dict[str, Any]:
    offsets: dict[str, int] = {}
    slot_counts: dict[str, int] = {}
    offset = 0
    for field in sorted(config.numeric_fields):
        count = len(config.numeric_fields[field].get("boundaries", [])) + 2
        offsets[field] = offset
        slot_counts[field] = count
        offset += count
    return {
        "offsets": offsets,
        "slot_counts": slot_counts,
        "total_slots": offset,
    }


def _validate_encoding_plan(
    config: Config,
    declaration: dict[str, Any],
    plan: dict[str, Any],
) -> tuple[int, int, int, list[list[str]], list[str], dict[str, Any]]:
    hash_dim = config.features.get("hash_dim")
    cross_hash_dim = config.features.get("cross_hash_dim")
    if isinstance(hash_dim, bool) or not isinstance(hash_dim, int) or hash_dim <= 0:
        raise PipelineError(
            E_HASH_DIM_ZERO,
            "category hash dimension must be a positive integer",
            field="features.hash_dim",
            value=hash_dim,
        )
    if isinstance(cross_hash_dim, bool) or not isinstance(cross_hash_dim, int) or cross_hash_dim <= 0:
        raise PipelineError(
            E_HASH_DIM_ZERO,
            "cross hash dimension must be a positive integer",
            field="features.cross_hash_dim",
            value=cross_hash_dim,
        )
    layout = numeric_layout(config)
    computed_length = layout["total_slots"] + hash_dim + cross_hash_dim
    declared_length = config.features.get("vector_length")
    if declared_length != computed_length:
        raise PipelineError(
            E_VECTOR_LENGTH_MISMATCH,
            "computed encoding vector length differs from config.features.vector_length",
            computed_length=computed_length,
            declared_length=declared_length,
        )

    cross_limit = declaration.get("cross_limit")
    hard_limit = config.features.get("crossing_limit_hard")
    if not isinstance(cross_limit, int) or cross_limit < 0:
        raise PipelineError(E_CROSS_LIMIT, "declared cross_limit is invalid", cross_limit=cross_limit)
    if not isinstance(hard_limit, int) or hard_limit < 0:
        raise PipelineError(E_CROSS_LIMIT, "global crossing_limit_hard is invalid", hard_limit=hard_limit)
    if cross_limit > hard_limit:
        raise PipelineError(
            E_CROSS_LIMIT,
            "declared cross_limit exceeds the global hard crossing limit",
            declared_cross_limit=cross_limit,
            crossing_limit_hard=hard_limit,
        )
    cross_definitions = plan.get("cross_definitions")
    if not isinstance(cross_definitions, list):
        raise PipelineError(E_INVALID_STAGE, "encoding plan cross_definitions must be a list")
    if len(cross_definitions) > cross_limit or len(cross_definitions) > hard_limit:
        raise PipelineError(
            E_CROSS_LIMIT,
            "requested cross features exceed the declared/global crossing limit",
            requested_cross_count=len(cross_definitions),
            declared_cross_limit=cross_limit,
            crossing_limit_hard=hard_limit,
        )
    if plan.get("cross_count") != len(cross_definitions):
        raise PipelineError(
            E_INVALID_STAGE,
            "plan.cross_count must equal the number of cross definitions",
            cross_count=plan.get("cross_count"),
            cross_definition_count=len(cross_definitions),
        )

    active_fields = plan.get("active_categorical_fields")
    if not isinstance(active_fields, list) or not active_fields:
        raise PipelineError(E_INVALID_STAGE, "plan.active_categorical_fields must be non-empty")
    if len(set(active_fields)) != len(active_fields):
        raise PipelineError(E_INVALID_STAGE, "active categorical fields contain duplicates")
    configured_fields = set(config.categorical_fields)
    invalid_fields = [field for field in active_fields if field not in configured_fields]
    if invalid_fields:
        raise PipelineError(
            E_INVALID_STAGE,
            "encoding plan contains unknown categorical fields",
            fields=invalid_fields,
        )
    for definition in cross_definitions:
        if not isinstance(definition, list) or len(definition) != 2:
            raise PipelineError(E_INVALID_STAGE, "cross definition must contain two fields")
        if any(field not in active_fields for field in definition):
            raise PipelineError(
                E_INVALID_STAGE,
                "cross definition references an inactive categorical field",
                cross_definition=definition,
                active_categorical_fields=active_fields,
            )
    return hash_dim, cross_hash_dim, computed_length, cross_definitions, active_fields, layout


def _feature_value(normalized_record: dict[str, Any], field: str) -> str:
    region, name = _split_field(field)
    return normalized_record["features"][region][name]["value"]


def _collision_metrics(values_by_namespace: Iterable[str], dimension: int, namespace: str) -> dict[str, Any]:
    buckets: dict[int, set[str]] = defaultdict(set)
    for value in values_by_namespace:
        bucket = stable_hash_int(namespace, value, digest_size=8) % dimension
        buckets[bucket].add(value)
    distinct = sum(len(values) for values in buckets.values())
    occupied = len(buckets)
    collision_pairs = sum(len(values) * (len(values) - 1) // 2 for values in buckets.values())
    return {
        "distinct_values": distinct,
        "occupied_slots": occupied,
        "collision_count": distinct - occupied,
        "collision_pairs": collision_pairs,
        "collision_rate": round((distinct - occupied) / distinct, 8) if distinct else 0.0,
    }


def _aggregate_collision_metrics(
    entries: Iterable[tuple[str, str, str]], dimension: int
) -> dict[str, Any]:
    buckets: dict[int, set[str]] = defaultdict(set)
    for namespace, value, label in entries:
        bucket = stable_hash_int(namespace, value, digest_size=8) % dimension
        buckets[bucket].add(label)
    distinct = sum(len(labels) for labels in buckets.values())
    occupied = len(buckets)
    collision_pairs = sum(len(labels) * (len(labels) - 1) // 2 for labels in buckets.values())
    return {
        "distinct_values": distinct,
        "occupied_slots": occupied,
        "collision_count": distinct - occupied,
        "collision_pairs": collision_pairs,
        "collision_rate": round((distinct - occupied) / distinct, 8) if distinct else 0.0,
    }


def encode_records(
    normalized_records: list[dict[str, Any]],
    config: Config,
    declaration: dict[str, Any],
    plan: dict[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    hash_dim, cross_hash_dim, vector_length, cross_definitions, active_fields, layout = (
        _validate_encoding_plan(config, declaration, plan)
    )
    hash_salt = str(config.features.get("hash_salt", "default"))
    numeric_base = 0
    category_base = layout["total_slots"]
    cross_base = category_base + hash_dim
    encoded: list[dict[str, Any]] = []

    category_distinct: dict[str, set[str]] = {field: set() for field in active_fields}
    cross_distinct: dict[int, set[str]] = {index: set() for index in range(len(cross_definitions))}

    batch_size = plan.get("batch_size")
    if isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size <= 0:
        batch_size = max(1, len(normalized_records))
    batch_size = min(batch_size, max(1, len(normalized_records)))

    for start in range(0, len(normalized_records), batch_size):
        batch = normalized_records[start : start + batch_size]
        for record in batch:
            sample_id = record["sample_id"]
            vector = np.zeros(vector_length, dtype=np.int32)
            numeric_slots: list[dict[str, Any]] = []
            category_slots: list[dict[str, Any]] = []
            cross_slots: list[dict[str, Any]] = []

            for field in sorted(config.numeric_fields):
                region, name = _split_field(field)
                feature = record["features"][region][name]
                bucket_index = int(feature["bucket_index"])
                local_slot = bucket_index if bucket_index >= 0 else layout["slot_counts"][field] - 1
                slot = numeric_base + layout["offsets"][field] + local_slot
                vector[slot] += 1
                numeric_slots.append(
                    {
                        "field": field,
                        "bucket_index": bucket_index,
                        "bucket_label": feature["bucket_label"],
                        "slot": slot,
                    }
                )

            for field in active_fields:
                value = _feature_value(record, field)
                category_distinct[field].add(value)
                namespace = f"{hash_salt}|category|{field}"
                slot = category_base + stable_hash_int(namespace, value, digest_size=8) % hash_dim
                vector[slot] += 1
                category_slots.append({"field": field, "value": value, "slot": slot})

            for cross_index, definition in enumerate(cross_definitions):
                values = [_feature_value(record, field) for field in definition]
                cross_key = "|".join(f"{field}={value}" for field, value in zip(definition, values))
                cross_distinct[cross_index].add(cross_key)
                namespace = f"{hash_salt}|cross|{'|'.join(definition)}"
                slot = cross_base + stable_hash_int(
                    namespace, cross_key, digest_size=8
                ) % cross_hash_dim
                vector[slot] += 1
                cross_slots.append(
                    {
                        "cross_index": cross_index,
                        "fields": list(definition),
                        "key": cross_key,
                        "slot": slot,
                    }
                )

            encoded.append(
                {
                    "sample_id": sample_id,
                    "vector_length": vector_length,
                    "vector": vector.tolist(),
                    "nonzero_count": int(np.count_nonzero(vector)),
                    "numeric_slots": numeric_slots,
                    "category_slots": category_slots,
                    "cross_slots": cross_slots,
                }
            )

    category_collisions = {
        field: _collision_metrics(
            sorted(category_distinct[field]),
            hash_dim,
            f"{hash_salt}|category|{field}",
        )
        for field in active_fields
    }
    cross_collisions = {
        str(index): _collision_metrics(
            sorted(cross_distinct[index]),
            cross_hash_dim,
            f"{hash_salt}|cross|{'|'.join(definition)}",
        )
        for index, definition in enumerate(cross_definitions)
    }
    category_vector_collisions = _aggregate_collision_metrics(
        (
            (f"{hash_salt}|category|{field}", value, f"{field}={value}")
            for field in active_fields
            for value in category_distinct[field]
        ),
        hash_dim,
    )
    cross_vector_collisions = _aggregate_collision_metrics(
        (
            (
                f"{hash_salt}|cross|{'|'.join(definition)}",
                cross_key,
                f"{index}:{cross_key}",
            )
            for index, definition in enumerate(cross_definitions)
            for cross_key in cross_distinct[index]
        ),
        cross_hash_dim,
    )
    summary = {
        "scale": declaration.get("scale"),
        "encoding_strategy": plan.get("encoding_strategy"),
        "vector_length": vector_length,
        "hash_dim": hash_dim,
        "cross_hash_dim": cross_hash_dim,
        "batch_size": batch_size,
        "active_categorical_fields": active_fields,
        "dropped_categorical_fields": [
            field for field in config.categorical_fields if field not in active_fields
        ],
        "cross_definitions": cross_definitions,
        "cross_count": len(cross_definitions),
        "encoded_sample_count": len(encoded),
        "category_collisions": category_collisions,
        "cross_collisions": cross_collisions,
        "category_vector_collisions": category_vector_collisions,
        "cross_vector_collisions": cross_vector_collisions,
        "total_category_collision_count": sum(
            item["collision_count"] for item in category_collisions.values()
        ),
        "total_cross_collision_count": sum(
            item["collision_count"] for item in cross_collisions.values()
        ),
        "category_vector_collision_count": category_vector_collisions["collision_count"],
        "cross_vector_collision_count": cross_vector_collisions["collision_count"],
        "collision_audit_mode": (
            "bounded_batch_probe" if plan.get("encoding_strategy") == "batched_approx" else "full_scan"
        ),
    }
    return encoded, summary
