"""Explicit sample assembly, discards, budget-aware sampling, and label statistics."""

from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any

from .config import Config
from .errors import E_INVALID_STAGE, PipelineError
from .util import hash_json, stable_hash_int


def _validated_labels(
    normalized_record: dict[str, Any], sample_id: str
) -> tuple[dict[str, int] | None, str | None, dict[str, Any] | None]:
    labels = normalized_record.get("labels", {})
    missing = [name for name in ("exposure", "click", "conversion") if labels.get(name) is None]
    if missing:
        return None, "missing_label", {"missing_labels": missing}
    validated: dict[str, int] = {}
    invalid: list[dict[str, Any]] = []
    for name in ("exposure", "click", "conversion"):
        value = labels[name]
        if isinstance(value, bool) or not isinstance(value, (int, float)) or int(value) not in (0, 1) or value not in (0, 1):
            invalid.append({"label": name, "value": value})
        else:
            validated[name] = int(value)
    if invalid:
        return None, "invalid_label", {"invalid_labels": invalid}
    return validated, None, None


def _reservoir_ids(records: list[dict[str, Any]], ratio: float, seed: int, scale: str) -> set[str]:
    if ratio >= 1.0 or len(records) <= 1:
        return {record["sample_id"] for record in records}
    target = max(1, min(len(records), int(round(len(records) * ratio))))
    strata: dict[tuple[str, str], list[tuple[int, str]]] = defaultdict(list)
    for record in records:
        source = str(record["source"])
        lang = str(record["lang"])
        rank = stable_hash_int(f"{seed}|{scale}|assembly-reservoir", record["sample_id"], 8)
        strata[(source, lang)].append((rank, record["sample_id"]))
    ordered_keys = sorted(strata)
    for key in ordered_keys:
        strata[key].sort()
    selected: list[str] = []
    positions = {key: 0 for key in ordered_keys}
    while len(selected) < target:
        progressed = False
        for key in ordered_keys:
            position = positions[key]
            values = strata[key]
            if position < len(values):
                selected.append(values[position][1])
                positions[key] = position + 1
                progressed = True
                if len(selected) >= target:
                    break
        if not progressed:
            break
    return set(selected)


def _label_stats(records: list[dict[str, Any]]) -> dict[str, Any]:
    count = len(records)
    result: dict[str, Any] = {"sample_count": count}
    for label in ("exposure", "click", "conversion"):
        distribution = Counter(int(record["labels"][label]) for record in records)
        positive = distribution.get(1, 0)
        result[f"{label}_positive_count"] = positive
        result[f"{label}_positive_rate"] = round(positive / count, 10) if count else 0.0
        result[f"{label}_distribution"] = {str(key): distribution.get(key, 0) for key in (0, 1)}
    return result


def _group_stats(records: list[dict[str, Any]], key_names: tuple[str, ...]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, ...], list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        grouped[tuple(str(record[name]) for name in key_names)].append(record)
    output: list[dict[str, Any]] = []
    for key in sorted(grouped):
        entry = {name: value for name, value in zip(key_names, key)}
        entry.update(_label_stats(grouped[key]))
        output.append(entry)
    return output


def assemble_samples(
    normalized_records: list[dict[str, Any]],
    encoded_records: list[dict[str, Any]],
    config: Config,
    declaration: dict[str, Any],
    plan: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any], dict[str, Any]]:
    encoded_by_id: dict[str, dict[str, Any]] = {}
    duplicate_encoded: set[str] = set()
    for record in encoded_records:
        sample_id = record.get("sample_id")
        if sample_id in encoded_by_id:
            duplicate_encoded.add(str(sample_id))
        encoded_by_id[str(sample_id)] = record
    normalized_ids = {record["sample_id"] for record in normalized_records}
    extra_encoded = sorted(set(encoded_by_id) - normalized_ids)
    if duplicate_encoded or extra_encoded:
        raise PipelineError(
            E_INVALID_STAGE,
            "encoded records do not align one-to-one with normalized records",
            duplicate_encoded_sample_ids=sorted(duplicate_encoded),
            extra_encoded_sample_ids=extra_encoded,
        )

    eligible: list[dict[str, Any]] = []
    discards: list[dict[str, Any]] = []
    for normalized in normalized_records:
        sample_id = normalized["sample_id"]
        encoded = encoded_by_id.get(sample_id)
        if encoded is None:
            discards.append(
                {
                    "sample_id": sample_id,
                    "reason": "encoding_failure",
                    "detail": {"message": "no encoded record for normalized sample"},
                    "source": normalized["metadata"]["source"],
                    "lang": normalized["metadata"]["lang"],
                    "labels": normalized["labels"],
                }
            )
            continue
        labels, reason, detail = _validated_labels(normalized, sample_id)
        if reason is not None:
            discards.append(
                {
                    "sample_id": sample_id,
                    "reason": reason,
                    "detail": detail,
                    "source": normalized["metadata"]["source"],
                    "lang": normalized["metadata"]["lang"],
                    "labels": normalized["labels"],
                }
            )
            continue
        eligible.append(
            {
                "sample_id": sample_id,
                "vector": encoded["vector"],
                "vector_length": encoded["vector_length"],
                "labels": labels,
                "source": normalized["metadata"]["source"],
                "lang": normalized["metadata"]["lang"],
            }
        )

    assembly_strategy = plan["assembly_strategy"]
    selected_ids = {record["sample_id"] for record in eligible}
    if assembly_strategy == "stratified_reservoir":
        seed = int(config.data["project"]["seed"])
        ratio = float(config.decision.get("reservoir_sample_ratio", 0.85))
        selected_ids = _reservoir_ids(eligible, ratio, seed, str(declaration.get("scale")))
        for record in eligible:
            if record["sample_id"] not in selected_ids:
                discards.append(
                    {
                        "sample_id": record["sample_id"],
                        "reason": "assembly_strategy_sampling",
                        "detail": {
                            "strategy": assembly_strategy,
                            "sample_ratio": ratio,
                        },
                        "source": record["source"],
                        "lang": record["lang"],
                        "labels": record["labels"],
                    }
                )
    elif assembly_strategy not in {"exact_materialize", "chunked_stream"}:
        raise PipelineError(
            E_INVALID_STAGE,
            "unknown sample assembly strategy",
            strategy=assembly_strategy,
        )

    samples = [record for record in eligible if record["sample_id"] in selected_ids]
    discards.sort(key=lambda record: record["sample_id"])
    summary = {
        "strategy": assembly_strategy,
        "input_count": len(normalized_records),
        "assembled_count": len(samples),
        "discarded_count": len(discards),
        "discarded_reasons": dict(
            sorted(Counter(record["reason"] for record in discards).items())
        ),
        "sampling": {
            "enabled": assembly_strategy == "stratified_reservoir",
            "sample_ratio": float(config.decision.get("reservoir_sample_ratio", 0.85))
            if assembly_strategy == "stratified_reservoir"
            else 1.0,
        },
    }
    stats_body = {
        "schema_version": 1,
        "scale": declaration.get("scale"),
        "input_count": len(normalized_records),
        "assembled_count": len(samples),
        "discarded_count": len(discards),
        "discarded_reasons": summary["discarded_reasons"],
        "group_by": list(config.data.get("stats", {}).get("group_by", ["source", "lang"])),
        "metadata_distribution": {
            "source": dict(sorted(Counter(record["source"] for record in samples).items())),
            "lang": dict(sorted(Counter(record["lang"] for record in samples).items())),
        },
        "overall": _label_stats(samples),
        "by_source": _group_stats(samples, ("source",)),
        "by_lang": _group_stats(samples, ("lang",)),
        "by_source_lang": _group_stats(samples, ("source", "lang")),
    }
    stats_body["content_digest"] = hash_json(stats_body)
    return samples, discards, summary, stats_body
