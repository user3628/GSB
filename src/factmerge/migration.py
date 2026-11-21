"""Version-1 evidence-manifest migration and equivalence-preserving export."""

from __future__ import annotations

import copy
import json
import re
from pathlib import Path
from typing import Any, Iterable

from .config import config_fingerprint, stage_component_digests, stage_fingerprints
from .errors import FactMergeError
from .evidence import (
    EVIDENCE_SCHEMA,
    build_entity_evidence,
    finalize_manifest,
    stage_content_digests,
)
from .parser import _parse_arabic_number, parse_claim
from .resolver import _comparison_display, _comparison_key, confidence_for_claim
from .units import normalize_claim
from .util import (
    atomic_write_bytes,
    canonical_json,
    digest_obj,
    digest_without,
    normalize_key,
    normalize_text,
)


def _unit_specs_by_id(config: dict[str, Any]) -> dict[str, tuple[str, dict[str, Any]]]:
    result: dict[str, tuple[str, dict[str, Any]]] = {}
    for category, group in config["unit_dictionary"].items():
        for spec in group["units"]:
            result[spec["id"]] = (category, spec)
    return result


def _clean_unit_text(text: str) -> str:
    normalized = normalize_text(text).normalized
    without_parentheses = re.sub(r"\([^)]*\)|（[^）]*）", "", normalized)
    return without_parentheses.strip(" []()（）:：")


def _legacy_unit_mapping(config: dict[str, Any]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for key, value in config["legacy_v1"].get("unit_text_mapping", {}).items():
        unit_id = value["unit_id"] if isinstance(value, dict) else str(value)
        mapping[normalize_key(key)] = unit_id
        mapping["".join(normalize_key(key).split())] = unit_id
    return mapping


def _map_legacy_unit(
    unit_text: str,
    record: dict[str, Any],
    config: dict[str, Any],
) -> tuple[str, dict[str, Any], str]:
    if not str(unit_text or "").strip():
        raise FactMergeError(
            "E_INVALID_FORMAT",
            "legacy v1 record has no unit",
            claim_id=str(record.get("claim_id") or record.get("record_id") or ""),
            entity_id=str(record.get("entity_id") or ""),
            details={"field": "unit"},
        )
    mapping = _legacy_unit_mapping(config)
    candidates = [
        normalize_key(unit_text),
        "".join(normalize_key(unit_text).split()),
        normalize_key(_clean_unit_text(unit_text)),
        "".join(normalize_key(_clean_unit_text(unit_text)).split()),
    ]
    unit_id = next((mapping[key] for key in candidates if key in mapping), None)
    if unit_id is None:
        raise FactMergeError(
            "E_INVALID_FORMAT",
            f"legacy v1 unit cannot be mapped: {unit_text!r}",
            claim_id=str(record.get("claim_id") or record.get("record_id") or ""),
            entity_id=str(record.get("entity_id") or ""),
            details={"field": "unit", "unit_text": unit_text},
        )
    specs = _unit_specs_by_id(config)
    if unit_id not in specs:
        raise FactMergeError(
            "E_INVALID_FORMAT",
            f"legacy v1 unit mapping refers to an unknown unit id: {unit_id!r}",
            claim_id=str(record.get("claim_id") or record.get("record_id") or ""),
            entity_id=str(record.get("entity_id") or ""),
            details={"unit_id": unit_id},
        )
    category, spec = specs[unit_id]
    return category, spec, unit_id


def _numeric_string(value: Any, record: dict[str, Any]) -> str:
    if isinstance(value, bool) or isinstance(value, float):
        raise FactMergeError(
            "E_INVALID_FORMAT",
            "legacy v1 numeric values must be strings or integers, never binary floats",
            claim_id=str(record.get("claim_id") or record.get("record_id") or ""),
            entity_id=str(record.get("entity_id") or ""),
            details={"field": "value", "value_type": type(value).__name__},
        )
    if isinstance(value, int):
        return str(value)
    if not isinstance(value, str) or not value.strip():
        raise FactMergeError(
            "E_INVALID_FORMAT",
            "legacy v1 record has no numeric value",
            claim_id=str(record.get("claim_id") or record.get("record_id") or ""),
            entity_id=str(record.get("entity_id") or ""),
        )
    return normalize_text(value).normalized.strip()


def _parse_legacy_number(value: str, config: dict[str, Any], record: dict[str, Any]) -> None:
    policy = str(config["legacy_v1"].get("number_policy", "thousands"))
    if policy not in {"thousands", "auto", "decimal"}:
        raise FactMergeError(
            "E_INVALID_FORMAT",
            f"unsupported legacy v1 number policy: {policy!r}",
            claim_id=str(record.get("claim_id") or record.get("record_id") or ""),
            entity_id=str(record.get("entity_id") or ""),
            details={"field": "legacy_v1.number_policy", "value": policy},
        )
    if policy == "decimal" and "," in value:
        raise FactMergeError(
            "E_INVALID_FORMAT",
            "legacy v1 number policy 'decimal' does not accept comma grouping",
            claim_id=str(record.get("claim_id") or record.get("record_id") or ""),
            entity_id=str(record.get("entity_id") or ""),
            details={"field": "value", "value": value, "number_policy": policy},
        )
    _parse_arabic_number(value)


def _legacy_source(record: dict[str, Any]) -> dict[str, Any]:
    source = {
        "claim_id": str(record.get("claim_id") or record.get("record_id") or ""),
        "entity_id": str(record.get("entity_id") or ""),
        "lang": str(record.get("lang") or "und"),
        "category": record.get("category"),
        "raw_value": record.get("value"),
        "unit": record.get("unit"),
        "sentence": record.get("sentence", ""),
        "source": record.get("source", "unknown"),
        "observed_at": record.get("observed_at"),
        "legacy_record": copy.deepcopy(record),
    }
    return source


def _decision_for_migrated(
    stage2: dict[str, Any],
    source: dict[str, Any],
    record: dict[str, Any],
    config: dict[str, Any],
    downgrade_codes: list[str],
) -> dict[str, Any]:
    decision_kind = str(record.get("decision") or "accepted")
    if decision_kind not in {"accepted", "demoted"}:
        decision_kind = "accepted"
    lang = str(stage2.get("lang") or record.get("lang") or "und")
    confidence = confidence_for_claim(source, lang, config)
    decision = {
        "claim_id": stage2["claim_id"],
        "entity_id": stage2["entity_id"],
        "category": stage2["category"],
        "decision": decision_kind,
        "reason_code": "LEGACY_V1_PRESERVED" if decision_kind == "accepted" else str(record.get("reason_code") or "LEGACY_V1_DEMOTED"),
        "reason": str(record.get("reason") or "decision preserved during legacy v1 migration"),
        "winner_claim_id": record.get("winner_claim_id"),
        "confidence": confidence,
        "comparison_value": _comparison_display(stage2, config),
        "comparison_key": _comparison_key(stage2, config),
        "tie_break": None,
        "stage2_digest": stage2["digest"],
        "migration_downgrades": downgrade_codes,
    }
    decision["digest"] = digest_without(decision, "digest")
    return decision


def _build_resolution(entity_id: str, decisions: list[dict[str, Any]]) -> dict[str, Any]:
    accepted: list[dict[str, Any]] = []
    demoted: list[dict[str, Any]] = []
    categories: dict[str, list[dict[str, Any]]] = {}
    for decision in decisions:
        categories.setdefault(decision["category"], []).append(decision)
        if decision["decision"] == "accepted":
            accepted.append(
                {
                    "category": decision["category"],
                    "claim_id": decision["claim_id"],
                    "value": decision["comparison_value"],
                    "confidence": decision["confidence"],
                    "supporting_claim_ids": [],
                }
            )
        else:
            demoted.append(
                {
                    "category": decision["category"],
                    "claim_id": decision["claim_id"],
                    "reason_code": decision["reason_code"],
                    "reason": decision["reason"],
                    "winner_claim_id": decision["winner_claim_id"],
                }
            )
    accepted.sort(key=lambda item: (item["category"], item["claim_id"]))
    demoted.sort(key=lambda item: (item["category"], item["claim_id"]))
    category_results = []
    for category in sorted(categories):
        items = categories[category]
        category_results.append(
            {
                "category": category,
                "status": "migrated",
                "conflict_detected": False,
                "distinct_value_count": len({item["comparison_key"] for item in items}),
                "accepted_claim_id": next((item["claim_id"] for item in items if item["decision"] == "accepted"), None),
                "value_groups": [],
                "claim_ids": sorted(item["claim_id"] for item in items),
                "review_claim_ids": [],
                "parse_error_claim_ids": [],
                "tie_break": None,
            }
        )
    result = {
        "entity_id": entity_id,
        "status": "migrated",
        "categories": category_results,
        "decisions": sorted(decisions, key=lambda item: item["claim_id"]),
        "accepted": accepted,
        "demoted": demoted,
        "conflict_detected": False,
    }
    result["digest"] = digest_without(result, "digest")
    return result


def load_legacy_v1_file(path: str | Path) -> tuple[list[dict[str, Any]], str]:
    try:
        text = Path(path).read_text(encoding="utf-8")
    except OSError as exc:
        raise FactMergeError("E_INVALID_FORMAT", f"cannot read legacy v1 evidence file: {exc}") from exc
    stripped = text.lstrip()
    if not stripped:
        return [], "jsonl"
    if stripped.startswith("["):
        try:
            rows = json.loads(stripped)
        except json.JSONDecodeError as exc:
            raise FactMergeError("E_INVALID_FORMAT", f"invalid legacy v1 JSON: {exc.msg}") from exc
        if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
            raise FactMergeError("E_INVALID_FORMAT", "legacy v1 array must contain JSON objects")
        return rows, "json-array"
    if stripped.startswith("{"):
        try:
            value = json.loads(stripped)
        except json.JSONDecodeError:
            value = None
        if isinstance(value, dict) and isinstance(value.get("records"), list):
            return list(value["records"]), "json-object"
    rows: list[dict[str, Any]] = []
    for line_no, line in enumerate(text.splitlines(), 1):
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError as exc:
            raise FactMergeError("E_INVALID_FORMAT", f"legacy v1 line {line_no}: {exc.msg}") from exc
        if not isinstance(row, dict):
            raise FactMergeError("E_INVALID_FORMAT", f"legacy v1 line {line_no} is not an object")
        rows.append(row)
    return rows, "jsonl"


def migrate_legacy_v1(
    records: Iterable[dict[str, Any]],
    config: dict[str, Any],
    *,
    source_format: str = "jsonl",
) -> dict[str, Any]:
    rows = [copy.deepcopy(row) for row in records]
    if not rows:
        raise FactMergeError("E_INVALID_FORMAT", "legacy v1 evidence list is empty")
    by_entity: dict[str, list[dict[str, Any]]] = {}
    stage1_by_claim: dict[str, dict[str, Any]] = {}
    stage2_by_claim: dict[str, dict[str, Any]] = {}
    source_by_claim: dict[str, dict[str, Any]] = {}
    downgrade_report: list[dict[str, Any]] = []
    unit_mappings: list[dict[str, Any]] = []

    for record in rows:
        claim_id = str(record.get("claim_id") or record.get("record_id") or "")
        entity_id = str(record.get("entity_id") or "")
        if not claim_id or not entity_id:
            raise FactMergeError(
                "E_INVALID_FORMAT",
                "legacy v1 record requires claim_id (or record_id) and entity_id",
                claim_id=claim_id or None,
                entity_id=entity_id or None,
            )
        category, unit_spec, unit_id = _map_legacy_unit(str(record.get("unit") or ""), record, config)
        raw_value = record.get("value")
        downgrade_codes: list[str] = []
        if isinstance(raw_value, str) and "," in raw_value:
            downgrade_codes.append("V1_THOUSANDS_QUANTIZED")
        if "conversion_factor" not in record and "factor" not in record:
            downgrade_codes.append("V1_CONVERSION_FACTOR_FILLED")
        sentence = str(record.get("sentence") or "")
        if category == "tenure":
            if raw_value in (None, "") and record.get("month"):
                raw_value = record["month"]
            raw_text = _numeric_string(raw_value, record) if not isinstance(raw_value, str) else normalize_text(raw_value).normalized.strip()
        else:
            raw_text = _numeric_string(raw_value, record)
            try:
                _parse_legacy_number(raw_text, config, record)
            except ValueError as exc:
                raise FactMergeError(
                    "E_INVALID_FORMAT",
                    f"legacy v1 numeric value cannot be parsed: {raw_text!r}",
                    claim_id=claim_id,
                    entity_id=entity_id,
                    details={"field": "value", "reason": str(exc)},
                ) from exc

        canonical_alias = unit_spec["aliases"][0]
        synthetic = {
            "claim_id": claim_id,
            "entity_id": entity_id,
            "lang": str(record.get("lang") or "und"),
            "category": category,
            "raw_value": raw_text,
            "unit": canonical_alias,
            "sentence": sentence,
            "source": record.get("source", "unknown"),
            "observed_at": record.get("observed_at"),
        }
        try:
            stage1 = parse_claim(synthetic, config)
        except FactMergeError as exc:
            raise FactMergeError(
                "E_INVALID_FORMAT",
                f"legacy v1 record cannot be normalized: {exc.message}",
                claim_id=claim_id,
                entity_id=entity_id,
                details={"parse_error": exc.as_dict()},
            ) from exc
        if category == "tenure" and stage1.get("interval", {}).get("interval_kind") == "point":
            downgrade_codes.append("V1_SINGLE_MONTH_EXPANDED")
        stage1["migration"] = {
            "source_schema_version": str(record.get("schema_version") or "1.0"),
            "source_field": "value",
            "original_unit": record.get("unit"),
            "mapped_unit_id": unit_id,
            "downgrade_codes": downgrade_codes,
        }
        stage1["digest"] = digest_without(stage1, "digest")
        stage2 = normalize_claim(stage1, config)
        stage2["migration"] = {"downgrade_codes": downgrade_codes}
        stage2["digest"] = digest_without(stage2, "digest")
        source = _legacy_source(record)
        source["category"] = category
        decision = _decision_for_migrated(stage2, source, record, config, downgrade_codes)
        stage1_by_claim[claim_id] = stage1
        stage2_by_claim[claim_id] = stage2
        source_by_claim[claim_id] = source
        by_entity.setdefault(entity_id, []).append(decision)
        unit_mappings.append(
            {
                "claim_id": claim_id,
                "free_text": record.get("unit"),
                "mapped_unit_id": unit_id,
                "factor": unit_spec["factor"],
                "factor_precision": unit_spec.get("factor_precision", 0),
                "downgrade_codes": downgrade_codes,
            }
        )
        downgrade_report.append({"claim_id": claim_id, "downgrade_codes": downgrade_codes})

    entities: list[dict[str, Any]] = []
    for entity_id in sorted(by_entity):
        resolution = _build_resolution(entity_id, by_entity[entity_id])
        entities.append(
            build_entity_evidence(resolution, stage1_by_claim, stage2_by_claim, source_by_claim)
        )

    corpus_digest = digest_obj({"legacy_v1_records": rows, "source_format": source_format})
    fingerprints = stage_fingerprints(config, corpus_digest)
    content_digests = stage_content_digests(entities)
    manifest = {
        "schema_version": EVIDENCE_SCHEMA,
        "manifest_schema": "factmerge.evidence",
        "config": copy.deepcopy(config),
        "config_digest": digest_obj(config),
        "version": {
            "corpus_digest": corpus_digest,
            "component_digests": stage_component_digests(config),
            "stage_fingerprints": fingerprints,
            "config_fingerprint": config_fingerprint(fingerprints),
            "stage_content_digests": content_digests,
            "content_fingerprint": digest_obj({"stage_content_digests": content_digests}),
        },
        "stage_content_digests": content_digests,
        "entities": entities,
        "summary": {
            "entity_count": len(entities),
            "claim_count": len(rows),
            "downgraded_count": sum(1 for item in downgrade_report if item["downgrade_codes"]),
        },
        "migration": {
            "source_schema_version": "1.0",
            "source_format": source_format,
            "record_count": len(rows),
            "unit_mappings": unit_mappings,
            "downgrades": downgrade_report,
            "original_records": rows,
        },
    }
    return finalize_manifest(manifest)


def migrate_legacy_v1_file(path: str | Path, config: dict[str, Any]) -> dict[str, Any]:
    rows, source_format = load_legacy_v1_file(path)
    return migrate_legacy_v1(rows, config, source_format=source_format)


def export_legacy_v1_bytes(manifest: dict[str, Any]) -> bytes:
    migration = manifest.get("migration")
    if not isinstance(migration, dict) or "original_records" not in migration:
        raise FactMergeError("E_INVALID_FORMAT", "manifest does not contain legacy v1 migration data")
    rows = migration["original_records"]
    source_format = migration.get("source_format", "jsonl")
    if source_format == "json-array":
        return (canonical_json(rows) + "\n").encode("utf-8")
    if source_format == "json-object":
        return (canonical_json({"schema_version": "1.0", "records": rows}) + "\n").encode("utf-8")
    return "".join(canonical_json(row) + "\n" for row in rows).encode("utf-8")


def export_legacy_v1(manifest: dict[str, Any], path: str | Path) -> None:
    atomic_write_bytes(path, export_legacy_v1_bytes(manifest))
