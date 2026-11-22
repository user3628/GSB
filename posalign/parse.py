"""Shard parsing, v1 compatibility upgrade, and reverse v1 export."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .canonical import read_jsonl, sha256_bytes, shard_id_for
from .config import GovernanceConfig
from .errors import BusinessError, E_INVALID_RECORD
from .normalize import CandidateResult, canonical_standard_tag, generate_candidates


@dataclass
class StopGate:
    """Checks graceful-stop conditions only at complete sentence boundaries."""

    stop_file: Path | None = None
    max_sentences: int | None = None
    completed: int = 0
    reason: str = "stop_requested"

    def after_sentence(self) -> None:
        self.completed += 1
        if self.max_sentences is not None and self.completed >= self.max_sentences:
            from .errors import StopRequested

            raise StopRequested(self.completed, "max_sentences_reached")
        if self.stop_file is not None and self.stop_file.exists():
            from .errors import StopRequested

            raise StopRequested(self.completed, "stop_file_detected")

    def check_requested(self) -> None:
        """Check a stop request at a non-sentence stage boundary."""

        if self.stop_file is not None and self.stop_file.exists():
            from .errors import StopRequested

            raise StopRequested(self.completed, "stop_file_detected")


def _require_string(value: Any, field: str, *, line: int, path: Path) -> str:
    if not isinstance(value, str) or not value.strip():
        raise BusinessError(
            E_INVALID_RECORD,
            f"{field} must be a non-empty string",
            {"path": str(path), "line": line, "field": field},
        )
    return value


def _string_list(value: Any, field: str, *, line: int, path: Path) -> list[str]:
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise BusinessError(
            E_INVALID_RECORD,
            f"{field} must be a list of strings",
            {"path": str(path), "line": line, "field": field},
        )
    return list(value)


def _record_annotator(raw: dict[str, Any], line: int, path: Path) -> str:
    value = raw.get("annotator", raw.get("annotator_id", "unknown"))
    if not isinstance(value, str) or not value:
        raise BusinessError(
            E_INVALID_RECORD,
            "annotator/annotator_id must be a non-empty string",
            {"path": str(path), "line": line},
        )
    return value


def _confidence_decimal(
    raw: dict[str, Any],
    version: int,
    line: int,
    path: Path,
) -> tuple[float | None, dict[str, Any] | None]:
    if "confidence" not in raw or raw["confidence"] is None:
        return None, None
    value = raw["confidence"]
    if version == 1:
        if isinstance(value, bool) or not isinstance(value, int):
            raise BusinessError(
                E_INVALID_RECORD,
                "structure_version 1 confidence must be an integer percentage",
                {"path": str(path), "line": line, "value": value},
            )
        if not 0 <= value <= 100:
            raise BusinessError(
                E_INVALID_RECORD,
                "structure_version 1 confidence must be within 0..100",
                {"path": str(path), "line": line, "value": value},
            )
        decimal = value / 100.0
        return decimal, {
            "from": value,
            "to": decimal,
            "basis": "integer percentage divided by 100",
            "rule": "structure_v1.confidence_scale=percent",
        }
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise BusinessError(
            E_INVALID_RECORD,
            "structure_version 2 confidence must be numeric",
            {"path": str(path), "line": line, "value": value},
        )
    decimal = float(value)
    if not 0.0 <= decimal <= 1.0:
        raise BusinessError(
            E_INVALID_RECORD,
            "structure_version 2 confidence must be within 0..1",
            {"path": str(path), "line": line, "value": value},
        )
    return decimal, None


def _candidate_payload(result: CandidateResult) -> dict[str, Any]:
    return result.as_dict()


def parse_record(
    raw: dict[str, Any],
    *,
    line: int,
    path: Path,
    config: GovernanceConfig,
) -> dict[str, Any]:
    sent_id = raw.get("sent_id")
    if not isinstance(sent_id, str) or not sent_id.strip():
        raise BusinessError(
            E_INVALID_RECORD,
            "sent_id must be a non-empty string",
            {"path": str(path), "line": line},
        )
    version_raw = raw.get("structure_version", raw.get("schema_version", 2))
    if isinstance(version_raw, bool) or not isinstance(version_raw, int) or version_raw not in (1, 2):
        raise BusinessError(
            E_INVALID_RECORD,
            "structure_version must be 1 or 2",
            {"path": str(path), "line": line, "sent_id": sent_id},
        )
    version = int(version_raw)
    tokens = _string_list(raw.get("tokens"), "tokens", line=line, path=path)
    old_tags = _string_list(
        raw.get("old_tags", raw.get("old_labels")), "old_tags", line=line, path=path
    )
    new_raw = raw.get("new_tags", raw.get("new_labels"))
    new_tags: list[str] | None
    if new_raw is None:
        new_tags = None
    else:
        new_tags = _string_list(new_raw, "new_tags", line=line, path=path)
    new_tokens_raw = raw.get("new_tokens")
    new_tokens: list[str] | None
    if new_tokens_raw is None:
        new_tokens = list(tokens) if new_tags is not None else None
    else:
        new_tokens = _string_list(new_tokens_raw, "new_tokens", line=line, path=path)
        if new_tags is None:
            raise BusinessError(
                E_INVALID_RECORD,
                "new_tokens requires new_tags",
                {"path": str(path), "line": line, "sent_id": sent_id},
            )
    annotator = _record_annotator(raw, line, path)
    confidence, confidence_conversion = _confidence_decimal(raw, version, line, path)

    unsplit = version == 1 and len(tokens) == 0
    if unsplit:
        if new_tags:
            raise BusinessError(
                E_INVALID_RECORD,
                "an unsegmented v1 sentence cannot contain new token tags",
                {"path": str(path), "line": line, "sent_id": sent_id},
            )
    else:
        if len(old_tags) != len(tokens):
            raise BusinessError(
                E_INVALID_RECORD,
                "old_tags length must match tokens length",
                {
                    "path": str(path),
                    "line": line,
                    "sent_id": sent_id,
                    "tokens": len(tokens),
                    "old_tags": len(old_tags),
                },
            )
        if new_tags is not None and new_tokens_raw is None and len(new_tags) != len(tokens):
            raise BusinessError(
                E_INVALID_RECORD,
                "new_tags length must match tokens length when new_tokens is absent",
                {
                    "path": str(path),
                    "line": line,
                    "sent_id": sent_id,
                    "tokens": len(tokens),
                    "new_tags": len(new_tags),
                },
            )
        if new_tags is not None and new_tokens_raw is not None and len(new_tags) != len(new_tokens or []):
            raise BusinessError(
                E_INVALID_RECORD,
                "new_tags length must match new_tokens length",
                {
                    "path": str(path),
                    "line": line,
                    "sent_id": sent_id,
                    "new_tokens": len(new_tokens or []),
                    "new_tags": len(new_tags),
                },
            )

    old_candidates = [_candidate_payload(generate_candidates(tag, config)) for tag in old_tags]
    new_normalized: list[dict[str, Any]] = []
    if new_tags is not None:
        for tag in new_tags:
            canonical, diagnostic = canonical_standard_tag(tag, config)
            item: dict[str, Any] = {"raw": tag, "tag": canonical}
            if diagnostic:
                item["diagnostic"] = diagnostic
            new_normalized.append(item)

    compatibility: dict[str, Any] | None = None
    roundtrip: dict[str, Any] | None = None
    if version == 1:
        reasons = ["free_string_old_tags_normalized"]
        if unsplit:
            reasons.append("zero_tokens_marked_unsegmented_not_alignable")
        if confidence_conversion is not None:
            reasons.append("confidence_integer_percent_scaled_to_fraction")
        compatibility = {
            "source_version": 1,
            "normalized_version": 2,
            "degraded": True,
            "reasons": reasons,
            "zero_tokens_rule": config.v1.zero_tokens if unsplit else None,
            "confidence_conversion": confidence_conversion,
        }
        if config.v1.preserve_original:
            roundtrip = {
                "sent_id": sent_id,
                "tokens": list(tokens),
                "old_tags": list(old_tags),
                "new_tags": list(new_tags) if new_tags is not None else None,
                "annotator_id": annotator,
                "structure_version": 1,
                "confidence": confidence_conversion["from"] if confidence_conversion else None,
            }

    return {
        "sent_id": sent_id,
        "tokens": list(tokens),
        "old_tags": list(old_tags),
        "new_tags": list(new_tags) if new_tags is not None else None,
        "new_tokens": list(new_tokens) if new_tokens is not None else None,
        "annotator": annotator,
        "source_version": version,
        "normalized_version": 2,
        "confidence": confidence,
        "confidence_conversion": confidence_conversion,
        "compatibility": compatibility,
        "v1_roundtrip": roundtrip,
        "unsplit": unsplit,
        "alignable": bool(tokens) and new_tags is not None and not unsplit,
        "old_candidates": old_candidates,
        "new_tag_normalization": new_normalized,
        "source_line": line,
    }


def parse_shard(
    path: str | Path,
    *,
    config: GovernanceConfig,
    stop_gate: StopGate | None = None,
) -> dict[str, Any]:
    shard_path = Path(path).resolve()
    raw_bytes = shard_path.read_bytes()
    shard_id = shard_id_for(shard_path)
    records: list[dict[str, Any]] = []
    seen_sent_ids: set[str] = set()
    labels: dict[str, dict[str, Any]] = {}
    for line, raw in read_jsonl(shard_path):
        record = parse_record(raw, line=line, path=shard_path, config=config)
        sent_id = record["sent_id"]
        if sent_id in seen_sent_ids:
            raise BusinessError(
                E_INVALID_RECORD,
                "duplicate sent_id within shard",
                {"path": str(shard_path), "line": line, "sent_id": sent_id},
            )
        seen_sent_ids.add(sent_id)
        records.append(record)
        for position, candidate in enumerate(record["old_candidates"]):
            key = candidate["normalized_old_tag"]
            entry = labels.setdefault(
                key,
                {
                    "normalized_old_tag": key,
                    "original_variants": set(),
                    "occurrences": 0,
                    "candidates": {},
                    "diagnostics": {},
                    "sent_ids": set(),
                },
            )
            entry["original_variants"].add(record["old_tags"][position])
            entry["occurrences"] += 1
            entry["sent_ids"].add(sent_id)
            for item in candidate["candidates"]:
                c = entry["candidates"].setdefault(
                    item["tag"],
                    {
                        "tag": item["tag"],
                        "source": item["source"],
                        "sources": set(),
                        "rules": set(),
                    },
                )
                c["sources"].update(item["sources"])
                c["rules"].update(item["rules"])
            for diag in candidate["diagnostics"]:
                key_diag = (
                    diag["code"],
                    tuple(diag.get("candidates", [])),
                )
                d = entry["diagnostics"].setdefault(
                    key_diag,
                    {
                        "code": diag["code"],
                        "message": diag["message"],
                        "candidates": list(diag.get("candidates", [])),
                        "occurrences": 0,
                        "sent_ids": set(),
                    },
                )
                d["occurrences"] += 1
                d["sent_ids"].add(sent_id)
        if stop_gate is not None:
            stop_gate.after_sentence()

    label_catalog: list[dict[str, Any]] = []
    for key in sorted(labels):
        item = labels[key]
        label_catalog.append(
            {
                "normalized_old_tag": key,
                "original_variants": sorted(item["original_variants"]),
                "occurrences": item["occurrences"],
                "sent_ids": sorted(item["sent_ids"]),
                "candidates": [
                    {
                        "tag": value["tag"],
                        "source": value["source"],
                        "sources": sorted(value["sources"]),
                        "rules": sorted(value["rules"]),
                    }
                    for _, value in sorted(item["candidates"].items())
                ],
                "diagnostics": [
                    {
                        "code": value["code"],
                        "message": value["message"],
                        "candidates": value["candidates"],
                        "occurrences": value["occurrences"],
                        "sent_ids": sorted(value["sent_ids"]),
                    }
                    for _, value in sorted(item["diagnostics"].items())
                ],
            }
        )

    stats = {
        "records": len(records),
        "v1_records": sum(1 for item in records if item["source_version"] == 1),
        "v2_records": sum(1 for item in records if item["source_version"] == 2),
        "v1_unsegmented": sum(1 for item in records if item["unsplit"]),
        "alignable_records": sum(1 for item in records if item["alignable"]),
        "unpaired_records": sum(
            1 for item in records if not item["unsplit"] and item["new_tags"] is None
        ),
        "migrated_token_tags": sum(len(item["old_tags"]) for item in records),
        "unknown_new_tags": sum(
            1
            for item in records
            for normalized in item["new_tag_normalization"]
            if normalized.get("diagnostic") == "unknown_new_tag"
        ),
        "labels": len(label_catalog),
    }
    return {
        "schema": "posalign.parse.v1",
        "shard_id": shard_id,
        "shard_digest": sha256_bytes(raw_bytes),
        "governance": config.components(),
        "records": records,
        "label_catalog": label_catalog,
        "stats": stats,
    }


def export_v1_records(parse_payloads: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return a strict v1-consumable stream for v1-origin records."""

    exported: list[dict[str, Any]] = []
    for payload in sorted(parse_payloads, key=lambda item: item["shard_id"]):
        for record in payload["records"]:
            if record["source_version"] != 1 or record["v1_roundtrip"] is None:
                continue
            source = record["v1_roundtrip"]
            item: dict[str, Any] = {
                "sent_id": source["sent_id"],
                "tokens": source["tokens"],
                "old_tags": source["old_tags"],
                "annotator_id": source["annotator_id"],
                "structure_version": 1,
            }
            if source["new_tags"] is not None:
                item["new_tags"] = source["new_tags"]
            if source["confidence"] is not None:
                item["confidence"] = source["confidence"]
            exported.append(item)
    return exported


def count_exportable_v1(parse_payloads: list[dict[str, Any]]) -> int:
    return sum(
        1
        for payload in parse_payloads
        for record in payload["records"]
        if record["source_version"] == 1
    )

