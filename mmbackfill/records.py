from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .canonical import atomic_write_json, canonical_bytes, digest_bytes, digest_obj, read_json
from .workspace import Workspace


RECORD_RE = re.compile(r"^batch-(\d{6,})\.json$")
REQUIRED_FIELDS = {
    "record_version",
    "batch_seq",
    "batch_id",
    "layout_config_version",
    "version_fingerprints",
    "input_content_fingerprint",
    "dataset_digest",
    "ownership_fingerprint",
    "assignment_summary",
    "images",
    "texts",
    "prev_record_digest",
}


@dataclass
class RecordScan:
    records: list[dict[str, Any]] = field(default_factory=list)
    ignored: list[dict[str, Any]] = field(default_factory=list)

    @property
    def latest_seq(self) -> int:
        return int(self.records[-1]["batch_seq"]) if self.records else 0

    @property
    def ledger_digest(self) -> str:
        values = [{"batch_seq": item["batch_seq"], "record_digest": item["_record_digest"]} for item in self.records]
        return digest_obj({"schema": "mmb-record-ledger-v1", "records": values})

    @property
    def record_digests(self) -> list[dict[str, Any]]:
        return [{"batch_seq": item["batch_seq"], "record_digest": item["_record_digest"]} for item in self.records]


def batch_record_path(workspace: Workspace, seq: int) -> Path:
    return workspace.records_dir / f"batch-{seq:06d}.json"


def write_batch_record(workspace: Workspace, payload: dict[str, Any]) -> str:
    envelope = {"record": payload, "record_sha256": digest_obj(payload)}
    seq = int(payload["batch_seq"])
    atomic_write_json(batch_record_path(workspace, seq), envelope)
    return envelope["record_sha256"]


def _validate_envelope(data: Any, filename_seq: int) -> tuple[dict[str, Any] | None, str | None]:
    if not isinstance(data, dict):
        return None, "record envelope is not an object"
    record = data.get("record")
    record_sha = data.get("record_sha256")
    if not isinstance(record, dict) or not isinstance(record_sha, str):
        return None, "record envelope has missing fields"
    actual = digest_obj(record)
    if actual != record_sha:
        return None, "record checksum mismatch"
    missing = REQUIRED_FIELDS.difference(record)
    if missing:
        return None, f"record is missing required fields: {sorted(missing)}"
    if record.get("record_version") != 1:
        return None, "unsupported record_version"
    if not isinstance(record.get("batch_seq"), int) or isinstance(record.get("batch_seq"), bool):
        return None, "batch_seq is not an integer"
    if record["batch_seq"] != filename_seq:
        return None, "batch_seq does not match record filename"
    if not isinstance(record.get("batch_id"), str) or not record["batch_id"]:
        return None, "batch_id is missing"
    if not isinstance(record.get("images"), list) or not isinstance(record.get("texts"), list):
        return None, "images/texts must be arrays"
    if not isinstance(record.get("version_fingerprints"), dict):
        return None, "version_fingerprints must be an object"
    if not isinstance(record.get("assignment_summary"), dict):
        return None, "assignment_summary must be an object"
    record = dict(record)
    record["_record_digest"] = record_sha
    return record, None


def scan_records(workspace: Workspace) -> RecordScan:
    scan = RecordScan()
    if not workspace.records_dir.exists():
        return scan
    candidates: list[tuple[int, str, Path]] = []
    for path in sorted(workspace.records_dir.glob("*.json")):
        match = RECORD_RE.match(path.name)
        if not match:
            if path.name.startswith("."):
                continue
            scan.ignored.append({"file": path.name, "reason": "unexpected record filename"})
            continue
        candidates.append((int(match.group(1)), match.group(0), path))
    candidates.sort()
    seen_seq: dict[int, str] = {}
    expected_prev: str | None = None
    chain_broken = False
    for seq, filename, path in candidates:
        if chain_broken:
            scan.ignored.append({"file": filename, "reason": "ignored after a sequence gap or corrupt record"})
            continue
        if seq in seen_seq:
            scan.ignored.append({"file": filename, "reason": "duplicate batch sequence"})
            chain_broken = True
            continue
        try:
            data = read_json(path)
        except (OSError, ValueError) as exc:
            scan.ignored.append({"file": filename, "reason": f"unreadable record: {exc}"})
            chain_broken = True
            continue
        record, error = _validate_envelope(data, seq)
        if error is not None:
            scan.ignored.append({"file": filename, "reason": error})
            chain_broken = True
            continue
        assert record is not None
        if seq != scan.latest_seq + 1:
            scan.ignored.append({"file": filename, "reason": "non-contiguous batch sequence"})
            chain_broken = True
            continue
        if record.get("prev_record_digest") != expected_prev:
            scan.ignored.append({"file": filename, "reason": "previous-record digest mismatch"})
            chain_broken = True
            continue
        scan.records.append(record)
        seen_seq[seq] = filename
        expected_prev = record["_record_digest"]
    return scan


def merge_record_dataset(records: list[dict[str, Any]]) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    images: dict[str, dict[str, Any]] = {}
    texts: dict[str, dict[str, Any]] = {}
    for record in records:
        for row in record["images"]:
            if isinstance(row, dict) and isinstance(row.get("image_id"), str):
                images[row["image_id"]] = dict(row)
        for row in record["texts"]:
            if isinstance(row, dict) and isinstance(row.get("text_id"), str):
                texts[row["text_id"]] = dict(row)
    return images, texts


def record_summaries(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    summaries: list[dict[str, Any]] = []
    for record in records:
        summaries.append(
            {
                "batch_seq": record["batch_seq"],
                "batch_id": record["batch_id"],
                "order": record.get("order", "id"),
                "layout_config_version": record["layout_config_version"],
                "input_content_fingerprint": record["input_content_fingerprint"],
                "dataset_digest": record["dataset_digest"],
                "record_digest": record["_record_digest"],
                "text_ids": record.get("submitted_text_ids", []),
                "entry_count": record.get("entry_count", 0),
                "assignment_summary": record["assignment_summary"],
            }
        )
    return summaries


def record_byte_digest(record: dict[str, Any]) -> str:
    return digest_bytes(canonical_bytes(record))
