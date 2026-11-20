"""Atomic per-step checkpoint log with safe prefix recovery and publishing."""

from __future__ import annotations

import copy
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .errors import invalid_state, stale_checkpoint
from .util import (
    atomic_write_bytes,
    digest_json,
    file_lock,
    fsync_directory,
    pretty_json_bytes,
    safe_component,
    state_digest,
)


def finalize_record(payload: dict[str, Any]) -> dict[str, Any]:
    record = copy.deepcopy(payload)
    record.pop("record_checksum", None)
    record["record_checksum"] = digest_json(record)
    return record


def record_payload_valid(record: Any) -> bool:
    if not isinstance(record, dict):
        return False
    checksum = record.get("record_checksum")
    if not isinstance(checksum, str):
        return False
    payload = copy.deepcopy(record)
    payload.pop("record_checksum", None)
    return checksum == digest_json(payload)


@dataclass
class CheckpointScan:
    records: dict[int, dict[str, Any]] = field(default_factory=dict)
    prefix: list[dict[str, Any]] = field(default_factory=list)
    issues: list[dict[str, Any]] = field(default_factory=list)
    duplicate_sequences: list[int] = field(default_factory=list)
    ignored_files: list[str] = field(default_factory=list)

    @property
    def last_step(self) -> int:
        return int(self.prefix[-1]["step_index"]) if self.prefix else 0

    @property
    def max_valid_step(self) -> int:
        return max(self.records, default=0)

    @property
    def complete_prefix(self) -> bool:
        return self.last_step == self.max_valid_step and not self.duplicate_sequences


class CheckpointStore:
    def __init__(self, workspace_root: str | Path) -> None:
        self.workspace_root = Path(workspace_root).resolve()
        self.checkpoint_root = self.workspace_root / "checkpoints"

    def series_dir(self, series_id: str) -> Path:
        return self.checkpoint_root / safe_component(series_id)

    def lock_path(self, series_id: str) -> Path:
        return self.workspace_root / "locks" / f"{safe_component(series_id)}.lock"

    def record_path(self, series_id: str, step_index: int) -> Path:
        return self.series_dir(series_id) / f"step-{step_index:08d}.json"

    def _validate_structure(
        self, record: Any, series_id: str, filename: str
    ) -> tuple[dict[str, Any] | None, str | None]:
        if not isinstance(record, dict):
            return None, "record is not a JSON object"
        if record.get("schema_version") != 1 or record.get("record_type") != "motion-step":
            return None, "unsupported record schema"
        if record.get("series_id") != series_id:
            return None, "record series_id does not match its log directory"
        step_index = record.get("step_index")
        sequence = record.get("sequence")
        if (
            isinstance(step_index, bool)
            or not isinstance(step_index, int)
            or step_index < 1
        ):
            return None, "invalid step_index"
        if sequence != step_index:
            return None, "sequence must be the in-series step index"
        if not record_payload_valid(record):
            return None, "record checksum mismatch"
        if not isinstance(record.get("state"), dict):
            return None, "missing state"
        if not isinstance(record.get("state_digest"), str):
            return None, "missing state digest"
        if record["state_digest"] != state_digest(record["state"]):
            return None, "state digest mismatch"
        if not isinstance(record.get("step_input_fingerprint"), str):
            return None, "missing step input fingerprint"
        if not isinstance(record.get("input_fingerprints"), dict):
            return None, "missing input fingerprints"
        if not isinstance(record.get("frame_summary"), dict):
            return None, "missing frame summary"
        if not isinstance(record.get("error"), dict):
            return None, "missing error summary"
        return record, None

    def scan(self, series_id: str) -> CheckpointScan:
        directory = self.series_dir(series_id)
        result = CheckpointScan()
        if not directory.exists():
            return result

        grouped: dict[int, list[tuple[str, dict[str, Any]]]] = {}
        for path in sorted(directory.glob("*.json")):
            try:
                with path.open("rb") as handle:
                    raw = json.load(handle)
            except (OSError, ValueError, UnicodeDecodeError) as exc:
                result.issues.append(
                    {"file": path.name, "reason": "unreadable_or_incomplete", "detail": str(exc)}
                )
                result.ignored_files.append(path.name)
                continue
            record, reason = self._validate_structure(raw, series_id, path.name)
            if record is None:
                result.issues.append(
                    {"file": path.name, "reason": "invalid_record", "detail": reason}
                )
                result.ignored_files.append(path.name)
                continue
            grouped.setdefault(int(record["sequence"]), []).append((path.name, record))

        for sequence, entries in sorted(grouped.items()):
            if len(entries) != 1:
                result.duplicate_sequences.append(sequence)
                result.issues.append(
                    {
                        "file": [name for name, _ in entries],
                        "reason": "duplicate_sequence",
                        "sequence": sequence,
                    }
                )
                continue
            result.records[sequence] = entries[0][1]

        expected = 1
        while expected in result.records:
            result.prefix.append(result.records[expected])
            expected += 1
        return result

    def load_step(self, series_id: str, step_index: int) -> dict[str, Any] | None:
        scan = self.scan(series_id)
        if 1 <= step_index <= scan.last_step:
            return scan.records.get(step_index)
        return None

    def append(
        self,
        record: dict[str, Any],
        *,
        expected_series_id: str | None = None,
        expected_next_step: int | None = None,
    ) -> None:
        series_id = record.get("series_id")
        if not isinstance(series_id, str) or not series_id:
            raise stale_checkpoint("checkpoint has no valid series_id")
        step_index = record.get("step_index")
        if expected_series_id is not None and series_id != expected_series_id:
            raise stale_checkpoint(
                "cross-series checkpoint publication rejected",
                series_id=series_id,
                step_index=step_index if isinstance(step_index, int) else None,
                expected_step=expected_next_step,
            )
        checked, reason = self._validate_structure(record, series_id, "<memory>")
        if checked is None:
            raise stale_checkpoint(
                f"checkpoint record rejected: {reason}",
                series_id=series_id,
                step_index=step_index if isinstance(step_index, int) else None,
            )
        if expected_next_step is not None and step_index != expected_next_step:
            raise stale_checkpoint(
                "checkpoint step is not the expected next step",
                series_id=series_id,
                step_index=step_index,
                expected_step=expected_next_step,
            )

        with file_lock(self.lock_path(series_id), exclusive=True):
            scan = self.scan(series_id)
            next_step = scan.last_step + 1
            if step_index != next_step:
                raise stale_checkpoint(
                    "checkpoint publication is stale, duplicate, or non-contiguous",
                    series_id=series_id,
                    step_index=step_index,
                    expected_step=next_step,
                )
            if scan.duplicate_sequences:
                raise stale_checkpoint(
                    "checkpoint log contains a duplicate sequence; repair before publishing",
                    series_id=series_id,
                    step_index=step_index,
                    expected_step=next_step,
                )
            path = self.record_path(series_id, step_index)
            # A non-canonical duplicate for this sequence is rejected by scan above.
            if path.exists():
                try:
                    with path.open("rb") as handle:
                        existing = json.load(handle)
                except (OSError, ValueError, UnicodeDecodeError):
                    existing = None
                if record_payload_valid(existing) and existing.get("sequence") == step_index:
                    raise stale_checkpoint(
                        "checkpoint step has already been published",
                        series_id=series_id,
                        step_index=step_index,
                        expected_step=next_step,
                    )
            atomic_write_bytes(path, pretty_json_bytes(record))

    def invalidate_from(self, series_id: str, step_index: int) -> list[str]:
        if step_index < 1:
            step_index = 1
        removed: list[str] = []
        with file_lock(self.lock_path(series_id), exclusive=True):
            directory = self.series_dir(series_id)
            if not directory.exists():
                return removed
            for path in sorted(directory.glob("*.json")):
                remove = False
                try:
                    with path.open("rb") as handle:
                        record = json.load(handle)
                    if not isinstance(record, dict):
                        remove = True
                    elif record.get("step_index") is None or int(record["step_index"]) >= step_index:
                        remove = True
                except (OSError, ValueError, TypeError, UnicodeDecodeError):
                    remove = True
                if remove:
                    try:
                        path.unlink()
                        removed.append(path.name)
                    except FileNotFoundError:
                        pass
            fsync_directory(directory)
        return removed

    def reset(self, series_id: str) -> list[str]:
        return self.invalidate_from(series_id, 1)

    def assert_resume_step(self, series_id: str, from_step: int) -> dict[str, Any]:
        scan = self.scan(series_id)
        if from_step < 0:
            raise invalid_state(
                "resume step must be non-negative",
                series_id=series_id,
                position=f"checkpoint_step:{from_step}",
            )
        if from_step == 0:
            if scan.last_step != 0:
                raise invalid_state(
                    "resume step 0 would replay already published checkpoints",
                    series_id=series_id,
                    position="checkpoint_step:0",
                    latest_valid_step=scan.last_step,
                )
            return {}
        if from_step != scan.last_step:
            position = f"checkpoint_step:{from_step}"
            if from_step in scan.duplicate_sequences:
                reason = "checkpoint sequence is duplicated and was ignored"
            elif from_step > scan.last_step:
                reason = "checkpoint is beyond the valid consecutive prefix"
            else:
                reason = "checkpoint is not the latest valid step"
            raise invalid_state(
                reason,
                series_id=series_id,
                position=position,
                latest_valid_step=scan.last_step,
            )
        return scan.records[from_step]
