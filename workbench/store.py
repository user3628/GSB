from __future__ import annotations

import contextlib
import shutil
from pathlib import Path
from typing import Any, Iterable

from .config import STAGE_INDEX, STAGE_NAMES
from .util import (
    StaleCheckpointError,
    WorkbenchError,
    atomic_write_json,
    digest,
    ensure_dir,
    exclusive_lock,
    read_json,
    sha256_bytes,
)


CHECKPOINT_FORMAT = 1


class CheckpointStore:
    """Per-stage append-only checkpoints with a separate publish high-water guard."""

    def __init__(self, root: Path):
        self.root = root.resolve()
        self.base = self.root / "checkpoints"
        self.stages_dir = self.base / "stages"
        self.lock_path = self.base / ".publish.lock"
        self.guard_path = self.base / ".publish_guard.json"
        self.invalidations_path = self.base / "invalidations.json"
        ensure_dir(self.base)
        ensure_dir(self.stages_dir)

    def stage_dir(self, stage: str) -> Path:
        self._validate_stage(stage)
        return self.stages_dir / stage

    def index_path(self, stage: str) -> Path:
        return self.stage_dir(stage) / "index.json"

    def records_dir(self, stage: str) -> Path:
        return self.stage_dir(stage) / "records"

    def publish(
        self,
        *,
        stage: str,
        artifact: dict[str, Any],
        sample_key: str,
        image_id: str | None,
        input_fingerprint: str,
        input_parts: dict[str, Any],
        config_version: str,
        upstream_digests: dict[str, str] | None = None,
        expected_sequence: int | None = None,
        expected_sample_key: str | None = None,
    ) -> dict[str, Any]:
        self._validate_stage(stage)
        with exclusive_lock(self.lock_path):
            index = self._load_index(stage)
            self._verify_locked(stage, index)
            self._repair_pending_guard_locked(stage, index)
            if expected_sample_key is not None and sample_key != expected_sample_key:
                raise StaleCheckpointError(
                    "checkpoint publication crossed sample identity",
                    {
                        "reason": "cross_sample_publish",
                        "expected_sample_key": expected_sample_key,
                        "received_sample_key": sample_key,
                        "stage": stage,
                    },
                )
            expected_next = index["sequence"] + 1
            if expected_sequence is not None and expected_sequence != expected_next:
                raise StaleCheckpointError(
                    "checkpoint sequence is stale or advanced",
                    {
                        "reason": "sequence_not_contiguous",
                        "stage": stage,
                        "expected_sequence": expected_sequence,
                        "required_next_sequence": expected_next,
                        "actual_sequence": index["sequence"],
                    },
                )
            existing_entry = index["records"].get(sample_key)
            if existing_entry:
                existing = self._read_record(stage, existing_entry)
                if (
                    existing["input_fingerprint"] == input_fingerprint
                    and existing["artifact_digest"] == digest(artifact)
                    and existing["config_version"] == config_version
                ):
                    raise StaleCheckpointError(
                        "an identical checkpoint has already been published",
                        {
                            "reason": "duplicate_publish",
                            "stage": stage,
                            "sample_key": sample_key,
                            "sequence": existing_entry["sequence"],
                        },
                    )

            sequence = expected_next
            artifact_digest = digest(artifact)
            record = {
                "checkpoint_format": CHECKPOINT_FORMAT,
                "stage": stage,
                "stage_index": STAGE_INDEX[stage],
                "sample_key": sample_key,
                "image_id": image_id,
                "sequence": sequence,
                "input_fingerprint": input_fingerprint,
                "input_parts": input_parts,
                "config_version": config_version,
                "artifact_digest": artifact_digest,
                "upstream_digests": upstream_digests or {},
                "artifact": artifact,
            }
            encoded = _record_bytes(record)
            record_sha = sha256_bytes(encoded)
            filename = f"{sequence:012d}__{digest(sample_key)[:12]}.json"
            relative_file = f"records/{filename}"
            record_path = self.stage_dir(stage) / relative_file
            ensure_dir(record_path.parent)
            atomic_write_json(record_path, record)

            entry = {
                "file": relative_file,
                "sha256": record_sha,
                "sequence": sequence,
                "sample_key": sample_key,
                "input_fingerprint": input_fingerprint,
                "artifact_digest": artifact_digest,
            }
            records = dict(index["records"])
            records[sample_key] = entry
            new_index = _index_payload(stage, sequence, records)
            self._write_index(stage, new_index)
            guard = self._load_guard()
            guard["stages"][stage] = _guard_entry(new_index)
            atomic_write_json(self.guard_path, guard)
            return record

    def get(self, stage: str, sample_key: str) -> dict[str, Any] | None:
        self._validate_stage(stage)
        with exclusive_lock(self.lock_path):
            index = self._load_index(stage)
            self._verify_locked(stage, index)
            entry = index["records"].get(sample_key)
            if not entry:
                return None
            return self._read_record(stage, entry)

    def all(self, stage: str) -> dict[str, dict[str, Any]]:
        self._validate_stage(stage)
        with exclusive_lock(self.lock_path):
            index = self._load_index(stage)
            self._verify_locked(stage, index)
            return {
                key: self._read_record(stage, entry)
                for key, entry in sorted(index["records"].items())
            }

    def summary(self, stage: str) -> dict[str, Any]:
        self._validate_stage(stage)
        try:
            with exclusive_lock(self.lock_path):
                index = self._load_index(stage)
                self._verify_locked(stage, index)
                guard = self._load_guard().get("stages", {}).get(stage)
                return {
                    "stage": stage,
                    "stage_index": STAGE_INDEX[stage],
                    "sequence": index["sequence"],
                    "record_count": len(index["records"]),
                    "snapshot_digest": index["snapshot_digest"],
                    "guard": guard,
                    "integrity": "ok",
                }
        except WorkbenchError as exc:
            return {
                "stage": stage,
                "stage_index": STAGE_INDEX[stage],
                "sequence": None,
                "record_count": None,
                "snapshot_digest": None,
                "guard": None,
                "integrity": "error",
                "error": exc.as_dict(),
            }

    def invalidate(
        self,
        keys_by_stage: dict[str, Iterable[str]],
        *,
        expected_current_sequences: dict[str, int] | None = None,
    ) -> dict[str, list[str]]:
        """Remove current records by bumping each stage snapshot sequence.

        Record files are append-only history; only the index's current pointer is
        removed, so a concurrent reader can never observe a partially written
        checkpoint. Callers pass expected sequences to detect a stale publisher.
        """

        removed: dict[str, list[str]] = {}
        with exclusive_lock(self.lock_path):
            for stage in STAGE_NAMES:
                keys = sorted(set(keys_by_stage.get(stage, [])))
                if not keys:
                    continue
                index = self._load_index(stage)
                self._verify_locked(stage, index)
                self._repair_pending_guard_locked(stage, index)
                expected = (expected_current_sequences or {}).get(stage)
                if expected is not None and expected != index["sequence"]:
                    raise StaleCheckpointError(
                        "checkpoint snapshot changed before invalidation",
                        {
                            "reason": "invalidation_sequence_mismatch",
                            "stage": stage,
                            "expected_sequence": expected,
                            "actual_sequence": index["sequence"],
                        },
                    )
                records = dict(index["records"])
                stage_removed: list[str] = []
                for key in keys:
                    if key in records:
                        del records[key]
                        stage_removed.append(key)
                if not stage_removed:
                    continue
                sequence = index["sequence"] + 1
                new_index = _index_payload(stage, sequence, records)
                self._write_index(stage, new_index)
                guard = self._load_guard()
                guard["stages"][stage] = _guard_entry(new_index)
                atomic_write_json(self.guard_path, guard)
                removed[stage] = stage_removed
        return removed

    def append_invalidations(self, entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not entries:
            return []
        with exclusive_lock(self.lock_path):
            payload = self._load_invalidations()
            event_sequence = int(payload.get("event_sequence", 0))
            stored: list[dict[str, Any]] = []
            for entry in entries:
                event_sequence += 1
                item = dict(entry)
                item["event_sequence"] = event_sequence
                payload["entries"].append(item)
                stored.append(item)
            payload["event_sequence"] = event_sequence
            payload["entries"] = payload["entries"][-200:]
            atomic_write_json(self.invalidations_path, payload, pretty=True)
            return stored

    def invalidations(self, limit: int = 50) -> list[dict[str, Any]]:
        with exclusive_lock(self.lock_path):
            payload = self._load_invalidations()
            entries = payload.get("entries", [])
            return list(entries[-max(0, limit) :])

    def verify_all(self) -> dict[str, Any]:
        return {stage: self.summary(stage) for stage in STAGE_NAMES}

    def clear_all(self) -> None:
        with exclusive_lock(self.lock_path):
            if self.stages_dir.exists():
                shutil.rmtree(self.stages_dir)
            ensure_dir(self.stages_dir)
            for path in (self.guard_path, self.invalidations_path):
                with contextlib.suppress(FileNotFoundError):
                    path.unlink()

    def _verify_locked(self, stage: str, index: dict[str, Any]) -> None:
        expected_snapshot = _snapshot_digest(stage, index["records"])
        if index.get("snapshot_digest") != expected_snapshot:
            raise StaleCheckpointError(
                "checkpoint snapshot digest does not match its record index",
                {
                    "reason": "snapshot_digest_mismatch",
                    "stage": stage,
                    "declared_digest": index.get("snapshot_digest"),
                    "actual_digest": expected_snapshot,
                },
            )
        for key, entry in index["records"].items():
            record = self._read_record(stage, entry)
            if record.get("stage") != stage:
                raise StaleCheckpointError(
                    "record stage does not match checkpoint directory",
                    {"reason": "stage_mismatch", "stage": stage, "sample_key": key},
                )
            if record.get("sample_key") != key:
                raise StaleCheckpointError(
                    "record sample_key does not match index key",
                    {"reason": "cross_sample_publish", "stage": stage, "sample_key": key},
                )
            if int(record.get("sequence", -1)) > int(index["sequence"]):
                raise StaleCheckpointError(
                    "record sequence is ahead of stage snapshot",
                    {"reason": "record_sequence_ahead", "stage": stage, "sample_key": key},
                )
        guard = self._load_guard()
        guard_entry = guard.get("stages", {}).get(stage)
        if index["sequence"] == 0:
            if guard_entry and int(guard_entry.get("sequence", 0)) > 0:
                raise StaleCheckpointError(
                    "stage snapshot was removed or replaced by an older empty snapshot",
                    {
                        "reason": "snapshot_sequence_regression",
                        "stage": stage,
                        "guard_sequence": guard_entry.get("sequence"),
                        "snapshot_sequence": 0,
                    },
                )
            return
        if not guard_entry:
            raise StaleCheckpointError(
                "publish high-water guard is missing for a non-empty stage",
                {"reason": "guard_missing", "stage": stage},
            )
        guard_sequence = int(guard_entry.get("sequence", -1))
        snapshot_sequence = int(index["sequence"])
        if snapshot_sequence < guard_sequence:
            raise StaleCheckpointError(
                "checkpoint snapshot sequence regressed",
                {
                    "reason": "snapshot_sequence_regression",
                    "stage": stage,
                    "guard_sequence": guard_sequence,
                    "snapshot_sequence": snapshot_sequence,
                },
            )
        if snapshot_sequence == guard_sequence and index["snapshot_digest"] != guard_entry.get("snapshot_digest"):
            raise StaleCheckpointError(
                "checkpoint snapshot was replaced with different contents",
                {
                    "reason": "snapshot_replaced",
                    "stage": stage,
                    "guard_digest": guard_entry.get("snapshot_digest"),
                    "snapshot_digest": index["snapshot_digest"],
                },
            )
        if snapshot_sequence > guard_sequence + 1:
            raise StaleCheckpointError(
                "checkpoint sequence advanced beyond the allowed contiguous window",
                {
                    "reason": "sequence_gap",
                    "stage": stage,
                    "guard_sequence": guard_sequence,
                    "snapshot_sequence": snapshot_sequence,
                },
            )

    def _repair_pending_guard_locked(self, stage: str, index: dict[str, Any]) -> None:
        guard = self._load_guard()
        entry = guard.get("stages", {}).get(stage)
        if entry is None or int(index["sequence"]) == int(entry.get("sequence", -1)) + 1:
            if index["sequence"] == 0 and entry is None:
                return
            guard["stages"][stage] = _guard_entry(index)
            atomic_write_json(self.guard_path, guard)

    def _load_index(self, stage: str) -> dict[str, Any]:
        path = self.index_path(stage)
        if not path.exists():
            return _index_payload(stage, 0, {})
        try:
            payload = read_json(path)
        except Exception as exc:
            raise StaleCheckpointError(
                "checkpoint index is unreadable",
                {"reason": "index_unreadable", "stage": stage, "error": str(exc)},
            ) from exc
        if not isinstance(payload, dict) or payload.get("stage") != stage:
            raise StaleCheckpointError(
                "checkpoint index has the wrong stage identity",
                {"reason": "index_stage_mismatch", "stage": stage},
            )
        if not isinstance(payload.get("records"), dict):
            raise StaleCheckpointError(
                "checkpoint index records are malformed",
                {"reason": "index_records_malformed", "stage": stage},
            )
        return payload

    def _write_index(self, stage: str, payload: dict[str, Any]) -> None:
        atomic_write_json(self.index_path(stage), payload)

    def _read_record(self, stage: str, entry: dict[str, Any]) -> dict[str, Any]:
        relative = entry.get("file")
        if not isinstance(relative, str) or not relative.startswith("records/") or ".." in Path(relative).parts:
            raise StaleCheckpointError(
                "checkpoint record path is unsafe",
                {"reason": "record_path_unsafe", "stage": stage, "file": relative},
            )
        path = self.stage_dir(stage) / relative
        if not path.exists():
            raise StaleCheckpointError(
                "checkpoint record file is missing",
                {"reason": "record_missing", "stage": stage, "file": relative},
            )
        data = path.read_bytes()
        actual_sha = sha256_bytes(data)
        if actual_sha != entry.get("sha256"):
            raise StaleCheckpointError(
                "checkpoint record digest does not match its index entry",
                {
                    "reason": "record_digest_mismatch",
                    "stage": stage,
                    "file": relative,
                    "declared_digest": entry.get("sha256"),
                    "actual_digest": actual_sha,
                },
            )
        try:
            record = read_json(path)
        except Exception as exc:
            raise StaleCheckpointError(
                "checkpoint record is unreadable",
                {"reason": "record_unreadable", "stage": stage, "file": relative, "error": str(exc)},
            ) from exc
        if _record_bytes(record) != data:
            raise StaleCheckpointError(
                "checkpoint record is not stored in canonical form",
                {"reason": "record_not_canonical", "stage": stage, "file": relative},
            )
        return record

    def _load_guard(self) -> dict[str, Any]:
        if not self.guard_path.exists():
            return {"format": CHECKPOINT_FORMAT, "stages": {}}
        try:
            payload = read_json(self.guard_path)
        except Exception as exc:
            raise StaleCheckpointError(
                "publish guard is unreadable",
                {"reason": "guard_unreadable", "error": str(exc)},
            ) from exc
        if not isinstance(payload, dict) or not isinstance(payload.get("stages"), dict):
            raise StaleCheckpointError("publish guard is malformed", {"reason": "guard_malformed"})
        return payload

    def _load_invalidations(self) -> dict[str, Any]:
        if not self.invalidations_path.exists():
            return {"format": CHECKPOINT_FORMAT, "event_sequence": 0, "entries": []}
        try:
            payload = read_json(self.invalidations_path)
        except Exception:
            return {"format": CHECKPOINT_FORMAT, "event_sequence": 0, "entries": []}
        if not isinstance(payload, dict) or not isinstance(payload.get("entries"), list):
            return {"format": CHECKPOINT_FORMAT, "event_sequence": 0, "entries": []}
        return payload

    @staticmethod
    def _validate_stage(stage: str) -> None:
        if stage not in STAGE_NAMES:
            raise WorkbenchError("E_STAGE_INVALID", "unknown pipeline stage", {"stage": stage})


def _snapshot_digest(stage: str, records: dict[str, Any]) -> str:
    return digest({"stage": stage, "records": records})


def _index_payload(stage: str, sequence: int, records: dict[str, Any]) -> dict[str, Any]:
    ordered = {key: records[key] for key in sorted(records)}
    return {
        "format": CHECKPOINT_FORMAT,
        "stage": stage,
        "sequence": sequence,
        "records": ordered,
        "snapshot_digest": _snapshot_digest(stage, ordered),
    }


def _guard_entry(index: dict[str, Any]) -> dict[str, Any]:
    return {
        "sequence": index["sequence"],
        "snapshot_digest": index["snapshot_digest"],
        "record_count": len(index["records"]),
    }


def _record_bytes(record: dict[str, Any]) -> bytes:
    from .util import canonical_json

    return (canonical_json(record) + "\n").encode("utf-8")


__all__ = ["CHECKPOINT_FORMAT", "CheckpointStore"]
