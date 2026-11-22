from __future__ import annotations

import json
import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from .digest import atomic_write_json, canonical_json_bytes, read_json, sha256_bytes, sha256_json
from .errors import E_INVALID_STAGE, E_STALE_CHECKPOINT, fail


STAGE_ORDER = ("validate", "gather", "quantize", "verify")
STAGE_INDEX = {stage: index + 1 for index, stage in enumerate(STAGE_ORDER)}
RECORD_VERSION = 1


def _safe_name(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "_", value)[:80]
    return f"{cleaned or 'batch'}-{sha256_bytes(value.encode('utf-8'))[:12]}"


class FileLock:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self._descriptor: int | None = None

    def __enter__(self) -> "FileLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._descriptor = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            if os.name == "nt":
                import msvcrt

                os.lseek(self._descriptor, 0, os.SEEK_SET)
                if os.fstat(self._descriptor).st_size == 0:
                    os.write(self._descriptor, b"\0")
                    os.fsync(self._descriptor)
                os.lseek(self._descriptor, 0, os.SEEK_SET)
                while True:
                    try:
                        msvcrt.locking(self._descriptor, msvcrt.LK_NBLCK, 1)
                        break
                    except OSError:
                        time.sleep(0.02)
            else:
                import fcntl

                fcntl.flock(self._descriptor, fcntl.LOCK_EX)
        except BaseException:
            os.close(self._descriptor)
            self._descriptor = None
            raise
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        descriptor = self._descriptor
        self._descriptor = None
        if descriptor is None:
            return
        try:
            if os.name == "nt":
                import msvcrt

                os.lseek(descriptor, 0, os.SEEK_SET)
                msvcrt.locking(descriptor, msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)


@dataclass(frozen=True)
class StageStatus:
    stage: str
    valid: bool
    present: bool
    sequence: int | None
    reasons: tuple[str, ...]
    record: Mapping[str, Any] | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "stage": self.stage,
            "valid": self.valid,
            "present": self.present,
            "sequence": self.sequence,
            "reasons": list(self.reasons),
        }


@dataclass(frozen=True)
class CheckpointInspection:
    batch_id: str
    stages: tuple[StageStatus, ...]
    earliest_invalid_stage: str | None
    reasons: tuple[str, ...]
    reused_stages: tuple[str, ...]
    invalidations: tuple[dict[str, Any], ...]

    @property
    def all_valid(self) -> bool:
        return self.earliest_invalid_stage is None

    def to_dict(self) -> dict[str, Any]:
        return {
            "batch_id": self.batch_id,
            "all_valid": self.all_valid,
            "earliest_invalid_stage": self.earliest_invalid_stage,
            "reasons": list(self.reasons),
            "reused_stages": list(self.reused_stages),
            "stages": [stage.to_dict() for stage in self.stages],
            "invalidations": list(self.invalidations),
        }


def _base_record(
    *,
    batch_id: str,
    partition_id: str,
    stage: str,
    sequence: int,
    previous_digest: str,
    input_fingerprint: str,
    manifest_fingerprint: str,
    manifest_version: int,
    config_version: str,
    product_digest: str,
    components: Mapping[str, Any],
    invalidated_stage: str | None = None,
    invalidation_reasons: tuple[str, ...] = (),
) -> dict[str, Any]:
    record: dict[str, Any] = {
        "record_version": RECORD_VERSION,
        "sequence": int(sequence),
        "batch_id": batch_id,
        "partition_id": partition_id,
        "shard_id": partition_id,
        "stage": stage,
        "stage_index": STAGE_INDEX.get(stage, 0),
        "input_fingerprint": input_fingerprint,
        "manifest_fingerprint": manifest_fingerprint,
        "manifest_version": int(manifest_version),
        "config_version": config_version,
        "product_digest": product_digest,
        "components": dict(components),
        "previous_digest": previous_digest,
        "invalidated_stage": invalidated_stage,
        "invalidation_reasons": list(invalidation_reasons),
    }
    record["record_digest"] = sha256_json(record)
    return record


def verify_record_digest(record: Mapping[str, Any]) -> None:
    expected = record.get("record_digest")
    actual = sha256_json({key: value for key, value in record.items() if key != "record_digest"})
    if expected != actual:
        raise fail(
            E_STALE_CHECKPOINT,
            "checkpoint record digest mismatch",
            reason="tamper",
            batch_id=record.get("batch_id"),
            sequence=record.get("sequence"),
            expected_digest=expected,
            actual_digest=actual,
        )


class CheckpointStore:
    def __init__(self, workspace: str | Path, batch_id: str, partition_id: str | None = None) -> None:
        self.workspace = Path(workspace)
        self.batch_id = batch_id
        self.partition_id = partition_id or batch_id
        self.safe_name = _safe_name(batch_id)
        self.root = self.workspace / "checkpoints" / self.safe_name
        self.records_dir = self.root / "records"
        self.lock_path = self.workspace / "checkpoints" / ".locks" / f"{self.safe_name}.lock"
        self.anchor_path = self.workspace / "checkpoints" / ".anchors" / f"{self.safe_name}.json"

    def lock(self) -> FileLock:
        return FileLock(self.lock_path)

    def _record_files(self) -> list[Path]:
        return sorted(self.records_dir.glob("*.json")) if self.records_dir.exists() else []

    def _read_records_unlocked(self, *, repair_anchor: bool = False) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        for path in self._record_files():
            try:
                record = read_json(path)
            except (OSError, json.JSONDecodeError) as exc:
                raise fail(
                    E_STALE_CHECKPOINT,
                    "checkpoint record cannot be decoded",
                    reason="tamper",
                    batch_id=self.batch_id,
                    path=str(path),
                ) from exc
            if not isinstance(record, dict):
                raise fail(
                    E_STALE_CHECKPOINT,
                    "checkpoint record is not an object",
                    reason="tamper",
                    batch_id=self.batch_id,
                    path=str(path),
                )
            if record.get("batch_id") != self.batch_id or record.get("partition_id") != self.partition_id:
                raise fail(
                    E_STALE_CHECKPOINT,
                    "checkpoint record belongs to a different batch partition",
                    reason="cross_shard",
                    batch_id=self.batch_id,
                    partition_id=self.partition_id,
                    record_batch_id=record.get("batch_id"),
                    record_partition_id=record.get("partition_id"),
                    path=str(path),
                )
            verify_record_digest(record)
            records.append(record)
        records.sort(key=lambda item: int(item.get("sequence", -1)))
        for expected, record in enumerate(records, start=1):
            actual_sequence = int(record.get("sequence", -1))
            if actual_sequence != expected:
                raise fail(
                    E_STALE_CHECKPOINT,
                    "checkpoint sequence is not contiguous",
                    reason="rollback" if actual_sequence < expected else "non_contiguous",
                    batch_id=self.batch_id,
                    expected_sequence=expected,
                    actual_sequence=record.get("sequence"),
                )
            previous = "" if expected == 1 else records[expected - 2]["record_digest"]
            if record.get("previous_digest") != previous:
                raise fail(
                    E_STALE_CHECKPOINT,
                    "checkpoint digest chain is broken",
                    reason="tamper",
                    batch_id=self.batch_id,
                    sequence=record.get("sequence"),
                    expected_previous=previous,
                    actual_previous=record.get("previous_digest"),
                )
            if expected > 1 and int(record.get("manifest_version", 0)) < int(
                records[expected - 2].get("manifest_version", 0)
            ):
                raise fail(
                    E_STALE_CHECKPOINT,
                    "manifest structure version regressed in checkpoint chain",
                    reason="rollback",
                    batch_id=self.batch_id,
                    sequence=record.get("sequence"),
                )
        self._verify_anchor_unlocked(records, repair=repair_anchor)
        return records

    def _verify_anchor_unlocked(self, records: list[dict[str, Any]], *, repair: bool) -> None:
        if not self.anchor_path.exists():
            if records and repair:
                self._write_anchor_unlocked(records)
            return
        try:
            anchor = read_json(self.anchor_path)
        except (OSError, json.JSONDecodeError) as exc:
            raise fail(
                E_STALE_CHECKPOINT,
                "checkpoint anchor cannot be decoded",
                reason="tamper",
                batch_id=self.batch_id,
                path=str(self.anchor_path),
            ) from exc
        if not isinstance(anchor, dict) or anchor.get("batch_id") != self.batch_id:
            raise fail(
                E_STALE_CHECKPOINT,
                "checkpoint anchor belongs to another batch",
                reason="cross_shard",
                batch_id=self.batch_id,
                anchor_batch_id=None if not isinstance(anchor, dict) else anchor.get("batch_id"),
            )
        anchor_sequence = int(anchor.get("high_watermark", 0))
        actual_sequence = len(records)
        if anchor_sequence > actual_sequence:
            raise fail(
                E_STALE_CHECKPOINT,
                "checkpoint ledger was rolled back behind its anchor",
                reason="rollback",
                batch_id=self.batch_id,
                anchor_sequence=anchor_sequence,
                actual_sequence=actual_sequence,
            )
        if actual_sequence > 0 and anchor_sequence == actual_sequence:
            last = records[-1]
            if anchor.get("head_digest") != last.get("record_digest"):
                raise fail(
                    E_STALE_CHECKPOINT,
                    "checkpoint head digest differs from its anchor",
                    reason="tamper",
                    batch_id=self.batch_id,
                    anchor_digest=anchor.get("head_digest"),
                    actual_digest=last.get("record_digest"),
                )
            if int(anchor.get("manifest_version", 0)) > int(last.get("manifest_version", 0)):
                raise fail(
                    E_STALE_CHECKPOINT,
                    "checkpoint manifest version is behind its anchor",
                    reason="rollback",
                    batch_id=self.batch_id,
                    anchor_manifest_version=anchor.get("manifest_version"),
                    actual_manifest_version=last.get("manifest_version"),
                )
        if actual_sequence > anchor_sequence and repair:
            self._write_anchor_unlocked(records)

    def _write_anchor_unlocked(self, records: list[dict[str, Any]]) -> None:
        atomic_write_json(
            self.anchor_path,
            {
                "batch_id": self.batch_id,
                "partition_id": self.partition_id,
                "high_watermark": 0 if not records else int(records[-1]["sequence"]),
                "head_digest": "" if not records else str(records[-1]["record_digest"]),
                "manifest_version": 0 if not records else int(records[-1]["manifest_version"]),
                "record_count": len(records),
            },
        )

    def read_records(self) -> list[dict[str, Any]]:
        with self.lock():
            return self._read_records_unlocked(repair_anchor=True)

    def _effective_stages(self, records: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
        current: dict[str, dict[str, Any]] = {}
        for record in records:
            stage = str(record.get("stage", ""))
            if stage == "invalidate":
                target = str(record.get("invalidated_stage", ""))
                if target in STAGE_INDEX:
                    for candidate in STAGE_ORDER[STAGE_INDEX[target] - 1 :]:
                        current.pop(candidate, None)
            elif stage in STAGE_INDEX:
                current[stage] = record
        return current

    def latest_effective(self) -> dict[str, dict[str, Any]]:
        with self.lock():
            records = self._read_records_unlocked(repair_anchor=True)
            return self._effective_stages(records)

    def invalidations(self) -> tuple[dict[str, Any], ...]:
        return tuple(record for record in self.read_records() if record.get("stage") == "invalidate")

    def inspect(self, expected_components: Mapping[str, Mapping[str, Any]]) -> CheckpointInspection:
        records = self.read_records()
        current = self._effective_stages(records)
        invalidation_records = tuple(record for record in records if record.get("stage") == "invalidate")
        statuses: list[StageStatus] = []
        earliest: str | None = None
        all_reasons: list[str] = []
        reused: list[str] = []
        for stage in STAGE_ORDER:
            record = current.get(stage)
            reasons: list[str] = []
            if earliest is not None:
                reasons.append("upstream_invalidated")
            elif record is None:
                reasons.append("missing")
            else:
                expected = dict(expected_components.get(stage, {}))
                actual = dict(record.get("components", {}))
                for key in sorted(set(expected).union(actual)):
                    if canonical_json_bytes(expected.get(key)) != canonical_json_bytes(actual.get(key)):
                        reasons.append(f"changed:{key}")
            valid = not reasons
            if not valid and earliest is None:
                earliest = stage
            if valid:
                reused.append(stage)
            for reason in reasons:
                if reason not in all_reasons:
                    all_reasons.append(reason)
            statuses.append(
                StageStatus(
                    stage=stage,
                    valid=valid,
                    present=record is not None,
                    sequence=None if record is None else int(record["sequence"]),
                    reasons=tuple(reasons),
                    record=record,
                )
            )
        return CheckpointInspection(
            batch_id=self.batch_id,
            stages=tuple(statuses),
            earliest_invalid_stage=earliest,
            reasons=tuple(all_reasons),
            reused_stages=tuple(reused),
            invalidations=invalidation_records,
        )

    def publish_stage(
        self,
        *,
        stage: str,
        components: Mapping[str, Any],
        input_fingerprint: str,
        manifest_fingerprint: str,
        manifest_version: int,
        config_version: str,
        product_digest: str,
    ) -> dict[str, Any]:
        if stage not in STAGE_INDEX:
            raise fail(E_INVALID_STAGE, "unknown checkpoint stage", stage=stage, batch_id=self.batch_id)
        with self.lock():
            records = self._read_records_unlocked(repair_anchor=True)
            current = self._effective_stages(records)
            for prior in STAGE_ORDER[: STAGE_INDEX[stage] - 1]:
                if prior not in current:
                    raise fail(
                        E_INVALID_STAGE,
                        "checkpoint prerequisite stage is missing",
                        batch_id=self.batch_id,
                        stage=stage,
                        missing_stage=prior,
                    )
            existing = current.get(stage)
            if existing is not None:
                reason = (
                    "duplicate"
                    if canonical_json_bytes(existing.get("components", {})) == canonical_json_bytes(dict(components))
                    else "out_of_sequence"
                )
                raise fail(
                    E_STALE_CHECKPOINT,
                    "stage is already published for the current generation",
                    reason=reason,
                    batch_id=self.batch_id,
                    stage=stage,
                    sequence=existing.get("sequence"),
                )
            if records and int(manifest_version) < int(records[-1].get("manifest_version", 0)):
                raise fail(
                    E_STALE_CHECKPOINT,
                    "manifest structure version would regress",
                    reason="rollback",
                    batch_id=self.batch_id,
                    previous_manifest_version=records[-1].get("manifest_version"),
                    new_manifest_version=manifest_version,
                )
            sequence = len(records) + 1
            record = _base_record(
                batch_id=self.batch_id,
                partition_id=self.partition_id,
                stage=stage,
                sequence=sequence,
                previous_digest="" if not records else str(records[-1]["record_digest"]),
                input_fingerprint=input_fingerprint,
                manifest_fingerprint=manifest_fingerprint,
                manifest_version=manifest_version,
                config_version=config_version,
                product_digest=product_digest,
                components=components,
            )
            self._write_record_unlocked(record)
            records.append(record)
            self._write_anchor_unlocked(records)
            return record

    def invalidate_from(
        self,
        *,
        target_stage: str,
        reasons: tuple[str, ...],
        components: Mapping[str, Any],
        current_input_fingerprint: str,
        manifest_fingerprint: str,
        manifest_version: int,
        config_version: str,
    ) -> dict[str, Any]:
        if target_stage not in STAGE_INDEX:
            raise fail(E_INVALID_STAGE, "unknown invalidation stage", stage=target_stage, batch_id=self.batch_id)
        with self.lock():
            records = self._read_records_unlocked(repair_anchor=True)
            current = self._effective_stages(records)
            if not any(stage in current for stage in STAGE_ORDER[STAGE_INDEX[target_stage] - 1 :]):
                return {}
            if records and int(manifest_version) < int(records[-1].get("manifest_version", 0)):
                raise fail(
                    E_STALE_CHECKPOINT,
                    "manifest structure version would regress",
                    reason="rollback",
                    batch_id=self.batch_id,
                    previous_manifest_version=records[-1].get("manifest_version"),
                    new_manifest_version=manifest_version,
                )
            sequence = len(records) + 1
            record = _base_record(
                batch_id=self.batch_id,
                partition_id=self.partition_id,
                stage="invalidate",
                sequence=sequence,
                previous_digest="" if not records else str(records[-1]["record_digest"]),
                input_fingerprint=current_input_fingerprint,
                manifest_fingerprint=manifest_fingerprint,
                manifest_version=manifest_version,
                config_version=config_version,
                product_digest="",
                components=components,
                invalidated_stage=target_stage,
                invalidation_reasons=reasons,
            )
            self._write_record_unlocked(record)
            records.append(record)
            self._write_anchor_unlocked(records)
            return record

    def _write_record_unlocked(self, record: Mapping[str, Any]) -> None:
        sequence = int(record["sequence"])
        stage = str(record["stage"])
        target = self.records_dir / f"{sequence:06d}-{stage}.json"
        if target.exists():
            raise fail(
                E_STALE_CHECKPOINT,
                "checkpoint sequence file already exists",
                reason="rollback",
                batch_id=self.batch_id,
                sequence=sequence,
                path=str(target),
            )
        atomic_write_json(target, record)
