"""Atomic stage checkpoints with monotonic publication under a file lock."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from .artifacts import ArtifactRef, safe_component
from .config import STAGE_ORDER
from .digest import atomic_write_json, read_json
from .errors import RopeError
from .locking import exclusive_file_lock


@dataclass(frozen=True)
class CheckpointRecord:
    shard_id: str
    shard_fingerprint: str
    config_version: str
    stage: str
    sequence: int
    artifact: ArtifactRef
    publication_status: str
    previous_artifact_digest: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "shard_id": self.shard_id,
            "shard_fingerprint": self.shard_fingerprint,
            "config_version": self.config_version,
            "stage": self.stage,
            "sequence": self.sequence,
            "artifact": self.artifact.to_dict(),
            "publication_status": self.publication_status,
            "previous_artifact_digest": self.previous_artifact_digest,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "CheckpointRecord":
        return cls(
            shard_id=str(value["shard_id"]),
            shard_fingerprint=str(value["shard_fingerprint"]),
            config_version=str(value["config_version"]),
            stage=str(value["stage"]),
            sequence=int(value["sequence"]),
            artifact=ArtifactRef.from_dict(value["artifact"]),
            publication_status=str(value.get("publication_status", "computed")),
            previous_artifact_digest=(
                None
                if value.get("previous_artifact_digest") is None
                else str(value["previous_artifact_digest"])
            ),
        )


class CheckpointStore:
    def __init__(self, workspace: str | Path) -> None:
        self.workspace = Path(workspace).resolve()

    def _directory(self, shard_id: str, config_version: str) -> Path:
        return (
            self.workspace
            / "checkpoints"
            / safe_component(shard_id)
            / safe_component(config_version)
        )

    def _state_path(self, shard_id: str, config_version: str) -> Path:
        return self._directory(shard_id, config_version) / "state.json"

    def _record_path(
        self, shard_id: str, config_version: str, sequence: int, stage: str
    ) -> Path:
        return (
            self._directory(shard_id, config_version)
            / "records"
            / f"{sequence:08d}-{safe_component(stage)}.json"
        )

    def _lock_path(self, shard_id: str, config_version: str) -> Path:
        return self._directory(shard_id, config_version) / "publish.lock"

    def _empty_state(self, shard_id: str, config_version: str) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "shard_id": shard_id,
            "config_version": config_version,
            "last_sequence": 0,
            "last_stage": None,
            "records": [],
        }

    def _scan_records(
        self, shard_id: str, config_version: str
    ) -> list[CheckpointRecord]:
        directory = self._directory(shard_id, config_version) / "records"
        if not directory.exists():
            return []
        records: list[CheckpointRecord] = []
        for path in sorted(directory.glob("*.json")):
            value = read_json(path)
            if not value:
                continue
            try:
                record = CheckpointRecord.from_dict(value)
            except (KeyError, TypeError, ValueError):
                continue
            if (
                record.shard_id == shard_id
                and record.config_version == config_version
            ):
                records.append(record)
        records.sort(key=lambda record: record.sequence)
        return records

    def _reconcile_locked(
        self, shard_id: str, config_version: str
    ) -> tuple[dict[str, Any], list[CheckpointRecord]]:
        state_path = self._state_path(shard_id, config_version)
        state = read_json(state_path)
        if state is None:
            state = self._empty_state(shard_id, config_version)
        records = self._scan_records(shard_id, config_version)
        accepted: list[CheckpointRecord] = []
        expected = 1
        previous_digest: str | None = None
        for record in records:
            if record.sequence != expected or record.stage != STAGE_ORDER[expected - 1]:
                break
            if record.previous_artifact_digest != previous_digest:
                break
            accepted.append(record)
            previous_digest = record.artifact.artifact_digest
            expected += 1
        state_changed = False
        if len(accepted) > int(state.get("last_sequence", 0)):
            state["last_sequence"] = len(accepted)
            state["last_stage"] = accepted[-1].stage if accepted else None
            state["records"] = [record.to_dict() for record in accepted]
            state_changed = True
        if state_changed:
            atomic_write_json(state_path, state)
        return state, accepted

    def progress(
        self, shard_id: str, config_version: str, shard_fingerprint: str
    ) -> list[CheckpointRecord]:
        with exclusive_file_lock(self._lock_path(shard_id, config_version)):
            _, records = self._reconcile_locked(shard_id, config_version)
        for record in records:
            if record.shard_fingerprint != shard_fingerprint:
                raise RopeError(
                    "E_STALE_CHECKPOINT",
                    "检查点的分片内容指纹与当前分片不匹配",
                    {
                        "shard_id": shard_id,
                        "sequence": record.sequence,
                        "expected_fingerprint": shard_fingerprint,
                        "checkpoint_fingerprint": record.shard_fingerprint,
                    },
                )
        return records

    def publish(
        self,
        *,
        shard_id: str,
        shard_fingerprint: str,
        config_version: str,
        stage: str,
        sequence: int,
        artifact: ArtifactRef,
        publication_status: str = "computed",
    ) -> CheckpointRecord:
        if stage not in STAGE_ORDER:
            raise RopeError(
                "E_STALE_CHECKPOINT", "未知阶段不能发布检查点", {"stage": stage}
            )
        with exclusive_file_lock(self._lock_path(shard_id, config_version)):
            state, records = self._reconcile_locked(shard_id, config_version)
            last_sequence = int(state.get("last_sequence", 0))
            expected_sequence = last_sequence + 1
            if sequence != expected_sequence:
                raise RopeError(
                    "E_STALE_CHECKPOINT",
                    "检查点序号落后、重复或跳跃",
                    {
                        "shard_id": shard_id,
                        "stage": stage,
                        "requested_sequence": int(sequence),
                        "expected_sequence": int(expected_sequence),
                        "last_sequence": int(last_sequence),
                    },
                )
            if sequence > len(STAGE_ORDER) or stage != STAGE_ORDER[sequence - 1]:
                raise RopeError(
                    "E_STALE_CHECKPOINT",
                    "检查点跨阶段错位发布",
                    {
                        "shard_id": shard_id,
                        "requested_stage": stage,
                        "expected_stage": (
                            STAGE_ORDER[sequence - 1]
                            if sequence <= len(STAGE_ORDER)
                            else None
                        ),
                        "sequence": int(sequence),
                    },
                )
            previous_digest = (
                records[-1].artifact.artifact_digest if records else None
            )
            record = CheckpointRecord(
                shard_id=shard_id,
                shard_fingerprint=shard_fingerprint,
                config_version=config_version,
                stage=stage,
                sequence=sequence,
                artifact=ArtifactRef.from_dict(artifact.to_dict()),
                publication_status=publication_status,
                previous_artifact_digest=previous_digest,
            )
            record_path = self._record_path(shard_id, config_version, sequence, stage)
            if record_path.exists():
                existing = read_json(record_path)
                if existing != record.to_dict():
                    raise RopeError(
                        "E_STALE_CHECKPOINT",
                        "同一序号已存在不同检查点",
                        {"path": str(record_path), "sequence": int(sequence)},
                    )
            else:
                atomic_write_json(record_path, record.to_dict())
            state = {
                "schema_version": 1,
                "shard_id": shard_id,
                "config_version": config_version,
                "last_sequence": sequence,
                "last_stage": stage,
                "records": [item.to_dict() for item in records] + [record.to_dict()],
            }
            atomic_write_json(self._state_path(shard_id, config_version), state)
            return record
