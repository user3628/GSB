"""Atomic per-stage checkpoints and concurrency-safe publication."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .config import STAGE_NAMES
from .errors import BusinessError, ErrorCode
from .util import (
    atomic_write_bytes,
    atomic_write_json,
    exclusive_file_lock,
    read_json,
    sha256_bytes,
    sha256_file,
)


SHARD_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
PREVIOUS_STAGE = {
    "encode": None,
    "schedule": "encode",
    "sample": "schedule",
    "quantize": "sample",
}


@dataclass(frozen=True)
class CheckpointRecord:
    shard_id: str
    stage: str
    sequence: int
    input_fingerprint: str
    config_version: str
    input_meta: dict[str, Any]
    artifact_sha256: str
    artifact_path: str
    artifact_size: int
    output_sha256: str | None = None

    def as_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "schema_version": 1,
            "shard_id": self.shard_id,
            "stage": self.stage,
            "sequence": self.sequence,
            "input_fingerprint": self.input_fingerprint,
            "config_version": self.config_version,
            "input_meta": self.input_meta,
            "artifact_sha256": self.artifact_sha256,
            "artifact_path": self.artifact_path,
            "artifact_size": self.artifact_size,
        }
        if self.output_sha256 is not None:
            result["output_sha256"] = self.output_sha256
        return result


class CheckpointStore:
    def __init__(self, root: Path):
        self.root = root.resolve()
        self.state_root = self.root / ".state"

    def shard_dir(self, shard_id: str) -> Path:
        self._validate_shard_id(shard_id)
        return self.state_root / "shards" / shard_id

    def checkpoint_path(self, shard_id: str, stage: str) -> Path:
        self._validate_stage(stage)
        return self.shard_dir(shard_id) / "checkpoints" / f"{stage}.json"

    def artifact_path_for(self, shard_id: str, stage: str, artifact_sha256: str) -> Path:
        self._validate_stage(stage)
        return self.shard_dir(shard_id) / "artifacts" / f"{stage}-{artifact_sha256}.bin"

    def output_path(self, shard_id: str) -> Path:
        self._validate_shard_id(shard_id)
        return self.root / "outputs" / f"{shard_id}.png"

    def shard_lock_path(self, shard_id: str) -> Path:
        self._validate_shard_id(shard_id)
        return self.state_root / "locks" / f"shard-{shard_id}.lock"

    def workspace_lock_path(self) -> Path:
        return self.state_root / "locks" / "workspace.lock"

    def read_record(self, shard_id: str, stage: str) -> CheckpointRecord | None:
        path = self.checkpoint_path(shard_id, stage)
        if not path.is_file():
            return None
        try:
            raw = read_json(path)
            record = self._record_from_dict(raw)
            if record.shard_id != shard_id or record.stage != stage:
                return None
            return record
        except (OSError, ValueError, TypeError, KeyError):
            return None

    def read_all_records(self, shard_id: str) -> dict[str, CheckpointRecord]:
        records: dict[str, CheckpointRecord] = {}
        for stage in STAGE_NAMES:
            record = self.read_record(shard_id, stage)
            if record is not None:
                records[stage] = record
        return records

    def next_sequence(self, shard_id: str) -> int:
        records = self.read_all_records(shard_id)
        return max((record.sequence for record in records.values()), default=0) + 1

    def artifact_path(self, record: CheckpointRecord) -> Path:
        candidate = (self.root / record.artifact_path).resolve()
        try:
            candidate.relative_to(self.root)
        except ValueError as exc:
            raise ValueError("artifact path escapes workspace") from exc
        return candidate

    def load_artifact(self, record: CheckpointRecord) -> bytes:
        path = self.artifact_path(record)
        data = path.read_bytes()
        if sha256_bytes(data) != record.artifact_sha256:
            raise ValueError("checkpoint artifact digest mismatch")
        return data

    def verify_record(self, record: CheckpointRecord) -> tuple[bool, str | None]:
        try:
            path = self.artifact_path(record)
        except ValueError as exc:
            return False, str(exc)
        if not path.is_file():
            return False, "artifact file is missing"
        if path.stat().st_size != record.artifact_size:
            return False, "artifact size mismatch"
        if sha256_file(path) != record.artifact_sha256:
            return False, "artifact digest mismatch"
        if record.output_sha256 is not None:
            output = self.output_path(record.shard_id)
            if not output.is_file():
                return False, "quantized PNG is missing"
            if sha256_file(output) != record.output_sha256:
                return False, "quantized PNG digest mismatch"
        return True, None

    def publish(
        self,
        *,
        shard_id: str,
        stage: str,
        expected_sequence: int,
        input_fingerprint: str,
        config_version: str,
        input_meta: dict[str, Any],
        artifact_bytes: bytes,
        output_sha256: str | None = None,
    ) -> CheckpointRecord:
        """Publish one stage under the shard lock.

        The content-addressed artifact is published first. The checkpoint is the
        commit marker, and is atomically replaced only after all sequence and
        predecessor checks pass.
        """

        self._validate_stage(stage)
        artifact_sha = sha256_bytes(artifact_bytes)
        artifact_path = self.artifact_path_for(shard_id, stage, artifact_sha)
        relative_artifact = artifact_path.relative_to(self.root).as_posix()
        with exclusive_file_lock(self.shard_lock_path(shard_id)):
            records = self.read_all_records(shard_id)
            previous_stage = PREVIOUS_STAGE[stage]
            if previous_stage is not None:
                predecessor = records.get(previous_stage)
                if predecessor is None:
                    raise BusinessError(
                        ErrorCode.E_INVALID_STAGE,
                        f"cannot publish {stage}: missing prerequisite stage {previous_stage}",
                        shard=shard_id,
                        field="stage",
                        details={"requested": stage, "missing": previous_stage},
                    )
                valid, reason = self.verify_record(predecessor)
                if not valid:
                    raise BusinessError(
                        ErrorCode.E_INVALID_STAGE,
                        f"cannot publish {stage}: prerequisite {previous_stage} is invalid",
                        shard=shard_id,
                        field="stage",
                        details={"requested": stage, "prerequisite": previous_stage, "reason": reason},
                    )

            current_sequence = max((record.sequence for record in records.values()), default=0)
            next_sequence = current_sequence + 1
            if expected_sequence != next_sequence:
                raise BusinessError(
                    ErrorCode.E_STALE_CHECKPOINT,
                    f"stale checkpoint sequence for {stage}: expected {next_sequence}, got {expected_sequence}",
                    shard=shard_id,
                    field="sequence",
                    details={"expected": next_sequence, "received": expected_sequence},
                )

            record = CheckpointRecord(
                shard_id=shard_id,
                stage=stage,
                sequence=expected_sequence,
                input_fingerprint=input_fingerprint,
                config_version=config_version,
                input_meta=input_meta,
                artifact_sha256=artifact_sha,
                artifact_path=relative_artifact,
                artifact_size=len(artifact_bytes),
                output_sha256=output_sha256,
            )
            atomic_write_bytes(artifact_path, artifact_bytes)
            atomic_write_json(self.checkpoint_path(shard_id, stage), record.as_dict())
            return record

    def _record_from_dict(self, raw: Any) -> CheckpointRecord:
        if not isinstance(raw, dict):
            raise ValueError("checkpoint must be an object")
        shard_id = raw["shard_id"]
        stage = raw["stage"]
        self._validate_shard_id(shard_id)
        self._validate_stage(stage)
        sequence = raw["sequence"]
        if isinstance(sequence, bool) or not isinstance(sequence, int) or sequence < 1:
            raise ValueError("invalid checkpoint sequence")
        input_fingerprint = raw["input_fingerprint"]
        config_version = raw["config_version"]
        input_meta = raw["input_meta"]
        artifact_sha256 = raw["artifact_sha256"]
        artifact_path = raw["artifact_path"]
        artifact_size = raw["artifact_size"]
        if not all(isinstance(item, str) and item for item in (input_fingerprint, config_version, artifact_sha256, artifact_path)):
            raise ValueError("invalid checkpoint string fields")
        if not isinstance(input_meta, dict):
            raise ValueError("invalid checkpoint input_meta")
        if isinstance(artifact_size, bool) or not isinstance(artifact_size, int) or artifact_size < 0:
            raise ValueError("invalid checkpoint artifact_size")
        output_sha = raw.get("output_sha256")
        if output_sha is not None and (not isinstance(output_sha, str) or not output_sha):
            raise ValueError("invalid output digest")
        return CheckpointRecord(
            shard_id=shard_id,
            stage=stage,
            sequence=sequence,
            input_fingerprint=input_fingerprint,
            config_version=config_version,
            input_meta=input_meta,
            artifact_sha256=artifact_sha256,
            artifact_path=artifact_path,
            artifact_size=artifact_size,
            output_sha256=output_sha,
        )

    @staticmethod
    def _validate_stage(stage: str) -> None:
        if stage not in STAGE_NAMES:
            raise BusinessError(
                ErrorCode.E_INVALID_STAGE,
                f"unknown stage: {stage}",
                field="stage",
                details={"stage": stage},
            )

    @staticmethod
    def _validate_shard_id(shard_id: str) -> None:
        if not SHARD_ID_PATTERN.fullmatch(shard_id):
            raise BusinessError(
                ErrorCode.E_INVALID_CONFIG,
                "shard id must match [A-Za-z0-9][A-Za-z0-9._-]{0,63}",
                shard=shard_id,
                field="id",
            )

