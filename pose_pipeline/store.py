"""Immutable artifacts, atomic checkpoints, and cross-process publication locks."""

from __future__ import annotations

import glob
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .constants import (
    SCHEMA_VERSION,
    STAGE_INDEX,
    STAGES,
)
from .errors import invalid_stage, stale_checkpoint
from .util import (
    FileLock,
    atomic_write_bytes,
    atomic_write_json,
    canonical_json_bytes,
    digest_json,
    pretty_json_bytes,
    read_json_file,
    safe_name,
    safe_unlink,
    verify_artifact,
)


@dataclass(frozen=True)
class StoreScan:
    records: tuple[dict[str, Any], ...]
    head: dict[str, Any] | None
    diagnostics: dict[str, Any]


class CheckpointStore:
    def __init__(self, workspace: str | Path) -> None:
        self.workspace = Path(workspace).resolve()
        self.checkpoint_dir = self.workspace / "checkpoints"
        self.artifact_dir = self.workspace / "artifacts"
        self.state_dir = self.workspace / ".state"
        self.inputs_dir = self.workspace / "inputs"
        self.exports_dir = self.workspace / "exports"
        self.lock_path = self.state_dir / "publish.lock"
        self.stop_path = self.state_dir / "stop.request"
        self.attempts_dir = self.state_dir / "attempts"

    def ensure(self) -> None:
        for directory in (
            self.checkpoint_dir,
            self.artifact_dir,
            self.state_dir,
            self.inputs_dir,
            self.exports_dir,
            self.attempts_dir,
        ):
            directory.mkdir(parents=True, exist_ok=True)

    def scan(self) -> StoreScan:
        self.ensure()
        malformed: list[str] = []
        valid_by_sequence: dict[int, list[dict[str, Any]]] = {}
        valid_count = 0
        for path_text in sorted(glob.glob(str(self.checkpoint_dir / "*.json"))):
            path = Path(path_text)
            try:
                record = read_json_file(path)
            except (OSError, ValueError, UnicodeDecodeError):
                malformed.append(path.name)
                continue
            if not _valid_record(record):
                malformed.append(path.name)
                continue
            valid_count += 1
            valid_by_sequence.setdefault(int(record["sequence"]), []).append(record)

        accepted: list[dict[str, Any]] = []
        expected_sequence = 1
        previous_digest = ""
        ignored_duplicates = 0
        while True:
            candidates = valid_by_sequence.get(expected_sequence, [])
            matching = [
                record
                for record in candidates
                if record.get("previous_record_digest", "") == previous_digest
            ]
            if not matching:
                break
            matching.sort(key=lambda record: str(record["record_digest"]))
            chosen = matching[0]
            accepted.append(chosen)
            previous_digest = str(chosen["record_digest"])
            ignored_duplicates += max(0, len(candidates) - 1)
            expected_sequence += 1

        # A valid chain must also satisfy the shard-local stage dependency
        # graph. A syntactically valid record whose upstream digest does not
        # match the latest accepted previous-stage artifact marks the end of
        # the recoverable prefix.
        dependency_validated: list[dict[str, Any]] = []
        latest_artifact: dict[tuple[str, str], str] = {}
        for record in accepted:
            stage = str(record["stage"])
            shard_id = str(record["shard_id"])
            stage_index = STAGE_INDEX[stage]
            if stage_index > 1:
                previous_stage = STAGES[stage_index - 2]
                previous_artifact = latest_artifact.get((shard_id, previous_stage))
                if previous_artifact is None or record.get("upstream_digest") != previous_artifact:
                    break
            latest_artifact[(shard_id, stage)] = str(record["artifact_digest"])
            dependency_validated.append(record)
        accepted = dependency_validated
        expected_sequence = len(accepted) + 1

        accepted_digests = {record["record_digest"] for record in accepted}
        orphan_records = max(0, valid_count - len(accepted) - ignored_duplicates)
        diagnostics = {
            "valid_record_files": valid_count,
            "accepted_records": len(accepted),
            "malformed_or_incomplete_records": len(malformed),
            "duplicate_sequence_records": ignored_duplicates,
            "orphan_or_discontinuous_records": orphan_records,
            "next_sequence": expected_sequence,
            "malformed_files": malformed,
            "accepted_record_digests": sorted(accepted_digests),
        }
        return StoreScan(
            records=tuple(accepted),
            head=accepted[-1] if accepted else None,
            diagnostics=diagnostics,
        )

    def head(self) -> dict[str, Any] | None:
        return self.scan().head

    def next_sequence(self) -> int:
        """Read the next sequence without reserving it.

        A caller should pass this value to publish(); if another process wins the
        race, the loser receives E_STALE_CHECKPOINT instead of silently appending.
        """

        scan = self.scan()
        return int(scan.diagnostics["next_sequence"])

    def latest_by_stage(self) -> dict[tuple[str, str], dict[str, Any]]:
        result: dict[tuple[str, str], dict[str, Any]] = {}
        for record in self.scan().records:
            result[(str(record["shard_id"]), str(record["stage"]))] = record
        return result

    def latest_for_shard(self, shard_id: str) -> dict[str, dict[str, Any]]:
        result: dict[str, dict[str, Any]] = {}
        for record in self.scan().records:
            if record["shard_id"] == shard_id:
                result[str(record["stage"])] = record
        return result

    def publish(
        self,
        *,
        shard_id: str,
        stage: str,
        input_fingerprint: str,
        config_version: str,
        artifact_digest: str,
        upstream_digest: str | None,
        expected_sequence: int | None = None,
        expected_shard_id: str | None = None,
        expected_stage: str | None = None,
        artifact_meta: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        self.ensure()
        try:
            safe_name(shard_id)
        except ValueError as exc:
            raise stale_checkpoint(
                "checkpoint shard_id is not a safe identifier",
                shard_id=shard_id,
                details={"reason": "cross_shard_mismatch"},
            ) from exc
        if stage not in STAGE_INDEX:
            raise invalid_stage("unknown checkpoint stage", shard_id=shard_id)
        if expected_shard_id is not None and expected_shard_id != shard_id:
            raise stale_checkpoint(
                "checkpoint publication was assigned to a different shard",
                shard_id=shard_id,
                details={
                    "reason": "cross_shard_mismatch",
                    "expected_shard_id": expected_shard_id,
                    "actual_shard_id": shard_id,
                },
            )
        if expected_stage is not None and expected_stage != stage:
            raise stale_checkpoint(
                "checkpoint publication was assigned to a different stage",
                shard_id=shard_id,
                details={
                    "reason": "cross_stage_mismatch",
                    "expected_stage": expected_stage,
                    "actual_stage": stage,
                },
            )
        with FileLock(self.lock_path):
            scan = self.scan()
            next_sequence = int(scan.diagnostics["next_sequence"])
            if expected_sequence is not None and int(expected_sequence) != next_sequence:
                raise stale_checkpoint(
                    "checkpoint sequence is stale, duplicated, or has a gap",
                    shard_id=shard_id,
                    details={
                        "reason": "stale_sequence",
                        "expected_sequence": int(expected_sequence),
                        "required_sequence": next_sequence,
                    },
                )
            latest = self.latest_by_stage()
            stage_index = STAGE_INDEX[stage]
            if stage_index > 1:
                previous_stage = STAGES[stage_index - 2]
                previous = latest.get((shard_id, previous_stage))
                if previous is None:
                    raise invalid_stage(
                        f"missing prerequisite stage {previous_stage} checkpoint",
                        shard_id=shard_id,
                        current_stage=None,
                        details={
                            "requested_stage": stage,
                            "required_stage": previous_stage,
                        },
                    )
                if not upstream_digest or previous["artifact_digest"] != upstream_digest:
                    raise invalid_stage(
                        f"upstream artifact does not match committed {previous_stage} checkpoint",
                        shard_id=shard_id,
                        current_stage=previous_stage,
                        details={
                            "requested_stage": stage,
                            "required_stage": previous_stage,
                            "expected_upstream_digest": previous["artifact_digest"],
                            "actual_upstream_digest": upstream_digest,
                        },
                    )
            current_same_stage = latest.get((shard_id, stage))
            if current_same_stage is not None:
                if current_same_stage["input_fingerprint"] == input_fingerprint:
                    raise stale_checkpoint(
                        "checkpoint publication duplicates the current stage record",
                        shard_id=shard_id,
                        details={
                            "reason": "duplicate_stage_record",
                            "stage": stage,
                            "sequence": next_sequence,
                            "recorded_artifact_digest": current_same_stage["artifact_digest"],
                            "offered_artifact_digest": artifact_digest,
                        },
                    )
            record: dict[str, Any] = {
                "schema_version": SCHEMA_VERSION,
                "sequence": next_sequence,
                "shard_sequence": stage_index,
                "shard_id": shard_id,
                "stage": stage,
                "input_fingerprint": input_fingerprint,
                "config_version": config_version,
                "artifact_digest": artifact_digest,
                "upstream_digest": upstream_digest,
                "previous_record_digest": (
                    scan.head["record_digest"] if scan.head is not None else ""
                ),
            }
            if artifact_meta:
                record["artifact_meta"] = artifact_meta
            record["record_digest"] = digest_json(record)
            filename = f"{next_sequence:012d}-{record['record_digest'][:16]}.json"
            destination = self.checkpoint_dir / filename
            atomic_write_bytes(destination, pretty_json_bytes(record))
            return record

    def artifact_path(self, shard_id: str, stage: str, artifact_digest: str) -> Path:
        safe_name(shard_id)
        if stage not in STAGE_INDEX:
            raise ValueError(f"unknown stage: {stage}")
        if not artifact_digest or any(
            character not in "0123456789abcdef" for character in artifact_digest
        ):
            raise ValueError("invalid artifact digest")
        return self.artifact_dir / shard_id / stage / f"{artifact_digest}.json"

    def write_artifact(self, artifact: dict[str, Any]) -> Path:
        if not verify_artifact(artifact):
            raise ValueError("artifact digest verification failed before write")
        shard_id = safe_name(str(artifact["shard_id"]))
        stage = str(artifact["stage"])
        digest = str(artifact["artifact_digest"])
        destination = self.artifact_path(shard_id, stage, digest)
        data = canonical_json_bytes(artifact)
        if destination.exists():
            try:
                existing = destination.read_bytes()
            except OSError:
                existing = b""
            if existing == data:
                return destination
        atomic_write_bytes(destination, data)
        return destination

    def load_artifact(self, record: dict[str, Any]) -> dict[str, Any] | None:
        try:
            path = self.artifact_path(
                str(record["shard_id"]), str(record["stage"]), str(record["artifact_digest"])
            )
            artifact = read_json_file(path)
        except (OSError, ValueError, KeyError, UnicodeDecodeError):
            return None
        if not isinstance(artifact, dict) or not verify_artifact(artifact):
            return None
        if artifact.get("shard_id") != record.get("shard_id"):
            return None
        if artifact.get("stage") != record.get("stage"):
            return None
        if artifact.get("artifact_digest") != record.get("artifact_digest"):
            return None
        return artifact

    def read_artifact_by_digest(
        self, shard_id: str, stage: str, artifact_digest: str
    ) -> dict[str, Any] | None:
        try:
            artifact = read_json_file(self.artifact_path(shard_id, stage, artifact_digest))
        except (OSError, ValueError, UnicodeDecodeError):
            return None
        if not isinstance(artifact, dict) or not verify_artifact(artifact):
            return None
        return artifact

    def record_attempt(
        self,
        *,
        shard_id: str,
        stage: str,
        artifact_digest: str,
        status: str,
        sequence_expected: int | None,
        error: dict[str, Any] | None = None,
    ) -> Path:
        self.ensure()
        payload = {
            "schema_version": SCHEMA_VERSION,
            "shard_id": shard_id,
            "stage": stage,
            "artifact_digest": artifact_digest,
            "status": status,
            "sequence_expected": sequence_expected,
            "error": error,
        }
        destination = (
            self.attempts_dir
            / f"{shard_id}-{stage}-{artifact_digest[:16]}-{status}.json"
        )
        atomic_write_json(destination, payload)
        return destination

    def list_uncommitted_artifacts(self) -> list[dict[str, Any]]:
        committed: set[tuple[str, str, str]] = set()
        for record in self.scan().records:
            committed.add(
                (
                    str(record["shard_id"]),
                    str(record["stage"]),
                    str(record["artifact_digest"]),
                )
            )
        results: list[dict[str, Any]] = []
        for path_text in sorted(glob.glob(str(self.artifact_dir / "*" / "*" / "*.json"))):
            path = Path(path_text)
            stage = path.parent.name
            shard_id = path.parent.parent.name
            digest = path.stem
            if (shard_id, stage, digest) in committed:
                continue
            results.append(
                {
                    "shard_id": shard_id,
                    "stage": stage,
                    "artifact_digest": digest,
                    "path": str(path),
                }
            )
        return results

    def request_stop(self) -> None:
        self.ensure()
        atomic_write_bytes(self.stop_path, b"stop\n")

    def clear_stop(self) -> None:
        safe_unlink(self.stop_path)

    def stop_requested(self) -> bool:
        return self.stop_path.exists()


def _valid_record(record: Any) -> bool:
    if not isinstance(record, dict):
        return False
    required = {
        "schema_version",
        "sequence",
        "shard_sequence",
        "shard_id",
        "stage",
        "input_fingerprint",
        "config_version",
        "artifact_digest",
        "upstream_digest",
        "previous_record_digest",
        "record_digest",
    }
    if not required.issubset(record):
        return False
    if record.get("schema_version") != SCHEMA_VERSION:
        return False
    try:
        sequence = int(record["sequence"])
        shard_sequence = int(record["shard_sequence"])
    except (TypeError, ValueError):
        return False
    if sequence < 1 or shard_sequence < 1:
        return False
    stage = record.get("stage")
    if stage not in STAGE_INDEX or shard_sequence != STAGE_INDEX[stage]:
        return False
    try:
        safe_name(str(record["shard_id"]))
    except ValueError:
        return False
    for field in ("input_fingerprint", "config_version", "artifact_digest", "record_digest"):
        value = record.get(field)
        if not isinstance(value, str) or len(value) != 64 or any(
            character not in "0123456789abcdef" for character in value
        ):
            return False
    upstream = record.get("upstream_digest")
    if upstream is not None and (
        not isinstance(upstream, str)
        or len(upstream) != 64
        or any(character not in "0123456789abcdef" for character in upstream)
    ):
        return False
    previous = record.get("previous_record_digest")
    if not isinstance(previous, str):
        return False
    if previous and (
        len(previous) != 64
        or any(character not in "0123456789abcdef" for character in previous)
    ):
        return False
    claimed = record["record_digest"]
    value = dict(record)
    value.pop("record_digest", None)
    return digest_json(value) == claimed
