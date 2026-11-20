"""Per-request checkpoint ledger with locking and contiguous sequences."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable

from .canonical import digest_file, digest_json
from .errors import invalid_stage, snapshot_conflict, stale_checkpoint
from .models import STAGE_INDEX, STAGES
from .storage import FileLock, atomic_write_json, move_atomically, read_json


ARTIFACT_SUFFIX = {
    "postprocess": ".npy",
    "normalize": ".npz",
    "decode": ".json",
    "summarize": ".json",
}


def _zero_padded(sequence: int) -> str:
    return f"{sequence:010d}"


class RequestStore:
    """Filesystem store for one request.

    A checkpoint is published only after its artifact has been moved into
    place. The checkpoint JSON itself is written with same-directory atomic
    replace, so a crash can leave an orphan artifact but never a half-written
    checkpoint record.
    """

    def __init__(
        self,
        workspace: str | Path,
        request_id: str,
        lock_timeout: float = 30.0,
    ) -> None:
        self.workspace = Path(workspace).resolve()
        self.request_id = request_id
        self.root = self.workspace / "requests" / request_id
        self.checkpoints_dir = self.root / "checkpoints"
        self.artifacts_dir = self.root / "artifacts"
        self.history_checkpoints = self.root / "history" / "checkpoints"
        self.history_artifacts = self.root / "history" / "artifacts"
        self.staging_dir = self.root / "staging"
        self.lock_path = self.root / "request.lock"
        self.state_path = self.root / "state.json"
        self.lock_timeout = lock_timeout
        for folder in (
            self.root,
            self.checkpoints_dir,
            self.artifacts_dir,
            self.history_checkpoints,
            self.history_artifacts,
            self.staging_dir,
        ):
            folder.mkdir(parents=True, exist_ok=True)

    def lock(self) -> FileLock:
        return FileLock(self.lock_path, timeout=self.lock_timeout)

    def artifact_path(self, stage: str) -> Path:
        return self.artifacts_dir / f"{stage}{ARTIFACT_SUFFIX[stage]}"

    def _initial_state(self) -> dict[str, Any]:
        return {
            "state_schema": 1,
            "request_id": self.request_id,
            "fields": {},
            "stage_fingerprints": {},
            "version_fingerprint": None,
            "config_version": None,
            "invalidations": [],
            "pending_invalidation": None,
        }

    def load_state(self) -> dict[str, Any]:
        if not self.state_path.exists():
            return self._initial_state()
        try:
            state = read_json(self.state_path)
        except (OSError, json.JSONDecodeError) as exc:
            raise snapshot_conflict(
                "request state is unreadable",
                request_id=self.request_id,
                reason="state_digest_mismatch",
                detail=str(exc),
            )
        if not isinstance(state, dict):
            raise snapshot_conflict(
                "request state is not an object",
                request_id=self.request_id,
                reason="state_digest_mismatch",
            )
        if state.get("request_id") not in (None, self.request_id):
            raise stale_checkpoint(
                "request state belongs to another request",
                request_id=self.request_id,
                state_request_id=state.get("request_id"),
            )
        initial = self._initial_state()
        initial.update(state)
        initial["request_id"] = self.request_id
        return initial

    def _write_state_locked(self, state: dict[str, Any]) -> None:
        atomic_write_json(self.state_path, state)

    def write_state(self, state: dict[str, Any]) -> None:
        with self.lock():
            self._write_state_locked(state)

    def _checkpoint_files(self, folder: Path) -> list[Path]:
        if not folder.exists():
            return []
        return sorted(folder.glob("*.json"))

    def _sequence_from_name(self, path: Path) -> int | None:
        try:
            return int(path.stem)
        except ValueError:
            return None

    def _max_sequence_locked(self) -> int:
        sequences: list[int] = []
        for folder in (self.checkpoints_dir, self.history_checkpoints):
            for path in self._checkpoint_files(folder):
                sequence = self._sequence_from_name(path)
                if sequence is not None:
                    sequences.append(sequence)
        return max(sequences, default=0)

    def next_sequence(self) -> int:
        with self.lock():
            return self._max_sequence_locked() + 1

    def _validate_record(self, record: dict[str, Any]) -> None:
        required = {
            "request_id",
            "stage",
            "sequence",
            "input_fingerprint",
            "config_version",
            "artifact_digest",
            "artifact_file_digest",
            "algorithm_version",
        }
        missing = sorted(required - set(record))
        if missing:
            raise snapshot_conflict(
                "checkpoint record is missing required fields",
                request_id=self.request_id,
                reason="checkpoint_schema_mismatch",
                missing=missing,
            )
        if record["request_id"] != self.request_id:
            raise stale_checkpoint(
                "checkpoint belongs to another request",
                request_id=self.request_id,
                record_request_id=record["request_id"],
            )
        if record["stage"] not in STAGE_INDEX:
            raise invalid_stage(
                "checkpoint has an unknown stage",
                request_id=self.request_id,
                stage=record["stage"],
            )
        sequence = record["sequence"]
        if isinstance(sequence, bool) or not isinstance(sequence, int) or sequence <= 0:
            raise snapshot_conflict(
                "checkpoint sequence must be a positive integer",
                request_id=self.request_id,
                reason="checkpoint_sequence_invalid",
                sequence=sequence,
            )
        for field in (
            "input_fingerprint",
            "config_version",
            "artifact_digest",
            "artifact_file_digest",
            "algorithm_version",
        ):
            if not isinstance(record[field], str) or not record[field]:
                raise snapshot_conflict(
                    "checkpoint digest field must be a non-empty string",
                    request_id=self.request_id,
                    reason="checkpoint_schema_mismatch",
                    field=field,
                )

    def load_active_checkpoints(self) -> dict[str, dict[str, Any]]:
        """Load and integrity-check current checkpoints by stage."""

        by_stage: dict[str, dict[str, Any]] = {}
        sequences: set[int] = set()
        for path in self._checkpoint_files(self.checkpoints_dir):
            try:
                record = read_json(path)
            except (OSError, json.JSONDecodeError) as exc:
                raise snapshot_conflict(
                    "checkpoint record is unreadable",
                    request_id=self.request_id,
                    reason="digest_mismatch",
                    path=str(path),
                    detail=str(exc),
                )
            if not isinstance(record, dict):
                raise snapshot_conflict(
                    "checkpoint record is not an object",
                    request_id=self.request_id,
                    reason="checkpoint_schema_mismatch",
                    path=str(path),
                )
            self._validate_record(record)
            filename_sequence = self._sequence_from_name(path)
            if filename_sequence != record["sequence"]:
                raise snapshot_conflict(
                    "checkpoint filename and sequence disagree",
                    request_id=self.request_id,
                    reason="digest_mismatch",
                    path=str(path),
                    filename_sequence=filename_sequence,
                    record_sequence=record["sequence"],
                )
            sequence = int(record["sequence"])
            if sequence in sequences:
                raise snapshot_conflict(
                    "duplicate checkpoint sequence",
                    request_id=self.request_id,
                    reason="digest_mismatch",
                    sequence=sequence,
                )
            sequences.add(sequence)
            stage = str(record["stage"])
            if stage in by_stage:
                raise snapshot_conflict(
                    "multiple active checkpoints for one stage",
                    request_id=self.request_id,
                    reason="digest_mismatch",
                    stage=stage,
                )
            artifact = self.artifact_path(stage)
            if not artifact.exists():
                raise snapshot_conflict(
                    "checkpoint artifact is missing",
                    request_id=self.request_id,
                    reason="digest_mismatch",
                    stage=stage,
                    path=str(artifact),
                )
            actual_digest = digest_file(artifact)
            if actual_digest != record["artifact_file_digest"]:
                raise snapshot_conflict(
                    "checkpoint artifact digest does not match",
                    request_id=self.request_id,
                    reason="digest_mismatch",
                    stage=stage,
                    expected=record["artifact_file_digest"],
                    actual=actual_digest,
                )
            by_stage[stage] = record
        return by_stage

    def checkpoint_for(self, stage: str) -> dict[str, Any] | None:
        if stage not in STAGE_INDEX:
            raise invalid_stage(
                "unknown stage", request_id=self.request_id, stage=stage
            )
        return self.load_active_checkpoints().get(stage)

    def publish_checkpoint(
        self,
        record: dict[str, Any],
        staging_artifact: str | Path,
    ) -> dict[str, Any]:
        """Publish one stage under lock and enforce contiguous sequences."""

        staging = Path(staging_artifact)
        required = {
            "request_id",
            "stage",
            "sequence",
            "input_fingerprint",
            "config_version",
            "artifact_digest",
            "algorithm_version",
        }
        missing = sorted(required - set(record))
        if missing:
            raise stale_checkpoint(
                "publication request is missing fields",
                request_id=self.request_id,
                missing=missing,
            )
        if record.get("request_id") != self.request_id:
            raise stale_checkpoint(
                "publication was addressed to a different request",
                request_id=self.request_id,
                record_request_id=record.get("request_id"),
            )
        stage = record.get("stage")
        if stage not in STAGE_INDEX:
            raise invalid_stage(
                "unknown stage", request_id=self.request_id, stage=stage
            )
        expected_sequence = record.get("sequence")
        if (
            isinstance(expected_sequence, bool)
            or not isinstance(expected_sequence, int)
            or expected_sequence <= 0
        ):
            raise stale_checkpoint(
                "publication sequence must be a positive integer",
                request_id=self.request_id,
                expected_sequence=expected_sequence,
            )
        with self.lock():
            state = self.load_state()
            if state.get("pending_invalidation"):
                self._finish_pending_invalidation_locked(state)
            by_stage = self.load_active_checkpoints()
            if stage in by_stage:
                raise stale_checkpoint(
                    "stage was already published",
                    request_id=self.request_id,
                    stage=stage,
                    existing_sequence=by_stage[stage]["sequence"],
                    attempted_sequence=expected_sequence,
                )
            stage_index = STAGE_INDEX[stage]
            if stage_index > 0:
                previous = STAGES[stage_index - 1]
                if previous not in by_stage:
                    raise invalid_stage(
                        "previous stage checkpoint is missing",
                        request_id=self.request_id,
                        stage=stage,
                        missing_stage=previous,
                    )
            expected_next = self._max_sequence_locked() + 1
            if expected_sequence != expected_next:
                raise stale_checkpoint(
                    "checkpoint sequence is not the next contiguous sequence",
                    request_id=self.request_id,
                    stage=stage,
                    expected_sequence=expected_sequence,
                    next_contiguous_sequence=expected_next,
                )
            if not staging.exists():
                raise stale_checkpoint(
                    "staged artifact does not exist",
                    request_id=self.request_id,
                    stage=stage,
                    path=str(staging),
                )
            artifact_file_digest = digest_file(staging)
            supplied_file_digest = record.get("artifact_file_digest")
            if supplied_file_digest not in (None, artifact_file_digest):
                raise snapshot_conflict(
                    "staged artifact digest does not match the publication",
                    request_id=self.request_id,
                    reason="digest_mismatch",
                    stage=stage,
                    expected=supplied_file_digest,
                    actual=artifact_file_digest,
                )
            final_artifact = self.artifact_path(stage)
            move_atomically(staging, final_artifact)
            checkpoint = {
                "checkpoint_schema": 1,
                "request_id": self.request_id,
                "stage": stage,
                "sequence": expected_sequence,
                "input_fingerprint": record["input_fingerprint"],
                "config_version": record["config_version"],
                "artifact_digest": record["artifact_digest"],
                "artifact_file_digest": artifact_file_digest,
                "artifact_path": final_artifact.relative_to(self.root).as_posix(),
                "algorithm_version": record["algorithm_version"],
                "stage_metadata": record.get("stage_metadata", {}),
            }
            self._validate_record(checkpoint)
            checkpoint_path = self.checkpoints_dir / f"{_zero_padded(expected_sequence)}.json"
            atomic_write_json(checkpoint_path, checkpoint)
            state["last_sequence"] = expected_sequence
            state["last_stage"] = stage
            self._write_state_locked(state)
            return checkpoint

    def _archive_file(self, source: Path, target: Path) -> None:
        if not source.exists():
            return
        if target.exists():
            source_sha = digest_file(source)
            target_sha = digest_file(target)
            if source_sha == target_sha:
                source.unlink()
                return
            raise snapshot_conflict(
                "archived checkpoint collision",
                request_id=self.request_id,
                reason="digest_mismatch",
                source=str(source),
                target=str(target),
            )
        move_atomically(source, target)

    def invalidate_from(
        self,
        earliest_stage: str,
        *,
        changed_fields: Iterable[str],
        reasons: Iterable[str],
        old_input_fingerprints: dict[str, Any],
        new_input_fingerprints: dict[str, Any],
        fields: dict[str, Any],
        stage_fingerprints: dict[str, Any],
        version_fingerprint: str,
        config_version: str,
    ) -> dict[str, Any]:
        if earliest_stage not in STAGE_INDEX:
            raise invalid_stage(
                "unknown invalidation stage",
                request_id=self.request_id,
                stage=earliest_stage,
            )
        invalidated_stages = list(STAGES[STAGE_INDEX[earliest_stage] :])
        with self.lock():
            state = self.load_state()
            if state.get("pending_invalidation"):
                self._finish_pending_invalidation_locked(state)
            active = self.load_active_checkpoints()
            affected_sequences = [
                active[stage]["sequence"]
                for stage in invalidated_stages
                if stage in active
            ]
            entry = {
                "earliest_stage": earliest_stage,
                "invalidated_stages": invalidated_stages,
                "reason": list(reasons),
                "changed_fields": sorted(set(changed_fields)),
                "old_input_fingerprints": old_input_fingerprints,
                "new_input_fingerprints": new_input_fingerprints,
                "archived_sequences": sorted(affected_sequences),
            }
            state["pending_invalidation"] = {
                "entry": entry,
                "fields": fields,
                "stage_fingerprints": stage_fingerprints,
                "version_fingerprint": version_fingerprint,
                "config_version": config_version,
            }
            self._write_state_locked(state)
            self._finish_pending_invalidation_locked(state)
            return entry

    def _finish_pending_invalidation_locked(self, state: dict[str, Any]) -> None:
        pending = state.get("pending_invalidation")
        if not pending:
            return
        entry = pending["entry"]
        for stage in entry["invalidated_stages"]:
            checkpoint = self.checkpoint_for(stage)
            if checkpoint is None:
                continue
            source_cp = (
                self.checkpoints_dir / f"{_zero_padded(checkpoint['sequence'])}.json"
            )
            target_cp = (
                self.history_checkpoints
                / f"{_zero_padded(checkpoint['sequence'])}.json"
            )
            source_artifact = self.artifact_path(stage)
            target_artifact = (
                self.history_artifacts
                / f"{_zero_padded(checkpoint['sequence'])}__{stage}{ARTIFACT_SUFFIX[stage]}"
            )
            self._archive_file(source_cp, target_cp)
            self._archive_file(source_artifact, target_artifact)
        invalidations = state.setdefault("invalidations", [])
        marker = digest_json(entry)
        if not any(digest_json(item) == marker for item in invalidations):
            invalidations.append(entry)
        state["fields"] = pending["fields"]
        state["stage_fingerprints"] = pending["stage_fingerprints"]
        state["version_fingerprint"] = pending["version_fingerprint"]
        state["config_version"] = pending["config_version"]
        state["pending_invalidation"] = None
        self._write_state_locked(state)

    def recover_pending_invalidation(self) -> None:
        with self.lock():
            state = self.load_state()
            if state.get("pending_invalidation"):
                self._finish_pending_invalidation_locked(state)

    def update_expected_state(
        self,
        *,
        fields: dict[str, Any],
        stage_fingerprints: dict[str, Any],
        version_fingerprint: str,
        config_version: str,
    ) -> None:
        with self.lock():
            state = self.load_state()
            state["fields"] = fields
            state["stage_fingerprints"] = stage_fingerprints
            state["version_fingerprint"] = version_fingerprint
            state["config_version"] = config_version
            self._write_state_locked(state)

    def history_summary(self) -> dict[str, Any]:
        active = self.load_active_checkpoints()
        history_sequences: list[int] = []
        for path in self._checkpoint_files(self.history_checkpoints):
            sequence = self._sequence_from_name(path)
            if sequence is not None:
                history_sequences.append(sequence)
        all_sequences = sorted(
            [record["sequence"] for record in active.values()] + history_sequences
        )
        return {
            "active_sequences": sorted(record["sequence"] for record in active.values()),
            "history_sequences": sorted(history_sequences),
            "all_sequences": all_sequences,
            "contiguous": all_sequences == list(range(1, len(all_sequences) + 1)),
        }
