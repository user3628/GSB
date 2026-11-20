from __future__ import annotations

import contextlib
import copy
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

from .errors import BusinessError, ErrorCode
from .util import (
    ZERO_DIGEST,
    atomic_write_json,
    canonical_json_bytes,
    digest_bytes,
    digest_file,
    file_lock,
)


STAGES = ("recall", "fusion", "summary")
STAGE_INDEX = {stage: index for index, stage in enumerate(STAGES)}
HEX_DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")
HISTORY_RE = re.compile(r"^([0-9]{8})\.json$")


@dataclass(frozen=True)
class CheckpointState:
    workspace_id: str
    scale: str
    sequence: int
    records: dict[str, dict[str, Any]]
    state_digest: str
    previous_state_digest: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "format_version": 1,
            "workspace_id": self.workspace_id,
            "scale": self.scale,
            "sequence": self.sequence,
            "records": self.records,
            "previous_state_digest": self.previous_state_digest,
            "state_digest": self.state_digest,
        }

    def record(self, stage: str) -> dict[str, Any] | None:
        return self.records.get(stage)


def _record_core(record: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in record.items() if key != "record_digest"}


def _record_digest(record: dict[str, Any]) -> str:
    return digest_bytes(canonical_json_bytes(_record_core(record)))


def _state_core(state: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in state.items() if key != "state_digest"}


def _state_digest(state: dict[str, Any]) -> str:
    return digest_bytes(canonical_json_bytes(_state_core(state)))


def _empty_state(workspace_id: str, scale: str) -> CheckpointState:
    core = {
        "format_version": 1,
        "workspace_id": workspace_id,
        "scale": scale,
        "sequence": 0,
        "records": {},
        "previous_state_digest": ZERO_DIGEST,
    }
    return CheckpointState(
        workspace_id=workspace_id,
        scale=scale,
        sequence=0,
        records={},
        state_digest=digest_bytes(canonical_json_bytes(core)),
        previous_state_digest=ZERO_DIGEST,
    )


def _is_hex_digest(value: Any) -> bool:
    return isinstance(value, str) and bool(HEX_DIGEST_RE.match(value))


class CheckpointStore:
    def __init__(self, workspace_root: Path, workspace_id: str):
        self.workspace_root = workspace_root
        self.workspace_id = workspace_id
        self.checkpoint_root = workspace_root / "checkpoints"

    @property
    def artifacts_root(self) -> Path:
        return self.workspace_root / "artifacts"

    def scale_dir(self, scale: str) -> Path:
        return self.checkpoint_root / scale

    def state_path(self, scale: str) -> Path:
        return self.scale_dir(scale) / "state.json"

    def history_dir(self, scale: str) -> Path:
        return self.scale_dir(scale) / "history"

    def pending_path(self, scale: str) -> Path:
        return self.scale_dir(scale) / "publish.pending.json"

    def lock_path(self, scale: str) -> Path:
        return self.scale_dir(scale) / "lock"

    def artifact_rel_path(self, scale: str, stage: str, digest: str) -> str:
        return f"artifacts/{scale}/{stage}/{digest}.json"

    def artifact_path(self, scale: str, stage: str, digest: str) -> Path:
        return self.workspace_root / self.artifact_rel_path(scale, stage, digest)

    def lock(self, scale: str, *, exclusive: bool = True, blocking: bool = True) -> Iterator[None]:
        return file_lock(self.lock_path(scale), exclusive=exclusive, blocking=blocking)

    def _checkpoint_decode_error(self, path: Path, *, reason: str, detail: str | None = None) -> BusinessError:
        return BusinessError(
            ErrorCode.STALE_CHECKPOINT,
            "checkpoint cannot be validated",
            {"path": str(path), "reason": reason, "detail": detail},
        )

    def _read_checkpoint_json(self, path: Path) -> dict[str, Any]:
        try:
            raw = path.read_bytes()
        except FileNotFoundError:
            raise self._checkpoint_decode_error(path, reason="MISSING_FILE") from None
        try:
            value = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise self._checkpoint_decode_error(path, reason="JSON_DECODE_ERROR", detail=str(exc)) from exc
        if not isinstance(value, dict):
            raise self._checkpoint_decode_error(path, reason="NOT_AN_OBJECT")
        return value

    def _validate_record(self, record: Any, *, expected_scale: str) -> dict[str, Any]:
        if not isinstance(record, dict):
            raise self._checkpoint_decode_error(self.state_path(expected_scale), reason="RECORD_NOT_OBJECT")
        stage = record.get("stage")
        if stage not in STAGES:
            raise self._checkpoint_decode_error(
                self.state_path(expected_scale),
                reason="UNKNOWN_STAGE",
                detail=str(stage),
            )
        if record.get("workspace_id") != self.workspace_id:
            raise self._checkpoint_decode_error(
                self.state_path(expected_scale),
                reason="WORKSPACE_REPLACEMENT",
                detail=stage,
            )
        if record.get("scale") != expected_scale:
            raise self._checkpoint_decode_error(
                self.state_path(expected_scale),
                reason="SCALE_MISMATCH",
                detail=f"record={record.get('scale')} expected={expected_scale}",
            )
        sequence = record.get("sequence")
        if isinstance(sequence, bool) or not isinstance(sequence, int) or sequence <= 0:
            raise self._checkpoint_decode_error(
                self.state_path(expected_scale),
                reason="INVALID_SEQUENCE",
                detail=stage,
            )
        if not _is_hex_digest(record.get("record_digest")):
            raise self._checkpoint_decode_error(
                self.state_path(expected_scale),
                reason="MISSING_RECORD_DIGEST",
                detail=stage,
            )
        if _record_digest(record) != record["record_digest"]:
            raise self._checkpoint_decode_error(
                self.state_path(expected_scale),
                reason="RECORD_DIGEST_MISMATCH",
                detail=stage,
            )
        for key in ("input_fingerprint", "config_version", "artifact_digest"):
            if not _is_hex_digest(record.get(key)):
                raise self._checkpoint_decode_error(
                    self.state_path(expected_scale),
                    reason=f"INVALID_{key.upper()}",
                    detail=stage,
                )
        expected_path = self.artifact_rel_path(expected_scale, stage, record["artifact_digest"])
        if record.get("artifact_path") != expected_path:
            raise self._checkpoint_decode_error(
                self.state_path(expected_scale),
                reason="ARTIFACT_PATH_REPLACEMENT",
                detail=stage,
            )
        return record

    def _validate_state_dict(self, raw: dict[str, Any], *, expected_scale: str) -> CheckpointState:
        if raw.get("format_version") != 1:
            raise self._checkpoint_decode_error(self.state_path(expected_scale), reason="FORMAT_VERSION")
        if raw.get("workspace_id") != self.workspace_id:
            raise self._checkpoint_decode_error(
                self.state_path(expected_scale),
                reason="WORKSPACE_REPLACEMENT",
            )
        if raw.get("scale") != expected_scale:
            raise self._checkpoint_decode_error(
                self.state_path(expected_scale),
                reason="SCALE_MISMATCH",
                detail=f"state={raw.get('scale')} expected={expected_scale}",
            )
        sequence = raw.get("sequence")
        if isinstance(sequence, bool) or not isinstance(sequence, int) or sequence < 0:
            raise self._checkpoint_decode_error(self.state_path(expected_scale), reason="INVALID_SEQUENCE")
        if not _is_hex_digest(raw.get("state_digest")) or not _is_hex_digest(raw.get("previous_state_digest")):
            raise self._checkpoint_decode_error(self.state_path(expected_scale), reason="INVALID_STATE_DIGEST")
        if _state_digest(raw) != raw["state_digest"]:
            raise self._checkpoint_decode_error(self.state_path(expected_scale), reason="STATE_DIGEST_MISMATCH")
        records_raw = raw.get("records")
        if not isinstance(records_raw, dict):
            raise self._checkpoint_decode_error(self.state_path(expected_scale), reason="RECORDS_NOT_OBJECT")
        records: dict[str, dict[str, Any]] = {}
        record_sequences: list[int] = []
        for stage, record in records_raw.items():
            if stage not in STAGES:
                raise self._checkpoint_decode_error(self.state_path(expected_scale), reason="UNKNOWN_STAGE", detail=stage)
            checked = self._validate_record(record, expected_scale=expected_scale)
            if checked["sequence"] > sequence:
                raise self._checkpoint_decode_error(
                    self.state_path(expected_scale),
                    reason="RECORD_SEQUENCE_AHEAD_OF_STATE",
                    detail=stage,
                )
            records[stage] = checked
            record_sequences.append(checked["sequence"])
        if len(record_sequences) != len(set(record_sequences)):
            raise self._checkpoint_decode_error(self.state_path(expected_scale), reason="DUPLICATE_RECORD_SEQUENCE")
        return CheckpointState(
            workspace_id=raw["workspace_id"],
            scale=raw["scale"],
            sequence=sequence,
            records=records,
            state_digest=raw["state_digest"],
            previous_state_digest=raw["previous_state_digest"],
        )

    def _history_files(self, scale: str) -> list[tuple[int, Path]]:
        directory = self.history_dir(scale)
        if not directory.exists():
            return []
        entries: list[tuple[int, Path]] = []
        for path in directory.iterdir():
            match = HISTORY_RE.match(path.name)
            if match:
                entries.append((int(match.group(1)), path))
        entries.sort()
        return entries

    def _validate_history(self, scale: str) -> tuple[CheckpointState | None, Path | None]:
        entries = self._history_files(scale)
        if not entries:
            return None, None
        expected_sequence = 1
        previous_digest = ZERO_DIGEST
        latest: CheckpointState | None = None
        latest_path: Path | None = None
        for sequence, path in entries:
            if sequence != expected_sequence:
                raise self._checkpoint_decode_error(path, reason="HISTORY_SEQUENCE_GAP")
            raw = self._read_checkpoint_json(path)
            state = self._validate_state_dict(raw, expected_scale=scale)
            if state.sequence != sequence:
                raise self._checkpoint_decode_error(path, reason="HISTORY_FILE_SEQUENCE_MISMATCH")
            if state.previous_state_digest != previous_digest:
                raise self._checkpoint_decode_error(path, reason="HISTORY_CHAIN_MISMATCH")
            previous_digest = state.state_digest
            latest = state
            latest_path = path
            expected_sequence += 1
        return latest, latest_path

    def _pending(self, scale: str) -> dict[str, Any] | None:
        path = self.pending_path(scale)
        if not path.exists():
            return None
        raw = self._read_checkpoint_json(path)
        required = ("scale", "sequence", "previous_state_digest", "state_digest")
        if any(key not in raw for key in required):
            raise self._checkpoint_decode_error(path, reason="INVALID_PENDING_MARKER")
        if raw["scale"] != scale:
            raise self._checkpoint_decode_error(path, reason="PENDING_SCALE_MISMATCH")
        return raw

    def _load_state_unlocked(self, scale: str, *, validate_pending: bool = True) -> CheckpointState:
        latest, latest_path = self._validate_history(scale)
        state_path = self.state_path(scale)
        current: CheckpointState | None = None
        if state_path.exists():
            current_raw = self._read_checkpoint_json(state_path)
            current = self._validate_state_dict(current_raw, expected_scale=scale)

        if latest is None:
            if current is not None and current.sequence > 0:
                raise self._checkpoint_decode_error(state_path, reason="HISTORY_MISSING")
            return current or _empty_state(self.workspace_id, scale)

        assert latest_path is not None
        if current is None:
            pending = self._pending(scale) if validate_pending else None
            if (
                pending
                and pending["sequence"] == latest.sequence
                and pending["state_digest"] == latest.state_digest
                and pending["previous_state_digest"] == ZERO_DIGEST
            ):
                return latest
            raise self._checkpoint_decode_error(state_path, reason="STATE_MISSING")

        if current.sequence < latest.sequence:
            pending = self._pending(scale) if validate_pending else None
            if (
                pending
                and pending["sequence"] == latest.sequence
                and pending["state_digest"] == latest.state_digest
                and pending["previous_state_digest"] == current.state_digest
            ):
                return latest
            raise self._checkpoint_decode_error(
                state_path,
                reason="SEQUENCE_ROLLBACK",
                detail=f"state={current.sequence} history={latest.sequence}",
            )
        if current.sequence > latest.sequence:
            raise self._checkpoint_decode_error(
                state_path,
                reason="STATE_AHEAD_OF_HISTORY",
                detail=f"state={current.sequence} history={latest.sequence}",
            )
        if current.state_digest != latest.state_digest:
            raise self._checkpoint_decode_error(
                state_path,
                reason="STATE_REPLACEMENT",
                detail=f"state={current.state_digest} history={latest.state_digest}",
            )
        return current

    def load_state(self, scale: str, *, exclusive: bool = False) -> CheckpointState:
        with self.lock(scale, exclusive=exclusive):
            return self._load_state_unlocked(scale)

    def validate_checkpoint_file(self, path: str | Path, *, expected_scale: str) -> CheckpointState:
        checkpoint_path = Path(path)
        raw = self._read_checkpoint_json(checkpoint_path)
        return self._validate_state_dict(raw, expected_scale=expected_scale)

    def _validate_upstream(
        self,
        state: CheckpointState,
        *,
        stage: str,
        upstream_fingerprints: dict[str, str],
    ) -> None:
        stage_index = STAGE_INDEX[stage]
        for upstream in STAGES[:stage_index]:
            record = state.records.get(upstream)
            if record is None:
                raise BusinessError(
                    ErrorCode.INVALID_STAGE,
                    "required upstream checkpoint is missing",
                    {"stage": stage, "missing": upstream, "reason": "MISSING_PREREQUISITE"},
                )
            expected = upstream_fingerprints.get(upstream)
            if expected is None or record["input_fingerprint"] != expected:
                raise BusinessError(
                    ErrorCode.INVALID_STAGE,
                    "required upstream checkpoint is stale",
                    {"stage": stage, "upstream": upstream, "reason": "STALE_PREREQUISITE"},
                )

    def _verify_artifact(self, scale: str, stage: str, artifact_digest: str) -> None:
        path = self.artifact_path(scale, stage, artifact_digest)
        if not path.exists():
            raise BusinessError(
                ErrorCode.INVALID_STAGE,
                "stage artifact is missing",
                {"scale": scale, "stage": stage, "path": str(path), "reason": "MISSING_ARTIFACT"},
            )
        actual = digest_file(path)
        if actual != artifact_digest:
            raise BusinessError(
                ErrorCode.STALE_CHECKPOINT,
                "stage artifact digest does not match the publication",
                {
                    "scale": scale,
                    "stage": stage,
                    "expected": artifact_digest,
                    "actual": actual,
                    "reason": "ARTIFACT_DIGEST_MISMATCH",
                },
            )

    def publish(
        self,
        scale: str,
        stage: str,
        *,
        input_fingerprint: str,
        config_version: str,
        artifact_digest: str,
        upstream_fingerprints: dict[str, str] | None = None,
        basis_sequence: int | None = None,
    ) -> dict[str, Any]:
        if stage not in STAGES:
            raise BusinessError(
                ErrorCode.INVALID_STAGE,
                "unknown stage",
                {"stage": stage, "allowed": list(STAGES)},
            )
        if not all(_is_hex_digest(value) for value in (input_fingerprint, config_version, artifact_digest)):
            raise BusinessError(ErrorCode.INVALID_INPUT, "publication digests must be sha256 hex")
        upstream_fingerprints = upstream_fingerprints or {}
        with self.lock(scale, exclusive=True):
            state = self._load_state_unlocked(scale)
            self._verify_artifact(scale, stage, artifact_digest)
            self._validate_upstream(state, stage=stage, upstream_fingerprints=upstream_fingerprints)

            stage_index = STAGE_INDEX[stage]
            for downstream in STAGES[stage_index + 1 :]:
                if downstream in state.records and stage not in state.records:
                    raise BusinessError(
                        ErrorCode.STALE_CHECKPOINT,
                        "downstream checkpoint exists without its stage record",
                        {"scale": scale, "stage": stage, "downstream": downstream, "reason": "STAGE_GAP"},
                    )

            existing = state.records.get(stage)
            if existing is not None:
                if existing["input_fingerprint"] == input_fingerprint:
                    if existing["artifact_digest"] == artifact_digest:
                        raise BusinessError(
                            ErrorCode.STALE_CHECKPOINT,
                            "duplicate stage publication",
                            {
                                "scale": scale,
                                "stage": stage,
                                "sequence": existing["sequence"],
                                "reason": "DUPLICATE_PUBLICATION",
                            },
                        )
                    raise BusinessError(
                        ErrorCode.STALE_CHECKPOINT,
                        "same input produced a different artifact digest",
                        {
                            "scale": scale,
                            "stage": stage,
                            "reason": "ARTIFACT_DIGEST_MISMATCH",
                        },
                    )

            if basis_sequence is not None and state.sequence != basis_sequence:
                raise BusinessError(
                    ErrorCode.STALE_CHECKPOINT,
                    "publication basis sequence is stale",
                    {
                        "scale": scale,
                        "stage": stage,
                        "basis_sequence": basis_sequence,
                        "current_sequence": state.sequence,
                        "reason": "PUBLICATION_BASIS_STALE",
                    },
                )

            new_sequence = state.sequence + 1
            history_path = self.history_dir(scale) / f"{new_sequence:08d}.json"
            if history_path.exists():
                raise self._checkpoint_decode_error(history_path, reason="SEQUENCE_COLLISION")

            record_core = {
                "format_version": 1,
                "workspace_id": self.workspace_id,
                "scale": scale,
                "stage": stage,
                "sequence": new_sequence,
                "input_fingerprint": input_fingerprint,
                "config_version": config_version,
                "artifact_path": self.artifact_rel_path(scale, stage, artifact_digest),
                "artifact_digest": artifact_digest,
                "publication_basis_sequence": state.sequence,
            }
            record = {**record_core, "record_digest": digest_bytes(canonical_json_bytes(record_core))}
            records = {
                name: copy.deepcopy(value)
                for name, value in state.records.items()
                if STAGE_INDEX[name] < stage_index
            }
            records[stage] = record
            previous_state_digest = ZERO_DIGEST if state.sequence == 0 else state.state_digest
            state_core = {
                "format_version": 1,
                "workspace_id": self.workspace_id,
                "scale": scale,
                "sequence": new_sequence,
                "records": records,
                "previous_state_digest": previous_state_digest,
            }
            new_state_digest = digest_bytes(canonical_json_bytes(state_core))
            new_state = {**state_core, "state_digest": new_state_digest}

            pending = {
                "format_version": 1,
                "scale": scale,
                "sequence": new_sequence,
                "previous_state_digest": previous_state_digest,
                "state_digest": new_state_digest,
            }
            atomic_write_json(self.pending_path(scale), pending)
            atomic_write_json(history_path, new_state)
            atomic_write_json(self.state_path(scale), new_state)
            with contextlib.suppress(FileNotFoundError):
                self.pending_path(scale).unlink()
            return record

    def stage_status(self, state: CheckpointState, stage: str) -> dict[str, Any] | None:
        record = state.records.get(stage)
        if record is None:
            return None
        return {
            "sequence": record["sequence"],
            "input_fingerprint": record["input_fingerprint"],
            "config_version": record["config_version"],
            "artifact_digest": record["artifact_digest"],
            "artifact_path": record["artifact_path"],
        }
