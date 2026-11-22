"""Atomic per-shard stage checkpoints with locked, ordered publication."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .errors import E_INVALID_STAGE, E_STALE_CHECKPOINT, PipelineError
from .manifest import STAGES
from .util import SCHEMA_VERSION, FileLock, atomic_write_json, digest_value, read_json


class CheckpointStore:
    """A state file per shard; every publication is a single atomic replace.

    Only complete records are ever referenced by state.json. A crash while
    writing the temporary file leaves the previous state intact, while an
    unreferenced temporary file is harmless.
    """

    def __init__(self, workspace: str | Path):
        self.workspace = Path(workspace)
        self.root = self.workspace / "checkpoints"
        self.root.mkdir(parents=True, exist_ok=True)

    def _shard_dir(self, shard_id: str) -> Path:
        return self.root / shard_id

    def _state_path(self, shard_id: str) -> Path:
        return self._shard_dir(shard_id) / "state.json"

    def _lock_path(self, shard_id: str) -> Path:
        return self._shard_dir(shard_id) / "state.lock"

    @staticmethod
    def _empty_state(shard_id: str) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "shard_id": shard_id,
            "last_sequence": 0,
            "records": [],
        }

    def _read_state_unlocked(self, shard_id: str) -> dict[str, Any]:
        path = self._state_path(shard_id)
        if not path.exists():
            return self._empty_state(shard_id)
        try:
            state = read_json(path)
        except (OSError, ValueError) as exc:
            raise PipelineError(
                code=E_INVALID_STAGE,
                message="checkpoint state is unreadable",
                stage="checkpoint",
                shard_id=shard_id,
                details={"error": str(exc)},
            ) from exc
        self._validate_state(state, shard_id)
        return state

    @staticmethod
    def _validate_state(state: dict[str, Any], shard_id: str) -> None:
        if (
            not isinstance(state, dict)
            or state.get("schema_version") != SCHEMA_VERSION
            or state.get("shard_id") != shard_id
            or not isinstance(state.get("records"), list)
            or not isinstance(state.get("last_sequence"), int)
        ):
            raise PipelineError(
                code=E_INVALID_STAGE,
                message="checkpoint state header is invalid",
                stage="checkpoint",
                shard_id=shard_id,
            )
        last = 0
        for index, record in enumerate(state["records"]):
            if not isinstance(record, dict):
                raise PipelineError(E_INVALID_STAGE, "checkpoint record is invalid", "checkpoint", shard_id)
            if index >= len(STAGES) or record.get("stage") != STAGES[index]:
                raise PipelineError(
                    code=E_INVALID_STAGE,
                    message="checkpoint stages are misaligned",
                    stage="checkpoint",
                    shard_id=shard_id,
                    details={"record_index": index, "record_stage": record.get("stage")},
                )
            sequence = record.get("sequence")
            if not isinstance(sequence, int) or sequence <= last or sequence > state["last_sequence"]:
                raise PipelineError(
                    code=E_INVALID_STAGE,
                    message="checkpoint sequence is not strictly monotonic",
                    stage="checkpoint",
                    shard_id=shard_id,
                    details={"record_index": index, "sequence": sequence},
                )
            if record.get("artifact_digest") != digest_value(record.get("artifact")):
                raise PipelineError(
                    code=E_INVALID_STAGE,
                    message="checkpoint artifact digest mismatch",
                    stage=record.get("stage", "checkpoint"),
                    shard_id=shard_id,
                    details={"sequence": sequence},
                )
            last = sequence
        if last > state["last_sequence"]:
            raise PipelineError(
                code=E_INVALID_STAGE,
                message="checkpoint high-watermark is below its records",
                stage="checkpoint",
                shard_id=shard_id,
            )

    def load_state(self, shard_id: str) -> dict[str, Any]:
        with FileLock(self._lock_path(shard_id)):
            return self._read_state_unlocked(shard_id)

    def prepare(
        self,
        shard_id: str,
        expected_stages: list[dict[str, Any]],
        *,
        content_fingerprint: str,
    ) -> dict[str, Any]:
        """Reuse the valid prefix and atomically truncate the first affected stage."""

        if [item["stage"] for item in expected_stages] != list(STAGES):
            raise PipelineError(
                code=E_INVALID_STAGE,
                message="expected stage chain must contain filter, normalize, sampling, validation",
                stage="checkpoint",
                shard_id=shard_id,
            )
        with FileLock(self._lock_path(shard_id)):
            state = self._read_state_unlocked(shard_id)
            records: list[dict[str, Any]] = state["records"]
            if len(records) > len(expected_stages):
                raise PipelineError(
                    code=E_INVALID_STAGE,
                    message="checkpoint has too many stage records",
                    stage="checkpoint",
                    shard_id=shard_id,
                )

            matching = 0
            for index, record in enumerate(records):
                expected = expected_stages[index]
                if record["stage"] != expected["stage"]:
                    raise PipelineError(
                        code=E_INVALID_STAGE,
                        message="checkpoint stage does not match the expected chain",
                        stage="checkpoint",
                        shard_id=shard_id,
                        details={
                            "record_index": index,
                            "record_stage": record["stage"],
                            "expected_stage": expected["stage"],
                        },
                    )
                if record["input_fingerprint"] != expected["input_fingerprint"]:
                    break
                if record["config_version"] != expected["config_version"]:
                    break
                matching += 1

            earliest_invalid_stage: str | None = None
            if matching < len(records):
                earliest_invalid_stage = expected_stages[matching]["stage"]
                state = {
                    "schema_version": SCHEMA_VERSION,
                    "shard_id": shard_id,
                    "last_sequence": int(state["last_sequence"]),
                    "records": records[:matching],
                }
                atomic_write_json(self._state_path(shard_id), state)

            return {
                "shard_id": shard_id,
                "records": records[:matching],
                "reused_stages": [record["stage"] for record in records[:matching]],
                "earliest_invalid_stage": earliest_invalid_stage,
                "last_sequence": int(state["last_sequence"]),
                "next_sequence": int(state["last_sequence"]) + 1,
                "content_fingerprint": content_fingerprint,
            }

    def publish(
        self,
        shard_id: str,
        *,
        sequence: int,
        stage: str,
        input_fingerprint: str,
        config_version: str,
        content_fingerprint: str,
        artifact: dict[str, Any],
    ) -> dict[str, Any]:
        """Publish one completed stage under an exclusive file lock."""

        artifact_digest = digest_value(artifact)
        record = {
            "schema_version": SCHEMA_VERSION,
            "sequence": int(sequence),
            "shard_id": shard_id,
            "stage": stage,
            "input_fingerprint": input_fingerprint,
            "config_version": config_version,
            "content_fingerprint": content_fingerprint,
            "artifact_digest": artifact_digest,
            "artifact": artifact,
        }
        with FileLock(self._lock_path(shard_id)):
            state = self._read_state_unlocked(shard_id)
            if int(sequence) <= int(state["last_sequence"]):
                raise PipelineError(
                    code=E_STALE_CHECKPOINT,
                    message="checkpoint sequence is stale or already published",
                    stage=stage,
                    shard_id=shard_id,
                    details={
                        "sequence": int(sequence),
                        "last_sequence": int(state["last_sequence"]),
                    },
                )
            expected_stage = (
                STAGES[len(state["records"])] if len(state["records"]) < len(STAGES) else "complete"
            )
            if (
                stage != expected_stage
                or int(sequence) != int(state["last_sequence"]) + 1
            ):
                raise PipelineError(
                    code=E_INVALID_STAGE,
                    message="checkpoint publication crosses the expected stage/sequence",
                    stage=stage,
                    shard_id=shard_id,
                    details={
                        "sequence": int(sequence),
                        "expected_sequence": int(state["last_sequence"]) + 1,
                        "expected_stage": expected_stage,
                    },
                )
            state["records"].append(record)
            state["last_sequence"] = int(sequence)
            atomic_write_json(self._state_path(shard_id), state)
        return record
