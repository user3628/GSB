"""Crash-safe checkpoint storage with process-level publication locks."""

from __future__ import annotations

import fcntl
import re
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator

from .canonical import (
    atomic_write_json,
    read_json,
    sha256_obj,
)
from .constants import (
    SCHEMA_VERSION,
    STAGE_INDEX,
    STAGES,
)
from .errors import EngineError

_CHECKPOINT_NAME_RE = re.compile(r"^(\d{12})\.json$")


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_hex_digest(value: Any) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


@dataclass
class RecoveredState:
    records: list[dict[str, Any]] = field(default_factory=list)
    latest_by_stage: dict[tuple[int, str], dict[str, Any]] = field(default_factory=dict)
    latest_by_shard: dict[int, dict[str, Any]] = field(default_factory=dict)
    last_shard_sequence: dict[int, int] = field(default_factory=dict)
    last_sequence: int = 0
    next_sequence: int = 1
    ignored: list[dict[str, Any]] = field(default_factory=list)

    def latest(self, shard_index: int, stage: str) -> dict[str, Any] | None:
        return self.latest_by_stage.get((shard_index, stage))

    def output_digest(self, shard_index: int, stage: str) -> str | None:
        record = self.latest(shard_index, stage)
        return None if record is None else record["output_digest"]

    def available_stages(self, shard_index: int) -> list[str]:
        return [
            stage for stage in STAGES if (shard_index, stage) in self.latest_by_stage
        ]

    def stage_history(self, shard_index: int, stage: str) -> list[dict[str, Any]]:
        return [
            record
            for record in self.records
            if record["shard_index"] == shard_index and record["stage"] == stage
        ]


class CheckpointStore:
    """One atomic JSON file per publication plus a real ``flock`` lock.

    Filenames carry the global monotonic sequence.  The shard sequence is stored
    inside the record and must advance by exactly one for each publication of
    that shard.  Recovery accepts only a contiguous prefix; corrupted records
    and everything after the first inconsistency are ignored.
    """

    def __init__(self, checkpoints_dir: Path, shard_names: dict[int, str] | None = None):
        self.checkpoints_dir = checkpoints_dir
        self.shard_names = dict(shard_names or {})
        self.lock_path = checkpoints_dir.parent / ".publish.lock"

    @contextmanager
    def _lock(self) -> Iterator[None]:
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        with self.lock_path.open("a+", encoding="utf-8") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    def _checkpoint_path(self, sequence: int) -> Path:
        return self.checkpoints_dir / f"{sequence:012d}.json"

    def _checkpoint_files(self) -> list[tuple[int, Path]]:
        self.checkpoints_dir.mkdir(parents=True, exist_ok=True)
        result: list[tuple[int, Path]] = []
        for path in self.checkpoints_dir.iterdir():
            if not path.is_file():
                continue
            match = _CHECKPOINT_NAME_RE.match(path.name)
            if match:
                result.append((int(match.group(1)), path))
        result.sort(key=lambda item: item[0])
        return result

    def _validate_record_structure(
        self,
        record: Any,
        *,
        expected_sequence: int,
        file_name: str,
    ) -> tuple[dict[str, Any] | None, str | None]:
        if not isinstance(record, dict):
            return None, "record is not a JSON object"
        required = {
            "schema_version",
            "record_type",
            "sequence",
            "shard_index",
            "shard_name",
            "stage",
            "stage_index",
            "shard_sequence",
            "input_fingerprint",
            "input_components",
            "config_fingerprint",
            "stage_config_fingerprint",
            "stage_config_components",
            "upstream_output_digests",
            "output_digest",
            "output",
            "record_digest",
        }
        missing = sorted(required - set(record))
        if missing:
            return None, f"missing fields: {','.join(missing)}"
        if record["schema_version"] != SCHEMA_VERSION:
            return None, "unsupported checkpoint schema_version"
        if record["record_type"] != "checkpoint":
            return None, "record_type is not checkpoint"
        if not _is_int(record["sequence"]) or record["sequence"] != expected_sequence:
            return None, "global sequence does not match filename"
        if record["sequence"] != int(file_name.split(".")[0]):
            return None, "global sequence does not match filename text"
        if not _is_int(record["shard_index"]) or record["shard_index"] < 0:
            return None, "invalid shard_index"
        if not isinstance(record["shard_name"], str) or not record["shard_name"]:
            return None, "invalid shard_name"
        if record["stage"] not in STAGES:
            return None, "unknown stage"
        if record["stage_index"] != STAGE_INDEX[record["stage"]]:
            return None, "stage_index does not match stage"
        if not _is_int(record["shard_sequence"]) or record["shard_sequence"] < 1:
            return None, "invalid shard_sequence"
        for key in (
            "input_fingerprint",
            "config_fingerprint",
            "stage_config_fingerprint",
            "output_digest",
            "record_digest",
        ):
            if not _is_hex_digest(record[key]):
                return None, f"invalid digest field: {key}"
        if not isinstance(record["input_components"], dict):
            return None, "input_components is not an object"
        if not isinstance(record["stage_config_components"], dict):
            return None, "stage_config_components is not an object"
        if not isinstance(record["upstream_output_digests"], dict):
            return None, "upstream_output_digests is not an object"
        if not isinstance(record["output"], dict):
            return None, "output is not an object"

        output_digest = sha256_obj(record["output"])
        if output_digest != record["output_digest"]:
            return None, "output digest mismatch"
        expected_record_digest = sha256_obj(
            {key: value for key, value in record.items() if key != "record_digest"}
        )
        if expected_record_digest != record["record_digest"]:
            return None, "record digest mismatch"
        if self.shard_names and self.shard_names.get(record["shard_index"]) != record["shard_name"]:
            return None, "record shard name does not match manifest"
        return record, None

    def _load_record(self, path: Path, sequence: int) -> tuple[dict[str, Any] | None, str | None]:
        try:
            record = read_json(path)
        except (OSError, ValueError, UnicodeDecodeError) as exc:
            return None, f"unreadable checkpoint: {exc}"
        return self._validate_record_structure(
            record,
            expected_sequence=sequence,
            file_name=path.name,
        )

    def scan(self) -> RecoveredState:
        """Read a contiguous, integrity-checked prefix of checkpoints."""

        state = RecoveredState()
        expected_global = 1
        expected_shard_sequences: dict[int, int] = {}
        for sequence, path in self._checkpoint_files():
            if sequence != expected_global:
                state.ignored.append(
                    {
                        "file": path.name,
                        "sequence": sequence,
                        "reason": "global sequence gap",
                    }
                )
                break
            record, reason = self._load_record(path, sequence)
            if record is None:
                state.ignored.append(
                    {
                        "file": path.name,
                        "sequence": sequence,
                        "reason": reason,
                    }
                )
                break
            shard = int(record["shard_index"])
            expected_shard = expected_shard_sequences.get(shard, 0) + 1
            if record["shard_sequence"] != expected_shard:
                state.ignored.append(
                    {
                        "file": path.name,
                        "sequence": sequence,
                        "reason": "shard sequence is not contiguous",
                        "shard_index": shard,
                        "expected_shard_sequence": expected_shard,
                        "actual_shard_sequence": record["shard_sequence"],
                    }
                )
                break

            state.records.append(record)
            state.latest_by_stage[(shard, record["stage"])] = record
            state.latest_by_shard[shard] = record
            state.last_shard_sequence[shard] = record["shard_sequence"]
            state.last_sequence = sequence
            expected_global += 1
            expected_shard_sequences[shard] = record["shard_sequence"]

        state.next_sequence = expected_global
        return state

    def next_shard_sequence(self, shard_index: int) -> int:
        state = self.scan()
        return state.last_shard_sequence.get(shard_index, 0) + 1

    def _make_record(
        self,
        *,
        sequence: int,
        shard_index: int,
        shard_name: str,
        stage: str,
        shard_sequence: int,
        input_fingerprint: str,
        input_components: dict[str, Any],
        config_fingerprint: str,
        stage_config_fingerprint: str,
        stage_config_components: dict[str, Any],
        upstream_output_digests: dict[str, str],
        output: dict[str, Any],
    ) -> dict[str, Any]:
        record: dict[str, Any] = {
            "schema_version": SCHEMA_VERSION,
            "record_type": "checkpoint",
            "sequence": sequence,
            "shard_index": shard_index,
            "shard_name": shard_name,
            "stage": stage,
            "stage_index": STAGE_INDEX[stage],
            "shard_sequence": shard_sequence,
            "input_fingerprint": input_fingerprint,
            "input_components": input_components,
            "config_fingerprint": config_fingerprint,
            "stage_config_fingerprint": stage_config_fingerprint,
            "stage_config_components": stage_config_components,
            "upstream_output_digests": upstream_output_digests,
            "output_digest": sha256_obj(output),
            "output": output,
        }
        record["record_digest"] = sha256_obj(record)
        return record

    def publish(
        self,
        *,
        shard_index: int,
        shard_name: str,
        stage: str,
        input_fingerprint: str,
        input_components: dict[str, Any],
        config_fingerprint: str,
        stage_config_fingerprint: str,
        stage_config_components: dict[str, Any],
        upstream_output_digests: dict[str, str],
        output: dict[str, Any],
        expected_shard_sequence: int | None = None,
    ) -> dict[str, Any]:
        """Validate and atomically publish one stage artifact.

        ``expected_shard_sequence`` is the caller's observed next sequence.  It
        is compared under the lock, so two racing workers cannot both advance
        the same shard; the loser receives ``E_STALE_CHECKPOINT``.
        """

        if stage not in STAGES:
            raise EngineError(
                "E_INVALID_STAGE",
                "unknown stage",
                {
                    "stage": stage,
                    "valid_stages": list(STAGES),
                    "available_stages": [],
                },
            )
        if self.shard_names and shard_index not in self.shard_names:
            raise EngineError(
                "E_INVALID_STAGE",
                "shard index is out of range",
                {
                    "shard_index": shard_index,
                    "valid_stages": list(STAGES),
                    "available_stages": [],
                },
            )

        with self._lock():
            state = self.scan()
            available = state.available_stages(shard_index)
            canonical_name = self.shard_names.get(shard_index)
            if canonical_name is not None and canonical_name != shard_name:
                raise EngineError(
                    "E_STALE_CHECKPOINT",
                    "checkpoint publication is misaligned across shards",
                    {
                        "reason": "cross_shard_misaligned",
                        "shard_index": shard_index,
                        "expected_shard_name": canonical_name,
                        "actual_shard_name": shard_name,
                        "valid_stages": list(STAGES),
                        "available_stages": available,
                    },
                )

            stage_position = STAGE_INDEX[stage]
            if stage_position > 0:
                prerequisite = STAGES[stage_position - 1]
                prerequisite_record = state.latest(shard_index, prerequisite)
                if prerequisite_record is None:
                    raise EngineError(
                        "E_INVALID_STAGE",
                        "a prerequisite stage artifact is missing",
                        {
                            "shard_index": shard_index,
                            "stage": stage,
                            "missing_stage": prerequisite,
                            "valid_stages": list(STAGES),
                            "available_stages": available,
                        },
                    )
                supplied = upstream_output_digests.get(prerequisite)
                if supplied != prerequisite_record["output_digest"]:
                    raise EngineError(
                        "E_STALE_CHECKPOINT",
                        "checkpoint is based on a stale upstream artifact",
                        {
                            "reason": "upstream_mismatch",
                            "shard_index": shard_index,
                            "stage": stage,
                            "upstream_stage": prerequisite,
                            "expected_output_digest": prerequisite_record["output_digest"],
                            "supplied_output_digest": supplied,
                            "valid_stages": list(STAGES),
                            "available_stages": available,
                        },
                    )

            required_shard_sequence = state.last_shard_sequence.get(shard_index, 0) + 1
            if (
                expected_shard_sequence is not None
                and expected_shard_sequence != required_shard_sequence
            ):
                reason = (
                    "duplicate_or_advanced"
                    if expected_shard_sequence > required_shard_sequence
                    else "stale_or_lagging"
                )
                raise EngineError(
                    "E_STALE_CHECKPOINT",
                    "shard checkpoint sequence did not advance exactly once",
                    {
                        "reason": reason,
                        "shard_index": shard_index,
                        "stage": stage,
                        "expected_shard_sequence": expected_shard_sequence,
                        "required_shard_sequence": required_shard_sequence,
                        "valid_stages": list(STAGES),
                        "available_stages": available,
                    },
                )

            sequence = state.next_sequence
            record = self._make_record(
                sequence=sequence,
                shard_index=shard_index,
                shard_name=shard_name,
                stage=stage,
                shard_sequence=required_shard_sequence,
                input_fingerprint=input_fingerprint,
                input_components=input_components,
                config_fingerprint=config_fingerprint,
                stage_config_fingerprint=stage_config_fingerprint,
                stage_config_components=stage_config_components,
                upstream_output_digests=upstream_output_digests,
                output=output,
            )
            atomic_write_json(self._checkpoint_path(sequence), record)
            return record

    def read_checkpoint(self, sequence: int) -> dict[str, Any]:
        path = self._checkpoint_path(sequence)
        record, reason = self._load_record(path, sequence)
        if record is None:
            raise EngineError(
                "E_INVALID_STAGE",
                "checkpoint record is invalid or unavailable",
                {
                    "sequence": sequence,
                    "reason": reason,
                    "valid_stages": list(STAGES),
                    "available_stages": [],
                },
            )
        return record

    def checkpoint_files(self) -> list[str]:
        return [path.name for _sequence, path in self._checkpoint_files()]
