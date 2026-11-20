"""Atomic, contiguous checkpoint journal with standard-library locking."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .canonical import canonical_json, object_fingerprint, sha256_bytes
from .errors import InvalidStageError, StaleCheckpointError
from .storage import FileLock, atomic_write_bytes, atomic_write_json, read_json, remove_if_exists


GENESIS = "0" * 64
_REQUIRED_FIELDS = {
    "format_version",
    "shard_id",
    "operator_index",
    "checkpoint_seq",
    "op_name",
    "op_fingerprint",
    "input_fingerprint",
    "config_version",
    "chain_prefix",
    "artifact_digest",
    "artifact_path",
    "shape",
    "dtype",
    "previous_checkpoint_sha256",
    "checkpoint_sha256",
}


def seal_record(record: dict[str, Any]) -> dict[str, Any]:
    sealed = dict(record)
    sealed.pop("checkpoint_sha256", None)
    sealed["checkpoint_sha256"] = object_fingerprint(sealed)
    return sealed


def record_without_seal(record: dict[str, Any]) -> dict[str, Any]:
    copy = dict(record)
    copy.pop("checkpoint_sha256", None)
    return copy


class CheckpointStore:
    def __init__(self, run_dir: str | Path, shard_id: str) -> None:
        self.run_dir = Path(run_dir).resolve()
        self.shard_id = shard_id
        self.shard_dir = self.run_dir / "checkpoints" / shard_id
        self.journal_path = self.shard_dir / "checkpoints.jsonl"
        self.invalidations_path = self.shard_dir / "invalidations.json"
        self.lock_path = self.run_dir / "locks" / f"{shard_id}.lock"

    def artifact_path(self, checkpoint_seq: int, digest: str) -> Path:
        return self.run_dir / "artifacts" / self.shard_id / f"stage-{checkpoint_seq:04d}-{digest[:20]}.npy"

    def artifact_relative_path(self, checkpoint_seq: int, digest: str) -> str:
        return self.artifact_path(checkpoint_seq, digest).relative_to(self.run_dir).as_posix()

    def _valid_record(self, record: Any, expected_seq: int, previous_sha: str, verify_artifact: bool) -> bool:
        if not isinstance(record, dict) or not _REQUIRED_FIELDS.issubset(record):
            return False
        try:
            if record["format_version"] != 1 or record["shard_id"] != self.shard_id:
                return False
            if record["checkpoint_seq"] != expected_seq or record["operator_index"] != expected_seq - 1:
                return False
            if record["previous_checkpoint_sha256"] != previous_sha:
                return False
            if record["checkpoint_sha256"] != object_fingerprint(record_without_seal(record)):
                return False
            artifact_path = self.run_dir / str(record["artifact_path"])
            artifact_path.resolve().relative_to((self.run_dir / "artifacts").resolve())
            if verify_artifact:
                if not artifact_path.is_file():
                    return False
                if sha256_bytes(artifact_path.read_bytes()) != record["artifact_digest"]:
                    return False
        except (OSError, TypeError, ValueError):
            return False
        return True

    def load_records(self, *, verify_artifacts: bool = True) -> list[dict[str, Any]]:
        """Load the longest valid contiguous checkpoint prefix.

        Malformed, incomplete, checksum-invalid, duplicate, and out-of-order
        records are ignored.  A gap stops the scan at the last consistent
        checkpoint.
        """

        if not self.journal_path.is_file():
            return []
        records: list[dict[str, Any]] = []
        seen: set[int] = set()
        previous_sha = GENESIS
        try:
            text = self.journal_path.read_text(encoding="utf-8")
        except (OSError, UnicodeError):
            return []
        for line in text.splitlines():
            if not line.strip():
                continue
            try:
                record = json.loads(line)
            except (json.JSONDecodeError, UnicodeError):
                continue
            if not isinstance(record, dict):
                continue
            sequence = record.get("checkpoint_seq")
            if not isinstance(sequence, int) or isinstance(sequence, bool):
                continue
            if sequence in seen:
                continue
            expected = len(records) + 1
            if sequence < expected:
                continue
            if sequence > expected:
                break
            if not self._valid_record(record, expected, previous_sha, verify_artifacts):
                break
            records.append(record)
            seen.add(sequence)
            previous_sha = record["checkpoint_sha256"]
        return records

    def _journal_bytes(self, records: list[dict[str, Any]]) -> bytes:
        if not records:
            return b""
        return ("\n".join(canonical_json(record) for record in records) + "\n").encode("utf-8")

    def publish_stage(
        self,
        *,
        stage_index: int,
        artifact_bytes: bytes,
        template: dict[str, Any],
    ) -> dict[str, Any]:
        if stage_index < 0:
            raise InvalidStageError("stage index must be non-negative", shard_id=self.shard_id, operator_index=stage_index)
        with FileLock(self.lock_path):
            records = self.load_records()
            expected_seq = len(records) + 1
            if stage_index != len(records):
                raise StaleCheckpointError(
                    "checkpoint sequence is not ready for this stage",
                    shard_id=self.shard_id,
                    operator_index=stage_index,
                    expected_operator_index=len(records),
                    published_checkpoint_seq=expected_seq - 1,
                    supplied_checkpoint_seq=stage_index + 1,
                )
            previous_sha = records[-1]["checkpoint_sha256"] if records else GENESIS
            if records:
                previous_artifact = records[-1]["artifact_digest"]
                if template.get("input_fingerprint") != previous_artifact:
                    raise StaleCheckpointError(
                        "stage input is not the currently published upstream artifact",
                        shard_id=self.shard_id,
                        operator_index=stage_index,
                        expected_input_fingerprint=previous_artifact,
                        supplied_input_fingerprint=template.get("input_fingerprint"),
                    )
            elif template.get("input_fingerprint") != template.get("tensor_fingerprint"):
                raise StaleCheckpointError(
                    "first stage input is not the planned source tensor",
                    shard_id=self.shard_id,
                    operator_index=stage_index,
                )

            artifact_digest = sha256_bytes(artifact_bytes)
            artifact_path = self.artifact_path(expected_seq, artifact_digest)
            artifact_relative = self.artifact_relative_path(expected_seq, artifact_digest)
            atomic_write_bytes(artifact_path, artifact_bytes)

            record: dict[str, Any] = {
                "format_version": 1,
                "shard_id": self.shard_id,
                "operator_index": int(stage_index),
                "checkpoint_seq": int(expected_seq),
                "op_name": template["op_name"],
                "op_fingerprint": template["op_fingerprint"],
                "input_fingerprint": template["input_fingerprint"],
                "config_version": template["config_version"],
                "chain_prefix": template["chain_prefix"],
                "operator_summary": template.get("operator_summary", {}),
                "artifact_digest": artifact_digest,
                "artifact_path": artifact_relative,
                "shape": list(template["shape"]),
                "dtype": template["dtype"],
                "expected_range": template.get("expected_range"),
                "execution": template.get("execution", {}),
                "previous_checkpoint_sha256": previous_sha,
            }
            record = seal_record(record)
            atomic_write_bytes(self.journal_path, self._journal_bytes(records + [record]))
            return record

    def _classify_invalidation(
        self,
        stage_index: int,
        old_record: dict[str, Any] | None,
        expected: dict[str, Any],
        expected_input_fingerprint: str,
    ) -> tuple[str, dict[str, Any]]:
        if old_record is None:
            return "chain_expanded", {}
        old_summary = old_record.get("operator_summary", {})
        new_summary = expected.get("operator_summary", {})
        details = {
            "old_artifact_digest": old_record.get("artifact_digest"),
            "old_op_fingerprint": old_record.get("op_fingerprint"),
            "new_op_fingerprint": expected.get("op_fingerprint"),
            "old_input_fingerprint": old_record.get("input_fingerprint"),
            "new_input_fingerprint": expected_input_fingerprint,
            "old_chain_prefix": old_record.get("chain_prefix"),
            "new_chain_prefix": expected.get("chain_prefix"),
            "old_config_version": old_record.get("config_version"),
            "new_config_version": expected.get("config_version"),
            "old_operator_summary": old_summary,
            "new_operator_summary": new_summary,
        }
        if old_record.get("config_version") != expected.get("config_version"):
            return "config_version_changed", details
        if old_record.get("input_fingerprint") != expected_input_fingerprint:
            if stage_index == 0:
                return "source_tensor_changed", details
            return "upstream_invalidated", details
        if old_summary.get("align_mode") != new_summary.get("align_mode") or old_summary.get(
            "align_corners"
        ) != new_summary.get("align_corners"):
            return "alignment_mode_changed", details
        if old_summary.get("name") == "interpolate" or new_summary.get("name") == "interpolate":
            if old_summary.get("mode") != new_summary.get("mode") or old_summary.get("scale") != new_summary.get(
                "scale"
            ):
                return "interpolation_parameters_changed", details
        if old_summary.get("name") in {"conv1d", "conv2d"} or new_summary.get("name") in {"conv1d", "conv2d"}:
            return "convolution_parameters_changed", details
        return "operator_parameters_changed", details

    def reconcile(
        self,
        expected_stages: list[dict[str, Any]],
        *,
        tensor_fingerprint: str,
    ) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
        """Invalidate the earliest mismatching stage and every downstream stage."""

        with FileLock(self.lock_path):
            records = self.load_records()
            invalid_at: int | None = None
            for index, expected in enumerate(expected_stages):
                if index >= len(records):
                    break
                record = records[index]
                if record.get("chain_prefix") != expected.get("chain_prefix"):
                    invalid_at = index
                    break
            if invalid_at is None and len(records) > len(expected_stages):
                invalid_at = len(expected_stages)

            if invalid_at is None:
                return records, None

            old_record = records[invalid_at] if invalid_at < len(records) else None
            if invalid_at < len(expected_stages):
                expected = expected_stages[invalid_at]
                expected_input = (
                    records[invalid_at - 1]["artifact_digest"]
                    if invalid_at > 0
                    else tensor_fingerprint
                )
                reason, details = self._classify_invalidation(
                    invalid_at, old_record, expected, expected_input
                )
            else:
                expected = {}
                reason = "chain_shortened"
                details = {
                    "old_artifact_digest": old_record.get("artifact_digest") if old_record else None,
                    "old_op_fingerprint": old_record.get("op_fingerprint") if old_record else None,
                    "new_op_fingerprint": None,
                    "old_input_fingerprint": old_record.get("input_fingerprint") if old_record else None,
                    "new_input_fingerprint": None,
                    "old_chain_prefix": old_record.get("chain_prefix") if old_record else None,
                    "new_chain_prefix": None,
                }

            removed = records[invalid_at:]
            remaining = records[:invalid_at]
            atomic_write_bytes(self.journal_path, self._journal_bytes(remaining))
            retained_paths = {record["artifact_path"] for record in remaining}
            for record in removed:
                relative = str(record.get("artifact_path", ""))
                if relative and relative not in retained_paths:
                    remove_if_exists(self.run_dir / relative)

            event = {
                "event": "invalidation",
                "shard_id": self.shard_id,
                "earliest_invalid_stage_index": int(invalid_at),
                "reason": reason,
                "affected_stages": list(range(invalid_at, max(len(expected_stages), len(records)))),
                "removed_checkpoint_seqs": [record["checkpoint_seq"] for record in removed],
                "last_valid_checkpoint_seq": remaining[-1]["checkpoint_seq"] if remaining else 0,
                **details,
            }
            self._append_invalidation_event(event)
            return remaining, event

    def _append_invalidation_event(self, event: dict[str, Any]) -> None:
        document: dict[str, Any]
        if self.invalidations_path.is_file():
            try:
                document = read_json(self.invalidations_path)
                if not isinstance(document, dict) or not isinstance(document.get("events"), list):
                    document = {"shard_id": self.shard_id, "events": []}
            except (OSError, ValueError):
                document = {"shard_id": self.shard_id, "events": []}
        else:
            document = {"shard_id": self.shard_id, "events": []}
        document["events"].append(event)
        atomic_write_json(self.invalidations_path, document)

    def invalidations(self) -> list[dict[str, Any]]:
        if not self.invalidations_path.is_file():
            return []
        try:
            document = read_json(self.invalidations_path)
        except (OSError, ValueError):
            return []
        events = document.get("events", []) if isinstance(document, dict) else []
        return events if isinstance(events, list) else []

