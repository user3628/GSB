"""Atomic, hash-chained stage checkpoints and cross-process publication locks."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .artifacts import make_artifact, read_artifact
from .config import STAGES
from .errors import (
    PipelineError,
    invalid_stage,
    stale_checkpoint,
)
from .util import (
    atomic_write_bytes,
    canonical_json_line,
    file_lock,
    move_to_quarantine,
    sha256_obj,
)
from .workspace import Workspace


CHECKPOINT_FORMAT = "llm-pipeline-checkpoint-v1"
_REQUIRED_FIELDS = {
    "format",
    "seq",
    "stage",
    "stage_seq",
    "stage_fingerprint",
    "fingerprint_components",
    "input_shards",
    "input_shard_fingerprint",
    "config_version",
    "config_revision",
    "artifact_digest",
    "artifact_path",
    "previous_record_digest",
    "record_checksum",
}


@dataclass
class CheckpointState:
    records: list[dict[str, Any]] = field(default_factory=list)
    latest_by_stage: dict[str, dict[str, Any]] = field(default_factory=dict)
    issues: list[dict[str, Any]] = field(default_factory=list)
    max_seq_seen: int = 0
    stage_counts: dict[str, int] = field(default_factory=dict)

    @property
    def current_seq(self) -> int:
        return int(self.records[-1]["seq"]) if self.records else 0

    def item(self, stage: str) -> dict[str, Any] | None:
        return self.latest_by_stage.get(stage)


class CheckpointStore:
    def __init__(self, workspace: Workspace) -> None:
        self.workspace = workspace

    def load(self) -> CheckpointState:
        return self._load_unlocked()

    def _load_unlocked(self) -> CheckpointState:
        workspace = self.workspace
        candidates: list[dict[str, Any]] = []
        issues: list[dict[str, Any]] = []
        max_seq_seen = 0
        for path in workspace.checkpoint_files():
            try:
                candidate, error = self._parse_checkpoint(path)
            except Exception as exc:  # defensive: malformed user-edited checkpoints
                candidate, error = None, {"path": path.name, "reason": "exception", "error": str(exc)}
            if candidate is None:
                issues.append(error or {"path": path.name, "reason": "invalid"})
                continue
            max_seq_seen = max(max_seq_seen, int(candidate["seq"]))
            candidates.append(candidate)

        by_seq: dict[int, list[dict[str, Any]]] = {}
        for candidate in candidates:
            by_seq.setdefault(int(candidate["seq"]), []).append(candidate)

        records: list[dict[str, Any]] = []
        latest_by_stage: dict[str, dict[str, Any]] = {}
        stage_counts: dict[str, int] = {stage: 0 for stage in STAGES}
        expected = 1
        previous_digest: str | None = None
        while expected in by_seq:
            group = by_seq[expected]
            if len(group) > 1:
                issues.append(
                    {
                        "reason": "duplicate_seq",
                        "seq": expected,
                        "files": sorted(item["_file"] for item in group),
                    }
                )
                break
            record = group[0]
            if record["previous_record_digest"] != previous_digest:
                issues.append(
                    {
                        "reason": "broken_previous_digest",
                        "seq": expected,
                        "file": record["_file"],
                    }
                )
                break
            if not records and record["stage"] != "normalize":
                issues.append(
                    {
                        "reason": "first_stage_is_not_normalize",
                        "seq": expected,
                        "file": record["_file"],
                    }
                )
                break
            stage = record["stage"]
            if int(record["stage_seq"]) != stage_counts[stage] + 1:
                issues.append(
                    {
                        "reason": "non_contiguous_stage_seq",
                        "seq": expected,
                        "stage": stage,
                        "file": record["_file"],
                        "expected_stage_seq": stage_counts[stage] + 1,
                        "actual_stage_seq": record["stage_seq"],
                    }
                )
                break
            dependency_error = self._dependency_error(record, latest_by_stage)
            if dependency_error is not None:
                issues.append(
                    {
                        "reason": "stage_dependency_mismatch",
                        "seq": expected,
                        "stage": stage,
                        "file": record["_file"],
                        "details": dependency_error,
                    }
                )
                break
            records.append(record)
            latest_by_stage[stage] = record
            stage_counts[stage] += 1
            previous_digest = record["record_digest"]
            expected += 1

        if any(seq > expected for seq in by_seq):
            issues.append(
                {
                    "reason": "ignored_after_contiguous_prefix",
                    "prefix_last_seq": expected - 1,
                    "ignored_seqs": sorted(seq for seq in by_seq if seq > expected),
                }
            )
        return CheckpointState(
            records=records,
            latest_by_stage=latest_by_stage,
            issues=issues,
            max_seq_seen=max_seq_seen,
            stage_counts={stage: stage_counts[stage] for stage in STAGES},
        )

    @staticmethod
    def _dependency_error(
        record: dict[str, Any], latest_by_stage: dict[str, dict[str, Any]]
    ) -> dict[str, Any] | None:
        stage = record["stage"]
        components = record["fingerprint_components"]

        def upstream(name: str) -> dict[str, Any] | None:
            return latest_by_stage.get(name)

        if stage == "normalize":
            return None
        if stage == "allocate":
            normalized = upstream("normalize")
            if normalized is None:
                return {"missing": "normalize"}
            if components.get("normalized_stage_fingerprint") != normalized["stage_fingerprint"]:
                return {
                    "upstream": "normalize",
                    "reason": "stage_fingerprint_mismatch",
                    "expected": normalized["stage_fingerprint"],
                    "actual": components.get("normalized_stage_fingerprint"),
                }
            if components.get("normalized_artifact_digest") != normalized["artifact_digest"]:
                return {
                    "upstream": "normalize",
                    "reason": "artifact_digest_mismatch",
                }
            return None
        if stage == "assemble":
            normalized = upstream("normalize")
            allocation = upstream("allocate")
            if normalized is None or allocation is None:
                return {"missing": [name for name, item in (("normalize", normalized), ("allocate", allocation)) if item is None]}
            if components.get("normalized_stage_fingerprint") != normalized["stage_fingerprint"]:
                return {"upstream": "normalize", "reason": "stage_fingerprint_mismatch"}
            if components.get("allocation_stage_fingerprint") != allocation["stage_fingerprint"]:
                return {"upstream": "allocate", "reason": "stage_fingerprint_mismatch"}
            if components.get("normalized_artifact_digest") != normalized["artifact_digest"]:
                return {"upstream": "normalize", "reason": "artifact_digest_mismatch"}
            if components.get("allocation_artifact_digest") != allocation["artifact_digest"]:
                return {"upstream": "allocate", "reason": "artifact_digest_mismatch"}
            return None
        if stage == "validate":
            assembly = upstream("assemble")
            if assembly is None:
                return {"missing": "assemble"}
            if components.get("assembly_stage_fingerprint") != assembly["stage_fingerprint"]:
                return {"upstream": "assemble", "reason": "stage_fingerprint_mismatch"}
            if components.get("assembly_artifact_digest") != assembly["artifact_digest"]:
                return {"upstream": "assemble", "reason": "artifact_digest_mismatch"}
            return None
        return {"reason": "unknown_stage"}

    def _parse_checkpoint(
        self, path: Path
    ) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
        try:
            raw = path.read_bytes()
            record = json.loads(raw.decode("utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            return None, {"path": path.name, "reason": "invalid_json", "error": str(exc)}
        if not isinstance(record, dict):
            return None, {"path": path.name, "reason": "record_is_not_object"}
        missing = sorted(_REQUIRED_FIELDS - set(record))
        if missing:
            return None, {
                "path": path.name,
                "reason": "incomplete_record",
                "missing": missing,
            }
        if record.get("format") != CHECKPOINT_FORMAT:
            return None, {"path": path.name, "reason": "unsupported_format"}
        for field_name in ("seq", "stage_seq", "config_revision"):
            if isinstance(record.get(field_name), bool) or not isinstance(
                record.get(field_name), int
            ):
                return None, {"path": path.name, "reason": f"invalid_{field_name}"}
        if int(record["seq"]) < 1 or int(record["stage_seq"]) < 1:
            return None, {"path": path.name, "reason": "non_positive_sequence"}
        if record.get("stage") not in STAGES:
            return None, {"path": path.name, "reason": "invalid_stage"}
        if not isinstance(record.get("fingerprint_components"), dict):
            return None, {"path": path.name, "reason": "invalid_components"}
        if not isinstance(record.get("input_shards"), list):
            return None, {"path": path.name, "reason": "invalid_input_shards"}
        for name in ("stage_fingerprint", "input_shard_fingerprint", "config_version", "artifact_digest"):
            if not isinstance(record.get(name), str):
                return None, {"path": path.name, "reason": f"invalid_{name}"}
        checksum = record.get("record_checksum")
        without_checksum = {key: value for key, value in record.items() if key != "record_checksum"}
        if not isinstance(checksum, str) or sha256_obj(without_checksum) != checksum:
            return None, {"path": path.name, "reason": "record_checksum_mismatch"}
        artifact_path = record.get("artifact_path")
        if not isinstance(artifact_path, str):
            return None, {"path": path.name, "reason": "invalid_artifact_path"}
        expected_artifact = f"artifacts/{record['stage']}/{record['artifact_digest']}.json"
        if artifact_path != expected_artifact:
            return None, {"path": path.name, "reason": "artifact_path_mismatch"}
        artifact_file = self.workspace.state_dir / artifact_path
        try:
            payload = read_artifact(artifact_file, stage=record["stage"])
        except PipelineError as exc:
            return None, {
                "path": path.name,
                "reason": "artifact_unreadable",
                "artifact_path": artifact_path,
                "error": exc.message,
            }
        actual_digest = sha256_obj(payload)
        if actual_digest != record["artifact_digest"]:
            return None, {
                "path": path.name,
                "reason": "artifact_digest_mismatch",
                "expected": record["artifact_digest"],
                "actual": actual_digest,
            }
        record = dict(record)
        record["_file"] = path.name
        record["_path"] = str(path)
        record["record_digest"] = sha256_obj(record_without_private(record))
        return record, None

    def artifact_payload(self, record: dict[str, Any]) -> dict[str, Any]:
        path = self.workspace.state_dir / record["artifact_path"]
        return read_artifact(path, stage=record["stage"])

    def publish(
        self,
        *,
        stage: str,
        payload: dict[str, Any],
        stage_fingerprint: str,
        fingerprint_components: dict[str, Any],
        input_shards: list[dict[str, Any]],
        input_shard_fingerprint: str,
        config_version: str,
        config_revision: int,
        expected_seq: int,
        check_stop: Any | None = None,
    ) -> dict[str, Any]:
        """Publish an artifact and checkpoint under an OS file lock.

        All validation happens inside the lock and before the first write.  A
        stale, duplicate, cross-stage, or gap-publishing caller therefore
        cannot overwrite an existing artifact or checkpoint.
        """
        if stage not in STAGES:
            raise invalid_stage(f"unknown stage: {stage}", {"stage": stage})
        self.workspace.ensure()
        artifact_digest, artifact_bytes = make_artifact(stage, payload)
        with file_lock(self.workspace.publish_lock_path):
            state = self._load_unlocked()
            current_seq = state.current_seq
            if current_seq + 1 != expected_seq:
                raise stale_checkpoint(
                    "checkpoint sequence is stale, duplicated, or not contiguous",
                    {
                        "stage": stage,
                        "expected_seq": expected_seq,
                        "current_seq": current_seq,
                        "next_seq": current_seq + 1,
                    },
                )
            self._check_prerequisites_locked(state, stage, fingerprint_components)
            stage_seq = state.stage_counts.get(stage, 0) + 1
            previous_record_digest = (
                state.records[-1]["record_digest"] if state.records else None
            )
            artifact_rel = f"artifacts/{stage}/{artifact_digest}.json"
            record_without_checksum: dict[str, Any] = {
                "format": CHECKPOINT_FORMAT,
                "seq": expected_seq,
                "stage": stage,
                "stage_seq": stage_seq,
                "stage_fingerprint": stage_fingerprint,
                "fingerprint_components": fingerprint_components,
                "input_shards": input_shards,
                "input_shard_fingerprint": input_shard_fingerprint,
                "config_version": config_version,
                "config_revision": config_revision,
                "artifact_digest": artifact_digest,
                "artifact_path": artifact_rel,
                "previous_record_digest": previous_record_digest,
            }
            record = dict(record_without_checksum)
            record["record_checksum"] = sha256_obj(record_without_checksum)

            # A retry after a crash can find an orphan artifact; its bytes are
            # content-addressed, so only accept it if it is byte-identical.
            artifact_path = self.workspace.artifact_path(stage, artifact_digest)
            if artifact_path.exists():
                existing = artifact_path.read_bytes()
                if existing != artifact_bytes:
                    # A damaged artifact is not a valid published object.  The
                    # checkpoint referencing it was already rejected by load;
                    # quarantine the bad bytes and repair the content-addressed
                    # slot while still holding the publication lock.
                    move_to_quarantine(
                        artifact_path,
                        self.workspace.quarantine_dir / "artifacts" / stage,
                    )
                    atomic_write_bytes(artifact_path, artifact_bytes)
            else:
                atomic_write_bytes(artifact_path, artifact_bytes)

            # Quarantine every non-prefix record at or beyond the new sequence
            # before writing, keeping the active ledger gap-free.
            self._quarantine_nonprefix_locked(state, expected_seq)
            checkpoint_path = self.workspace.checkpoint_dir / f"checkpoint-{expected_seq:012d}.json"
            if checkpoint_path.exists():
                move_to_quarantine(checkpoint_path, self.workspace.quarantine_dir)
            atomic_write_bytes(checkpoint_path, canonical_json_line(record))
            if check_stop is not None:
                check_stop()
            return record

    def _check_prerequisites_locked(
        self,
        state: CheckpointState,
        stage: str,
        components: dict[str, Any],
    ) -> None:
        def require(upstream_stage: str) -> dict[str, Any]:
            record = state.item(upstream_stage)
            if record is None:
                raise invalid_stage(
                    f"missing prerequisite stage artifact: {upstream_stage}",
                    {"stage": stage, "missing_prerequisite": upstream_stage},
                )
            return record

        if stage == "normalize":
            return
        if stage == "allocate":
            normalized = require("normalize")
            expected = components.get("normalized_artifact_digest")
            expected_fingerprint = components.get("normalized_stage_fingerprint")
            if normalized["artifact_digest"] != expected or (
                expected_fingerprint is not None
                and normalized["stage_fingerprint"] != expected_fingerprint
            ):
                raise stale_checkpoint(
                    "normalize artifact does not match the allocate stage input",
                    {
                        "stage": stage,
                        "prerequisite": "normalize",
                        "expected_digest": expected,
                        "current_digest": normalized["artifact_digest"],
                        "expected_fingerprint": expected_fingerprint,
                        "current_fingerprint": normalized["stage_fingerprint"],
                    },
                )
            return
        if stage == "assemble":
            normalized = require("normalize")
            allocation = require("allocate")
            if (
                normalized["artifact_digest"] != components.get("normalized_artifact_digest")
                or allocation["artifact_digest"]
                != components.get("allocation_artifact_digest")
                or (
                    components.get("normalized_stage_fingerprint") is not None
                    and normalized["stage_fingerprint"]
                    != components.get("normalized_stage_fingerprint")
                )
                or (
                    components.get("allocation_stage_fingerprint") is not None
                    and allocation["stage_fingerprint"]
                    != components.get("allocation_stage_fingerprint")
                )
            ):
                raise stale_checkpoint(
                    "upstream artifacts do not match the assemble stage input",
                    {
                        "stage": stage,
                        "normalize_expected": components.get("normalized_artifact_digest"),
                        "normalize_current": normalized["artifact_digest"],
                        "allocate_expected": components.get("allocation_artifact_digest"),
                        "allocate_current": allocation["artifact_digest"],
                        "normalize_fingerprint_expected": components.get(
                            "normalized_stage_fingerprint"
                        ),
                        "normalize_fingerprint_current": normalized["stage_fingerprint"],
                        "allocate_fingerprint_expected": components.get(
                            "allocation_stage_fingerprint"
                        ),
                        "allocate_fingerprint_current": allocation["stage_fingerprint"],
                    },
                )
            return
        if stage == "validate":
            assembly = require("assemble")
            expected_fingerprint = components.get("assembly_stage_fingerprint")
            if assembly["artifact_digest"] != components.get(
                "assembly_artifact_digest"
            ) or (
                expected_fingerprint is not None
                and assembly["stage_fingerprint"] != expected_fingerprint
            ):
                raise stale_checkpoint(
                    "assembly artifact does not match the validate stage input",
                    {
                        "stage": stage,
                        "expected_digest": components.get("assembly_artifact_digest"),
                        "current_digest": assembly["artifact_digest"],
                        "expected_fingerprint": expected_fingerprint,
                        "current_fingerprint": assembly["stage_fingerprint"],
                    },
                )

    def _quarantine_nonprefix_locked(self, state: CheckpointState, new_seq: int) -> None:
        active_paths = {record["_path"] for record in state.records}
        for path in self.workspace.checkpoint_files():
            if str(path) in active_paths:
                continue
            try:
                parsed = json.loads(path.read_text(encoding="utf-8"))
                seq = int(parsed.get("seq", 0))
            except Exception:
                seq = -1
            if seq >= new_seq or seq < 0:
                move_to_quarantine(path, self.workspace.quarantine_dir)


def record_without_private(record: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in record.items()
        if key not in {"_file", "_path", "record_digest"}
    }
