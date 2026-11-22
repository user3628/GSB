"""Atomic per-shard stage checkpoints with strict sequence and lock semantics."""

from __future__ import annotations

import os
import re
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping
from urllib.parse import quote, unquote

from .canonical import atomic_write_json, canonical_json_bytes, file_lock, read_json, sha256_json
from .errors import AudioQError


STAGES = ("frame", "metrics", "fusion", "diagnostics")
STAGE_INDEX = {stage: index for index, stage in enumerate(STAGES)}
_FILE_RE = re.compile(r"^(?P<seq>[0-9]{8})-(?P<stage>[a-z]+)\.json$")


@dataclass
class ShardScan:
    shard_id: str
    records: list[dict[str, Any]]
    all_valid_records: list[dict[str, Any]]
    invalid: list[dict[str, Any]]
    duplicate_sequences: list[int]
    gaps: list[int]

    @property
    def last_seq(self) -> int:
        return int(self.records[-1]["seq"]) if self.records else 0

    @property
    def latest_by_stage(self) -> dict[str, dict[str, Any]]:
        result: dict[str, dict[str, Any]] = {}
        for record in self.records:
            result[str(record["stage"])] = record
        return result


class CheckpointManager:
    """Workspace-local checkpoint store.

    Sequence numbers are per shard and become the only ordering authority. The
    store uses one workspace lock, atomic rename publication and ``fsync`` so a
    crash cannot expose a half-written JSON record.
    """

    def __init__(self, state_dir: Path | str) -> None:
        self.state_dir = Path(state_dir)
        self.root = self.state_dir / "checkpoints"
        self.quarantine_dir = self.state_dir / "quarantine"
        self.lock_path = self.state_dir / ".checkpoints.lock"
        self.stop_path = self.state_dir / "STOP_REQUEST.json"
        self.root.mkdir(parents=True, exist_ok=True)
        self.quarantine_dir.mkdir(parents=True, exist_ok=True)
        self.state_dir.mkdir(parents=True, exist_ok=True)

    def _shard_dir(self, shard_id: str) -> Path:
        if not isinstance(shard_id, str) or not shard_id:
            raise AudioQError("E_STALE_CHECKPOINT", "shard id is empty")
        return self.root / quote(shard_id, safe="")

    @staticmethod
    def _record_digest(record: Mapping[str, Any]) -> str:
        return sha256_json({key: value for key, value in record.items() if key != "record_digest"})

    def _load_record(self, path: Path, expected_shard: str) -> tuple[dict[str, Any] | None, str | None]:
        match = _FILE_RE.match(path.name)
        if not match:
            return None, "unexpected_checkpoint_filename"
        try:
            raw = path.read_bytes()
            value = read_json(path)
        except (OSError, AudioQError):
            return None, "unreadable_or_invalid_json"
        if not isinstance(value, dict):
            return None, "record_not_object"
        required = {
            "record_version",
            "seq",
            "shard_id",
            "stage",
            "input_fingerprint",
            "config_fingerprint",
            "artifact_digest",
            "artifact",
            "record_digest",
        }
        if not required.issubset(value):
            return None, "incomplete_record"
        if value.get("record_version") != 1:
            return None, "unsupported_record_version"
        if not isinstance(value.get("seq"), int) or isinstance(value.get("seq"), bool) or value["seq"] <= 0:
            return None, "invalid_sequence"
        if int(match.group("seq")) != value["seq"]:
            return None, "filename_sequence_mismatch"
        if value.get("stage") not in STAGES or match.group("stage") != value.get("stage"):
            return None, "filename_stage_mismatch"
        if value.get("shard_id") != expected_shard:
            return None, "cross_shard_record"
        if not isinstance(value.get("artifact"), dict):
            return None, "artifact_not_object"
        if value.get("artifact_digest") != sha256_json(value["artifact"]):
            return None, "artifact_digest_mismatch"
        if value.get("record_digest") != self._record_digest(value):
            return None, "record_digest_mismatch"
        if not isinstance(value.get("input_fingerprint"), str) or not value["input_fingerprint"]:
            return None, "missing_input_fingerprint"
        if not isinstance(value.get("config_fingerprint"), str) or not value["config_fingerprint"]:
            return None, "missing_config_fingerprint"
        if raw and not raw.endswith(b"\n"):
            return None, "truncated_record"
        return value, None

    def _scan_locked(self, shard_id: str, *, repair: bool) -> ShardScan:
        shard_dir = self._shard_dir(shard_id)
        valid: list[dict[str, Any]] = []
        invalid: list[dict[str, Any]] = []
        paths_by_seq: dict[int, list[Path]] = {}
        if shard_dir.exists():
            for path in sorted(shard_dir.glob("*.json")):
                record, reason = self._load_record(path, shard_id)
                if record is None:
                    invalid.append({"path": str(path), "reason": reason})
                else:
                    valid.append(record)
                    paths_by_seq.setdefault(int(record["seq"]), []).append(path)

        duplicate_sequences = sorted(seq for seq, paths in paths_by_seq.items() if len(paths) > 1)
        duplicate_set = set(duplicate_sequences)
        valid = [record for record in valid if int(record["seq"]) not in duplicate_set]
        for seq in duplicate_sequences:
            for path in paths_by_seq[seq]:
                invalid.append({"path": str(path), "reason": "duplicate_sequence"})
        valid.sort(key=lambda item: int(item["seq"]))

        gaps: list[int] = []
        expected = 1
        consistent: list[dict[str, Any]] = []
        for record in valid:
            seq = int(record["seq"])
            if seq != expected:
                gaps.extend(range(expected, seq))
                break
            consistent.append(record)
            expected += 1
        if consistent and valid:
            last_consistent_seq = int(consistent[-1]["seq"])
            beyond = [record for record in valid if int(record["seq"]) > last_consistent_seq]
            for record in beyond:
                invalid.append(
                    {
                        "path": str(shard_dir / f"{int(record['seq']):08d}-{record['stage']}.json"),
                        "reason": "after_sequence_gap",
                    }
                )

        if repair and invalid:
            seen_paths: set[str] = set()
            for issue in invalid:
                source = Path(issue["path"])
                key = str(source)
                if key in seen_paths or not source.exists():
                    continue
                seen_paths.add(key)
                target = self.quarantine_dir / quote(shard_id, safe="") / f"{source.name}.{os.getpid()}.bad"
                target.parent.mkdir(parents=True, exist_ok=True)
                counter = 0
                while target.exists():
                    counter += 1
                    target = target.with_name(f"{source.name}.{os.getpid()}.{counter}.bad")
                os.replace(source, target)
        return ShardScan(
            shard_id=shard_id,
            records=consistent,
            all_valid_records=valid,
            invalid=invalid,
            duplicate_sequences=duplicate_sequences,
            gaps=sorted(set(gaps)),
        )

    def scan_shard(self, shard_id: str, *, repair: bool = False) -> ShardScan:
        if repair:
            with file_lock(self.lock_path):
                return self._scan_locked(shard_id, repair=True)
        return self._scan_locked(shard_id, repair=False)

    def list_shards(self) -> list[str]:
        result = []
        for path in sorted(self.root.iterdir()):
            if path.is_dir():
                result.append(unquote(path.name))
        return result

    def latest_by_stage(self, shard_id: str, *, repair: bool = False) -> dict[str, dict[str, Any]]:
        return self.scan_shard(shard_id, repair=repair).latest_by_stage

    def latest_record(self, shard_id: str, *, repair: bool = False) -> dict[str, Any] | None:
        records = self.scan_shard(shard_id, repair=repair).records
        return records[-1] if records else None

    def publish_checkpoint(
        self,
        *,
        shard_id: str,
        stage: str,
        input_fingerprint: str,
        config_fingerprint: str,
        artifact: Mapping[str, Any],
        version_components: Mapping[str, Any] | None = None,
        prerequisites: Mapping[str, str] | None = None,
        expected_seq: int | None = None,
        allow_idempotent: bool = False,
        metadata: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        if stage not in STAGES:
            raise AudioQError(
                "E_INVALID_STAGE",
                "unknown stage",
                shard_id=shard_id,
                stage=stage,
                observed={"stage": stage, "allowed": list(STAGES)},
            )
        if not isinstance(artifact, Mapping):
            raise AudioQError("E_INVALID_STAGE", "artifact must be an object", shard_id=shard_id, stage=stage)
        artifact_dict = dict(artifact)
        artifact_shard = artifact_dict.get("shard_id")
        if artifact_shard is not None and artifact_shard != shard_id:
            raise AudioQError(
                "E_STALE_CHECKPOINT",
                "artifact belongs to a different shard",
                shard_id=shard_id,
                stage=stage,
                observed={"target_shard": shard_id, "artifact_shard": artifact_shard, "reason": "cross_shard"},
            )
        if not isinstance(input_fingerprint, str) or not input_fingerprint:
            raise AudioQError("E_STALE_CHECKPOINT", "input fingerprint is required", shard_id=shard_id, stage=stage)
        if not isinstance(config_fingerprint, str) or not config_fingerprint:
            raise AudioQError("E_STALE_CHECKPOINT", "config fingerprint is required", shard_id=shard_id, stage=stage)

        with file_lock(self.lock_path):
            scan = self._scan_locked(shard_id, repair=True)
            latest = scan.latest_by_stage
            upstream = STAGES[: STAGE_INDEX[stage]]
            for upstream_stage in upstream:
                if upstream_stage not in latest:
                    raise AudioQError(
                        "E_INVALID_STAGE",
                        "a prerequisite stage artifact is missing",
                        shard_id=shard_id,
                        stage=stage,
                        observed={"missing_stage": upstream_stage, "required_stages": list(upstream)},
                    )
                expected_digest = latest[upstream_stage]["artifact_digest"]
                if prerequisites is not None:
                    supplied = prerequisites.get(upstream_stage)
                    if supplied is None:
                        raise AudioQError(
                            "E_INVALID_STAGE",
                            "prerequisite artifact digest was not supplied",
                            shard_id=shard_id,
                            stage=stage,
                            observed={"missing_stage": upstream_stage},
                        )
                    if supplied != expected_digest:
                        raise AudioQError(
                            "E_STALE_CHECKPOINT",
                            "prerequisite artifact has been superseded",
                            shard_id=shard_id,
                            stage=stage,
                            observed={
                                "upstream_stage": upstream_stage,
                                "supplied_digest": supplied,
                                "latest_digest": expected_digest,
                                "reason": "stale_prerequisite",
                            },
                        )
            current_stage_record = latest.get(stage)
            if current_stage_record is not None:
                for existing in scan.all_valid_records:
                    if existing["stage"] != stage or existing["input_fingerprint"] != input_fingerprint:
                        continue
                    # Only the latest active record for this stage is a duplicate.
                    # An older matching record may be republished after a config
                    # change is reverted; otherwise old results could masquerade
                    # as the current version through an idempotent return.
                    if existing["seq"] != current_stage_record["seq"]:
                        continue
                    if allow_idempotent and existing["artifact_digest"] == sha256_json(artifact_dict):
                        return existing
                    raise AudioQError(
                        "E_STALE_CHECKPOINT",
                        "a checkpoint for this stage and input already exists",
                        shard_id=shard_id,
                        stage=stage,
                        observed={
                            "reason": "duplicate_stage_fingerprint",
                            "existing_seq": existing["seq"],
                            "input_fingerprint": input_fingerprint,
                        },
                    )

            next_seq = scan.last_seq + 1
            if expected_seq is not None:
                if isinstance(expected_seq, bool) or not isinstance(expected_seq, int) or expected_seq != next_seq:
                    raise AudioQError(
                        "E_STALE_CHECKPOINT",
                        "checkpoint sequence is stale or non-contiguous",
                        shard_id=shard_id,
                        stage=stage,
                        observed={
                            "expected_seq": expected_seq,
                            "next_seq": next_seq,
                            "last_seq": scan.last_seq,
                            "reason": "stale_sequence",
                        },
                    )
            record: dict[str, Any] = {
                "record_version": 1,
                "seq": next_seq,
                "shard_id": shard_id,
                "stage": stage,
                "input_fingerprint": input_fingerprint,
                "config_fingerprint": config_fingerprint,
                "artifact_digest": sha256_json(artifact_dict),
                "artifact": artifact_dict,
                "version_components": dict(version_components or {}),
                "prerequisites": dict(prerequisites or {}),
                "metadata": dict(metadata or {}),
            }
            record["record_digest"] = self._record_digest(record)
            path = self._shard_dir(shard_id) / f"{next_seq:08d}-{stage}.json"
            path.parent.mkdir(parents=True, exist_ok=True)
            try:
                atomic_write_json(path, record, pretty=False)
            except OSError as exc:
                raise AudioQError(
                    "E_IO",
                    "failed to atomically publish checkpoint",
                    shard_id=shard_id,
                    stage=stage,
                    observed={"path": str(path), "reason": str(exc)},
                ) from exc
            return record

    publish = publish_checkpoint

    def read_artifact(self, shard_id: str, stage: str, *, repair: bool = False) -> dict[str, Any] | None:
        record = self.latest_by_stage(shard_id, repair=repair).get(stage)
        return dict(record["artifact"]) if record else None

    def summary(self, shards: Iterable[str] | None = None, *, repair: bool = False) -> dict[str, Any]:
        shard_ids = list(shards) if shards is not None else self.list_shards()
        items: list[dict[str, Any]] = []
        for shard_id in shard_ids:
            scan = self.scan_shard(shard_id, repair=repair)
            stages = {}
            latest = scan.latest_by_stage
            for stage in STAGES:
                record = latest.get(stage)
                stages[stage] = (
                    {
                        "present": True,
                        "seq": record["seq"],
                        "input_fingerprint": record["input_fingerprint"],
                        "config_fingerprint": record["config_fingerprint"],
                        "artifact_digest": record["artifact_digest"],
                    }
                    if record
                    else {"present": False}
                )
            items.append(
                {
                    "shard_id": shard_id,
                    "consistent_last_seq": scan.last_seq,
                    "stages": stages,
                    "invalid_records": scan.invalid,
                    "duplicate_sequences": scan.duplicate_sequences,
                    "gaps": scan.gaps,
                }
            )
        return {"checkpoint_schema_version": 1, "shards": items}

    def request_stop(self, *, reason: str = "user_request") -> dict[str, Any]:
        payload = {"stop_requested": True, "reason": reason}
        atomic_write_json(self.stop_path, payload, pretty=True)
        return payload

    def stop_requested(self) -> bool:
        return self.stop_path.exists()

    def stop_payload(self) -> dict[str, Any] | None:
        if not self.stop_path.exists():
            return None
        try:
            value = read_json(self.stop_path)
            return value if isinstance(value, dict) else {"stop_requested": True, "reason": "unreadable"}
        except AudioQError:
            return {"stop_requested": True, "reason": "unreadable"}

    def clear_stop(self) -> None:
        try:
            self.stop_path.unlink()
        except FileNotFoundError:
            pass

    def clear_all(self) -> None:
        with file_lock(self.lock_path):
            if self.root.exists():
                shutil.rmtree(self.root)
            self.root.mkdir(parents=True, exist_ok=True)
            self.quarantine_dir.mkdir(parents=True, exist_ok=True)
            self.clear_stop()
