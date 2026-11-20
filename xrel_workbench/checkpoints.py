from __future__ import annotations

from pathlib import Path
from typing import Any

from .errors import BusinessError, E_INVALID_STAGE, E_STALE_CHECKPOINT
from .locking import FileLock
from .util import atomic_write_json, digest_file, digest_value, read_json


VALID_STAGES = {"stage1", "stage2", "stage3", "stage4", "stage4_aggregate"}
STAGE_PREREQUISITES = {
    "stage1": None,
    "stage2": "stage1",
    "stage3": "stage2",
    "stage4": "stage3",
    "stage4_aggregate": "stage4",
}


class CheckpointStore:
    def __init__(self, workspace_root: Path) -> None:
        self.workspace_root = workspace_root.resolve()
        self.directory = self.workspace_root / "state" / "checkpoints"
        self.lock_path = self.workspace_root / "state" / "checkpoint.lock"
        self.directory.mkdir(parents=True, exist_ok=True)

    def records(self) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        for path in sorted(self.directory.glob("*.json")):
            try:
                record = read_json(path)
            except (OSError, ValueError) as exc:
                raise BusinessError(E_STALE_CHECKPOINT, f"corrupt checkpoint record {path.name}: {exc}") from exc
            if not isinstance(record, dict):
                raise BusinessError(E_STALE_CHECKPOINT, f"checkpoint record is not an object: {path.name}")
            record["_path"] = path.name
            records.append(record)
        records.sort(key=lambda item: int(item["seq"]))
        expected = 1
        previous_digest = None
        for record in records:
            if int(record["seq"]) != expected:
                raise BusinessError(
                    E_STALE_CHECKPOINT,
                    f"non-contiguous checkpoint sequence: expected {expected}, found {record['seq']}",
                )
            if previous_digest is not None and record.get("previous_record_digest") != previous_digest:
                raise BusinessError(E_STALE_CHECKPOINT, f"broken checkpoint chain at seq {record['seq']}")
            payload = {key: value for key, value in record.items() if key != "_path"}
            previous_digest = digest_value(payload)
            expected += 1
        return records

    def latest_by_unit(self) -> dict[tuple[str, str], dict[str, Any]]:
        latest: dict[tuple[str, str], dict[str, Any]] = {}
        for record in self.records():
            latest[(record["stage"], record["shard"])] = record
        return latest

    def status(self) -> dict[str, Any]:
        records = self.records()
        return {
            "count": len(records),
            "last_seq": int(records[-1]["seq"]) if records else 0,
            "records": [
                {key: value for key, value in record.items() if key != "_path"}
                for record in records
            ],
        }

    def latest(
        self,
        stage: str,
        shard: str,
        *,
        fingerprint: str | None = None,
    ) -> dict[str, Any] | None:
        latest = self.latest_by_unit().get((stage, shard))
        if latest is None:
            return None
        if fingerprint is not None and latest.get("fingerprint") != fingerprint:
            return None
        return latest

    def _artifact_path(self, relative_path: str) -> Path:
        return self.workspace_root / relative_path

    def _validate_prerequisites(
        self,
        latest: dict[tuple[str, str], dict[str, Any]],
        stage: str,
        shard: str,
        input_digest: str,
        expected_shards: list[str] | None,
    ) -> None:
        prerequisite = STAGE_PREREQUISITES[stage]
        if prerequisite is None:
            return
        if stage == "stage4_aggregate":
            shards = expected_shards or sorted(
                key[1] for key in latest if key[0] == "stage4" and key[1] != "ALL"
            )
            if not shards:
                raise BusinessError(E_INVALID_STAGE, "stage4 aggregate has no stage4 shard checkpoints")
            digest_map: dict[str, str] = {}
            for shard_name in shards:
                record = latest.get(("stage4", shard_name))
                if record is None:
                    raise BusinessError(
                        E_INVALID_STAGE,
                        f"missing prerequisite stage4 checkpoint for shard {shard_name}",
                    )
                artifact_path = self._artifact_path(record["artifact_path"])
                if not artifact_path.exists() or digest_file(artifact_path) != record["artifact_digest"]:
                    raise BusinessError(
                        E_INVALID_STAGE,
                        f"missing or modified prerequisite artifact: {record['artifact_path']}",
                    )
                digest_map[shard_name] = record["artifact_digest"]
            expected_digest = digest_value(digest_map)
            if input_digest != expected_digest:
                raise BusinessError(
                    E_STALE_CHECKPOINT,
                    "stage4 aggregate input digest is misaligned with prerequisite artifacts",
                    {"expected": expected_digest, "actual": input_digest},
                )
            return
        record = latest.get((prerequisite, shard))
        if record is None:
            raise BusinessError(
                E_INVALID_STAGE,
                f"missing prerequisite checkpoint {prerequisite}/{shard}",
            )
        artifact_path = self._artifact_path(record["artifact_path"])
        if not artifact_path.exists() or digest_file(artifact_path) != record["artifact_digest"]:
            raise BusinessError(
                E_INVALID_STAGE,
                f"missing or modified prerequisite artifact: {record['artifact_path']}",
            )
        if input_digest != record["artifact_digest"]:
            raise BusinessError(
                E_STALE_CHECKPOINT,
                f"{stage}/{shard} input digest is not aligned with {prerequisite}/{shard}",
                {"expected": record["artifact_digest"], "actual": input_digest},
            )

    def publish(
        self,
        *,
        stage: str,
        shard: str,
        input_digest: str,
        config_version: str,
        artifact_path: str,
        fingerprint: str,
        expected_seq: int | None = None,
        expected_shards: list[str] | None = None,
    ) -> dict[str, Any]:
        if stage not in VALID_STAGES:
            raise BusinessError(E_INVALID_STAGE, f"unknown stage: {stage}")
        artifact = self._artifact_path(artifact_path)
        if not artifact.exists():
            raise BusinessError(E_INVALID_STAGE, f"artifact does not exist: {artifact_path}")
        artifact_digest = digest_file(artifact)
        with FileLock(self.lock_path):
            records = self.records()
            latest = {
                (record["stage"], record["shard"]): record for record in records
            }
            last_seq = int(records[-1]["seq"]) if records else 0
            next_seq = last_seq + 1
            if expected_seq is not None and int(expected_seq) != next_seq:
                raise BusinessError(
                    E_STALE_CHECKPOINT,
                    "checkpoint sequence is stale or non-contiguous",
                    {"expected": next_seq, "actual": expected_seq},
                )
            duplicate = latest.get((stage, shard))
            if (
                duplicate is not None
                and duplicate.get("input_digest") == input_digest
                and duplicate.get("artifact_digest") == artifact_digest
                and duplicate.get("fingerprint") == fingerprint
            ):
                raise BusinessError(
                    E_STALE_CHECKPOINT,
                    f"duplicate checkpoint publication for {stage}/{shard}",
                    {"existing_seq": duplicate["seq"]},
                )
            self._validate_prerequisites(latest, stage, shard, input_digest, expected_shards)
            previous_digest = None
            if records:
                previous_digest = digest_value(
                    {key: value for key, value in records[-1].items() if key != "_path"}
                )
            record = {
                "seq": next_seq,
                "stage": stage,
                "shard": shard,
                "input_digest": input_digest,
                "config_version": config_version,
                "artifact_digest": artifact_digest,
                "artifact_path": Path(artifact_path).as_posix(),
                "fingerprint": fingerprint,
                "previous_record_digest": previous_digest,
            }
            target = self.directory / f"{next_seq:08d}-{stage}-{shard}.json"
            atomic_write_json(target, record, indent=2)
            return record

    def verify_artifact(self, stage: str, shard: str, fingerprint: str) -> dict[str, Any] | None:
        record = self.latest(stage, shard, fingerprint=fingerprint)
        if record is None:
            return None
        artifact = self._artifact_path(record["artifact_path"])
        if not artifact.exists() or digest_file(artifact) != record["artifact_digest"]:
            return None
        return record
