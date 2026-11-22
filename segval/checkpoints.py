"""Atomic, sequenced stage checkpoints with cross-process publication locking."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .canonical import atomic_write_bytes, canonical_bytes, digest_obj, locked, read_json
from .errors import invalid_stage, stale_checkpoint
from .shards import load_shard_file


STAGE_ORDINAL = {"decode": 1, "match": 2, "aggregate": 3}


class CheckpointStore:
    def __init__(self, workspace: Path) -> None:
        self.workspace = Path(workspace)
        self.checkpoint_dir = self.workspace / "checkpoints"
        self.journal_dir = self.checkpoint_dir / "_journal"
        self.lock_path = self.workspace / ".checkpoint.lock"

    def _current_path(self, shard_index: int, stage: str) -> Path:
        return self.checkpoint_dir / f"shard-{shard_index:04d}" / f"{stage}.json"

    def _journal_path(self, sequence: int) -> Path:
        return self.journal_dir / f"{sequence:012d}.json"

    def _read_journal(self) -> list[dict[str, Any]]:
        if not self.journal_dir.exists():
            return []
        records: list[dict[str, Any]] = []
        for path in sorted(self.journal_dir.glob("*.json")):
            try:
                record = read_json(path)
            except Exception:
                continue
            if isinstance(record, dict) and isinstance(record.get("sequence"), int):
                records.append(record)
        return records

    def _read_current_files(self) -> list[dict[str, Any]]:
        if not self.checkpoint_dir.exists():
            return []
        records: list[dict[str, Any]] = []
        for path in sorted(self.checkpoint_dir.glob("shard-*/[a-z]*.json")):
            try:
                record = read_json(path)
            except Exception:
                continue
            if isinstance(record, dict) and isinstance(record.get("sequence"), int):
                records.append(record)
        return records

    def _current_map(self) -> dict[tuple[int, str], dict[str, Any]]:
        latest: dict[tuple[int, str], dict[str, Any]] = {}
        for record in self._read_journal() + self._read_current_files():
            key = (int(record["shard_index"]), record["stage"])
            existing = latest.get(key)
            if existing is None or int(record["sequence"]) > int(existing["sequence"]):
                latest[key] = record
        return latest

    def list(self) -> list[dict[str, Any]]:
        records = list(self._current_map().values())
        records.sort(
            key=lambda item: (
                int(item["shard_index"]),
                STAGE_ORDINAL.get(item["stage"], 99),
                int(item["sequence"]),
            )
        )
        return records

    def get(self, shard_index: int, stage: str) -> dict[str, Any] | None:
        return self._current_map().get((int(shard_index), stage))

    def get_by_shard_id(self, shard_id: str, stage: str) -> dict[str, Any] | None:
        for record in self.list():
            if record["shard_id"] == shard_id and record["stage"] == stage:
                return record
        return None

    def history(self) -> list[dict[str, Any]]:
        records = self._read_journal()
        records.sort(key=lambda item: int(item["sequence"]))
        return records

    def max_sequence(self) -> int:
        records = self._read_journal() + self._read_current_files()
        return max((int(record["sequence"]) for record in records), default=0)

    def publish(
        self,
        *,
        shard_id: str,
        shard_index: int,
        stage: str,
        input_fingerprint: str,
        config_fingerprint: str,
        config_version: str,
        artifact: dict[str, Any],
        upstream_digest: str | None = None,
        expected_prev_sequence: int | None = None,
    ) -> dict[str, Any]:
        if stage not in STAGE_ORDINAL:
            raise invalid_stage("Only decode, match, and aggregate stage checkpoints exist.", {"stage": stage})
        if not isinstance(artifact, dict):
            raise invalid_stage("Checkpoint artifact must be an object.", {"stage": stage})

        with locked(self.lock_path), locked(self.workspace / ".shard.lock"):
            current_map = self._current_map()
            journal = self._read_journal()
            max_sequence = max(
                [int(item["sequence"]) for item in journal]
                + [int(item["sequence"]) for item in current_map.values()]
                + [0]
            )
            if expected_prev_sequence is not None and expected_prev_sequence != max_sequence:
                raise stale_checkpoint(
                    "Publication expected a different preceding sequence.",
                    {
                        "expected_prev_sequence": expected_prev_sequence,
                        "actual_prev_sequence": max_sequence,
                        "shard_id": shard_id,
                        "stage": stage,
                    },
                )

            shard_file = self.workspace / "shards" / f"shard-{int(shard_index):04d}.json"
            current_shard_id = None
            if shard_file.exists():
                shard = load_shard_file(shard_file)
                from .shards import shard_content_digest

                current_shard_id = shard["shard_id"]
                current_content_digest = shard_content_digest(shard)
            else:
                current_content_digest = artifact.get("shard_content_digest")
            if current_shard_id is not None and current_shard_id != shard_id:
                raise stale_checkpoint(
                    "The shard_id does not match the current shard at this index.",
                    {
                        "shard_index": shard_index,
                        "current_shard_id": current_shard_id,
                        "incoming_shard_id": shard_id,
                    },
                )
            incoming_content = artifact.get("shard_content_digest")
            if (
                current_content_digest is not None
                and incoming_content is not None
                and incoming_content != current_content_digest
            ):
                raise stale_checkpoint(
                    "The shard changed after this worker read it.",
                    {
                        "shard_id": shard_id,
                        "stage": stage,
                        "incoming_shard_content_digest": incoming_content,
                        "current_shard_content_digest": current_content_digest,
                    },
                )

            if stage != "decode":
                prior_stage = "decode" if stage == "match" else "match"
                prior = current_map.get((int(shard_index), prior_stage))
                if prior is None:
                    raise invalid_stage(
                        f"Cannot publish {stage} before {prior_stage}.",
                        {
                            "shard_id": shard_id,
                            "shard_index": shard_index,
                            "stage": stage,
                            "missing_stage": prior_stage,
                        },
                    )
                expected_upstream = prior.get("artifact_digest")
                if upstream_digest is not None and upstream_digest != expected_upstream:
                    raise invalid_stage(
                        f"{prior_stage} changed; {stage} must be rebuilt from the current upstream artifact.",
                        {
                            "shard_id": shard_id,
                            "shard_index": shard_index,
                            "stage": stage,
                            "upstream_digest": upstream_digest,
                            "current_upstream_digest": expected_upstream,
                        },
                    )

            existing = current_map.get((int(shard_index), stage))
            if existing is not None:
                if existing.get("shard_id") != shard_id:
                    raise stale_checkpoint(
                        "shard_id cannot change for a checkpointed shard_index.",
                        {
                            "shard_index": shard_index,
                            "existing_shard_id": existing.get("shard_id"),
                            "incoming_shard_id": shard_id,
                        },
                    )
                if existing.get("input_fingerprint") == input_fingerprint:
                    raise stale_checkpoint(
                        "Duplicate or stale publication for an unchanged stage input.",
                        {
                            "shard_id": shard_id,
                            "stage": stage,
                            "existing_sequence": existing.get("sequence"),
                            "input_fingerprint": input_fingerprint,
                        },
                    )
            artifact_digest = artifact.get("artifact_digest")
            if not isinstance(artifact_digest, str) or not artifact_digest:
                artifact_digest = digest_obj(artifact)
            sequence = max_sequence + 1
            record = {
                "schema_version": 1,
                "sequence": sequence,
                "shard_id": shard_id,
                "shard_index": int(shard_index),
                "stage": stage,
                "stage_ordinal": STAGE_ORDINAL[stage],
                "input_fingerprint": input_fingerprint,
                "config_fingerprint": config_fingerprint,
                "config_version": config_version,
                "artifact_digest": artifact_digest,
                "upstream_digest": upstream_digest,
                "shard_content_digest": artifact.get("shard_content_digest", current_content_digest),
                "artifact": artifact,
            }
            payload = canonical_bytes(record)
            atomic_write_bytes(self._journal_path(sequence), payload)
            atomic_write_bytes(self._current_path(int(shard_index), stage), payload)
            return record

    def status(self) -> dict[str, Any]:
        records = self.list()
        journal = self.history()
        return {
            "record_count": len(records),
            "journal_count": len(journal),
            "max_sequence": max((int(item["sequence"]) for item in journal), default=0),
            "last_records": records[-8:],
        }


def checkpoint_summary(record: dict[str, Any]) -> dict[str, Any]:
    return {
        key: record.get(key)
        for key in (
            "sequence",
            "shard_id",
            "shard_index",
            "stage",
            "stage_ordinal",
            "input_fingerprint",
            "config_version",
            "artifact_digest",
            "upstream_digest",
        )
    }

