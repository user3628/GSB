"""Atomic stage checkpoints, validation, and concurrent publish protection."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from .config import STAGES, STAGE_NAMES
from .errors import error
from .locking import FileLock
from .utils import atomic_write_json, canonical_json_bytes, load_json, sha256_bytes


_CHECKPOINT_NAME_RE = re.compile(r"^(\d{8})-([a-z_]+)\.json(?:\.invalidated)?$")


def _record_digest(record: dict[str, Any]) -> str:
    unsigned = {key: value for key, value in record.items() if key != "record_digest"}
    return sha256_bytes(canonical_json_bytes(unsigned))


class CheckpointStore:
    def __init__(self, artifacts_dir: Path):
        self.artifacts_dir = artifacts_dir.resolve()
        self.checkpoint_dir = self.artifacts_dir / "checkpoints"
        self.archive_dir = self.checkpoint_dir / "archive"
        self.lock_path = self.artifacts_dir / ".checkpoint.lock"
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self.archive_dir.mkdir(parents=True, exist_ok=True)

    def _read_candidates(self) -> tuple[list[tuple[Path, dict[str, Any]]], list[dict[str, Any]]]:
        candidates: list[tuple[Path, dict[str, Any]]] = []
        ignored: list[dict[str, Any]] = []
        for path in sorted(self.checkpoint_dir.glob("*.json")):
            payload = load_json(path)
            if not isinstance(payload, dict):
                ignored.append({"file": path.name, "reason": "unparseable_or_incomplete"})
                continue
            required = {
                "schema_version",
                "seq",
                "stage",
                "stage_number",
                "input_digest",
                "dataset_digest",
                "config_version",
                "artifact_digest",
                "stage_fingerprint",
                "previous_record_digest",
                "record_digest",
            }
            if not required.issubset(payload):
                ignored.append({"file": path.name, "reason": "missing_fields"})
                continue
            if payload.get("schema_version") != 1:
                ignored.append({"file": path.name, "reason": "schema_mismatch"})
                continue
            if payload.get("record_digest") != _record_digest(payload):
                ignored.append({"file": path.name, "reason": "record_digest_mismatch"})
                continue
            if not isinstance(payload.get("seq"), int) or payload["seq"] < 1:
                ignored.append({"file": path.name, "reason": "invalid_sequence"})
                continue
            if payload.get("stage") not in STAGE_NAMES or STAGE_NAMES[payload["stage"]] != payload.get("stage_number"):
                ignored.append({"file": path.name, "reason": "invalid_stage_identity"})
                continue
            match = _CHECKPOINT_NAME_RE.match(path.name)
            if match is None or int(match.group(1)) != payload["seq"] or match.group(2) != payload["stage"]:
                ignored.append({"file": path.name, "reason": "filename_identity_mismatch"})
                continue
            candidates.append((path, payload))
        candidates.sort(key=lambda item: (item[1]["seq"], item[0].name))
        counts: dict[int, int] = {}
        for _, payload in candidates:
            counts[payload["seq"]] = counts.get(payload["seq"], 0) + 1
        unique: list[tuple[Path, dict[str, Any]]] = []
        for path, payload in candidates:
            if counts[payload["seq"]] > 1:
                ignored.append({"file": path.name, "reason": "duplicate_sequence"})
            else:
                unique.append((path, payload))
        return unique, ignored

    def active(self) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Return the longest valid stage prefix and ignored record diagnostics."""

        unique, ignored = self._read_candidates()
        active: list[dict[str, Any]] = []
        expected_previous = None
        for path, payload in unique:
            expected_stage = len(active) + 1
            if expected_stage > 4:
                ignored.append({"file": path.name, "reason": "stage_overflow"})
                break
            if payload["stage_number"] != expected_stage:
                ignored.append({"file": path.name, "reason": "stage_gap_or_misorder"})
                break
            if active and payload["seq"] <= active[-1]["seq"]:
                ignored.append({"file": path.name, "reason": "non_monotonic_sequence"})
                break
            if expected_previous is None:
                if payload["previous_record_digest"] != "0" * 64:
                    ignored.append({"file": path.name, "reason": "broken_chain_start"})
                    break
            elif payload["previous_record_digest"] != expected_previous:
                ignored.append({"file": path.name, "reason": "broken_record_chain"})
                break
            active.append(payload)
            expected_previous = payload["record_digest"]
        return active, ignored

    def highwater(self) -> int:
        maximum = 0
        for directory in (self.checkpoint_dir, self.archive_dir):
            for path in directory.iterdir():
                match = _CHECKPOINT_NAME_RE.match(path.name)
                if match is not None:
                    maximum = max(maximum, int(match.group(1)))
        return maximum

    def publish(
        self,
        *,
        stage: str,
        input_digest: str,
        config_version: str,
        artifact_digest: str,
        stage_fingerprint: str,
        dataset_digest: str | None = None,
        expected_seq: int | None = None,
    ) -> dict[str, Any]:
        if stage not in STAGE_NAMES:
            raise error("E_INVALID_STAGE", "未知阶段名", stage=stage)
        stage_number = STAGE_NAMES[stage]
        with FileLock(self.lock_path):
            active, _ = self.active()
            highwater = self.highwater()
            next_seq = highwater + 1
            if expected_seq is not None and expected_seq != next_seq:
                raise error(
                    "E_STALE_CHECKPOINT",
                    "检查点序号已落后或不是下一连续序号",
                    requested_seq=expected_seq,
                    expected_seq=next_seq,
                    highwater=highwater,
                )
            if stage_number <= len(active):
                raise error(
                    "E_STALE_CHECKPOINT",
                    "阶段已发布或发布顺序落后",
                    stage=stage,
                    active_stages=[record["stage"] for record in active],
                )
            if stage_number != len(active) + 1:
                raise error(
                    "E_INVALID_STAGE",
                    "非法阶段跳转或缺少前置检查点",
                    stage=stage,
                    expected_stage=STAGES.get(len(active) + 1),
                    active_stages=[record["stage"] for record in active],
                )
            previous_digest = active[-1]["record_digest"] if active else "0" * 64
            record: dict[str, Any] = {
                "schema_version": 1,
                "seq": next_seq,
                "stage": stage,
                "stage_number": stage_number,
                "input_digest": input_digest,
                "dataset_digest": dataset_digest or input_digest,
                "config_version": config_version,
                "artifact_digest": artifact_digest,
                "stage_fingerprint": stage_fingerprint,
                "previous_record_digest": previous_digest,
            }
            record["record_digest"] = _record_digest(record)
            path = self.checkpoint_dir / f"{next_seq:08d}-{stage}.json"
            atomic_write_json(path, record)
            return record

    def archive_from_stage(self, earliest_stage: int) -> list[dict[str, Any]]:
        """Archive affected active records while preserving global high-water marks."""

        if not 1 <= earliest_stage <= 4:
            raise error("E_INVALID_STAGE", "非法失效阶段", stage=earliest_stage)
        with FileLock(self.lock_path):
            active, _ = self.active()
            archived: list[dict[str, Any]] = []
            for record in active:
                if record["stage_number"] < earliest_stage:
                    continue
                path = self.checkpoint_dir / f"{record['seq']:08d}-{record['stage']}.json"
                destination = self.archive_dir / f"{record['seq']:08d}-{record['stage']}.json.invalidated"
                if path.exists():
                    path.replace(destination)
                archived.append(record)
            return archived

    def diagnostics(self) -> dict[str, Any]:
        active, ignored = self.active()
        return {
            "active": active,
            "ignored": ignored,
            "highwater": self.highwater(),
        }
