"""Atomic checkpoint ledger with real file locks and replacement detection."""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

from .canonical import (
    FileLock,
    atomic_write_bytes,
    atomic_write_json,
    canonical_json_bytes,
    digest_obj,
    read_json,
    resolve_inside,
    sha256_bytes,
    sha256_file,
)
from .errors import TelemetryError
from .fingerprints import STAGE_DEPENDENCIES, STAGE_RANK


ZERO_HASH = "0" * 64
REQUIRED_RECORD_FIELDS = (
    "seq",
    "run_id",
    "stage",
    "config_version",
    "config_fingerprint",
    "input_digest",
    "artifact_digest",
    "fingerprint",
    "catalog",
)


def record_hash(record: dict[str, Any]) -> str:
    content = dict(record)
    content.pop("record_hash", None)
    return digest_obj(content)


class CheckpointStore:
    def __init__(self, root: Path | str) -> None:
        self.root = Path(root).resolve()
        self.state_dir = self.root / "state"
        self.ledger_path = self.state_dir / "checkpoints.jsonl"
        self.anchor_path = self.state_dir / "checkpoints.anchor.json"
        self.pending_path = self.state_dir / "checkpoint.pending.json"
        self.lock_path = self.state_dir / "publish.lock"

    # ------------------------------------------------------------------ reads
    def records(self) -> list[dict[str, Any]]:
        with FileLock(self.lock_path, purpose="checkpoint ledger"):
            self._recover_pending_locked()
            records, _ = self._load_locked()
            return copy.deepcopy(records)

    def head(self) -> dict[str, Any] | None:
        records = self.records()
        return records[-1] if records else None

    def next_seq(self) -> int:
        head = self.head()
        return 1 if head is None else int(head["seq"]) + 1

    def sequence(self) -> list[int]:
        return [int(record["seq"]) for record in self.records()]

    def summary(self) -> dict[str, Any]:
        records = self.records()
        return {
            "record_count": len(records),
            "sequence": [int(record["seq"]) for record in records],
            "head_seq": int(records[-1]["seq"]) if records else None,
            "head_stage": records[-1]["stage"] if records else None,
            "head_run_id": records[-1]["run_id"] if records else None,
            "records": [
                {
                    "seq": record["seq"],
                    "run_id": record["run_id"],
                    "stage": record["stage"],
                    "config_version": record.get("config_version"),
                    "config_fingerprint": record.get("config_fingerprint"),
                    "input_digest": record.get("input_digest"),
                    "artifact_digest": record.get("artifact_digest"),
                    "fingerprint": record.get("fingerprint"),
                }
                for record in records
            ],
        }

    def verify(self) -> dict[str, Any]:
        with FileLock(self.lock_path, purpose="checkpoint ledger"):
            self._recover_pending_locked()
            records, anchor = self._load_locked()
            self._validate_catalog_files(records[-1]["catalog"]) if records else None
            return {
                "valid": True,
                "record_count": len(records),
                "sequence": [record["seq"] for record in records],
                "anchor": anchor,
            }

    # --------------------------------------------------------------- publish
    def publish(self, record: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(record, dict):
            raise TelemetryError("E_INVALID_STAGE", "检查点记录必须是对象 / record must be an object", {})
        missing = [field for field in REQUIRED_RECORD_FIELDS if field not in record]
        if missing:
            raise TelemetryError(
                "E_INVALID_STAGE",
                "检查点记录缺少字段 / checkpoint record is incomplete",
                {"missing_fields": missing},
            )
        stage = record.get("stage")
        if stage not in STAGE_RANK:
            raise TelemetryError(
                "E_INVALID_STAGE",
                "未知阶段 / unknown stage",
                {"stage": stage, "allowed": sorted(STAGE_RANK)},
            )
        seq = record.get("seq")
        if isinstance(seq, bool) or not isinstance(seq, int):
            raise TelemetryError(
                "E_STALE_CHECKPOINT",
                "检查点序号必须为整数 / sequence must be an integer",
                {"kind": "invalid_sequence", "seq": seq},
            )

        with FileLock(self.lock_path, purpose="checkpoint ledger"):
            self._recover_pending_locked()
            records, anchor = self._load_locked()
            head = records[-1] if records else None
            expected_seq = 1 if head is None else int(head["seq"]) + 1
            head_seq = 0 if head is None else int(head["seq"])
            if seq <= head_seq:
                raise TelemetryError(
                    "E_STALE_CHECKPOINT",
                    "检查点序号落后或重复 / stale or duplicate checkpoint sequence",
                    {
                        "kind": "duplicate" if seq == head_seq else "rollback",
                        "provided_seq": seq,
                        "head_seq": head_seq,
                        "expected_seq": expected_seq,
                    },
                )
            if seq > expected_seq:
                raise TelemetryError(
                    "E_STALE_CHECKPOINT",
                    "检查点序号跳跃 / checkpoint sequence gap",
                    {
                        "kind": "sequence_gap",
                        "provided_seq": seq,
                        "head_seq": head_seq,
                        "expected_seq": expected_seq,
                    },
                )

            if head is not None and record["run_id"] == head["run_id"]:
                if record.get("config_fingerprint") != head.get("config_fingerprint"):
                    raise TelemetryError(
                        "E_STALE_CHECKPOINT",
                        "同一运行内配置指纹变化 / config fingerprint changed inside a run",
                        {
                            "kind": "config_changed",
                            "run_id": record["run_id"],
                            "head_config_fingerprint": head.get("config_fingerprint"),
                            "provided_config_fingerprint": record.get("config_fingerprint"),
                        },
                    )
                previous_rank = STAGE_RANK[head["stage"]]
                current_rank = STAGE_RANK[stage]
                if current_rank <= previous_rank or current_rank > previous_rank + 1:
                    raise TelemetryError(
                        "E_STALE_CHECKPOINT",
                        "同一运行内阶段错位 / cross-stage checkpoint ordering",
                        {
                            "kind": "cross_stage",
                            "run_id": record["run_id"],
                            "previous_stage": head["stage"],
                            "provided_stage": stage,
                        },
                    )
            elif head is None and stage != "normalize":
                raise TelemetryError(
                    "E_INVALID_STAGE",
                    "首次发布必须从归一化阶段开始 / first publication must be normalize",
                    {"stage": stage},
                )

            catalog = record.get("catalog")
            if not isinstance(catalog, dict):
                raise TelemetryError(
                    "E_INVALID_STAGE", "catalog 必须是映射 / catalog must be a mapping", {}
                )
            self._validate_prerequisites(stage, record, catalog)
            self._validate_catalog_files(catalog)
            stage_entry = catalog.get(stage)
            if not isinstance(stage_entry, dict):
                raise TelemetryError(
                    "E_INVALID_STAGE",
                    "catalog 缺少当前阶段产物 / catalog lacks current stage artifact",
                    {"stage": stage},
                )
            if stage_entry.get("stage_fingerprint") != record.get("fingerprint"):
                raise TelemetryError(
                    "E_STALE_CHECKPOINT",
                    "阶段指纹与产物目录不一致 / stage fingerprint mismatch",
                    {
                        "kind": "tampered",
                        "stage": stage,
                        "record_fingerprint": record.get("fingerprint"),
                        "catalog_fingerprint": stage_entry.get("stage_fingerprint"),
                    },
                )
            if stage_entry.get("artifact_digest") != record.get("artifact_digest"):
                raise TelemetryError(
                    "E_STALE_CHECKPOINT",
                    "产物摘要与记录不一致 / artifact digest mismatch",
                    {
                        "kind": "tampered",
                        "stage": stage,
                        "record_artifact_digest": record.get("artifact_digest"),
                        "catalog_artifact_digest": stage_entry.get("artifact_digest"),
                    },
                )

            new_record = copy.deepcopy(record)
            expected_previous_hash = (
                record_hash(head) if head is not None else ZERO_HASH
            )
            supplied_previous = new_record.get("prev_record_hash")
            if supplied_previous is not None and supplied_previous != expected_previous_hash:
                raise TelemetryError(
                    "E_STALE_CHECKPOINT",
                    "检查点前序哈希不匹配 / checkpoint predecessor hash mismatch",
                    {
                        "kind": "tampered",
                        "expected_prev_record_hash": expected_previous_hash,
                        "provided_prev_record_hash": supplied_previous,
                    },
                )
            new_record["prev_record_hash"] = expected_previous_hash
            new_record["record_hash"] = record_hash(new_record)
            records.append(new_record)
            self._commit_locked(records, old_anchor=anchor)
            return {
                "accepted": True,
                "seq": new_record["seq"],
                "stage": new_record["stage"],
                "record_hash": new_record["record_hash"],
                "head_seq": new_record["seq"],
            }

    # -------------------------------------------------------------- internals
    def _build_anchor(self, records: list[dict[str, Any]], ledger_bytes: bytes) -> dict[str, Any]:
        head = records[-1] if records else None
        return {
            "schema_version": 1,
            "ledger_digest": sha256_bytes(ledger_bytes),
            "head_seq": int(head["seq"]) if head else 0,
            "head_record_hash": record_hash(head) if head else ZERO_HASH,
            "config_fingerprint": head.get("config_fingerprint") if head else None,
            "record_count": len(records),
        }

    def _read_bytes(self, path: Path) -> bytes | None:
        try:
            return path.read_bytes()
        except FileNotFoundError:
            return None

    @staticmethod
    def _digest_or_missing(data: bytes | None) -> str:
        return "MISSING" if data is None else sha256_bytes(data)

    def _recover_pending_locked(self) -> None:
        if not self.pending_path.exists():
            return
        try:
            pending = read_json(self.pending_path)
        except Exception as exc:
            raise TelemetryError(
                "E_STALE_CHECKPOINT",
                "检查点恢复标记不可读 / pending marker is unreadable",
                {"kind": "tampered", "reason": str(exc)},
            ) from exc
        ledger_bytes = self._read_bytes(self.ledger_path)
        anchor_bytes = self._read_bytes(self.anchor_path)
        ledger_digest = self._digest_or_missing(ledger_bytes)
        anchor_digest = self._digest_or_missing(anchor_bytes)
        old = pending.get("old", {})
        new = pending.get("new", {})
        if (
            ledger_digest == new.get("ledger_digest")
            and anchor_digest == old.get("anchor_digest")
        ):
            anchor_content = new.get("anchor_content")
            if not isinstance(anchor_content, dict):
                raise TelemetryError(
                    "E_STALE_CHECKPOINT",
                    "检查点恢复标记缺少锚点 / pending marker lacks anchor content",
                    {"kind": "tampered"},
                )
            atomic_write_bytes(
                self.anchor_path, canonical_json_bytes(anchor_content)
            )
            self.pending_path.unlink(missing_ok=True)
            return
        if (
            ledger_digest == new.get("ledger_digest")
            and anchor_digest == new.get("anchor_digest")
        ):
            self.pending_path.unlink(missing_ok=True)
            return
        if (
            ledger_digest == old.get("ledger_digest")
            and anchor_digest == old.get("anchor_digest")
        ):
            self.pending_path.unlink(missing_ok=True)
            return
        raise TelemetryError(
            "E_STALE_CHECKPOINT",
            "检查点持久化状态不一致 / checkpoint persistence state is inconsistent",
            {
                "kind": "tampered",
                "ledger_digest": ledger_digest,
                "anchor_digest": anchor_digest,
                "pending": pending,
            },
        )

    def _load_locked(self) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
        ledger_bytes = self._read_bytes(self.ledger_path)
        anchor_bytes = self._read_bytes(self.anchor_path)
        if ledger_bytes is None and anchor_bytes is None:
            return [], None
        if ledger_bytes is None:
            raise TelemetryError(
                "E_STALE_CHECKPOINT",
                "检查点账本缺失但锚点存在 / ledger missing while anchor exists",
                {"kind": "rollback"},
            )
        if anchor_bytes is None:
            raise TelemetryError(
                "E_STALE_CHECKPOINT",
                "检查点锚点缺失 / checkpoint anchor is missing",
                {"kind": "tampered"},
            )
        try:
            anchor = json.loads(anchor_bytes.decode("utf-8"))
        except Exception as exc:
            raise TelemetryError(
                "E_STALE_CHECKPOINT",
                "检查点锚点损坏 / checkpoint anchor is corrupt",
                {"kind": "tampered", "reason": str(exc)},
            ) from exc
        if sha256_bytes(ledger_bytes) != anchor.get("ledger_digest"):
            # A smaller/older ledger is a rollback; a same-size or larger rewrite is tamper.
            try:
                records_probe = self._parse_records(
                    ledger_bytes, tolerate_errors=False
                )
            except TelemetryError:
                raise TelemetryError(
                    "E_STALE_CHECKPOINT",
                    "检查点账本被篡改或损坏 / checkpoint ledger is corrupted or tampered",
                    {
                        "kind": "tampered",
                        "anchor_head_seq": anchor.get("head_seq"),
                        "expected_ledger_digest": anchor.get("ledger_digest"),
                        "actual_ledger_digest": sha256_bytes(ledger_bytes),
                    },
                ) from None
            probe_head = records_probe[-1]["seq"] if records_probe else 0
            kind = "rollback" if probe_head < int(anchor.get("head_seq", 0)) else "tampered"
            raise TelemetryError(
                "E_STALE_CHECKPOINT",
                "检查点账本摘要不匹配 / checkpoint ledger digest mismatch",
                {
                    "kind": kind,
                    "anchor_head_seq": anchor.get("head_seq"),
                    "ledger_head_seq": probe_head,
                    "expected_ledger_digest": anchor.get("ledger_digest"),
                    "actual_ledger_digest": sha256_bytes(ledger_bytes),
                },
            )
        records = self._parse_records(ledger_bytes, tolerate_errors=False)
        head_seq = int(records[-1]["seq"]) if records else 0
        if head_seq < int(anchor.get("head_seq", 0)):
            raise TelemetryError(
                "E_STALE_CHECKPOINT",
                "检查点序号回退 / checkpoint sequence rollback",
                {
                    "kind": "rollback",
                    "anchor_head_seq": anchor.get("head_seq"),
                    "ledger_head_seq": head_seq,
                },
            )
        if head_seq > int(anchor.get("head_seq", 0)):
            raise TelemetryError(
                "E_STALE_CHECKPOINT",
                "检查点账本超前于锚点 / ledger is ahead of anchor",
                {"kind": "tampered", "anchor_head_seq": anchor.get("head_seq"), "ledger_head_seq": head_seq},
            )
        expected_head_hash = record_hash(records[-1]) if records else ZERO_HASH
        if expected_head_hash != anchor.get("head_record_hash"):
            raise TelemetryError(
                "E_STALE_CHECKPOINT",
                "检查点头部哈希不匹配 / checkpoint head hash mismatch",
                {
                    "kind": "tampered",
                    "anchor_head_record_hash": anchor.get("head_record_hash"),
                    "actual_head_record_hash": expected_head_hash,
                },
            )
        anchor_config = anchor.get("config_fingerprint")
        ledger_config = records[-1].get("config_fingerprint") if records else None
        if anchor_config != ledger_config:
            raise TelemetryError(
                "E_STALE_CHECKPOINT",
                "检查点配置指纹不一致 / checkpoint config fingerprint mismatch",
                {
                    "kind": "config_changed",
                    "anchor_config_fingerprint": anchor_config,
                    "ledger_config_fingerprint": ledger_config,
                },
            )
        return records, anchor

    def _parse_records(
        self, ledger_bytes: bytes, *, tolerate_errors: bool
    ) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        previous_hash = ZERO_HASH
        for line_number, line in enumerate(ledger_bytes.splitlines(), start=1):
            if not line.strip():
                continue
            try:
                record = json.loads(line.decode("utf-8"))
            except Exception as exc:
                if tolerate_errors:
                    return records
                raise TelemetryError(
                    "E_STALE_CHECKPOINT",
                    "检查点记录不可解析 / checkpoint record is invalid JSON",
                    {"kind": "tampered", "line": line_number, "reason": str(exc)},
                ) from exc
            expected_seq = len(records) + 1
            if record.get("seq") != expected_seq:
                if tolerate_errors:
                    return records
                raise TelemetryError(
                    "E_STALE_CHECKPOINT",
                    "检查点序号不连续 / checkpoint sequence is not contiguous",
                    {
                        "kind": "tampered",
                        "line": line_number,
                        "expected_seq": expected_seq,
                        "found_seq": record.get("seq"),
                    },
                )
            if record.get("prev_record_hash") != previous_hash:
                if tolerate_errors:
                    return records
                raise TelemetryError(
                    "E_STALE_CHECKPOINT",
                    "检查点哈希链断裂 / checkpoint hash chain is broken",
                    {"kind": "tampered", "line": line_number},
                )
            stored_hash = record.get("record_hash")
            unhashed = dict(record)
            unhashed.pop("record_hash", None)
            if stored_hash != record_hash(unhashed):
                if tolerate_errors:
                    return records
                raise TelemetryError(
                    "E_STALE_CHECKPOINT",
                    "检查点记录哈希不匹配 / checkpoint record hash mismatch",
                    {"kind": "tampered", "line": line_number},
                )
            records.append(record)
            previous_hash = stored_hash
        return records

    def _commit_locked(
        self, records: list[dict[str, Any]], *, old_anchor: dict[str, Any] | None
    ) -> None:
        old_ledger_bytes = self._read_bytes(self.ledger_path)
        old_anchor_bytes = self._read_bytes(self.anchor_path)
        ledger_bytes = b"".join(canonical_json_bytes(record) for record in records)
        new_anchor = self._build_anchor(records, ledger_bytes)
        new_anchor_bytes = canonical_json_bytes(new_anchor)
        pending = {
            "schema_version": 1,
            "old": {
                "ledger_digest": self._digest_or_missing(old_ledger_bytes),
                "anchor_digest": self._digest_or_missing(old_anchor_bytes),
            },
            "new": {
                "ledger_digest": sha256_bytes(ledger_bytes),
                "anchor_digest": sha256_bytes(new_anchor_bytes),
                "anchor_content": new_anchor,
            },
        }
        atomic_write_json(self.pending_path, pending)
        atomic_write_bytes(self.ledger_path, ledger_bytes)
        atomic_write_bytes(self.anchor_path, new_anchor_bytes)
        self.pending_path.unlink(missing_ok=True)

    def _validate_prerequisites(
        self,
        stage: str,
        record: dict[str, Any],
        catalog: dict[str, Any],
    ) -> None:
        prerequisites = record.get("prerequisites", {})
        if not isinstance(prerequisites, dict):
            raise TelemetryError(
                "E_INVALID_STAGE",
                "prerequisites 必须是映射 / prerequisites must be a mapping",
                {"stage": stage},
            )
        for dependency in STAGE_DEPENDENCIES[stage]:
            if dependency not in catalog:
                raise TelemetryError(
                    "E_INVALID_STAGE",
                    "缺失前置阶段产物 / prerequisite stage artifact is missing",
                    {"stage": stage, "missing_prerequisite": dependency},
                )
            if dependency not in prerequisites:
                raise TelemetryError(
                    "E_INVALID_STAGE",
                    "记录未声明前置阶段 / prerequisite not declared",
                    {"stage": stage, "missing_prerequisite": dependency},
                )
            catalog_fingerprint = catalog[dependency].get("stage_fingerprint")
            if prerequisites[dependency] != catalog_fingerprint:
                raise TelemetryError(
                    "E_INVALID_STAGE",
                    "前置阶段指纹不匹配 / prerequisite fingerprint mismatch",
                    {
                        "stage": stage,
                        "dependency": dependency,
                        "declared": prerequisites[dependency],
                        "catalog": catalog_fingerprint,
                    },
                )

    def _validate_catalog_files(self, catalog: dict[str, Any]) -> None:
        for stage, entry in catalog.items():
            if stage not in STAGE_RANK:
                raise TelemetryError(
                    "E_INVALID_STAGE",
                    "产物目录包含未知阶段 / catalog contains unknown stage",
                    {"stage": stage},
                )
            if not isinstance(entry, dict):
                raise TelemetryError(
                    "E_INVALID_STAGE", "产物目录项格式错误 / invalid catalog entry", {"stage": stage}
                )
            self._validate_artifact_entry(stage, entry)
            for source_id, source_entry in entry.get("sources", {}).items():
                self._validate_artifact_entry(
                    f"{stage}:{source_id}", source_entry
                )
            grid_entry = entry.get("grid")
            if isinstance(grid_entry, dict):
                self._validate_artifact_entry(f"{stage}:grid", grid_entry)

    def _validate_artifact_entry(self, label: str, entry: dict[str, Any]) -> None:
        path_value = entry.get("artifact_path")
        expected_digest = entry.get("artifact_digest")
        if not path_value or not expected_digest:
            raise TelemetryError(
                "E_INVALID_STAGE",
                "阶段产物缺少路径或摘要 / artifact entry is incomplete",
                {"artifact": label},
            )
        try:
            artifact_path = resolve_inside(self.root, path_value)
        except TelemetryError:
            raise
        if not artifact_path.exists():
            raise TelemetryError(
                "E_INVALID_STAGE",
                "阶段产物文件不存在 / stage artifact is missing",
                {"artifact": label, "path": path_value},
            )
        actual_digest = sha256_file(artifact_path)
        if actual_digest != expected_digest:
            raise TelemetryError(
                "E_STALE_CHECKPOINT",
                "阶段产物摘要不匹配 / stage artifact digest mismatch",
                {
                    "kind": "tampered",
                    "artifact": label,
                    "path": path_value,
                    "expected_digest": expected_digest,
                    "actual_digest": actual_digest,
                },
            )

