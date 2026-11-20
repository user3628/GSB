"""Atomic, hash-chained audit record storage with monotonic publication."""

from __future__ import annotations

import copy
import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import numpy as np

from .canonical import (
    GENESIS_HASH,
    atomic_write_json,
    atomic_write_npz,
    canonical_digest,
    load_npz_arrays,
    read_json,
    sha256_file,
)
from .errors import (
    E_AUDIT_TAMPERED,
    E_INVALID_ROLLBACK,
    E_INVALID_STAGE,
    E_NO_VALID_VERSION,
    E_STALE_VERSION,
    E_VERSION_ROLLBACK,
    AuditError,
)
from .locking import FileLock

_RECORD_NAME = re.compile(r"^(\d{8})\.json$")
_HEX64 = re.compile(r"^[0-9a-f]{64}$")


class AuditStore:
    schema_version = 1

    def __init__(self, root: str | Path, anchor_path: str | Path) -> None:
        self.root = Path(root)
        self.anchor_path = Path(anchor_path)
        self.records_dir = self.root / "records"
        self.stages_dir = self.root / "stages"
        self.head_path = self.root / "HEAD.json"
        self.tx_path = self.root / "TX.json"
        self.lock_path = self.root / "lock"

    def initialize(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        self.records_dir.mkdir(parents=True, exist_ok=True)
        self.stages_dir.mkdir(parents=True, exist_ok=True)
        with FileLock(self.lock_path):
            if self.head_path.exists():
                if not self.anchor_path.exists():
                    head = read_json(self.head_path)
                    self._validate_head(head)
                    atomic_write_json(self.anchor_path, self._anchor_for(head))
                return
            head = self._genesis_head()
            atomic_write_json(self.head_path, head)
            atomic_write_json(self.anchor_path, self._anchor_for(head))

    def latest_version(self) -> int:
        with FileLock(self.lock_path):
            head, _, _ = self._validated_state_unlocked()
            return int(head["version"])

    def active_version(self) -> int:
        with FileLock(self.lock_path):
            head, _, _ = self._validated_state_unlocked()
            return int(head["active_version"])

    def load_record(self, version: int | None = None) -> dict[str, Any]:
        with FileLock(self.lock_path):
            head, records, _ = self._validated_state_unlocked()
            if version is None:
                version = int(head["active_version"])
                if version == 0:
                    raise AuditError(
                        E_NO_VALID_VERSION,
                        "workspace has no budget-compliant calibration version",
                        active_version=0,
                        latest_version=int(head["version"]),
                    )
            selected = self._select_record(records, version)
            return copy.deepcopy(selected)

    def history(self) -> dict[str, Any]:
        with FileLock(self.lock_path):
            head, records, _ = self._validated_state_unlocked()
            return {
                "latest_version": int(head["version"]),
                "active_version": int(head["active_version"]),
                "records": [copy.deepcopy(record) for record in records],
            }

    def load_stage(self, version: int) -> tuple[dict[str, Any], dict[str, np.ndarray]]:
        with FileLock(self.lock_path):
            _, records, _ = self._validated_state_unlocked()
            record = self._select_record(records, version)
            archive = record.get("stage_archive")
            if not isinstance(archive, dict) or not archive.get("path") or not archive.get("digest"):
                raise AuditError(
                    E_INVALID_STAGE,
                    "audit version has no persisted stage archive",
                    version=version,
                )
            stage_path = (self.root / str(archive["path"])).resolve()
            try:
                stage_path.relative_to(self.stages_dir.resolve())
            except ValueError as exc:
                raise AuditError(
                    E_AUDIT_TAMPERED,
                    "stage archive path escapes the audit workspace",
                    version=version,
                    path=str(archive["path"]),
                ) from exc
            if not stage_path.exists():
                raise AuditError(
                    E_INVALID_STAGE,
                    "required stage archive is missing",
                    version=version,
                    path=str(archive["path"]),
                )
            actual_digest = sha256_file(stage_path)
            if actual_digest != archive["digest"]:
                raise AuditError(
                    E_AUDIT_TAMPERED,
                    "stage archive digest does not match the audit record",
                    version=version,
                    expected=archive["digest"],
                    actual=actual_digest,
                )
            try:
                arrays = load_npz_arrays(stage_path)
            except Exception as exc:
                raise AuditError(
                    E_AUDIT_TAMPERED,
                    f"stage archive cannot be decoded: {exc}",
                    version=version,
                ) from exc
            return copy.deepcopy(record), arrays

    def publish_calibration(
        self,
        *,
        base_version: int,
        result: dict[str, Any],
        arrays: Mapping[str, np.ndarray],
        inputs: dict[str, Any],
        layer_fingerprints: dict[str, str],
        budget_config_fingerprint: str,
        execution: dict[str, Any],
    ) -> dict[str, Any]:
        """Publish a complete calibration result as the next immutable version.

        Publication computes the stage file first. A transaction marker then makes
        the record/HEAD update recoverable after interruption at any instruction.
        """

        with FileLock(self.lock_path):
            head, records, _ = self._validated_state_unlocked()
            current_version = int(head["version"])
            if base_version != current_version:
                raise AuditError(
                    E_STALE_VERSION,
                    "calibration was based on a stale audit version",
                    base_version=base_version,
                    current_version=current_version,
                )
            target_version = base_version + 1
            if self._record_path(target_version).exists() or target_version <= current_version:
                raise AuditError(
                    E_STALE_VERSION,
                    "target version has already been published",
                    target_version=target_version,
                    current_version=current_version,
                )

            result_summary = canonical_digest(result)
            usable = bool(result.get("budget", {}).get("usable", False))
            over_layers = list(result.get("budget", {}).get("over_budget_layers", []))
            active_after = target_version if usable else self._latest_usable_version(records)
            stage_name = f"{target_version:08d}.npz"
            stage_digest = atomic_write_npz(self.stages_dir / stage_name, arrays)

            body = {
                "schema_version": self.schema_version,
                "version": target_version,
                "kind": "calibration",
                "base_version": base_version,
                "prev_record_hash": head["record_hash"],
                "active_after_publish": active_after,
                "inputs": inputs,
                "layer_fingerprints": dict(sorted(layer_fingerprints.items())),
                "budget_config_fingerprint": budget_config_fingerprint,
                "result": result,
                "result_summary": result_summary,
                "usable": usable,
                "over_budget_layers": over_layers,
                "execution": execution,
                "stage_archive": {
                    "path": f"stages/{stage_name}",
                    "digest": stage_digest,
                    "array_count": len(arrays),
                },
            }
            record_hash = canonical_digest(body)
            record = {**body, "record_hash": record_hash}
            new_head = {
                "schema_version": self.schema_version,
                "version": target_version,
                "prev_record_hash": head["record_hash"],
                "record_hash": record_hash,
                "active_version": active_after,
            }
            transaction = {
                "schema_version": self.schema_version,
                "target_version": target_version,
                "expected_prev_version": current_version,
                "expected_prev_hash": head["record_hash"],
                "record_hash": record_hash,
                "head": new_head,
            }
            atomic_write_json(self.tx_path, transaction)
            atomic_write_json(self._record_path(target_version), record)
            atomic_write_json(self.head_path, new_head)
            atomic_write_json(self.anchor_path, self._anchor_for(new_head))
            self.tx_path.unlink(missing_ok=True)
            return {"record": record, "active_version": active_after}

    def rollback(self, to_version: int | None = None) -> dict[str, Any]:
        with FileLock(self.lock_path):
            head, records, _ = self._validated_state_unlocked()
            previous_active = int(head["active_version"])
            if to_version is None:
                usable_versions = [int(record["version"]) for record in records if record.get("usable")]
                if not usable_versions:
                    raise AuditError(
                        E_NO_VALID_VERSION,
                        "no budget-compliant calibration version is available",
                        latest_version=int(head["version"]),
                    )
                to_version = max(usable_versions)
            record = self._select_record(records, to_version)
            if not record.get("usable"):
                raise AuditError(
                    E_INVALID_ROLLBACK,
                    "requested rollback target is not budget-compliant",
                    target_version=to_version,
                    over_budget_layers=record.get("over_budget_layers", []),
                )
            new_head = {
                **head,
                "active_version": int(to_version),
            }
            atomic_write_json(self.head_path, new_head)
            return {
                "previous_active_version": previous_active,
                "active_version": int(to_version),
                "record": copy.deepcopy(record),
            }

    def _validated_state_unlocked(self) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, Any]]:
        self._recover_transaction_unlocked()
        if not self.head_path.exists():
            raise AuditError(E_INVALID_STAGE, "audit workspace is not initialized")
        head = read_json(self.head_path)
        self._validate_head(head)
        records = self._load_and_validate_records_unlocked(head)
        anchor = read_json(self.anchor_path) if self.anchor_path.exists() else None
        if anchor is None:
            raise AuditError(E_AUDIT_TAMPERED, "audit anchor is missing")
        self._validate_anchor_unlocked(head, records, anchor)
        active = int(head["active_version"])
        if active < 0 or active > int(head["version"]):
            raise AuditError(E_AUDIT_TAMPERED, "active version is outside the audit history", active_version=active)
        if active > 0:
            active_record = records[active - 1]
            if not active_record.get("usable"):
                raise AuditError(
                    E_AUDIT_TAMPERED,
                    "active version points to a budget-failing record",
                    active_version=active,
                )
        return head, records, anchor

    def _recover_transaction_unlocked(self) -> None:
        if not self.tx_path.exists():
            return
        try:
            transaction = read_json(self.tx_path)
            target_version = int(transaction["target_version"])
            record_hash = str(transaction["record_hash"])
            new_head = transaction["head"]
        except Exception as exc:
            raise AuditError(E_AUDIT_TAMPERED, "audit transaction marker is malformed") from exc
        if new_head.get("version") != target_version or new_head.get("record_hash") != record_hash:
            raise AuditError(E_AUDIT_TAMPERED, "audit transaction marker is inconsistent")
        record_path = self._record_path(target_version)
        if not record_path.exists():
            self.tx_path.unlink(missing_ok=True)
            return
        record = read_json(record_path)
        self._validate_record_hash(record)
        if record.get("record_hash") != record_hash:
            raise AuditError(E_AUDIT_TAMPERED, "transaction record hash does not match the staged record")
        head = read_json(self.head_path)
        self._validate_head(head)
        if int(head["version"]) == target_version and head["record_hash"] == record_hash:
            self._complete_transaction_anchor_unlocked(new_head)
            self.tx_path.unlink(missing_ok=True)
            return
        if (
            int(head["version"]) == int(transaction["expected_prev_version"])
            and head["record_hash"] == transaction["expected_prev_hash"]
        ):
            stage = record.get("stage_archive", {})
            stage_path = self.root / str(stage.get("path", ""))
            if not stage_path.exists() or sha256_file(stage_path) != stage.get("digest"):
                raise AuditError(
                    E_INVALID_STAGE,
                    "cannot complete interrupted publish because its stage archive is missing or corrupt",
                    version=target_version,
                )
            atomic_write_json(self.head_path, new_head)
            self._complete_transaction_anchor_unlocked(new_head)
            self.tx_path.unlink(missing_ok=True)
            return
        raise AuditError(
            E_AUDIT_TAMPERED,
            "audit transaction cannot be reconciled with HEAD",
            target_version=target_version,
        )

    def _complete_transaction_anchor_unlocked(self, head: dict[str, Any]) -> None:
        anchor = self._anchor_for(head)
        if self.anchor_path.exists():
            current = read_json(self.anchor_path)
            if int(current.get("version", -1)) > int(head["version"]):
                raise AuditError(E_VERSION_ROLLBACK, "HEAD is older than the external audit anchor")
            if int(current.get("version", -1)) == int(head["version"]) and current.get("record_hash") != head["record_hash"]:
                raise AuditError(E_AUDIT_TAMPERED, "HEAD record hash differs from the external audit anchor")
        atomic_write_json(self.anchor_path, anchor)

    def _load_and_validate_records_unlocked(self, head: dict[str, Any]) -> list[dict[str, Any]]:
        versions = self._scan_versions_unlocked()
        expected = list(range(1, int(head["version"]) + 1))
        if versions != expected:
            if versions and (not expected or versions[-1] > expected[-1]):
                raise AuditError(
                    E_VERSION_ROLLBACK,
                    "audit records exist beyond the published HEAD version",
                    head_version=int(head["version"]),
                    record_versions=versions,
                )
            raise AuditError(
                E_VERSION_ROLLBACK,
                "audit history is missing published versions",
                head_version=int(head["version"]),
                record_versions=versions,
            )
        records: list[dict[str, Any]] = []
        previous_hash = GENESIS_HASH
        for version in versions:
            record = read_json(self._record_path(version))
            self._validate_record_hash(record)
            if int(record.get("version", -1)) != version:
                raise AuditError(
                    E_AUDIT_TAMPERED,
                    "record version does not match its filename",
                    expected=version,
                    actual=record.get("version"),
                )
            if record.get("prev_record_hash") != previous_hash:
                raise AuditError(
                    E_AUDIT_TAMPERED,
                    "audit hash chain is broken",
                    version=version,
                )
            if canonical_digest(record.get("result")) != record.get("result_summary"):
                raise AuditError(
                    E_AUDIT_TAMPERED,
                    "result summary does not match the stored result",
                    version=version,
                )
            if not isinstance(record.get("layer_fingerprints"), dict):
                raise AuditError(E_AUDIT_TAMPERED, "record is missing layer parameter fingerprints", version=version)
            if not record.get("budget_config_fingerprint"):
                raise AuditError(E_AUDIT_TAMPERED, "record is missing its budget configuration fingerprint", version=version)
            records.append(record)
            previous_hash = record["record_hash"]
        if int(head["version"]) == 0:
            if head["record_hash"] != GENESIS_HASH:
                raise AuditError(E_AUDIT_TAMPERED, "genesis HEAD hash is invalid")
        elif not records or head["record_hash"] != records[-1]["record_hash"]:
            raise AuditError(E_AUDIT_TAMPERED, "HEAD does not reference the latest audit record")
        return records

    def _validate_anchor_unlocked(
        self,
        head: dict[str, Any],
        records: list[dict[str, Any]],
        anchor: dict[str, Any],
    ) -> None:
        try:
            anchor_version = int(anchor["version"])
            anchor_hash = str(anchor["record_hash"])
        except Exception as exc:
            raise AuditError(E_AUDIT_TAMPERED, "audit anchor is malformed") from exc
        head_version = int(head["version"])
        if anchor_version > head_version:
            raise AuditError(
                E_VERSION_ROLLBACK,
                "audit directory was replaced with an older HEAD",
                anchor_version=anchor_version,
                head_version=head_version,
            )
        if anchor_version == head_version:
            if anchor_hash != head["record_hash"]:
                raise AuditError(
                    E_AUDIT_TAMPERED,
                    "audit anchor record hash differs from HEAD",
                    anchor_version=anchor_version,
                )
            return
        if head_version - anchor_version == 1 and records:
            expected_previous = records[-1].get("prev_record_hash")
            if anchor_hash == expected_previous:
                atomic_write_json(self.anchor_path, self._anchor_for(head))
                return
        raise AuditError(
            E_VERSION_ROLLBACK,
            "audit anchor lags the published history and cannot be safely advanced",
            anchor_version=anchor_version,
            head_version=head_version,
        )

    def _scan_versions_unlocked(self) -> list[int]:
        versions: list[int] = []
        for entry in self.records_dir.iterdir() if self.records_dir.exists() else []:
            match = _RECORD_NAME.match(entry.name)
            if match and entry.is_file():
                versions.append(int(match.group(1)))
        return sorted(versions)

    def _validate_head(self, head: dict[str, Any]) -> None:
        if not isinstance(head, dict):
            raise AuditError(E_AUDIT_TAMPERED, "HEAD is not a mapping")
        if head.get("schema_version") != self.schema_version:
            raise AuditError(E_AUDIT_TAMPERED, "unsupported HEAD schema version")
        try:
            version = int(head["version"])
            active = int(head["active_version"])
        except Exception as exc:
            raise AuditError(E_AUDIT_TAMPERED, "HEAD has invalid version fields") from exc
        if version < 0 or active < 0 or active > version:
            raise AuditError(E_AUDIT_TAMPERED, "HEAD versions are out of range")
        if not isinstance(head.get("record_hash"), str) or not _HEX64.match(head["record_hash"]):
            raise AuditError(E_AUDIT_TAMPERED, "HEAD has an invalid record hash")
        if version == 0 and head["record_hash"] != GENESIS_HASH:
            raise AuditError(E_AUDIT_TAMPERED, "HEAD genesis hash is invalid")

    def _validate_record_hash(self, record: dict[str, Any]) -> None:
        if not isinstance(record, dict) or "record_hash" not in record:
            raise AuditError(E_AUDIT_TAMPERED, "audit record is malformed")
        body = {key: value for key, value in record.items() if key != "record_hash"}
        expected = canonical_digest(body)
        if expected != record["record_hash"]:
            raise AuditError(
                E_AUDIT_TAMPERED,
                "audit record hash mismatch",
                version=record.get("version"),
                expected=expected,
                actual=record.get("record_hash"),
            )

    def _latest_usable_version(self, records: list[dict[str, Any]]) -> int:
        for record in reversed(records):
            if record.get("usable"):
                return int(record["version"])
        return 0

    def _select_record(self, records: list[dict[str, Any]], version: int) -> dict[str, Any]:
        if version <= 0 or version > len(records):
            raise AuditError(
                E_INVALID_STAGE,
                "requested audit version does not exist",
                requested_version=version,
                available_versions=[record["version"] for record in records],
            )
        record = records[version - 1]
        if int(record.get("version", -1)) != version:
            raise AuditError(E_AUDIT_TAMPERED, "audit record order is inconsistent")
        return record

    def _record_path(self, version: int) -> Path:
        return self.records_dir / f"{version:08d}.json"

    def _genesis_head(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "version": 0,
            "prev_record_hash": None,
            "record_hash": GENESIS_HASH,
            "active_version": 0,
        }

    def _anchor_for(self, head: dict[str, Any]) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "version": int(head["version"]),
            "record_hash": head["record_hash"],
        }

