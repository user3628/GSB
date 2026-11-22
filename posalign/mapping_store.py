"""Versioned, atomically published mapping tables with replacement detection."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .canonical import (
    atomic_write_json,
    digest_json,
    read_json,
)
from .errors import BusinessError, E_STALE_MAPPING
from .filelock import exclusive_file_lock
from .textnorm import normalize_label


@dataclass(frozen=True)
class MappingValidation:
    status: str
    version: int = 0
    sequence: int = 0
    mapping_digest: str | None = None
    alias_fingerprint: str | None = None
    adjudication_fingerprint: str | None = None
    input_digest: str | None = None
    requires_rebuild: bool = False
    rebuild_reasons: tuple[str, ...] = ()

    def as_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "version": self.version,
            "sequence": self.sequence,
            "mapping_digest": self.mapping_digest,
            "alias_fingerprint": self.alias_fingerprint,
            "adjudication_fingerprint": self.adjudication_fingerprint,
            "input_digest": self.input_digest,
            "requires_rebuild": self.requires_rebuild,
            "rebuild_reasons": list(self.rebuild_reasons),
        }


class MappingStore:
    def __init__(self, workspace: str | Path) -> None:
        self.workspace = Path(workspace).resolve()
        self.root = self.workspace / "mapping"
        self.lock_path = self.root / "lock"
        self.ledger_path = self.root / "ledger.json"
        self.current_path = self.root / "current.json"
        self.versions_dir = self.root / "versions"
        self.stale_path = self.root / "STALE.json"
        self.anchor_path = self.workspace / "state" / "mapping_anchor.json"

    def _version_path(self, version: int) -> Path:
        return self.versions_dir / f"{version:08d}.json"

    def _table_core(self, table: dict[str, Any]) -> dict[str, Any]:
        return table.get("mapping", {})

    def _table_digest(self, table: dict[str, Any]) -> str:
        return digest_json(self._table_core(table))

    def _load_ledger_unchecked(self) -> dict[str, Any] | None:
        return self._read_mapping_json(self.ledger_path, "mapping ledger")

    def _load_current_unchecked(self) -> dict[str, Any] | None:
        return self._read_mapping_json(self.current_path, "current mapping")

    def _load_anchor_unchecked(self) -> dict[str, Any] | None:
        return self._read_mapping_json(self.anchor_path, "state mapping anchor")

    def _read_mapping_json(
        self, path: Path, label: str
    ) -> dict[str, Any] | None:
        try:
            return read_json(path, default=None)
        except BusinessError as exc:
            self._raise_stale(
                "tampered",
                f"{label} is not valid JSON",
                path=str(path),
                cause=exc.message,
            )

    def _stale_payload(self) -> dict[str, Any] | None:
        return read_json(self.stale_path, default=None)

    def _raise_stale(self, classification: str, reason: str, **details: Any) -> None:
        raise BusinessError(
            E_STALE_MAPPING,
            "mapping table failed whole-replacement validation",
            {
                "classification": classification,
                "reason": reason,
                "preserved_current": str(self.current_path),
                "preserved_versions": str(self.versions_dir),
                **details,
            },
        )

    def _validate_locked(
        self,
        *,
        expected_alias_fingerprint: str | None = None,
        expected_adjudication_fingerprint: str | None = None,
        expected_input_digest: str | None = None,
    ) -> MappingValidation:
        current = self._load_current_unchecked()
        ledger = self._load_ledger_unchecked()
        anchor = self._load_anchor_unchecked()
        version_files = sorted(self.versions_dir.glob("*.json")) if self.versions_dir.exists() else []

        if current is None:
            if ledger is not None or version_files:
                self._raise_stale(
                    "tampered",
                    "current mapping is missing while ledger or version archives exist",
                )
            return MappingValidation(status="empty")
        if ledger is None:
            self._raise_stale("tampered", "mapping ledger is missing")
        if not isinstance(current, dict) or not isinstance(ledger, dict):
            self._raise_stale("tampered", "mapping files are not JSON objects")
        try:
            version = int(current["version"])
            sequence = int(current["sequence"])
            latest_version = int(ledger["latest_version"])
            latest_sequence = int(ledger["latest_sequence"])
        except (KeyError, TypeError, ValueError) as exc:
            self._raise_stale("tampered", "mapping metadata is incomplete", cause=str(exc))

        if version < latest_version:
            self._raise_stale(
                "rollback",
                "current mapping version is older than the ledger",
                current_version=version,
                ledger_version=latest_version,
            )
        if version > latest_version:
            self._raise_stale(
                "tampered",
                "current mapping version is ahead of the ledger",
                current_version=version,
                ledger_version=latest_version,
            )
        if version != latest_version or sequence != latest_sequence:
            self._raise_stale(
                "tampered",
                "current mapping version/sequence disagrees with the ledger",
                current_version=version,
                current_sequence=sequence,
                ledger_version=latest_version,
                ledger_sequence=latest_sequence,
            )
        if latest_sequence != latest_version:
            self._raise_stale(
                "tampered",
                "latest mapping version and sequence are not contiguous",
                ledger_version=latest_version,
                ledger_sequence=latest_sequence,
            )

        computed_digest = self._table_digest(current)
        if current.get("mapping_digest") != computed_digest:
            self._raise_stale(
                "tampered",
                "mapping summary digest mismatch",
                expected=current.get("mapping_digest"),
                computed=computed_digest,
            )
        for field in (
            "mapping_digest",
            "alias_fingerprint",
            "adjudication_fingerprint",
            "input_digest",
        ):
            ledger_field = f"latest_{field}"
            if current.get(field) != ledger.get(ledger_field):
                self._raise_stale(
                    "tampered",
                    f"{field} disagrees with the ledger",
                    current=current.get(field),
                    ledger=ledger.get(ledger_field),
                )

        archive_path = self._version_path(version)
        if not archive_path.is_file():
            self._raise_stale(
                "tampered",
                "current version archive is missing",
                archive=str(archive_path),
            )
        try:
            archive = self._version_path(version).read_bytes()
            current_bytes = self.current_path.read_bytes()
        except OSError as exc:
            self._raise_stale("tampered", "mapping archive cannot be read", cause=str(exc))
        if archive != current_bytes:
            self._raise_stale(
                "tampered",
                "current mapping differs byte-for-byte from its archived version",
            )
        archive_versions = {
            int(path.stem) for path in version_files if path.stem.isdigit()
        }
        highest_archive = max(archive_versions, default=0)
        if highest_archive > latest_version:
            self._raise_stale(
                "rollback",
                "newer mapping archive exists but current/ledger point to an older version",
                highest_archive=highest_archive,
                ledger_version=latest_version,
            )
        if highest_archive < latest_version:
            self._raise_stale(
                "tampered",
                "latest mapping archive is missing",
                highest_archive=highest_archive,
                ledger_version=latest_version,
            )
        expected_versions = set(range(1, latest_version + 1))
        if archive_versions != expected_versions:
            self._raise_stale(
                "tampered",
                "mapping version archives are not contiguous",
                expected=sorted(expected_versions),
                actual=sorted(archive_versions),
            )

        history = ledger.get("history")
        if not isinstance(history, list) or len(history) != latest_version:
            self._raise_stale(
                "tampered",
                "mapping ledger history is missing or has a version gap",
                history_length=len(history) if isinstance(history, list) else None,
                latest_version=latest_version,
            )
        for expected_version, entry in enumerate(history, 1):
            if not isinstance(entry, dict):
                self._raise_stale("tampered", "mapping ledger history entry is invalid")
            if entry.get("version") != expected_version or entry.get("sequence") != expected_version:
                self._raise_stale(
                    "tampered",
                    "mapping ledger version/sequence is not contiguous",
                    expected_version=expected_version,
                    entry=entry,
                )
            archive_table = self._load_archive_table(expected_version)
            archive_digest = self._table_digest(archive_table)
            if archive_table.get("mapping_digest") != archive_digest:
                self._raise_stale(
                    "tampered",
                    "historical mapping archive digest mismatch",
                    version=expected_version,
                )
            for field in (
                "mapping_digest",
                "alias_fingerprint",
                "adjudication_fingerprint",
                "input_digest",
            ):
                if archive_table.get(field) != entry.get(field):
                    self._raise_stale(
                        "tampered",
                        "historical mapping archive disagrees with the ledger",
                        version=expected_version,
                        field=field,
                    )
            if expected_version == latest_version:
                for field in (
                    "mapping_digest",
                    "alias_fingerprint",
                    "adjudication_fingerprint",
                    "input_digest",
                ):
                    if entry.get(field) != ledger.get(f"latest_{field}"):
                        self._raise_stale(
                            "tampered",
                            "latest mapping ledger entry disagrees with ledger metadata",
                            version=expected_version,
                            field=field,
                        )

        if anchor is None:
            self._raise_stale(
                "tampered",
                "state mapping anchor is missing",
            )
        else:
            try:
                anchor_version = int(anchor["version"])
            except (KeyError, TypeError, ValueError):
                self._raise_stale("tampered", "state mapping anchor is invalid")
            if anchor_version > version:
                self._raise_stale(
                    "rollback",
                    "state anchor references a newer mapping version",
                    anchor_version=anchor_version,
                    current_version=version,
                )
            if (
                anchor_version != version
                or anchor.get("mapping_digest") != current.get("mapping_digest")
                or anchor.get("sequence") != sequence
            ):
                self._raise_stale(
                    "tampered",
                    "state mapping anchor disagrees with the current table",
                )

        reasons: list[str] = []
        if expected_alias_fingerprint and expected_alias_fingerprint != current.get(
            "alias_fingerprint"
        ):
            reasons.append("alias_fingerprint_changed")
        if expected_adjudication_fingerprint and expected_adjudication_fingerprint != current.get(
            "adjudication_fingerprint"
        ):
            reasons.append("adjudication_fingerprint_changed")
        if expected_input_digest and expected_input_digest != current.get("input_digest"):
            reasons.append("mapping_input_digest_changed")
        status = "outdated" if reasons else "valid"
        return MappingValidation(
            status=status,
            version=version,
            sequence=sequence,
            mapping_digest=current.get("mapping_digest"),
            alias_fingerprint=current.get("alias_fingerprint"),
            adjudication_fingerprint=current.get("adjudication_fingerprint"),
            input_digest=current.get("input_digest"),
            requires_rebuild=bool(reasons),
            rebuild_reasons=tuple(reasons),
        )

    def _load_archive_table(self, version: int) -> dict[str, Any]:
        path = self._version_path(version)
        try:
            value = read_json(path, default=None)
        except BusinessError as exc:
            self._raise_stale(
                "tampered",
                "historical mapping archive is not valid JSON",
                version=version,
                cause=exc.message,
            )
        if not isinstance(value, dict) or value.get("version") != version:
            self._raise_stale(
                "tampered",
                "historical mapping archive metadata is invalid",
                version=version,
            )
        return value

    def validate(
        self,
        *,
        expected_alias_fingerprint: str | None = None,
        expected_adjudication_fingerprint: str | None = None,
        expected_input_digest: str | None = None,
        require_not_stale: bool = False,
    ) -> MappingValidation:
        with exclusive_file_lock(self.lock_path):
            if require_not_stale:
                stale = self._stale_payload()
                if stale is not None:
                    self._raise_stale(
                        "superseded",
                        "mapping table is marked stale by an invalidation",
                        stale=stale,
                    )
            return self._validate_locked(
                expected_alias_fingerprint=expected_alias_fingerprint,
                expected_adjudication_fingerprint=expected_adjudication_fingerprint,
                expected_input_digest=expected_input_digest,
            )

    def mark_stale(self, reasons: list[dict[str, Any]]) -> dict[str, Any]:
        with exclusive_file_lock(self.lock_path):
            current = self._load_current_unchecked()
            payload = {
                "schema": "posalign.mapping_stale.v1",
                "reason": "upstream_stage_invalidated",
                "details": reasons,
                "preserved_version": current.get("version") if current else 0,
            }
            atomic_write_json(self.stale_path, payload)
            return payload

    def stale(self) -> dict[str, Any] | None:
        return self._stale_payload()

    def clear_stale(self) -> None:
        with exclusive_file_lock(self.lock_path):
            self.stale_path.unlink(missing_ok=True)

    def publish(
        self,
        mapping: dict[str, Any],
        *,
        alias_fingerprint: str,
        adjudication_fingerprint: str,
        input_digest: str,
        sequence: int | None = None,
    ) -> dict[str, Any]:
        with exclusive_file_lock(self.lock_path):
            validation = self._validate_locked()
            ledger = self._load_ledger_unchecked()
            if validation.status == "empty":
                version = 1
                next_sequence = 1
            else:
                version = validation.version + 1
                next_sequence = validation.sequence + 1
            if sequence is not None and sequence != next_sequence:
                raise BusinessError(
                    E_STALE_MAPPING,
                    "mapping sequence must advance contiguously",
                    {
                        "requested_sequence": sequence,
                        "expected_sequence": next_sequence,
                        "classification": "stale_sequence",
                    },
                )
            mapping_digest = digest_json(mapping)
            table: dict[str, Any] = {
                "schema": "posalign.mapping_table.v1",
                "version": version,
                "sequence": next_sequence,
                "alias_fingerprint": alias_fingerprint,
                "adjudication_fingerprint": adjudication_fingerprint,
                "input_digest": input_digest,
                "mapping_digest": mapping_digest,
                "mapping": mapping,
            }
            history = list(ledger.get("history", [])) if ledger else []
            history.append(
                {
                    "version": version,
                    "sequence": next_sequence,
                    "mapping_digest": mapping_digest,
                    "alias_fingerprint": alias_fingerprint,
                    "adjudication_fingerprint": adjudication_fingerprint,
                    "input_digest": input_digest,
                }
            )
            new_ledger = {
                "schema": "posalign.mapping_ledger.v1",
                "latest_version": version,
                "latest_sequence": next_sequence,
                "latest_mapping_digest": mapping_digest,
                "latest_alias_fingerprint": alias_fingerprint,
                "latest_adjudication_fingerprint": adjudication_fingerprint,
                "latest_input_digest": input_digest,
                "history": history,
            }
            atomic_write_json(self._version_path(version), table)
            atomic_write_json(self.current_path, table)
            atomic_write_json(self.ledger_path, new_ledger)
            atomic_write_json(
                self.anchor_path,
                {
                    "schema": "posalign.mapping_anchor.v1",
                    "version": version,
                    "sequence": next_sequence,
                    "mapping_digest": mapping_digest,
                },
            )
            self.stale_path.unlink(missing_ok=True)
            return table

    def current(
        self,
        *,
        allow_stale: bool = False,
        expected_input_digest: str | None = None,
    ) -> dict[str, Any] | None:
        with exclusive_file_lock(self.lock_path):
            if not allow_stale:
                stale = self._stale_payload()
                if stale is not None:
                    self._raise_stale(
                        "superseded",
                        "mapping table is marked stale by an invalidation",
                        stale=stale,
                    )
            validation = self._validate_locked(
                expected_input_digest=expected_input_digest,
            )
            if validation.status == "empty":
                return None
            return self._load_current_unchecked()

    def resolved(self, *, allow_stale: bool = False) -> list[dict[str, Any]]:
        table = self.current(allow_stale=allow_stale)
        if table is None:
            return []
        return list(table["mapping"].get("resolved", []))

    def candidates(
        self,
        *,
        label: str | None = None,
        allow_stale: bool = False,
    ) -> dict[str, Any]:
        table = self.current(allow_stale=allow_stale)
        if table is None:
            return {}
        index = table["mapping"].get("candidate_index", {})
        if label is None:
            return index
        normalized = normalize_label(label)
        return index.get(normalized, [])

    def status(self) -> dict[str, Any]:
        try:
            validation = self.validate()
        except BusinessError as exc:
            return {
                "ok": False,
                "error": exc.to_dict()["error"],
            }
        stale = self._stale_payload()
        return {
            "ok": True,
            "mapping": validation.as_dict(),
            "stale_marker": stale,
        }

