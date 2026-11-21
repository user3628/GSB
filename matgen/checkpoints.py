"""Atomic stage checkpoints, replay transactions, and stale-snapshot detection."""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

from .canonical import (
    FileLock,
    atomic_write_bytes,
    atomic_write_json,
    canonical_json_bytes,
    fingerprint,
    read_json,
    sha256_bytes,
)
from .config import ANCHOR_SCHEMA, CHECKPOINT_SCHEMA, LEDGER_SCHEMA, config_version_label
from .errors import (
    E_INVALID_RECIPE,
    E_INVALID_STAGE,
    E_IO,
    E_STALE_CHECKPOINT,
    MatgenError,
)


STAGE_NAMES = ("control", "noise", "palette", "diagnostics")
STOP_FILE_NAME = "STOP"
PENDING_SCHEMA = "matgen.pending.v1"


def _stage_error(
    *,
    code: str,
    message: str,
    material_id: str | None,
    stage: str | None = None,
    details: dict[str, Any] | None = None,
) -> MatgenError:
    return MatgenError(
        code=code,
        message=message,
        material_id=material_id,
        stage=stage,
        details=details or {},
    )


def validate_material_id(material_id: object) -> str:
    if not isinstance(material_id, str) or not material_id:
        raise MatgenError(
            code=E_INVALID_RECIPE,
            message="material_id must be a non-empty string",
        )
    if (
        material_id in {".", ".."}
        or "\x00" in material_id
        or "/" in material_id
        or "\\" in material_id
        or len(material_id) > 160
    ):
        raise MatgenError(
            code=E_INVALID_RECIPE,
            message="material_id is not safe as a checkpoint directory name",
            material_id=material_id,
        )
    return material_id


def classify_snapshot_transition(
    *,
    candidate_sequence: int | None,
    current_sequence: int | None,
    candidate_input_fingerprint: str | None = None,
    current_input_fingerprint: str | None = None,
    candidate_digest: str | None = None,
    current_digest: str | None = None,
) -> str:
    """Classify a snapshot transition as normal_advance, rollback, or tamper."""

    if current_sequence is None:
        return "normal_advance" if candidate_sequence == 1 else "tamper"
    if candidate_sequence is None:
        return "rollback"
    if candidate_sequence <= current_sequence:
        return "rollback"
    if candidate_sequence != current_sequence + 1:
        return "tamper"
    if (
        candidate_input_fingerprint is not None
        and current_input_fingerprint is not None
        and candidate_input_fingerprint != current_input_fingerprint
    ):
        return "tamper"
    if candidate_digest is not None and current_digest is not None and candidate_digest == current_digest:
        return "tamper"
    return "normal_advance"


class CheckpointStore:
    """Workspace-backed checkpoint repository shared by processes."""

    def __init__(self, workspace: str | Path) -> None:
        self.workspace = Path(workspace).resolve()
        self.root = self.workspace / ".matgen"
        self.checkpoint_root = self.root / "checkpoints"
        self.artifact_root = self.root / "artifacts"
        self.pending_root = self.root / "pending"
        self.ledger_path = self.root / "ledger.jsonl"
        self.lock_path = self.root / "lock"
        self.anchor_path = self.workspace / ".matgen.anchor.json"
        self.stop_path = self.root / STOP_FILE_NAME
        self.root.mkdir(parents=True, exist_ok=True)
        self.checkpoint_root.mkdir(parents=True, exist_ok=True)
        self.artifact_root.mkdir(parents=True, exist_ok=True)
        self.pending_root.mkdir(parents=True, exist_ok=True)

    def _lock(self) -> FileLock:
        return FileLock(self.lock_path)

    def checkpoint_path(self, material_id: str, stage_index: int) -> Path:
        stage = STAGE_NAMES[stage_index]
        return self.checkpoint_root / material_id / f"{stage_index + 1:02d}_{stage}.json"

    def artifact_dir(self, material_id: str, stage_index: int) -> Path:
        stage = STAGE_NAMES[stage_index]
        return self.artifact_root / material_id / f"{stage_index + 1:02d}_{stage}"

    def _empty_anchor(self) -> dict[str, Any]:
        return {
            "schema": ANCHOR_SCHEMA,
            "global_sequence": 0,
            "entry_count": 0,
            "ledger_sha256": sha256_bytes(b""),
            "last_entry_digest": "",
        }

    def _ensure_ledger_files(self) -> None:
        if not self.ledger_path.exists() and not self.anchor_path.exists():
            atomic_write_bytes(self.ledger_path, b"")
            atomic_write_json(self.anchor_path, self._empty_anchor())
        elif self.ledger_path.exists() and not self.anchor_path.exists():
            entries, ledger_bytes = self._parse_ledger()
            atomic_write_json(self.anchor_path, self._anchor_for(entries, ledger_bytes))
        elif self.anchor_path.exists() and not self.ledger_path.exists():
            anchor = read_json(self.anchor_path)
            if anchor.get("entry_count", 0) != 0:
                raise _stage_error(
                    code=E_STALE_CHECKPOINT,
                    message="ledger is missing relative to the durable workspace anchor",
                    material_id=None,
                    details={"transition": "rollback", "cause": "missing_ledger"},
                )
            atomic_write_bytes(self.ledger_path, b"")

    def _parse_ledger(self) -> tuple[list[dict[str, Any]], bytes]:
        try:
            ledger_bytes = self.ledger_path.read_bytes()
        except OSError as exc:
            raise _stage_error(
                code=E_IO,
                message="unable to read checkpoint ledger",
                material_id=None,
                details={"path": str(self.ledger_path), "reason": str(exc)},
            ) from exc
        entries: list[dict[str, Any]] = []
        previous_digest = ""
        for line_number, raw_line in enumerate(ledger_bytes.splitlines(), start=1):
            if not raw_line:
                continue
            try:
                entry = json.loads(raw_line.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise _stage_error(
                    code=E_STALE_CHECKPOINT,
                    message="checkpoint ledger contains a malformed entry",
                    material_id=None,
                    details={"transition": "tamper", "line": line_number},
                ) from exc
            entry_digest = entry.pop("entry_digest", None)
            if entry.get("previous_entry_digest") != previous_digest:
                raise _stage_error(
                    code=E_STALE_CHECKPOINT,
                    message="checkpoint ledger hash chain is broken",
                    material_id=None,
                    details={"transition": "tamper", "line": line_number},
                )
            expected_digest = sha256_bytes(canonical_json_bytes(entry))
            if expected_digest != entry_digest:
                raise _stage_error(
                    code=E_STALE_CHECKPOINT,
                    message="checkpoint ledger entry digest does not match",
                    material_id=None,
                    details={"transition": "tamper", "line": line_number},
                )
            previous_digest = expected_digest
            entries.append(entry)
        return entries, ledger_bytes

    def _anchor_for(self, entries: list[dict[str, Any]], ledger_bytes: bytes) -> dict[str, Any]:
        last_digest = ""
        if entries:
            unhashed = dict(entries[-1])
            unhashed.pop("entry_digest", None)
            last_digest = sha256_bytes(canonical_json_bytes(unhashed))
        return {
            "schema": ANCHOR_SCHEMA,
            "global_sequence": entries[-1]["global_sequence"] if entries else 0,
            "entry_count": len(entries),
            "ledger_sha256": sha256_bytes(ledger_bytes),
            "last_entry_digest": last_digest,
        }

    def _load_ledger_and_anchor(self) -> tuple[list[dict[str, Any]], dict[str, Any], bytes]:
        self._ensure_ledger_files()
        try:
            anchor = read_json(self.anchor_path)
        except MatgenError as exc:
            raise _stage_error(
                code=E_STALE_CHECKPOINT,
                message="durable workspace anchor is unreadable",
                material_id=None,
                details={"transition": "tamper"},
            ) from exc
        if not isinstance(anchor, dict) or anchor.get("schema") != ANCHOR_SCHEMA:
            raise _stage_error(
                code=E_STALE_CHECKPOINT,
                message="durable workspace anchor has an invalid schema",
                material_id=None,
                details={"transition": "tamper"},
            )
        entries, ledger_bytes = self._parse_ledger()
        actual_anchor = self._anchor_for(entries, ledger_bytes)
        if actual_anchor != anchor:
            pending_files = list(self.pending_root.glob("*.json"))
            can_repair = (
                bool(pending_files)
                and anchor.get("entry_count", 0) + 1 == len(entries)
                and anchor.get("global_sequence", 0) + 1
                == (entries[-1]["global_sequence"] if entries else -1)
            )
            if can_repair:
                atomic_write_json(self.anchor_path, actual_anchor)
                anchor = actual_anchor
            else:
                transition = (
                    "rollback"
                    if len(entries) < int(anchor.get("entry_count", 0))
                    else "tamper"
                )
                raise _stage_error(
                    code=E_STALE_CHECKPOINT,
                    message="workspace ledger does not match its durable anchor",
                    material_id=None,
                    details={
                        "transition": transition,
                        "anchor_entry_count": anchor.get("entry_count"),
                        "actual_entry_count": len(entries),
                    },
                )
        return entries, anchor, ledger_bytes

    @staticmethod
    def _ledger_state(entries: list[dict[str, Any]]) -> dict[str, dict[int, dict[str, Any]]]:
        state: dict[str, dict[int, dict[str, Any]]] = {}
        for entry in entries:
            material_id = entry.get("material_id")
            if not isinstance(material_id, str):
                continue
            material_state = state.setdefault(material_id, {})
            if entry.get("event") == "stage_commit":
                stage_index = int(entry["stage_index"])
                material_state[stage_index] = {
                    "kind": "commit",
                    "event": entry,
                }
            elif entry.get("event") == "invalidate":
                from_stage = int(entry["from_stage_index"])
                for stage_index in range(from_stage, len(STAGE_NAMES)):
                    material_state[stage_index] = {
                        "kind": "invalidated",
                        "event": entry,
                    }
        return state

    def _make_ledger_entry(
        self,
        *,
        entries: list[dict[str, Any]],
        event: str,
        material_id: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        previous_digest = ""
        if entries:
            previous_without_digest = dict(entries[-1])
            previous_digest = sha256_bytes(canonical_json_bytes(previous_without_digest))
        core = {
            "schema": LEDGER_SCHEMA,
            "event": event,
            "global_sequence": (entries[-1]["global_sequence"] + 1) if entries else 1,
            "material_id": material_id,
            "previous_entry_digest": previous_digest,
            **payload,
        }
        core["entry_digest"] = sha256_bytes(canonical_json_bytes(core))
        return core

    def _append_ledger(
        self,
        *,
        entries: list[dict[str, Any]],
        ledger_bytes: bytes,
        entry: dict[str, Any],
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        line = canonical_json_bytes(entry) + b"\n"
        new_bytes = ledger_bytes + line
        atomic_write_bytes(self.ledger_path, new_bytes)
        new_entries = entries + [entry]
        anchor = self._anchor_for(new_entries, new_bytes)
        atomic_write_json(self.anchor_path, anchor)
        return new_entries, anchor

    def _load_record_file(self, path: Path) -> dict[str, Any]:
        try:
            record = read_json(path)
        except MatgenError as exc:
            raise _stage_error(
                code=E_STALE_CHECKPOINT,
                message="checkpoint record is unreadable",
                material_id=None,
                details={"transition": "tamper", "path": str(path)},
            ) from exc
        if not isinstance(record, dict) or record.get("schema") != CHECKPOINT_SCHEMA:
            raise _stage_error(
                code=E_STALE_CHECKPOINT,
                message="checkpoint record has an invalid schema",
                material_id=record.get("material_id") if isinstance(record, dict) else None,
                details={"transition": "tamper", "path": str(path)},
            )
        record_digest = record.get("record_digest")
        core = {key: value for key, value in record.items() if key != "record_digest"}
        expected_digest = sha256_bytes(canonical_json_bytes(core))
        if record_digest != expected_digest:
            raise _stage_error(
                code=E_STALE_CHECKPOINT,
                message="checkpoint record digest does not match its content",
                material_id=record.get("material_id"),
                stage=record.get("stage"),
                details={"transition": "tamper", "path": str(path)},
            )
        return record

    def _artifact_summary(self, material_id: str, stage_index: int) -> list[dict[str, Any]]:
        directory = self.artifact_dir(material_id, stage_index)
        if not directory.exists():
            return []
        summary = []
        for path in sorted(directory.rglob("*")):
            if path.is_file():
                data = path.read_bytes()
                summary.append(
                    {
                        "name": path.relative_to(directory).as_posix(),
                        "size": len(data),
                        "sha256": sha256_bytes(data),
                    }
                )
        return summary

    def _verify_record_against_state(
        self,
        *,
        record: dict[str, Any],
        state_entry: dict[str, Any] | None,
        verify_artifacts: bool,
    ) -> None:
        material_id = str(record.get("material_id"))
        stage = str(record.get("stage"))
        stage_index = int(record.get("stage_index", -1))
        if stage_index < 0 or stage_index >= len(STAGE_NAMES) or STAGE_NAMES[stage_index] != stage:
            raise _stage_error(
                code=E_STALE_CHECKPOINT,
                message="checkpoint stage identity is invalid",
                material_id=material_id,
                stage=stage,
                details={"transition": "tamper"},
            )
        expected_path = self.checkpoint_path(material_id, stage_index)
        if record.get("material_id") != material_id:
            raise _stage_error(
                code=E_STALE_CHECKPOINT,
                message="checkpoint material identity is inconsistent",
                material_id=material_id,
                stage=stage,
                details={"transition": "tamper"},
            )
        if state_entry is None or state_entry.get("kind") != "commit":
            raise _stage_error(
                code=E_STALE_CHECKPOINT,
                message="checkpoint record has no matching ledger commit",
                material_id=material_id,
                stage=stage,
                details={"transition": "tamper", "path": str(expected_path)},
            )
        ledger_event = state_entry["event"]
        if record.get("record_digest") != ledger_event.get("record_digest"):
            current_sequence = int(ledger_event.get("sequence", 0))
            candidate_sequence = int(record.get("sequence", 0))
            transition = (
                "rollback" if candidate_sequence < current_sequence else "tamper"
            )
            raise _stage_error(
                code=E_STALE_CHECKPOINT,
                message="checkpoint record does not match the committed ledger digest",
                material_id=material_id,
                stage=stage,
                details={
                    "transition": transition,
                    "candidate_sequence": candidate_sequence,
                    "ledger_sequence": current_sequence,
                },
            )
        for key in (
            "sequence",
            "stage_revision",
            "global_sequence",
            "upstream_record_digest",
            "stage_input_fingerprint",
            "recipe_fingerprint",
            "config_version",
            "artifact_digest",
        ):
            if record.get(key) != ledger_event.get(key):
                raise _stage_error(
                    code=E_STALE_CHECKPOINT,
                    message="checkpoint metadata differs from its ledger commit",
                    material_id=material_id,
                    stage=stage,
                    details={"transition": "tamper", "field": key},
                )
        if verify_artifacts:
            actual_summary = self._artifact_summary(material_id, stage_index)
            if actual_summary != record.get("artifact_summary"):
                raise _stage_error(
                    code=E_STALE_CHECKPOINT,
                    message="stage artifact summary no longer matches the checkpoint",
                    material_id=material_id,
                    stage=stage,
                    details={"transition": "tamper", "cause": "artifact_summary_mismatch"},
                )
            artifact_digest = fingerprint(actual_summary)
            if artifact_digest != record.get("artifact_digest"):
                raise _stage_error(
                    code=E_STALE_CHECKPOINT,
                    message="stage artifact digest does not match the checkpoint",
                    material_id=material_id,
                    stage=stage,
                    details={"transition": "tamper", "cause": "artifact_digest_mismatch"},
                )

    def _recover_pending(self) -> None:
        entries, _anchor, _ledger_bytes = self._load_ledger_and_anchor()
        ledger_by_global = {int(entry["global_sequence"]): entry for entry in entries}
        for pending_path in sorted(self.pending_root.glob("*.json")):
            try:
                pending = read_json(pending_path)
            except MatgenError:
                pending_path.unlink(missing_ok=True)
                continue
            record = pending.get("record") if isinstance(pending, dict) else None
            if not isinstance(record, dict):
                pending_path.unlink(missing_ok=True)
                continue
            global_sequence = int(record.get("global_sequence", -1))
            ledger_entry = ledger_by_global.get(global_sequence)
            if ledger_entry is None or ledger_entry.get("record_digest") != record.get("record_digest"):
                pending_path.unlink(missing_ok=True)
                continue
            material_id = validate_material_id(record.get("material_id"))
            stage_index = int(record.get("stage_index"))
            target_path = self.checkpoint_path(material_id, stage_index)
            if target_path.exists():
                existing = self._load_record_file(target_path)
                if existing.get("record_digest") != record.get("record_digest"):
                    raise _stage_error(
                        code=E_STALE_CHECKPOINT,
                        message="pending checkpoint recovery found a conflicting record",
                        material_id=material_id,
                        stage=STAGE_NAMES[stage_index],
                        details={"transition": "tamper"},
                    )
            else:
                atomic_write_json(target_path, record)
            pending_path.unlink(missing_ok=True)

    def verify_integrity(self) -> dict[str, Any]:
        with self._lock():
            self._recover_pending()
            entries, anchor, _ledger_bytes = self._load_ledger_and_anchor()
            state = self._ledger_state(entries)
            for material_dir in sorted(self.checkpoint_root.glob("*")):
                if not material_dir.is_dir():
                    continue
                material_id = material_dir.name
                for path in sorted(material_dir.glob("*.json")):
                    record = self._load_record_file(path)
                    if record.get("material_id") != material_id:
                        raise _stage_error(
                            code=E_STALE_CHECKPOINT,
                            message="checkpoint material directory and record disagree",
                            material_id=str(record.get("material_id")),
                            details={"transition": "tamper", "path": str(path)},
                        )
                    stage_index = int(record.get("stage_index", -1))
                    state_entry = state.get(material_id, {}).get(stage_index)
                    self._verify_record_against_state(
                        record=record,
                        state_entry=state_entry,
                        verify_artifacts=True,
                    )
            for material_id, material_state in state.items():
                for stage_index, state_entry in material_state.items():
                    if state_entry.get("kind") != "commit":
                        continue
                    path = self.checkpoint_path(material_id, stage_index)
                    if not path.exists():
                        raise _stage_error(
                            code=E_STALE_CHECKPOINT,
                            message="committed checkpoint record is missing",
                            material_id=material_id,
                            stage=STAGE_NAMES[stage_index],
                            details={"transition": "tamper", "cause": "missing_record"},
                        )
            return {
                "global_sequence": anchor["global_sequence"],
                "entry_count": anchor["entry_count"],
                "ledger_sha256": anchor["ledger_sha256"],
                "material_count": len(state),
            }

    def load_stage(self, material_id: str, stage_index: int) -> dict[str, Any]:
        validate_material_id(material_id)
        if not 0 <= stage_index < len(STAGE_NAMES):
            raise _stage_error(
                code=E_INVALID_STAGE,
                message="stage index is out of range",
                material_id=material_id,
                details={"stage_index": stage_index},
            )
        with self._lock():
            self._recover_pending()
            entries, _anchor, _ledger_bytes = self._load_ledger_and_anchor()
            state = self._ledger_state(entries)
            self._cleanup_invalidated_records(state)
            path = self.checkpoint_path(material_id, stage_index)
            if not path.exists():
                raise _stage_error(
                    code=E_INVALID_STAGE,
                    message="requested stage checkpoint is missing",
                    material_id=material_id,
                    stage=STAGE_NAMES[stage_index],
                    details={"stage_index": stage_index},
                )
            record = self._load_record_file(path)
            state_entry = state.get(material_id, {}).get(stage_index)
            self._verify_record_against_state(
                record=record,
                state_entry=state_entry,
                verify_artifacts=False,
            )
            return record

    def load_artifact_bytes(self, material_id: str, stage_index: int) -> dict[str, bytes]:
        record = self.load_stage(material_id, stage_index)
        directory = self.artifact_dir(material_id, stage_index)
        loaded: dict[str, bytes] = {}
        for entry in record["artifact_summary"]:
            path = directory / entry["name"]
            try:
                data = path.read_bytes()
            except OSError as exc:
                raise _stage_error(
                    code=E_STALE_CHECKPOINT,
                    message="checkpoint artifact file is missing",
                    material_id=material_id,
                    stage=STAGE_NAMES[stage_index],
                    details={"transition": "tamper", "path": str(path)},
                ) from exc
            if len(data) != entry["size"] or sha256_bytes(data) != entry["sha256"]:
                raise _stage_error(
                    code=E_STALE_CHECKPOINT,
                    message="checkpoint artifact file was modified",
                    material_id=material_id,
                    stage=STAGE_NAMES[stage_index],
                    details={"transition": "tamper", "path": str(path)},
                )
            loaded[entry["name"]] = data
        return loaded

    def _write_artifacts(self, material_id: str, stage_index: int, artifacts: dict[str, bytes]) -> list[dict[str, Any]]:
        directory = self.artifact_dir(material_id, stage_index)
        if directory.exists():
            shutil.rmtree(directory)
        directory.mkdir(parents=True, exist_ok=True)
        summary = []
        for name in sorted(artifacts):
            if not name or name in {".", ".."} or "/" in name or "\\" in name or "\x00" in name:
                raise _stage_error(
                    code=E_INVALID_RECIPE,
                    message="artifact names must be simple file names",
                    material_id=material_id,
                    stage=STAGE_NAMES[stage_index],
                    details={"name": name},
                )
            data = artifacts[name]
            atomic_write_bytes(directory / name, data)
            summary.append({"name": name, "size": len(data), "sha256": sha256_bytes(data)})
        return summary

    def publish_stage(
        self,
        *,
        material_id: str,
        stage_index: int,
        recipe_fingerprint: str,
        stage_input_fingerprint: str,
        config_version: str,
        artifacts: dict[str, bytes],
        upstream_record_digest: str = "",
    ) -> dict[str, Any]:
        validate_material_id(material_id)
        if not 0 <= stage_index < len(STAGE_NAMES):
            raise _stage_error(
                code=E_INVALID_STAGE,
                message="stage index is out of range",
                material_id=material_id,
                details={"stage_index": stage_index},
            )
        stage = STAGE_NAMES[stage_index]
        with self._lock():
            self._verify_integrity_locked()
            entries, anchor, ledger_bytes = self._load_ledger_and_anchor()
            path = self.checkpoint_path(material_id, stage_index)
            if path.exists():
                raise _stage_error(
                    code=E_STALE_CHECKPOINT,
                    message="stage checkpoint already exists; duplicate publication is stale",
                    material_id=material_id,
                    stage=stage,
                    details={"transition": "rollback", "cause": "duplicate_stage"},
                )
            material_commit_sequences = [
                int(entry["sequence"])
                for entry in entries
                if entry.get("event") == "stage_commit"
                and entry.get("material_id") == material_id
            ]
            sequence = max(material_commit_sequences, default=0) + 1
            stage_revision = (
                1
                + sum(
                    1
                    for entry in entries
                    if entry.get("event") == "stage_commit"
                    and entry.get("material_id") == material_id
                    and int(entry.get("stage_index", -1)) == stage_index
                )
            )
            if stage_index > 0:
                previous_path = self.checkpoint_path(material_id, stage_index - 1)
                if not previous_path.exists():
                    raise _stage_error(
                        code=E_INVALID_STAGE,
                        message="required upstream stage checkpoint is missing",
                        material_id=material_id,
                        stage=stage,
                        details={"required_stage": STAGE_NAMES[stage_index - 1]},
                    )
                previous_record = self._load_record_file(previous_path)
                if previous_record.get("material_id") != material_id:
                    raise _stage_error(
                        code=E_STALE_CHECKPOINT,
                        message="upstream checkpoint belongs to another material",
                        material_id=material_id,
                        stage=stage,
                        details={"transition": "tamper", "upstream_material_id": previous_record.get("material_id")},
                    )
                if previous_record.get("record_digest") != upstream_record_digest:
                    raise _stage_error(
                        code=E_STALE_CHECKPOINT,
                        message="upstream record digest does not match the supplied dependency",
                        material_id=material_id,
                        stage=stage,
                        details={"transition": "tamper", "cause": "upstream_digest_mismatch"},
                    )
            elif upstream_record_digest:
                raise _stage_error(
                    code=E_STALE_CHECKPOINT,
                    message="control stage cannot declare an upstream checkpoint",
                    material_id=material_id,
                    stage=stage,
                    details={"transition": "tamper"},
                )

            summary = self._write_artifacts(material_id, stage_index, artifacts)
            artifact_digest = fingerprint(summary)
            record_core = {
                "schema": CHECKPOINT_SCHEMA,
                "material_id": material_id,
                "stage_index": stage_index,
                "stage": stage,
                "sequence": sequence,
                "stage_revision": stage_revision,
                "global_sequence": int(anchor["global_sequence"]) + 1,
                "recipe_fingerprint": recipe_fingerprint,
                "stage_input_fingerprint": stage_input_fingerprint,
                "config_version": config_version,
                "artifact_digest": artifact_digest,
                "artifact_summary": summary,
                "upstream_record_digest": upstream_record_digest,
            }
            record = dict(record_core)
            record["record_digest"] = sha256_bytes(canonical_json_bytes(record_core))
            pending_path = self.pending_root / f"{record['global_sequence']:020d}_{material_id}.json"
            atomic_write_json(
                pending_path,
                {"schema": PENDING_SCHEMA, "record": record},
            )
            leader_payload = {
                "stage_index": stage_index,
                "stage": stage,
                "sequence": sequence,
                "stage_revision": stage_revision,
                "recipe_fingerprint": recipe_fingerprint,
                "stage_input_fingerprint": stage_input_fingerprint,
                "config_version": config_version,
                "artifact_digest": artifact_digest,
                "artifact_summary": summary,
                "upstream_record_digest": upstream_record_digest,
                "record_digest": record["record_digest"],
                "record": record,
            }
            leader_entry = self._make_ledger_entry(
                entries=entries,
                event="stage_commit",
                material_id=material_id,
                payload=leader_payload,
            )
            self._append_ledger(
                entries=entries,
                ledger_bytes=ledger_bytes,
                entry=leader_entry,
            )
            atomic_write_json(path, record)
            pending_path.unlink(missing_ok=True)
            return record

    def _verify_integrity_locked(self) -> dict[str, Any]:
        self._recover_pending()
        entries, anchor, _ledger_bytes = self._load_ledger_and_anchor()
        state = self._ledger_state(entries)
        self._cleanup_invalidated_records(state)
        for material_dir in sorted(self.checkpoint_root.glob("*")):
            if not material_dir.is_dir():
                continue
            material_id = material_dir.name
            for path in sorted(material_dir.glob("*.json")):
                record = self._load_record_file(path)
                stage_index = int(record.get("stage_index", -1))
                self._verify_record_against_state(
                    record=record,
                    state_entry=state.get(material_id, {}).get(stage_index),
                    verify_artifacts=True,
                )
        for material_id, material_state in state.items():
            for stage_index, state_entry in material_state.items():
                if state_entry.get("kind") != "commit":
                    continue
                if not self.checkpoint_path(material_id, stage_index).exists():
                    raise _stage_error(
                        code=E_STALE_CHECKPOINT,
                        message="committed checkpoint record is missing",
                        material_id=material_id,
                        stage=STAGE_NAMES[stage_index],
                        details={"transition": "tamper", "cause": "missing_record"},
                    )
        return {
            "global_sequence": anchor["global_sequence"],
            "entry_count": anchor["entry_count"],
        }

    def _cleanup_invalidated_records(self, state: dict[str, dict[int, dict[str, Any]]]) -> None:
        for material_id, material_state in state.items():
            for stage_index, state_entry in material_state.items():
                if state_entry.get("kind") != "invalidated":
                    continue
                self.checkpoint_path(material_id, stage_index).unlink(missing_ok=True)
                directory = self.artifact_dir(material_id, stage_index)
                if directory.exists():
                    shutil.rmtree(directory)

    def invalidate_from(self, material_id: str, stage_index: int, *, reason: str) -> dict[str, Any] | None:
        validate_material_id(material_id)
        if not 0 <= stage_index < len(STAGE_NAMES):
            raise _stage_error(
                code=E_INVALID_STAGE,
                message="stage index is out of range",
                material_id=material_id,
                details={"stage_index": stage_index},
            )
        with self._lock():
            self._verify_integrity_locked()
            entries, _anchor, ledger_bytes = self._load_ledger_and_anchor()
            existing = [
                index
                for index in range(stage_index, len(STAGE_NAMES))
                if self.checkpoint_path(material_id, index).exists()
            ]
            if not existing:
                return None
            payload = {
                "from_stage_index": stage_index,
                "reason": reason,
                "invalidated_stages": [STAGE_NAMES[index] for index in existing],
            }
            invalidation_entry = self._make_ledger_entry(
                entries=entries,
                event="invalidate",
                material_id=material_id,
                payload=payload,
            )
            self._append_ledger(
                entries=entries,
                ledger_bytes=ledger_bytes,
                entry=invalidation_entry,
            )
            for index in range(stage_index, len(STAGE_NAMES)):
                self.checkpoint_path(material_id, index).unlink(missing_ok=True)
                directory = self.artifact_dir(material_id, index)
                if directory.exists():
                    shutil.rmtree(directory)
            return invalidation_entry

    def status(self) -> dict[str, Any]:
        integrity = self.verify_integrity()
        with self._lock():
            materials: dict[str, dict[str, Any]] = {}
            for material_dir in sorted(self.checkpoint_root.glob("*")):
                if not material_dir.is_dir():
                    continue
                material_id = material_dir.name
                stage_records = []
                for stage_index, stage_name in enumerate(STAGE_NAMES):
                    path = self.checkpoint_path(material_id, stage_index)
                    if not path.exists():
                        continue
                    record = self._load_record_file(path)
                    stage_records.append(
                        {
                            "stage_index": stage_index,
                            "stage": stage_name,
                            "sequence": record["sequence"],
                            "stage_revision": record["stage_revision"],
                            "global_sequence": record["global_sequence"],
                            "stage_input_fingerprint": record["stage_input_fingerprint"],
                            "artifact_digest": record["artifact_digest"],
                            "record_digest": record["record_digest"],
                        }
                    )
                materials[material_id] = {"stages": stage_records}
            return {
                "workspace": str(self.workspace),
                "integrity": integrity,
                "stop_requested": self.stop_path.exists(),
                "materials": {material_id: materials[material_id] for material_id in sorted(materials)},
            }

    def request_stop(self) -> None:
        atomic_write_bytes(self.stop_path, b"stop requested\n")

    def clear_stop(self) -> None:
        self.stop_path.unlink(missing_ok=True)

    def stop_requested(self) -> bool:
        return self.stop_path.exists()

    def get_version_report(self, config: dict[str, Any]) -> dict[str, Any]:
        return {
            "algorithm_version": config["algorithm_version"],
            "config_version": config_version_label(config),
            "checkpoint_schema": CHECKPOINT_SCHEMA,
            "ledger_schema": LEDGER_SCHEMA,
            "anchor_schema": ANCHOR_SCHEMA,
        }

