from __future__ import annotations

import hashlib
import json
import os
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .errors import PlannerError


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, sort_keys=True, indent=2, ensure_ascii=False, allow_nan=False)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)
    try:
        directory_fd = os.open(path.parent, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(directory_fd)
    except OSError:
        pass
    finally:
        os.close(directory_fd)


def _request_signature_sha(request_signature: str) -> str:
    return hashlib.sha256(request_signature.encode("utf-8")).hexdigest()


class IntentJournal:
    """Durable build intent log and persisted-plan directory."""

    def __init__(self, runtime_dir: Path) -> None:
        self.runtime_dir = runtime_dir.resolve()
        self.intent_dir = self.runtime_dir / "intents"
        self.plan_dir = self.runtime_dir / "plans"
        self.recovery_dir = self.runtime_dir / "recovery"
        for directory in (self.intent_dir, self.plan_dir, self.recovery_dir):
            directory.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    def _intent_path(self, intent_id: str) -> Path:
        return self.intent_dir / f"{intent_id}.json"

    def _plan_path(self, relative_path: str) -> Path:
        path = (self.runtime_dir / relative_path).resolve()
        try:
            path.relative_to(self.plan_dir)
        except ValueError as exc:
            raise RuntimeError("plan path escaped the runtime plan directory") from exc
        return path

    def begin(self, query_id: str, snapshot_version: int, request_signature: str) -> dict[str, Any]:
        with self._lock:
            intent_id = uuid.uuid4().hex
            signature_sha = _request_signature_sha(request_signature)
            record = {
                "intent_id": intent_id,
                "query_id": query_id,
                "snapshot_version": snapshot_version,
                "request_signature": request_signature,
                "request_signature_sha256": signature_sha,
                "state": "started",
                "started_at": utc_now(),
                "pid": os.getpid(),
                "plan_relpath": f"plans/{signature_sha}-{intent_id}.json",
            }
            _atomic_write_json(self._intent_path(intent_id), record)
            return record

    def write_plan(self, record: dict[str, Any], plan: dict[str, Any]) -> Path:
        path = self._plan_path(record["plan_relpath"])
        _atomic_write_json(path, plan)
        return path

    def complete(self, record: dict[str, Any], plan_digest: str) -> None:
        with self._lock:
            path = self._intent_path(record["intent_id"])
            current = self._read_intent(path)
            current.update(
                {
                    "state": "completed",
                    "completed_at": utc_now(),
                    "plan_digest": plan_digest,
                }
            )
            _atomic_write_json(path, current)

    def abort(self, record: dict[str, Any], error_code: str, message: str) -> None:
        with self._lock:
            path = self._intent_path(record["intent_id"])
            current = self._read_intent(path)
            current.update(
                {
                    "state": "aborted",
                    "aborted_at": utc_now(),
                    "error_code": error_code,
                    "message": message,
                }
            )
            _atomic_write_json(path, current)

    @staticmethod
    def _read_intent(path: Path) -> dict[str, Any]:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise RuntimeError(f"cannot read intent {path}: {exc}") from exc
        if not isinstance(payload, dict):
            raise RuntimeError(f"intent {path} is not a JSON object")
        return payload

    def recover(self) -> dict[str, Any]:
        """Resolve started intents using only facts persisted on disk."""

        with self._lock:
            scanned = 0
            recovered_aborted = 0
            completed = 0
            already_aborted = 0
            corrupt = 0
            entries: list[dict[str, Any]] = []
            for path in sorted(self.intent_dir.glob("*.json")):
                scanned += 1
                try:
                    record = self._read_intent(path)
                except RuntimeError:
                    corrupt += 1
                    entries.append(
                        {
                            "intent_file": path.name,
                            "state": "corrupt",
                            "action": "left_unchanged",
                        }
                    )
                    continue

                state = record.get("state")
                entry = {
                    "intent_id": record.get("intent_id", path.stem),
                    "query_id": record.get("query_id"),
                    "snapshot_version": record.get("snapshot_version"),
                    "previous_state": state,
                }
                if state == "started":
                    orphan_plan = None
                    plan_relpath = record.get("plan_relpath")
                    if isinstance(plan_relpath, str):
                        try:
                            candidate = self._plan_path(plan_relpath)
                        except RuntimeError:
                            candidate = None
                        if candidate is not None and candidate.exists():
                            orphan_plan = candidate
                    plan_removed = False
                    if orphan_plan is not None:
                        orphan_plan.unlink()
                        plan_removed = True
                    record.update(
                        {
                            "state": "aborted_recovered",
                            "recovered_at": utc_now(),
                            "recovery_reason": "process terminated after intent was persisted and before completion was persisted",
                        }
                    )
                    _atomic_write_json(path, record)
                    recovered_aborted += 1
                    entry.update(
                        {
                            "state": "aborted_recovered",
                            "plan_rolled_back": plan_removed,
                            "action": (
                                "marked_aborted_and_removed_orphan_plan"
                                if plan_removed
                                else "marked_aborted_without_orphan_plan"
                            ),
                        }
                    )
                elif state == "completed":
                    completed += 1
                    plan_exists = False
                    plan_relpath = record.get("plan_relpath")
                    if isinstance(plan_relpath, str):
                        try:
                            plan_exists = self._plan_path(plan_relpath).exists()
                        except RuntimeError:
                            plan_exists = False
                    entry.update(
                        {
                            "state": "completed",
                            "plan_exists": plan_exists,
                            "action": "left_unchanged",
                        }
                    )
                elif state in {"aborted", "aborted_recovered"}:
                    already_aborted += 1
                    entry.update({"state": state, "action": "left_unchanged"})
                else:
                    corrupt += 1
                    entry.update({"state": "unknown_state", "action": "left_unchanged"})
                entries.append(entry)

            report = {
                "report_version": 1,
                "scanned_at": utc_now(),
                "source": "persisted_intent_files_only",
                "intent_directory": str(self.intent_dir),
                "counts": {
                    "scanned": scanned,
                    "recovered_aborted": recovered_aborted,
                    "completed": completed,
                    "already_aborted": already_aborted,
                    "corrupt": corrupt,
                },
                "entries": entries,
            }
            _atomic_write_json(self.recovery_dir / "latest.json", report)
            return report

    def latest_recovery_report(self) -> dict[str, Any]:
        path = self.recovery_dir / "latest.json"
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise PlannerError(
                "E_RECOVERY_REPORT_NOT_FOUND",
                "No persisted recovery report is available.",
                {"path": str(path)},
            ) from exc
        if not isinstance(payload, dict):
            raise PlannerError(
                "E_RECOVERY_REPORT_NOT_FOUND",
                "The persisted recovery report is malformed.",
                {"path": str(path)},
            )
        return payload

