from __future__ import annotations

import uuid
from pathlib import Path
from typing import Any

from .canonical import atomic_write_json, read_json
from .editing import apply_edit
from .errors import CabinError, invalid_argument, stale_version
from .workspace import Workspace


class EditTransactions:
    def __init__(self, workspace: Workspace) -> None:
        self.workspace = workspace

    def _path(self, edit_id: str) -> Path:
        return self.workspace.pending_dir / f"{edit_id}.json"

    def _load_pending(self, edit_id: str) -> dict[str, Any]:
        path = self._path(edit_id)
        if not path.exists():
            raise CabinError(
                "E_NOT_FOUND",
                "pending edit does not exist",
                details={"edit_id": edit_id},
                http_status=404,
            )
        try:
            return read_json(path)
        except (OSError, ValueError) as exc:
            raise CabinError(
                "E_PENDING_EDIT_CORRUPT",
                "pending edit is unreadable",
                details={"edit_id": edit_id, "reason": str(exc)},
                http_status=409,
            ) from exc

    def _save_pending(self, pending: dict[str, Any]) -> None:
        atomic_write_json(self._path(pending["edit_id"]), pending)

    def prepare(
        self,
        *,
        operation: str,
        parameters: dict[str, Any],
        expected_version: int,
        shard_id: str,
        edit_id: str | None = None,
    ) -> dict[str, Any]:
        edit_id = edit_id or "edit-" + uuid.uuid4().hex
        with self.workspace.locks.hold("state"):
            state = self.workspace.load_state()
            applied = apply_edit(
                state,
                operation=operation,
                parameters=parameters,
                expected_version=expected_version,
                shard_id=shard_id,
            )
            pending = {
                "edit_id": edit_id,
                "status": "prepared",
                "operation": operation,
                "parameters": parameters,
                "base_version": expected_version,
                "shard_id": shard_id,
                "prepared_manifest_digest": state["manifest_digest"],
                "candidate_version": applied["edit"]["version"],
            }
            self._save_pending(pending)
            return pending

    def _mark_stale(self, pending: dict[str, Any], error: CabinError) -> None:
        pending["status"] = "stale"
        pending["last_error"] = error.to_dict()["error"]
        self._save_pending(pending)

    def _publish_locked(self, pending: dict[str, Any], state: dict[str, Any]) -> dict[str, Any]:
        if pending.get("status") == "published":
            raise stale_version(
                "edit was already published",
                edit_id=pending["edit_id"],
                reason="duplicate_publish",
                published_version=pending.get("published_version"),
            )
        if pending.get("status") == "discarded":
            raise stale_version(
                "edit was discarded and cannot be published",
                edit_id=pending["edit_id"],
                reason="discarded_edit",
            )
        try:
            updated = apply_edit(
                state,
                operation=pending["operation"],
                parameters=pending["parameters"],
                expected_version=pending["base_version"],
                shard_id=pending["shard_id"],
            )
        except CabinError as exc:
            if exc.code == "E_STALE_VERSION":
                self._mark_stale(pending, exc)
                exc.details["edit_id"] = pending["edit_id"]
                exc.details["pending_status"] = "stale"
            raise
        self.workspace.save_state(updated)
        pending["status"] = "published"
        pending["published_version"] = updated["edit"]["version"]
        pending["published_manifest_digest"] = updated["manifest_digest"]
        self._save_pending(pending)
        return {
            "ok": True,
            "status": "published",
            "edit_id": pending["edit_id"],
            "edit_version": updated["edit"]["version"],
            "operation": pending["operation"],
            "shard_id": pending["shard_id"],
        }

    def publish(self, edit_id: str) -> dict[str, Any]:
        with self.workspace.locks.hold("state"):
            pending = self._load_pending(edit_id)
            state = self.workspace.load_state()
            return self._publish_locked(pending, state)

    def retry(self, edit_id: str, *, expected_version: int | None = None) -> dict[str, Any]:
        with self.workspace.locks.hold("state"):
            pending = self._load_pending(edit_id)
            if pending.get("status") == "published":
                raise stale_version(
                    "edit was already published",
                    edit_id=edit_id,
                    reason="duplicate_publish",
                )
            if pending.get("status") == "discarded":
                raise stale_version(
                    "edit was discarded and cannot be retried",
                    edit_id=edit_id,
                    reason="discarded_edit",
                )
            state = self.workspace.load_state()
            target_version = state["edit"]["version"] if expected_version is None else expected_version
            pending["base_version"] = target_version
            pending["status"] = "prepared"
            self._save_pending(pending)
            return self._publish_locked(pending, state)

    def discard(self, edit_id: str) -> dict[str, Any]:
        with self.workspace.locks.hold("state"):
            pending = self._load_pending(edit_id)
            if pending.get("status") == "published":
                raise stale_version(
                    "published edit cannot be discarded",
                    edit_id=edit_id,
                    reason="already_published",
                )
            pending["status"] = "discarded"
            self._save_pending(pending)
            return {
                "ok": True,
                "status": "discarded",
                "edit_id": edit_id,
            }

    def list_pending(self) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        for path in sorted(self.workspace.pending_dir.glob("*.json")):
            try:
                result.append(read_json(path))
            except (OSError, ValueError):
                result.append({"edit_id": path.stem, "status": "corrupt"})
        return result

    def apply_direct(
        self,
        *,
        operation: str,
        parameters: dict[str, Any],
        expected_version: int,
        shard_id: str,
        edit_id: str | None = None,
    ) -> dict[str, Any]:
        edit_id = edit_id or "edit-" + uuid.uuid4().hex
        with self.workspace.locks.hold("state"):
            state = self.workspace.load_state()
            try:
                updated = apply_edit(
                    state,
                    operation=operation,
                    parameters=parameters,
                    expected_version=expected_version,
                    shard_id=shard_id,
                )
            except CabinError as exc:
                if exc.code == "E_STALE_VERSION":
                    pending = {
                        "edit_id": edit_id,
                        "status": "stale",
                        "operation": operation,
                        "parameters": parameters,
                        "base_version": expected_version,
                        "shard_id": shard_id,
                        "prepared_manifest_digest": state["manifest_digest"],
                        "last_error": exc.to_dict()["error"],
                    }
                    self._save_pending(pending)
                    exc.details["edit_id"] = edit_id
                    exc.details["pending_status"] = "stale"
                raise
            self.workspace.save_state(updated)
            return {
                "ok": True,
                "status": "applied",
                "edit_id": edit_id,
                "edit_version": updated["edit"]["version"],
                "config_version": updated["config_version"],
                "operation": operation,
                "shard_id": shard_id,
                "audit": updated["edit"]["audit"][-1],
            }
