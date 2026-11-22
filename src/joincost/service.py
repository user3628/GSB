from __future__ import annotations

import copy
import hashlib
import os
import threading
import time
import uuid
from pathlib import Path
from typing import Any

from .errors import (
    BusinessError,
    E_CONFLICTING_REQUEST,
    E_FORMAT_CORRUPT,
    E_FORMAT_UNSUPPORTED,
    E_INPUT_INVALID,
    E_NOT_FOUND,
    E_REQUEST_ABORTED,
    E_VERSION_RETIRED,
)
from .formats import decode_snapshot
from .fixed import MAX_ROWS
from .model import PlanOptions, StatsSnapshot, canonical_json, parse_predicate
from .optimizer import build_plan
from .store import Store


def _business_error_from_payload(payload: dict[str, Any]) -> BusinessError:
    error = payload.get("error", payload)
    return BusinessError(
        str(error.get("code", "E_INTERNAL")),
        str(error.get("message", "stored error")),
        dict(error.get("details", {})),
    )


class Service:
    def __init__(
        self,
        store: Store,
        *,
        workspace_root: str | Path = ".",
        recover_on_start: bool = True,
        build_delay_ms: int | None = None,
    ):
        self.store = store
        self.workspace_root = Path(workspace_root).resolve()
        self._writer_lock = threading.RLock()
        self._build_lock = threading.Lock()
        if build_delay_ms is None:
            try:
                build_delay_ms = int(os.environ.get("JCS_BUILD_DELAY_MS", "0"))
            except ValueError:
                build_delay_ms = 0
        self.build_delay_ms = max(0, build_delay_ms)
        self.startup_recovery = store.recover() if recover_on_start else None

    def _require_snapshot(self, version: int) -> StatsSnapshot:
        return self.store.get_snapshot(version)

    def _validated_source_path(self, path: str | Path) -> Path:
        candidate = Path(path)
        if not candidate.is_absolute():
            candidate = self.workspace_root / candidate
        resolved = candidate.resolve()
        try:
            resolved.relative_to(self.workspace_root)
        except ValueError as exc:
            raise BusinessError(
                E_INPUT_INVALID,
                "snapshot path must stay inside the workspace",
                {"path": str(path), "workspace_root": str(self.workspace_root)},
            ) from exc
        if not resolved.is_file():
            raise BusinessError(
                E_INPUT_INVALID,
                "snapshot path does not exist or is not a file",
                {"path": str(path)},
            )
        return resolved

    def import_snapshot_path(
        self,
        path: str | Path,
        *,
        format_hint: str | None = None,
        expected_version: int | None = None,
        expected_digest: str | None = None,
    ) -> dict[str, Any]:
        source = self._validated_source_path(path)
        data = source.read_bytes()
        return self.import_snapshot_bytes(
            data,
            source=str(source.relative_to(self.workspace_root)),
            format_hint=format_hint,
            expected_version=expected_version,
            expected_digest=expected_digest,
        )

    def import_snapshot_bytes(
        self,
        data: bytes,
        *,
        source: str | None = None,
        format_hint: str | None = None,
        expected_version: int | None = None,
        expected_digest: str | None = None,
    ) -> dict[str, Any]:
        try:
            decoded = decode_snapshot(data, format_hint=format_hint)
        except BusinessError as exc:
            if exc.code == E_FORMAT_UNSUPPORTED:
                report = exc.details.get("import_report")
                if isinstance(report, dict):
                    self.store.record_import_report(
                        source=source, report=report, code=E_FORMAT_UNSUPPORTED
                    )
            raise
        snapshot = decoded.snapshot
        report = decoded.report
        if expected_version is not None and snapshot.version != expected_version:
            raise BusinessError(
                E_FORMAT_CORRUPT,
                "decoded snapshot version does not match the expected version",
                {
                    "expected_version": expected_version,
                    "decoded_version": snapshot.version,
                },
            )
        if expected_digest is not None and snapshot.digest != expected_digest:
            raise BusinessError(
                E_FORMAT_CORRUPT,
                "decoded snapshot digest does not match the expected digest",
                {"expected_digest": expected_digest, "decoded_digest": snapshot.digest},
            )
        with self._writer_lock:
            current = self.store.current_snapshot_binding()
            if current is not None and snapshot.version < current["version"]:
                raise BusinessError(
                    E_VERSION_RETIRED,
                    "incoming snapshot version is older than the current version",
                    {
                        "incoming_version": snapshot.version,
                        "current_version": current["version"],
                        "current_digest": current["digest"],
                    },
                )
            duplicate = (
                current is not None
                and snapshot.version == current["version"]
                and snapshot.digest == current["digest"]
            )
            intent_id: str | None = None
            if not duplicate:
                intent_id = f"snapshot:{snapshot.version}:{uuid.uuid4().hex}"
                self.store.write_intent(
                    intent_id=intent_id,
                    kind="snapshot_commit",
                    reference_version=snapshot.version,
                    payload={
                        "snapshot_version": snapshot.version,
                        "digest": snapshot.digest,
                        "source": source,
                    },
                )
            try:
                replacement = self.store.commit_snapshot(
                    snapshot,
                    report,
                    source=source,
                    intent_id=intent_id,
                )
            except Exception as exc:
                if intent_id is not None:
                    self.store.mark_intent(
                        intent_id,
                        "ABORTED",
                        {
                            "error": str(exc),
                            "snapshot_version": snapshot.version,
                        },
                    )
                raise
        return {
            "ok": True,
            "import_report": report,
            "replacement": replacement,
            "current_snapshot": self.store.current_snapshot_binding(),
            "intent_id": intent_id,
        }

    def _normalize_plan_request(self, payload: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(payload, dict):
            raise BusinessError(E_INPUT_INVALID, "plan request must be a JSON object")
        request_id = payload.get("request_id")
        if not isinstance(request_id, str) or not request_id.strip() or len(request_id) > 128:
            raise BusinessError(
                E_INPUT_INVALID, "request_id must be a non-empty string of at most 128 chars"
            )
        snapshot_version = payload.get("snapshot_version")
        if (
            isinstance(snapshot_version, bool)
            or not isinstance(snapshot_version, int)
            or snapshot_version < 0
            or snapshot_version > MAX_ROWS
        ):
            raise BusinessError(
                E_INPUT_INVALID,
                "snapshot_version must be an integer in the signed 64-bit non-negative range",
            )
        predicates_raw = payload.get("predicates", [])
        if not isinstance(predicates_raw, list):
            raise BusinessError(E_INPUT_INVALID, "predicates must be an array")
        predicates = [parse_predicate(item, index) for index, item in enumerate(predicates_raw)]
        options = PlanOptions.from_mapping(payload.get("options"))
        hash_payload = {
            "snapshot_version": snapshot_version,
            "predicates": [predicate.public_dict() for predicate in predicates],
            "options": {
                "max_relations": options.max_relations,
                "tolerance_ppm": options.tolerance_ppm,
            },
        }
        request_hash = hashlib.sha256(
            canonical_json(hash_payload).encode("utf-8")
        ).hexdigest()
        return {
            "request_id": request_id,
            "snapshot_version": snapshot_version,
            "predicates": predicates,
            "options": options,
            "request_hash": request_hash,
        }

    def submit_plan(self, payload: dict[str, Any]) -> dict[str, Any]:
        normalized = self._normalize_plan_request(payload)
        request_id = normalized["request_id"]
        request_hash = normalized["request_hash"]
        snapshot_version = normalized["snapshot_version"]
        with self._build_lock:
            try:
                existing_record = self.store.get_plan_request(request_id)
            except BusinessError as exc:
                if exc.code != E_NOT_FOUND:
                    raise
                existing_record = None
            if existing_record is not None:
                if existing_record["request_hash"] != request_hash:
                    raise BusinessError(
                        E_CONFLICTING_REQUEST,
                        "request id was previously submitted with different content",
                        {
                            "request_id": request_id,
                            "existing_hash": existing_record["request_hash"],
                            "incoming_hash": request_hash,
                        },
                    )
                if existing_record["status"] == "COMPLETED":
                    return copy.deepcopy(existing_record["response"])
                if existing_record["status"] in {"FAILED", "ABORTED"}:
                    raise _business_error_from_payload(existing_record["error"] or {})
                raise BusinessError(
                    E_REQUEST_ABORTED,
                    "the request id is still marked running; restart recovery is required",
                    {"request_id": request_id, "state": existing_record["status"]},
                )
            snapshot = self._require_snapshot(snapshot_version)
            begin = self.store.begin_plan_request(
                request_id=request_id,
                request_hash=request_hash,
                snapshot_version=snapshot.version,
                snapshot_digest=snapshot.digest,
            )
            if begin["existing"]:
                state = begin["state"]
                if state == "COMPLETED":
                    return copy.deepcopy(begin["response"])
                if state in {"FAILED", "ABORTED"}:
                    raise _business_error_from_payload(begin["error"] or {})
                if state == "RUNNING":
                    raise BusinessError(
                        E_REQUEST_ABORTED,
                        "the request id is still marked running; restart recovery is required",
                        {"request_id": request_id},
                    )
                raise BusinessError(
                    E_CONFLICTING_REQUEST,
                    "request id is in an unknown state",
                    {"request_id": request_id, "state": state},
                )
            if self.build_delay_ms:
                time.sleep(self.build_delay_ms / 1000.0)
            try:
                response = build_plan(
                    snapshot,
                    normalized["predicates"],
                    normalized["options"],
                    request_id=request_id,
                )
                current = self.store.current_snapshot_binding()
                retired = current is None or current["version"] != snapshot.version
                response["version_binding"].update(
                    {
                        "version_retired_during_build": retired,
                        "completed_on_bound_snapshot": True,
                        "current_version_at_completion": current["version"]
                        if current
                        else None,
                    }
                )
                response["execution"] = {
                    "executed_once": True,
                    "request_hash": request_hash,
                    "build_serialized": True,
                }
                self.store.complete_plan_request(request_id, response)
                return copy.deepcopy(response)
            except BusinessError as exc:
                self.store.fail_plan_request(request_id, exc.to_dict())
                raise
            except Exception as exc:
                error = BusinessError(
                    "E_INTERNAL",
                    "unexpected plan build failure",
                    {"request_id": request_id, "reason": str(exc)},
                )
                self.store.fail_plan_request(request_id, error.to_dict())
                raise error from exc

    def get_plan(self, request_id: str) -> dict[str, Any]:
        record = self.store.get_plan_request(request_id)
        if record["status"] == "COMPLETED":
            return record["response"]
        if record["status"] in {"FAILED", "ABORTED"}:
            raise _business_error_from_payload(record["error"] or {})
        return {
            "ok": True,
            "request_id": request_id,
            "status": record["status"],
            "snapshot_version": record["snapshot_version"],
            "snapshot_digest": record["snapshot_digest"],
        }

    def current_snapshot(self) -> dict[str, Any]:
        view = self.store.current_snapshot_view()
        if view["current_snapshot"] is None:
            return {
                "ok": True,
                "current_snapshot": None,
                "snapshots": view["snapshots"],
                "message": "no snapshot has been imported",
            }
        return {
            "ok": True,
            "current_snapshot": view["current_snapshot"],
            "snapshots": view["snapshots"],
        }

    def snapshot(self, version: int) -> dict[str, Any]:
        return {"ok": True, "snapshot": self.store.get_snapshot(version).public_dict()}

    def imports(self, limit: int = 50) -> dict[str, Any]:
        return {"ok": True, "imports": self.store.list_import_reports(limit)}

    def recovery(self) -> dict[str, Any]:
        latest = self.store.latest_recovery_report()
        return {
            "ok": True,
            "startup_recovery": self.startup_recovery,
            "latest_recovery_report": latest,
        }

    def recover_when_idle(self) -> dict[str, Any]:
        with self._build_lock:
            with self._writer_lock:
                report = self.store.recover()
        return {"ok": True, "recovery_report": report}

    def request_status(self, request_id: str) -> dict[str, Any]:
        record = self.store.get_plan_request(request_id)
        record.pop("response", None)
        record.pop("error", None)
        return {"ok": True, "request": record}

    def health(self) -> dict[str, Any]:
        return {
            "ok": True,
            "service": "joincost-lab",
            "loopback_only": True,
            "current_snapshot": self.store.current_snapshot_binding(),
        }

