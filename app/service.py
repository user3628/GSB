from __future__ import annotations

import json
import threading
import time
from pathlib import Path
from typing import Any

from app.benchmark import compensation_benchmark
from app.compiler import (
    CompiledSnapshot,
    cascade_events,
    compile_snapshot,
    compute_snapshot_digest,
    compiled_state_payload,
    route_plan_payload,
    snapshot_payload_json,
)
from app.db import SQLiteStore
from app.errors import WorkbenchError
from app.models import IncrementalRequest, Route, SnapshotRequest


class ConfigService:
    def __init__(
        self,
        db_path: str | Path,
        *,
        enable_failpoints: bool = False,
    ) -> None:
        self.store = SQLiteStore(db_path)
        self.enable_failpoints = enable_failpoints
        self._lock = threading.RLock()
        self._current: CompiledSnapshot | None = None
        self._startup_recovery: dict[str, Any] | None = None

    @property
    def current(self) -> CompiledSnapshot | None:
        return self._current

    def startup(self) -> None:
        with self._lock:
            self.store.initialize()
            self._startup_recovery = self.store.recover()
            self._current = self._load_latest_runtime()

    def _load_latest_runtime(self) -> CompiledSnapshot | None:
        row = self.store.latest_snapshot()
        if row is None:
            return None
        return self._runtime_from_snapshot_row(row)

    def _runtime_from_snapshot_row(self, row: dict[str, Any]) -> CompiledSnapshot:
        payload = json.loads(row["payload_json"])
        version = int(payload["version"])
        if version != int(row["version"]):
            raise RuntimeError("persisted snapshot payload/version mismatch")
        routes = [Route.model_validate(item) for item in payload["routes"]]
        digest = compute_snapshot_digest(version, routes)
        if digest != row["digest"]:
            raise RuntimeError("persisted snapshot checksum mismatch")
        return compile_snapshot(version, routes, digest=digest)

    @staticmethod
    def _failpoint(name: str, enabled: bool) -> None:
        if enabled and name == "crash-after-intent":
            while True:
                time.sleep(1.0)

    def submit_snapshot(
        self,
        request: SnapshotRequest,
        *,
        failpoint: str | None = None,
    ) -> dict[str, Any]:
        with self._lock:
            current = self._current
            current_version = current.version if current else 0
            if current is not None and request.version <= current.version:
                raise WorkbenchError(
                    "E_SNAPSHOT_INVALID",
                    "full snapshot version must be strictly greater than current version",
                    details={
                        "current_version": current.version,
                        "requested_version": request.version,
                    },
                )

            routes = list(request.routes)
            digest = compute_snapshot_digest(request.version, routes)
            if request.digest is not None and request.digest != digest:
                raise WorkbenchError(
                    "E_SNAPSHOT_INVALID",
                    "snapshot digest mismatch",
                    details={"expected_digest": digest, "provided_digest": request.digest},
                )

            intent_id = self.store.create_intent(
                kind="FULL_SNAPSHOT",
                base_version=current_version,
                target_version=request.version,
                payload_digest=digest,
                fingerprint=request.digest,
                detail={"route_count": len(routes), "mode": "replace_all"},
            )
            self._failpoint(
                failpoint or "", self.enable_failpoints and failpoint is not None
            )

            try:
                events = cascade_events(current, compile_snapshot(request.version, routes, digest), full_snapshot=True)
                runtime = compile_snapshot(
                    request.version, routes, digest=digest, events=events
                )
                committed = self.store.commit_compiled(
                    runtime,
                    intent_id=intent_id,
                    expected_base_version=current_version,
                    payload_json=snapshot_payload_json(request.version, routes),
                )
            except Exception as exc:
                self.store.update_intent(
                    intent_id,
                    "FAILED",
                    error_code="E_SNAPSHOT_INVALID",
                    detail={"exception": type(exc).__name__, "message": str(exc)},
                )
                raise WorkbenchError(
                    "E_SNAPSHOT_INVALID",
                    "snapshot compilation failed",
                    details={"exception": type(exc).__name__, "message": str(exc)},
                ) from exc

            if not committed:
                self.store.update_intent(
                    intent_id,
                    "REJECTED",
                    error_code="E_STALE_BASE",
                    detail={"current_version": self.store.current_version()},
                )
                raise WorkbenchError(
                    "E_STALE_BASE",
                    "snapshot base changed before commit",
                    details={"base_version": current_version},
                )

            self._current = runtime
            return self._commit_response(
                runtime, intent_id, base_version=current_version
            )

    def submit_incremental(
        self,
        request: IncrementalRequest,
        *,
        failpoint: str | None = None,
    ) -> dict[str, Any]:
        with self._lock:
            current = self._current
            if current is None or request.base_version != current.version:
                raise WorkbenchError(
                    "E_STALE_BASE",
                    "incremental base_version is not the current committed version",
                    details={
                        "base_version": request.base_version,
                        "current_version": current.version if current else 0,
                    },
                )

            merged: dict[str, Route] = {
                plan.route.route_id: plan.route for plan in current.routes
            }
            original = {route_id: route.normalized_payload() for route_id, route in merged.items()}

            for route_id in request.delete_route_ids:
                if route_id not in merged:
                    raise WorkbenchError(
                        "E_SNAPSHOT_INVALID",
                        "delete_route_ids references an unknown route",
                        details={"route_id": route_id},
                    )
                del merged[route_id]
            for route in request.upserts:
                merged[route.route_id] = route

            changed = {
                route_id
                for route_id, route in merged.items()
                if original.get(route_id) != route.normalized_payload()
            }
            removed = set(original) - set(merged)
            if not changed and not removed:
                raise WorkbenchError(
                    "E_SNAPSHOT_INVALID",
                    "incremental request does not change any route",
                    details={"base_version": request.base_version},
                )

            target_version = current.version + 1
            routes = list(merged.values())
            digest = compute_snapshot_digest(target_version, routes)
            if request.digest is not None and request.digest != digest:
                raise WorkbenchError(
                    "E_SNAPSHOT_INVALID",
                    "incremental result digest mismatch",
                    details={"expected_digest": digest, "provided_digest": request.digest},
                )

            intent_id = self.store.create_intent(
                kind="INCREMENTAL",
                base_version=request.base_version,
                target_version=target_version,
                payload_digest=digest,
                fingerprint=request.digest,
                detail={
                    "upsert_route_ids": sorted(route.route_id for route in request.upserts),
                    "delete_route_ids": sorted(request.delete_route_ids),
                },
            )
            self._failpoint(
                failpoint or "", self.enable_failpoints and failpoint is not None
            )

            try:
                unversioned = compile_snapshot(target_version, routes, digest)
                events = cascade_events(current, unversioned, full_snapshot=False)
                runtime = compile_snapshot(
                    target_version, routes, digest=digest, events=events
                )
                committed = self.store.commit_compiled(
                    runtime,
                    intent_id=intent_id,
                    expected_base_version=current.version,
                    payload_json=snapshot_payload_json(target_version, routes),
                )
            except Exception as exc:
                self.store.update_intent(
                    intent_id,
                    "FAILED",
                    error_code="E_SNAPSHOT_INVALID",
                    detail={"exception": type(exc).__name__, "message": str(exc)},
                )
                raise WorkbenchError(
                    "E_SNAPSHOT_INVALID",
                    "incremental compilation failed",
                    details={"exception": type(exc).__name__, "message": str(exc)},
                ) from exc

            if not committed:
                self.store.update_intent(
                    intent_id,
                    "REJECTED",
                    error_code="E_STALE_BASE",
                    detail={"current_version": self.store.current_version()},
                )
                raise WorkbenchError(
                    "E_STALE_BASE",
                    "another incremental commit won the base_version race",
                    details={
                        "base_version": request.base_version,
                        "current_version": self.store.current_version(),
                    },
                )

            self._current = runtime
            return self._commit_response(
                runtime, intent_id, base_version=request.base_version
            )

    def _commit_response(
        self, runtime: CompiledSnapshot, intent_id: int, *, base_version: int
    ) -> dict[str, Any]:
        return {
            "ok": True,
            "intent_id": intent_id,
            "base_version": base_version,
            "version": runtime.version,
            "digest": runtime.digest,
            "route_count": runtime.route_count,
            "artifact_count": len(runtime.artifacts),
            "event_count": len(runtime.events),
            "events": list(runtime.events),
        }

    def state(self, version: int | None = None) -> dict[str, Any]:
        if version is None:
            snapshot = self._current
            if snapshot is None:
                return {
                    "version": 0,
                    "digest": None,
                    "route_count": 0,
                    "routes": [],
                    "upstream_aggregates": [],
                    "artifact_summary": {},
                    "match_index": {"reverse": [], "forward": []},
                    "events": [],
                    "recovery": self._startup_recovery,
                }
            events = self.store.artifact_events(snapshot.version)
            payload = compiled_state_payload(snapshot, events=events)
            payload["recovery"] = self._startup_recovery
            payload["recent_commits"] = [
                commit
                for commit in self.store.recent_commits()
                if int(commit["version"]) <= snapshot.version
            ]
            return payload
        snapshot, events = self._load_version(version)
        return compiled_state_payload(snapshot, events=events)

    def _load_version(self, version: int) -> tuple[CompiledSnapshot, list[dict[str, Any]]]:
        row = self.store.snapshot(version)
        if row is None:
            raise WorkbenchError(
                "E_ROUTE_NOT_FOUND",
                "snapshot version not found",
                details={"version": version},
            )
        return self._runtime_from_snapshot_row(row), self.store.artifact_events(version)

    def versions(self) -> dict[str, Any]:
        return {
            "versions": self.store.list_snapshots(),
            "current_version": self._current.version if self._current else 0,
            "recent_commits": self.store.recent_commits(),
        }

    def explain_budget(
        self, route_id: str, version: int | None = None
    ) -> dict[str, Any]:
        if version is None:
            snapshot = self._current
            if snapshot is None:
                raise WorkbenchError(
                    "E_ROUTE_NOT_FOUND", "no committed configuration exists"
                )
        else:
            snapshot, _ = self._load_version(version)
        plan = snapshot.routes_by_id.get(route_id)
        if plan is None:
            raise WorkbenchError(
                "E_ROUTE_NOT_FOUND",
                "route not found in snapshot",
                details={"route_id": route_id, "version": snapshot.version},
            )
        upstream_aggregate = next(
            (
                aggregate.payload()
                for aggregate in snapshot.upstream_aggregates
                if aggregate.upstream == plan.route.upstream
            ),
            None,
        )
        return {
            "version": snapshot.version,
            "snapshot_digest": snapshot.digest,
            "route": route_plan_payload(plan),
            "upstream_aggregate": upstream_aggregate,
            "benchmark": compensation_benchmark(),
        }

    def benchmark(self) -> dict[str, Any]:
        return compensation_benchmark()

    def intents(self, limit: int = 100) -> dict[str, Any]:
        return {
            "intents": self.store.list_intents(limit),
            "recovery_reports": self.store.recovery_reports(),
        }

    def recovery(self) -> dict[str, Any]:
        reports = self.store.recovery_reports()
        return {
            "startup_recovery": self._startup_recovery,
            "reports": reports,
        }
