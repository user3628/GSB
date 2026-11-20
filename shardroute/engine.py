from __future__ import annotations

import copy
import json
import os
import sys
import threading
import uuid
from concurrent.futures import Future
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from .cache import build_route_state
from .datastore import SQLiteDataStore
from .errors import (
    INJECTION_CODES,
    INJECTION_MESSAGES,
    BusinessError,
    error,
)
from .merge import merge_rows
from .models import PhysicalTable, RouteState, Snapshot
from .persistence import MetadataStore
from .routing import build_routing_plan
from .snapshot import (
    finalize_snapshot_document,
    load_snapshot_document as parse_snapshot_document,
    load_snapshot_file,
    validate_patch_checksum,
)
from .util import (
    SAFE_VERSION,
    fingerprint as make_fingerprint,
    read_json,
    require_plain_int,
    utc_now,
)


class _ReadWriteLock:
    def __init__(self) -> None:
        self._condition = threading.Condition()
        self._readers = 0
        self._writer = False
        self._waiting_writers = 0

    @contextmanager
    def read(self) -> Iterator[None]:
        with self._condition:
            while self._writer or self._waiting_writers:
                self._condition.wait()
            self._readers += 1
        try:
            yield
        finally:
            with self._condition:
                self._readers -= 1
                if self._readers == 0:
                    self._condition.notify_all()

    @contextmanager
    def write(self) -> Iterator[None]:
        with self._condition:
            self._waiting_writers += 1
            try:
                while self._writer or self._readers:
                    self._condition.wait()
            finally:
                self._waiting_writers -= 1
            self._writer = True
        try:
            yield
        finally:
            with self._condition:
                self._writer = False
                self._condition.notify_all()


class _PendingQuery:
    def __init__(self, fingerprint: str):
        self.fingerprint = fingerprint
        self.future: Future[dict[str, Any]] = Future()


class ShardRouteService:
    """Thread-safe integration surface for snapshots, routing and recovery."""

    def __init__(
        self,
        *,
        metadata_db: str | Path,
        data_dir: str | Path,
        snapshot_file: str | Path | None = None,
        auto_load_snapshot: bool = False,
    ):
        self.store = MetadataStore(metadata_db)
        self.data = SQLiteDataStore(data_dir)
        self._writer_lock = threading.Lock()
        self._state_lock = _ReadWriteLock()
        self._state: RouteState | None = None
        self._pending_lock = threading.Lock()
        self._pending: dict[str, _PendingQuery] = {}
        self.run_id = f"run:{uuid.uuid4().hex}"
        self._closed = False
        self._close_lock = threading.Lock()
        self.run_info = self.store.begin_run(run_id=self.run_id, pid=os.getpid())
        recovery = self._recover()
        self.recovery_report = recovery
        if snapshot_file is not None and self._state is None:
            document = read_json(snapshot_file)
            self.load_snapshot(document)
        elif snapshot_file is not None and auto_load_snapshot:
            document = read_json(snapshot_file)
            parsed = parse_snapshot_document(document)
            current = self._current_state_optional()
            if current is None or parsed.version > current.version:
                self.load_snapshot(document)

    @staticmethod
    def _parse_persisted_snapshot(row: dict[str, Any]) -> Snapshot:
        document = json.loads(row["document_json"])
        snapshot = parse_snapshot_document(document)
        if snapshot.version != row["version"] or snapshot.checksum != row["checksum"]:
            raise error(
                "E_SNAPSHOT_INVALID",
                "persisted snapshot metadata does not match its document",
                persisted_version=row["version"],
                document_version=snapshot.version,
                persisted_checksum=row["checksum"],
                document_checksum=snapshot.checksum,
            )
        return snapshot

    def _recover(self) -> dict[str, Any]:
        active_row = self.store.get_active_row()
        active_before = active_row["version"] if active_row else None
        valid_rows: list[tuple[dict[str, Any], Snapshot]] = []
        rejected_rows: list[dict[str, Any]] = []
        for row in self.store.list_snapshot_rows():
            try:
                valid_rows.append((row, self._parse_persisted_snapshot(row)))
            except Exception as exc:
                rejected_rows.append(
                    {
                        "version": row["version"],
                        "reason": str(exc),
                    }
                )
        valid_rows.sort(key=lambda pair: pair[1].version, reverse=True)
        selected_row, selected_snapshot = valid_rows[0] if valid_rows else (None, None)
        active_matches = bool(
            active_row
            and selected_snapshot
            and active_row["version"] == selected_snapshot.version
            and active_row["checksum"] == selected_snapshot.checksum
        )
        selected_version = selected_snapshot.version if selected_snapshot is not None else None
        rolled_back = bool(active_row is not None and not active_matches)
        if active_before != selected_version:
            rolled_back = True

        aborted_queries = self.store.abort_running_queries("startup_recovery")
        aborted_intents = self.store.abort_other_unfinished_intents("startup_recovery")

        if active_before != selected_version or (active_row is not None and not active_matches):
            rollback_intent = self.store.new_intent_id("recovery-rollback")
            self.store.create_intent(
                intent_id=rollback_intent,
                kind="RECOVERY_ROLLBACK",
                routing_version=selected_snapshot.version,
                detail={
                    "active_version_before": active_before,
                    "selected_complete_version": selected_snapshot.version,
                    "rejected_snapshot_rows": rejected_rows,
                },
            )
            if selected_snapshot is not None:
                self.store.set_active_version(
                    selected_snapshot.version, selected_snapshot.checksum
                )
            else:
                self.store.clear_active_state()
            self.store.complete_intent(rollback_intent, "COMMITTED")

        cache_rebuilt_tables: list[str] = []
        cache_intent: str | None = None
        if selected_snapshot is not None:
            cache_intent = self.store.new_intent_id("recovery-cache")
            self.store.create_intent(
                intent_id=cache_intent,
                kind="CACHE_REBUILD",
                routing_version=selected_snapshot.version,
                detail={
                    "source": "startup_recovery",
                    "tables": sorted(selected_snapshot.tables),
                },
            )
            route_state = build_route_state(selected_snapshot)
            self.store.record_cache_rebuild(route_state=route_state, intent_id=cache_intent)
            with self._state_lock.write():
                self._state = route_state
            cache_rebuilt_tables = sorted(route_state.caches)

        previous = self.run_info.get("previous_run")
        crash_window = bool(previous and previous.get("graceful_stop_at") is None)
        report = {
            "report_basis": "persisted_sqlite_metadata_only",
            "persisted_facts_only": True,
            "generated_at": utc_now(),
            "startup_run_id": self.run_id,
            "previous_process": previous,
            "previous_unclean_run_count": self.run_info.get(
                "previous_unclean_run_count", 0
            ),
            "crash_window_detected": crash_window,
            "complete_exit_report_available": bool(
                previous and previous.get("graceful_stop_at") is not None
            ),
            "active_version_before_recovery": active_before,
            "active_version_after_recovery": (
                selected_snapshot.version if selected_snapshot is not None else None
            ),
            "last_complete_snapshot_version": (
                selected_snapshot.version if selected_snapshot is not None else None
            ),
            "rolled_back_to_last_complete_snapshot": rolled_back,
            "rejected_snapshot_rows": rejected_rows,
            "aborted_intents": aborted_intents,
            "aborted_query_batches": aborted_queries,
            "cache_rebuild": {
                "routing_version": (
                    selected_snapshot.version if selected_snapshot is not None else None
                ),
                "tables": cache_rebuilt_tables,
            },
            "report_limitation": (
                "A forced process termination cannot emit a complete shutdown report. "
                "This report is reconstructed only from committed metadata and intent rows."
            ),
        }
        report["report_id"] = self.store.insert_recovery_report(report)
        return report

    def _current_state_optional(self) -> RouteState | None:
        with self._state_lock.read():
            return self._state

    def current_state(self) -> RouteState:
        with self._state_lock.read():
            state = self._state
        if state is None:
            raise error(
                "E_NOT_FOUND",
                "no committed routing snapshot is active",
                recovery_report_available=self.recovery_report is not None,
            )
        return state

    def _install_state(self, state: RouteState) -> None:
        with self._state_lock.write():
            self._state = state

    def load_snapshot(
        self,
        document: dict[str, Any],
        *,
        crash_after_prepare: bool = False,
    ) -> dict[str, Any]:
        with self._writer_lock:
            snapshot = parse_snapshot_document(document)
            current = self._current_state_optional()
            if current is not None and snapshot.version <= current.version:
                raise error(
                    "E_VERSION_ROLLBACK",
                    "snapshot version must increase monotonically",
                    requested_version=snapshot.version,
                    current_version=current.version,
                )
            intent_id = self.store.new_intent_id("snapshot")
            self.store.create_intent(
                intent_id=intent_id,
                kind="SNAPSHOT_COMMIT",
                routing_version=snapshot.version,
                detail={
                    "version": snapshot.version,
                    "checksum": snapshot.checksum,
                    "previous_version": current.version if current else None,
                    "affected_tables": sorted(snapshot.tables),
                    "mode": "full",
                },
            )
            cache_intent_id = self.store.new_intent_id("cache")
            self.store.create_intent(
                intent_id=cache_intent_id,
                kind="CACHE_REBUILD",
                routing_version=snapshot.version,
                detail={
                    "source": "full_snapshot_commit",
                    "tables": sorted(snapshot.tables),
                    "mode": "full",
                },
            )
            try:
                route_state = build_route_state(snapshot)
            except BaseException:
                self.store.complete_intent(cache_intent_id, "FAILED")
                self.store.complete_intent(intent_id, "FAILED")
                raise
            if crash_after_prepare:
                sys.stdout.flush()
                sys.stderr.flush()
                os._exit(86)
            self.store.commit_snapshot(
                route_state=route_state,
                intent_id=intent_id,
                cache_intent_id=cache_intent_id,
            )
            self._install_state(route_state)
            return {
                "ok": True,
                "mode": "full",
                "previous_version": current.version if current else None,
                "active_version": route_state.version,
                "checksum": route_state.snapshot.checksum,
                "affected_tables": sorted(route_state.caches),
                "rebuilt_tables": sorted(route_state.caches),
                "reused_tables": [],
                "cache": self._cache_payload(route_state),
            }

    def load_snapshot_file(
        self, path: str | Path, *, crash_after_prepare: bool = False
    ) -> dict[str, Any]:
        snapshot = load_snapshot_file(path)
        return self.load_snapshot(
            snapshot.document, crash_after_prepare=crash_after_prepare
        )

    def apply_patch(
        self,
        patch: dict[str, Any],
        *,
        crash_after_prepare: bool = False,
    ) -> dict[str, Any]:
        if not isinstance(patch, dict):
            raise error("E_BAD_REQUEST", "snapshot patch must be a JSON object")
        with self._writer_lock:
            current = self._current_state_optional()
            if current is None:
                raise error("E_NOT_FOUND", "cannot patch before a snapshot is active")
            base_version = require_plain_int(patch.get("base_version"), "base_version")
            target_version = require_plain_int(patch.get("target_version"), "target_version")
            if base_version != current.version:
                raise error(
                    "E_STALE_BASE",
                    "patch base_version is not the current routing version",
                    base_version=base_version,
                    current_version=current.version,
                )
            if target_version <= current.version:
                raise error(
                    "E_VERSION_ROLLBACK",
                    "patch target_version must be greater than the current version",
                    target_version=target_version,
                    current_version=current.version,
                )
            validate_patch_checksum(patch)
            patch_tables = patch.get("tables")
            if not isinstance(patch_tables, dict) or not patch_tables:
                raise error(
                    "E_BAD_REQUEST",
                    "patch tables must be a non-empty object of affected logical tables",
                )
            remove_tables = patch.get("remove_tables", [])
            if not isinstance(remove_tables, list) or any(
                not isinstance(name, str) for name in remove_tables
            ):
                raise error("E_BAD_REQUEST", "remove_tables must be an array of strings")

            candidate_document = copy.deepcopy(current.snapshot.document)
            candidate_document["snapshot_version"] = target_version
            candidate_document["generated_at"] = patch.get("generated_at", utc_now())
            patch_algorithms = patch.get("algorithms")
            if patch_algorithms is not None:
                if not isinstance(patch_algorithms, dict):
                    raise error("E_BAD_REQUEST", "patch algorithms must be an object")
                algorithms = copy.deepcopy(candidate_document.get("algorithms", {}))
                algorithms.update(copy.deepcopy(patch_algorithms))
                candidate_document["algorithms"] = algorithms
            for table_name, table_definition in patch_tables.items():
                candidate_document["tables"][table_name] = copy.deepcopy(table_definition)
            for table_name in remove_tables:
                candidate_document["tables"].pop(table_name, None)
            candidate_document.pop("checksum", None)
            candidate_document = finalize_snapshot_document(candidate_document)
            candidate_snapshot = parse_snapshot_document(candidate_document)

            explicitly_affected = set(patch_tables) | set(remove_tables)
            affected = set(explicitly_affected)
            for table_name, table_spec in candidate_snapshot.tables.items():
                previous_spec = current.snapshot.tables.get(table_name)
                if previous_spec is None or previous_spec != table_spec:
                    affected.add(table_name)
            # Tables that inherit from an affected table are discovered by the
            # semantic comparison above because their resolved mapping changed.
            intent_id = self.store.new_intent_id("patch")
            self.store.create_intent(
                intent_id=intent_id,
                kind="SNAPSHOT_PATCH",
                routing_version=candidate_snapshot.version,
                detail={
                    "base_version": current.version,
                    "target_version": candidate_snapshot.version,
                    "checksum": candidate_snapshot.checksum,
                    "affected_tables": sorted(affected),
                    "remove_tables": remove_tables,
                },
            )
            cache_intent_id = self.store.new_intent_id("cache")
            self.store.create_intent(
                intent_id=cache_intent_id,
                kind="CACHE_REBUILD",
                routing_version=candidate_snapshot.version,
                detail={
                    "source": "incremental_patch",
                    "base_version": current.version,
                    "tables": sorted(affected),
                    "mode": "incremental",
                },
            )
            try:
                route_state = build_route_state(
                    candidate_snapshot,
                    affected_tables=tuple(sorted(affected)),
                    previous=current,
                )
            except BaseException:
                self.store.complete_intent(cache_intent_id, "FAILED")
                self.store.complete_intent(intent_id, "FAILED")
                raise
            if crash_after_prepare:
                sys.stdout.flush()
                sys.stderr.flush()
                os._exit(86)
            self.store.commit_snapshot(
                route_state=route_state,
                intent_id=intent_id,
                cache_intent_id=cache_intent_id,
            )
            self._install_state(route_state)
            rebuilt = sorted(name for name in affected if name in route_state.caches)
            reused = sorted(name for name in route_state.caches if name not in affected)
            return {
                "ok": True,
                "mode": "patch",
                "base_version": current.version,
                "active_version": route_state.version,
                "checksum": route_state.snapshot.checksum,
                "affected_tables": sorted(affected),
                "rebuilt_tables": rebuilt,
                "reused_tables": reused,
                "cache": self._cache_payload(route_state),
            }

    @staticmethod
    def _cache_payload(state: RouteState) -> dict[str, Any]:
        return {
            "routing_version": state.version,
            "atomic_generation": state.version,
            "tables": {
                name: state.caches[name].as_dict() for name in sorted(state.caches)
            },
        }

    def _injection_error(self, injection: str, request: dict[str, Any], state: RouteState) -> BusinessError:
        code = INJECTION_CODES[injection]
        return BusinessError(
            code=code,
            message=INJECTION_MESSAGES[injection],
            context={
                "injection": injection,
                "request_id": request.get("request_id"),
                "logical_table": request.get("logical_table"),
                "bound_routing_version": state.version,
                "current_version": state.version,
                "state_mutated": False,
            },
        )

    def _bind_version(self, request: dict[str, Any]) -> RouteState:
        requested_version = request.get("routing_version")
        with self._state_lock.read():
            state = self._state
            if state is None:
                raise error("E_NOT_FOUND", "no active routing snapshot")
            if requested_version is not None:
                if isinstance(requested_version, bool) or not isinstance(requested_version, int):
                    raise error(
                        "E_BAD_REQUEST",
                        "routing_version must be an integer",
                        routing_version=requested_version,
                    )
                if requested_version != state.version:
                    raise error(
                        "E_VERSION_RETIRED",
                        "requested routing version is not the active committed version",
                        requested_version=requested_version,
                        current_version=state.version,
                        request_id=request.get("request_id"),
                    )
            return state

    @staticmethod
    def _page_settings(request: dict[str, Any]) -> tuple[int, int | None]:
        offset = request.get("offset", 0)
        limit = request.get("limit", 1000)
        offset = require_plain_int(offset, "offset")
        if offset < 0:
            raise error("E_BAD_REQUEST", "offset must be non-negative", offset=offset)
        if limit is not None:
            limit = require_plain_int(limit, "limit")
            if limit < 0:
                raise error("E_BAD_REQUEST", "limit must be non-negative or null", limit=limit)
        return offset, limit

    def _execute_owner(
        self,
        request: dict[str, Any],
        *,
        intent_id: str,
    ) -> dict[str, Any]:
        injection = request.get("inject")
        if injection is not None and not isinstance(injection, str):
            raise error("E_BAD_REQUEST", "inject must be a string")
        allowed_injections = set(INJECTION_CODES) | {
            "crash_after_intent",
            "crash_mid_merge",
        }
        if injection is not None and injection not in allowed_injections:
            raise error(
                "E_BAD_REQUEST",
                "unknown injection",
                injection=injection,
                supported=sorted(allowed_injections),
            )
        if injection == "crash_after_intent":
            sys.stdout.flush()
            sys.stderr.flush()
            os._exit(86)

        state = self._bind_version(request)
        if injection in INJECTION_CODES and injection != "execution_interrupt":
            raise self._injection_error(injection, request, state)

        offset, limit = self._page_settings(request)
        plan = build_routing_plan(state, request)
        table = state.snapshot.tables[request["logical_table"]]
        physical_by_key = {
            (item.logical_db, item.table, item.shard): item for item in table.physical_tables
        }
        rows_by_target: list[tuple[PhysicalTable, list[dict[str, Any]]]] = []
        execution_trace: list[dict[str, Any]] = []
        for index, descriptor in enumerate(plan["physical_tables"]):
            target = physical_by_key[
                (descriptor["logical_db"], descriptor["table"], descriptor["shard"])
            ]
            rows = self.data.fetch_rows(target, table, plan["query_predicate"])
            execution_trace.append(
                {
                    "step": "execute_physical_table",
                    "order": index,
                    "logical_db": target.logical_db,
                    "physical_table": target.table,
                    "shard": target.shard,
                    "row_count": len(rows),
                    "status": "completed",
                }
            )
            rows_by_target.append((target, rows))
            if index == 0 and injection == "crash_mid_merge":
                sys.stdout.flush()
                sys.stderr.flush()
                os._exit(86)
            if index == 0 and injection == "execution_interrupt":
                raise error(
                    "E_EXECUTION_ABORTED",
                    "execution interrupted by deterministic injection",
                    injection=injection,
                    request_id=request.get("request_id"),
                    completed_physical_tables=[descriptor],
                    planned_physical_tables=plan["physical_tables"],
                    stage="cross_table_merge",
                )
        if injection == "execution_interrupt" and not plan["physical_tables"]:
            raise error(
                "E_EXECUTION_ABORTED",
                "execution interrupted by deterministic injection before any rows were read",
                injection=injection,
                request_id=request.get("request_id"),
                completed_physical_tables=[],
                planned_physical_tables=[],
                stage="route_execution",
            )

        merged = merge_rows(
            table,
            plan["physical_tables"],
            rows_by_target,
            offset=offset,
            limit=limit,
        )
        execution_trace.append(
            {
                "step": "merge_and_window",
                "conflict_rule": table.conflict_rule,
                "conflict_count": len(merged["conflicts"]),
                "total_after_conflict": merged["page"]["total_after_conflict"],
                "returned": merged["page"]["returned"],
                "offset": offset,
                "limit": limit,
            }
        )
        current = self._current_state_optional()
        current_version = current.version if current else None
        return {
            "ok": True,
            "request_id": request["request_id"],
            "logical_table": request["logical_table"],
            "query_type": request["query_type"],
            "requested_routing_version": request.get("routing_version"),
            "routing_version": state.version,
            "bound_snapshot_checksum": state.snapshot.checksum,
            "current_version_after": current_version,
            "version_replaced_during_request": current_version != state.version,
            "plan_checksum": plan["plan_checksum"],
            "physical_tables": plan["physical_tables"],
            "matched_shards": plan["matched_shards"],
            "cache": plan["cache"],
            "route_trace": [*plan["trace"], *execution_trace],
            "rows": merged["rows"],
            "row_sources": merged["row_sources"],
            "conflicts": merged["conflicts"],
            "page": merged["page"],
            "idempotent_replay": False,
        }

    @staticmethod
    def _load_stored_error(error_payload: dict[str, Any]) -> BusinessError:
        return BusinessError(
            code=str(error_payload.get("code", "E_INTERNAL")),
            message=str(error_payload.get("message", "stored query failure")),
            context=error_payload.get("context", {}),
        )

    def _replay_or_none(
        self, request_id: str, fingerprint: str
    ) -> dict[str, Any] | None:
        row = self.store.get_query_batch(request_id)
        if row is None:
            return None
        if row["fingerprint"] != fingerprint:
            raise error(
                "E_REQUEST_CONFLICT",
                "request_id was already used with a different payload",
                request_id=request_id,
            )
        if row["status"] == "SUCCEEDED":
            result = json.loads(row["result_json"])
            result["idempotent_replay"] = True
            return {"result": result, "error": None}
        if row["status"] in {"FAILED", "ABORTED"}:
            return {
                "result": None,
                "error": self._load_stored_error(json.loads(row["error_json"])),
            }
        return {
            "result": None,
            "error": error(
                "E_EXECUTION_ABORTED",
                "query batch is still marked RUNNING and has no live owner",
                request_id=request_id,
            ),
        }

    def execute_query(self, request: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(request, dict):
            raise error("E_BAD_REQUEST", "query request must be a JSON object")
        request_id = request.get("request_id")
        if not isinstance(request_id, str) or not SAFE_VERSION.fullmatch(request_id):
            raise error(
                "E_BAD_REQUEST",
                "request_id is required and must contain only URL-safe characters",
                request_id=request_id,
            )
        fingerprint_material = copy.deepcopy(request)
        fingerprint_material.pop("request_id", None)
        request_fingerprint = make_fingerprint(fingerprint_material)
        with self._pending_lock:
            pending = self._pending.get(request_id)
            if pending is None:
                pending = _PendingQuery(request_fingerprint)
                self._pending[request_id] = pending
                owner = True
            else:
                owner = False
        if not owner:
            if pending.fingerprint != request_fingerprint:
                raise error(
                    "E_REQUEST_CONFLICT",
                    "request_id is concurrently used with a different payload",
                    request_id=request_id,
                )
            outcome = pending.future.result()
            if outcome["error"] is not None:
                raise outcome["error"]
            result = copy.deepcopy(outcome["result"])
            result["idempotent_replay"] = True
            return result

        batch_created = False
        intent_id = f"query:{request_id}"
        try:
            replay = self._replay_or_none(request_id, request_fingerprint)
            if replay is not None:
                pending.future.set_result(replay)
                if replay["error"] is not None:
                    raise replay["error"]
                result = copy.deepcopy(replay["result"])
                result["idempotent_replay"] = True
                return result
            self.store.create_query_batch(
                request_id=request_id,
                fingerprint=request_fingerprint,
                routing_version=request.get("routing_version"),
                intent_id=intent_id,
                request_detail=request,
            )
            batch_created = True
            result = self._execute_owner(request, intent_id=intent_id)
            self.store.finalize_query_batch(
                request_id=request_id,
                status="SUCCEEDED",
                result=result,
                intent_id=intent_id,
            )
            outcome = {"result": result, "error": None}
            pending.future.set_result(outcome)
            return result
        except BusinessError as exc:
            if batch_created:
                try:
                    self.store.finalize_query_batch(
                        request_id=request_id,
                        status=(
                            "ABORTED" if exc.code == "E_EXECUTION_ABORTED" else "FAILED"
                        ),
                        error_payload=exc.to_error(),
                        intent_id=intent_id,
                    )
                except Exception:
                    pass
            outcome = {"result": None, "error": exc}
            if not pending.future.done():
                pending.future.set_result(outcome)
            raise
        except BaseException as exc:
            wrapped = error(
                "E_INTERNAL",
                "unexpected query execution failure",
                request_id=request_id,
                failure=repr(exc),
            )
            if batch_created:
                try:
                    self.store.finalize_query_batch(
                        request_id=request_id,
                        status="FAILED",
                        error_payload=wrapped.to_error(),
                        intent_id=intent_id,
                    )
                except Exception:
                    pass
            outcome = {"result": None, "error": wrapped}
            if not pending.future.done():
                pending.future.set_result(outcome)
            raise wrapped from exc

    def cache_summary(self) -> dict[str, Any]:
        state = self.current_state()
        return self._cache_payload(state)

    def state_summary(self) -> dict[str, Any]:
        with self._state_lock.read():
            state = self._state
        snapshot_rows = self.store.list_snapshot_rows()
        result: dict[str, Any] = {
            "ok": True,
            "run_id": self.run_id,
            "pid": os.getpid(),
            "active_version": state.version if state else None,
            "active_checksum": state.snapshot.checksum if state else None,
            "active_generated_at": state.snapshot.generated_at if state else None,
            "active_committed_at": state.committed_at if state else None,
            "affected_tables": list(state.affected_tables) if state else [],
            "caches": self._cache_payload(state) if state else None,
            "logical_tables": {
                name: {
                    "logical_db": table.logical_db,
                    "shard_key": table.shard_key,
                    "shard_key_type": table.shard_key_type,
                    "algorithm": table.algorithm.as_cache_dict(),
                    "conflict_rule": table.conflict_rule,
                    "physical_tables": [item.as_dict() for item in table.physical_tables],
                }
                for name, table in sorted(state.snapshot.tables.items())
            }
            if state
            else {},
            "snapshots": [
                {
                    "version": row["version"],
                    "checksum": row["checksum"],
                    "generated_at": row["generated_at"],
                    "committed_at": row["committed_at"],
                }
                for row in snapshot_rows
            ],
            "metadata_counts": self.store.counts(),
            "recent_query_batches": self.store.list_query_batches(),
            "recovery_report": self.store.latest_recovery_report(),
            "pending_requests_in_process": len(self._pending),
        }
        return result

    def latest_recovery_report(self) -> dict[str, Any] | None:
        return self.store.latest_recovery_report()

    def close(self, *, graceful: bool = True) -> None:
        with self._close_lock:
            if self._closed:
                return
            self._closed = True
            if graceful:
                self.store.mark_graceful_stop(self.run_id)

