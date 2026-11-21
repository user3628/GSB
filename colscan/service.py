from __future__ import annotations

import hashlib
import json
import re
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .errors import BusinessError, error
from .predicate import validate_predicate
from .repository import StateStore, json_hash, process_instance_id
from .scanner import QuerySpec, ScanBudget, ScanEngine
from .snapshot import Snapshot, load_snapshot_file, snapshot_from_document


QUERY_ID_PATTERN = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")
ERROR_HTTP_STATUS = {
    "E_SNAPSHOT_INVALID": 422,
    "E_BLOCK_CORRUPT": 422,
    "E_SCAN_QUEUE_FULL": 429,
    "E_VERSION_RETIRED": 409,
    "E_QUERY_INVALID": 400,
    "E_QUERY_CONFLICT": 409,
    "E_QUERY_NOT_FOUND": 404,
    "E_QUERY_NOT_READY": 409,
    "E_STATE_CONFLICT": 409,
    "E_TASK_MEMORY_EXCEEDED": 422,
    "E_ROUND_BUDGET_EXCEEDED": 422,
    "E_INTERNAL": 500,
}


@dataclass
class QueryRun:
    query_id: str
    event: threading.Event = field(default_factory=threading.Event)
    thread: threading.Thread | None = None


class ColumnarService:
    def __init__(self, state_path: str | Path, *, task_delay_ms: int | None = None) -> None:
        self.store = StateStore(state_path)
        self.store.initialize()
        self.engine = ScanEngine(self.store, task_delay_ms=task_delay_ms)
        self.instance_id = process_instance_id()
        self._runs_lock = threading.Lock()
        self._runs: dict[str, QueryRun] = {}
        self._recovery_reports: list[dict[str, Any]] = []
        self._recovery_done = False

    def load_snapshot(self, path: str | Path) -> dict[str, Any]:
        snapshot = load_snapshot_file(path)
        installed = self.store.install_snapshot(snapshot)
        installed["columns"] = len(snapshot.columns)
        installed["row_blocks"] = len(snapshot.row_blocks)
        installed["row_count"] = snapshot.row_count
        return installed

    def current_snapshot(self) -> dict[str, Any]:
        info = self.store.active_snapshot_info()
        if info is None:
            raise error(
                "E_SNAPSHOT_INVALID",
                "no active snapshot is loaded",
                http_status=409,
                details={"reason": "no_active_snapshot"},
            )
        document = info.pop("document")
        info["row_count"] = int(document["row_count"])
        info["columns"] = [
            {
                "name": column["name"],
                "dtype": column["dtype"],
                "nullable": column["nullable"],
                "null_count": column["null_count"],
                "encoding": column.get("encoding"),
            }
            for column in document["columns"]
        ]
        info["row_blocks"] = len(document["row_blocks"])
        return info

    def snapshot_blocks(self, version: int | None = None) -> dict[str, Any]:
        if version is None:
            info = self.store.active_snapshot_info()
            if info is None:
                raise error(
                    "E_SNAPSHOT_INVALID",
                    "no active snapshot is loaded",
                    http_status=409,
                    details={"reason": "no_active_snapshot"},
                )
            data = info["document"]
        else:
            data = self.store.snapshot_document(version)
        return {
            "version": int(data["version"]),
            "block_size": int(data["block_size"]),
            "row_count": int(data["row_count"]),
            "row_blocks": data["row_blocks"],
            "columns": [
                {
                    "name": column["name"],
                    "dtype": column["dtype"],
                    "encoding": column.get("encoding"),
                    "blocks": [
                        {
                            "block_seq": block["block_seq"],
                            "block_id": block["block_id"],
                            "start_row": block["start_row"],
                            "row_count": block["row_count"],
                            "encoding": block["encoding"],
                            "raw_size": block["raw_size"],
                            "compressed_size": block["compressed_size"],
                            "compression_ratio": block["compression_ratio"],
                            "encoding_meta": block.get("encoding_meta", {}),
                        }
                        for block in column["blocks"]
                    ],
                }
                for column in data["columns"]
            ],
        }

    def submit_query(self, payload: dict[str, Any], *, wait_seconds: float = 0.0) -> tuple[dict[str, Any], bool]:
        normalized = self._normalize_query(payload)
        query_id = str(payload["query_id"])
        request_hash = json_hash(normalized)
        existing = self.store.query(query_id)
        if existing is not None:
            if existing["request_hash"] != request_hash:
                raise error(
                    "E_QUERY_CONFLICT",
                    "query_id already exists with a different request",
                    http_status=409,
                    details={
                        "query_id": query_id,
                        "existing_request_hash": existing["request_hash"],
                        "submitted_request_hash": request_hash,
                    },
                )
            if existing["status"] in ("PENDING", "RUNNING", "RECOVERING"):
                run = self._ensure_run(query_id, created=False)
                run.event.wait(max(0.0, float(wait_seconds)))
            return self.query_status(query_id), False
        active_info = self.store.active_snapshot_info()
        if active_info is None:
            raise error(
                "E_SNAPSHOT_INVALID",
                "no active snapshot is loaded",
                http_status=409,
                details={"reason": "no_active_snapshot"},
            )
        active_version = int(active_info["version"])
        requested_version = payload.get("snapshot_version")
        if requested_version is not None and int(requested_version) != active_version:
            raise error(
                "E_VERSION_RETIRED",
                "query snapshot version is not the active version",
                http_status=409,
                details={"requested_version": int(requested_version), "active_version": active_version},
            )
        snapshot = snapshot_from_document(active_info["document"], verify_checksum=True)
        spec = self._validate_query(normalized, snapshot)
        block_count = spec.block_end - spec.block_start
        if block_count > spec.budget.max_concurrent_tasks + spec.budget.queue_capacity:
            raise error(
                "E_SCAN_QUEUE_FULL",
                "selected block count exceeds the bounded scheduler queue",
                http_status=429,
                details={
                    "selected_blocks": block_count,
                    "max_concurrent_tasks": spec.budget.max_concurrent_tasks,
                    "queue_capacity": spec.budget.queue_capacity,
                    "admission_limit": spec.budget.max_concurrent_tasks + spec.budget.queue_capacity,
                },
            )
        shards = [
            (block_seq, snapshot.row_blocks[block_seq].block_id)
            for block_seq in range(spec.block_start, spec.block_end)
        ]
        query, created = self.store.create_query(
            query_id=query_id,
            request_hash=request_hash,
            request=normalized,
            snapshot_version=active_version,
            shards=shards,
            window_offset=spec.offset,
            window_limit=spec.limit,
        )
        if not created:
            if query["request_hash"] != request_hash:
                raise error(
                    "E_QUERY_CONFLICT",
                    "query_id already exists with a different request",
                    http_status=409,
                    details={"query_id": query_id},
                )
            current = self.store.query(query_id)
            if current is not None and current["status"] in ("PENDING", "RUNNING", "RECOVERING"):
                run = self._ensure_run(query_id, created=False)
                run.event.wait(max(0.0, float(wait_seconds)))
            return self.query_status(query_id), False
        self._ensure_run(query_id, created=True)
        return self.query_status(query_id), True

    def _normalize_query(self, payload: dict[str, Any]) -> dict[str, Any]:
        query_id = str(payload.get("query_id", ""))
        if not QUERY_ID_PATTERN.match(query_id):
            raise error(
                "E_QUERY_INVALID",
                "query_id must be 1-128 characters from A-Z, a-z, 0-9, _, ., :, or -",
                http_status=400,
                details={"query_id": query_id},
            )
        columns = payload.get("columns")
        if not isinstance(columns, list) or not columns or any(not isinstance(value, str) for value in columns):
            raise error("E_QUERY_INVALID", "columns must be a non-empty string array", http_status=400)
        if len(set(columns)) != len(columns):
            raise error("E_QUERY_INVALID", "columns must not contain duplicates", http_status=400)
        budget_raw = payload.get("budget") or {}
        budget = ScanBudget(
            max_concurrent_tasks=int(budget_raw.get("max_concurrent_tasks", 4)),
            per_task_memory_bytes=int(budget_raw.get("per_task_memory_bytes", 1_048_576)),
            round_time_budget_ms=float(budget_raw.get("round_time_budget_ms", 100.0)),
            queue_capacity=int(budget_raw.get("queue_capacity", 128)),
            degrade_mode=str(budget_raw.get("degrade_mode", "split")),
        )
        if budget.max_concurrent_tasks < 1:
            raise error("E_QUERY_INVALID", "budget.max_concurrent_tasks must be >= 1", http_status=400)
        if budget.per_task_memory_bytes < 1:
            raise error("E_QUERY_INVALID", "budget.per_task_memory_bytes must be >= 1", http_status=400)
        if budget.round_time_budget_ms <= 0:
            raise error("E_QUERY_INVALID", "budget.round_time_budget_ms must be > 0", http_status=400)
        if budget.queue_capacity < 0:
            raise error("E_QUERY_INVALID", "budget.queue_capacity must be >= 0", http_status=400)
        if budget.degrade_mode not in ("split", "strict"):
            raise error("E_QUERY_INVALID", "budget.degrade_mode must be split or strict", http_status=400)
        offset = int(payload.get("offset", 0))
        if offset < 0:
            raise error("E_QUERY_INVALID", "offset must be >= 0", http_status=400)
        limit = payload.get("limit")
        if limit is not None:
            limit = int(limit)
            if limit < 1:
                raise error("E_QUERY_INVALID", "limit must be >= 1", http_status=400)
        predicate = payload.get("predicate")
        if predicate is not None and not isinstance(predicate, dict):
            raise error("E_QUERY_INVALID", "predicate must be an object", http_status=400)
        snapshot_version = payload.get("snapshot_version")
        normalized: dict[str, Any] = {
            "columns": list(columns),
            "predicate": predicate,
            "block_start": payload.get("block_start"),
            "block_end": payload.get("block_end"),
            "offset": offset,
            "limit": limit,
            "budget": budget.to_dict(),
        }
        if snapshot_version is not None:
            normalized["snapshot_version"] = int(snapshot_version)
        return normalized

    def _validate_query(self, normalized: dict[str, Any], snapshot: Snapshot) -> QuerySpec:
        column_types = {column.name: column.dtype for column in snapshot.columns}
        for name in normalized["columns"]:
            if name not in column_types:
                raise error("E_QUERY_INVALID", f"unknown column: {name}", http_status=400, details={"column": name})
        validate_predicate(normalized["predicate"], column_types)
        block_start = normalized.get("block_start")
        if block_start is None:
            block_start = 0
        block_end = normalized.get("block_end")
        if block_end is None:
            block_end = len(snapshot.row_blocks)
        block_start = int(block_start)
        block_end = int(block_end)
        if not (0 <= block_start < block_end <= len(snapshot.row_blocks)):
            raise error(
                "E_QUERY_INVALID",
                "block range must satisfy 0 <= block_start < block_end <= row_blocks",
                http_status=400,
                details={
                    "block_start": block_start,
                    "block_end": block_end,
                    "row_blocks": len(snapshot.row_blocks),
                },
            )
        normalized["block_start"] = block_start
        normalized["block_end"] = block_end
        budget_data = normalized["budget"]
        return QuerySpec(
            columns=tuple(normalized["columns"]),
            predicate=normalized["predicate"],
            block_start=block_start,
            block_end=block_end,
            offset=int(normalized["offset"]),
            limit=normalized["limit"],
            budget=ScanBudget(
                max_concurrent_tasks=int(budget_data["max_concurrent_tasks"]),
                per_task_memory_bytes=int(budget_data["per_task_memory_bytes"]),
                round_time_budget_ms=float(budget_data["round_time_budget_ms"]),
                queue_capacity=int(budget_data["queue_capacity"]),
                degrade_mode=str(budget_data["degrade_mode"]),
            ),
        )

    def _ensure_run(self, query_id: str, *, created: bool) -> QueryRun:
        with self._runs_lock:
            run = self._runs.get(query_id)
            if run is not None:
                return run
            run = QueryRun(query_id)
            self._runs[query_id] = run
            thread = threading.Thread(
                target=self._run_query,
                args=(query_id, run),
                name=f"query-{query_id[:20]}",
                daemon=True,
            )
            run.thread = thread
            thread.start()
            return run

    def _run_query(self, query_id: str, run: QueryRun) -> None:
        try:
            query = self.store.query(query_id)
            if query is None or query["status"] in ("COMPLETE", "FAILED"):
                return
            self.engine.execute(query)
        except Exception as exc:
            payload = exc.to_dict()["error"] if isinstance(exc, BusinessError) else {
                "code": "E_INTERNAL",
                "message": str(exc),
                "details": {"type": type(exc).__name__},
            }
            self.store.set_query_status(query_id, "FAILED", error_payload=payload)
        finally:
            run.event.set()

    def query_status(self, query_id: str) -> dict[str, Any]:
        query = self.store.query(query_id)
        if query is None:
            raise error("E_QUERY_NOT_FOUND", "query does not exist", http_status=404, details={"query_id": query_id})
        shards = self.store.shards(query_id)
        status_counts: dict[str, int] = {}
        for shard in shards:
            status_counts[shard["status"]] = status_counts.get(shard["status"], 0) + 1
        return {
            "query_id": query["query_id"],
            "status": query["status"],
            "snapshot_version": query["snapshot_version"],
            "request": query["request"],
            "request_hash": query["request_hash"],
            "total_blocks": query["total_blocks"],
            "committed_blocks": query["committed_blocks"],
            "progress": round(query["committed_blocks"] / query["total_blocks"], 6) if query["total_blocks"] else 1.0,
            "shard_status_counts": status_counts,
            "commit_generation": query["commit_generation"],
            "total_matched": query["total_matched"],
            "window": {"offset": query["window_offset"], "limit": query["window_limit"]},
            "created_at": query["created_at"],
            "started_at": query["started_at"],
            "completed_at": query["completed_at"],
            "error": query["error"],
        }

    def query_shards(self, query_id: str) -> list[dict[str, Any]]:
        if self.store.query(query_id) is None:
            raise error("E_QUERY_NOT_FOUND", "query does not exist", http_status=404, details={"query_id": query_id})
        return self.store.shards(query_id)

    def query_decisions(self, query_id: str) -> list[dict[str, Any]]:
        if self.store.query(query_id) is None:
            raise error("E_QUERY_NOT_FOUND", "query does not exist", http_status=404, details={"query_id": query_id})
        return self.store.decisions(query_id)

    def query_result(
        self,
        query_id: str,
        *,
        offset: int | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        payload = self.store.result(query_id)
        if payload is None:
            raise error("E_QUERY_NOT_FOUND", "query does not exist", http_status=404, details={"query_id": query_id})
        if payload["status"] in ("PENDING", "RUNNING", "RECOVERING"):
            query = self.store.query(query_id)
            raise error(
                "E_QUERY_NOT_READY",
                "query has not committed a complete result generation",
                http_status=409,
                details={
                    "query_id": query_id,
                    "status": payload["status"],
                    "committed_blocks": query["committed_blocks"] if query else 0,
                    "total_blocks": query["total_blocks"] if query else 0,
                },
            )
        if payload["status"] == "FAILED":
            stored = payload["error"] or {"code": "E_INTERNAL", "message": "query failed", "details": {}}
            code = str(stored.get("code", "E_INTERNAL"))
            raise BusinessError(
                code,
                str(stored.get("message", "query failed")),
                http_status=ERROR_HTTP_STATUS.get(code, 422),
                details=dict(stored.get("details", {})),
            )
        result = payload["result"]
        if result is None or payload["commit_generation"] < 1:
            raise error(
                "E_QUERY_NOT_READY",
                "query has no complete committed result generation",
                http_status=409,
                details={"query_id": query_id, "commit_generation": payload["commit_generation"]},
            )
        use_default_window = offset is None and limit is None
        if use_default_window:
            offset = payload["window_offset"]
            limit = payload["window_limit"]
        elif offset is None:
            offset = 0
        if offset < 0:
            raise error("E_QUERY_INVALID", "offset must be >= 0", http_status=400)
        if limit is not None and limit < 1:
            raise error("E_QUERY_INVALID", "limit must be >= 1", http_status=400)
        all_rows = result["rows"]
        selected_rows = all_rows[offset:] if limit is None else all_rows[offset : offset + limit]
        stable_hash = hashlib.sha256(
            json.dumps(
                {"columns": result["columns"], "rows": selected_rows},
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
            ).encode("utf-8")
        ).hexdigest()
        query = self.store.query(query_id)
        return {
            "query_id": query_id,
            "snapshot_version": query["snapshot_version"] if query else None,
            "commit_generation": payload["commit_generation"],
            "columns": result["columns"],
            "rows": selected_rows,
            "total_matched": payload["total_matched"],
            "window": {"offset": offset, "limit": limit, "row_count": len(selected_rows)},
            "order_key": result["order_key"],
            "result_sha256": stable_hash,
        }

    def recovery_report(self, query_id: str | None = None) -> dict[str, Any]:
        return {
            "source": "sqlite_persisted_facts",
            "complete_exit_report_available": False,
            "note": "A forcibly terminated process cannot produce a clean exit report; this report reconstructs only persisted state.",
            "reports": self.store.recovery_reports(query_id),
        }

    def recover_on_startup(self) -> list[dict[str, Any]]:
        if self._recovery_done:
            return self._recovery_reports
        reports = self.store.recover_incomplete_queries(self.instance_id)
        self._recovery_reports = reports
        self._recovery_done = True
        for report in reports:
            self._ensure_run(str(report["query_id"]), created=False)
        return reports

