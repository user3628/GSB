from __future__ import annotations

import json
import math
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Sequence

from .config import GatewayConfig
from .domain import (
    LEGAL_TRANSITIONS,
    InferenceRequestSpec,
    RequestStatus,
    RouteTable,
    assert_legal_transition,
    canonical_json,
    payload_digest,
)
from .errors import GatewayError


SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS routes (
    version INTEGER PRIMARY KEY,
    checksum TEXT NOT NULL,
    generated_at TEXT NOT NULL,
    payload TEXT NOT NULL,
    source_payload TEXT,
    committed_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS backend_runtime (
    route_version INTEGER NOT NULL,
    backend_id TEXT NOT NULL,
    endpoint TEXT NOT NULL,
    vram_limit_mb REAL NOT NULL,
    concurrency_limit INTEGER NOT NULL,
    vram_per_sample_mb REAL NOT NULL,
    estimated_latency_ms INTEGER NOT NULL,
    failure_threshold INTEGER NOT NULL,
    in_flight INTEGER NOT NULL DEFAULT 0,
    vram_reserved_mb REAL NOT NULL DEFAULT 0,
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    total_failures INTEGER NOT NULL DEFAULT 0,
    timeout_count INTEGER NOT NULL DEFAULT 0,
    total_completed INTEGER NOT NULL DEFAULT 0,
    state TEXT NOT NULL DEFAULT 'HEALTHY',
    updated_at TEXT NOT NULL,
    PRIMARY KEY (route_version, backend_id),
    FOREIGN KEY (route_version) REFERENCES routes(version)
);

CREATE TABLE IF NOT EXISTS requests (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id TEXT NOT NULL UNIQUE,
    payload_digest TEXT NOT NULL,
    canonical_payload TEXT NOT NULL,
    model_id TEXT NOT NULL,
    sample_count INTEGER NOT NULL,
    latency_cap_ms INTEGER NOT NULL,
    priority INTEGER NOT NULL,
    tenant_id TEXT NOT NULL,
    fault_mode TEXT,
    route_version INTEGER NOT NULL,
    backend_id TEXT NOT NULL,
    endpoint TEXT NOT NULL,
    vram_demand_mb REAL NOT NULL,
    vram_reserved_mb REAL NOT NULL DEFAULT 0,
    estimated_latency_ms INTEGER NOT NULL,
    route_global_concurrency_limit INTEGER,
    status TEXT NOT NULL,
    error_code TEXT,
    error_detail TEXT,
    result_json TEXT,
    cancel_requested INTEGER NOT NULL DEFAULT 0,
    round_id INTEGER,
    dispatch_seq INTEGER,
    recovery_count INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    completed_at TEXT,
    FOREIGN KEY (route_version) REFERENCES routes(version)
);

CREATE INDEX IF NOT EXISTS idx_requests_queue
    ON requests(status, priority DESC, seq ASC);
CREATE INDEX IF NOT EXISTS idx_requests_backend
    ON requests(route_version, backend_id, status);
CREATE INDEX IF NOT EXISTS idx_requests_round
    ON requests(round_id, dispatch_seq);

CREATE TABLE IF NOT EXISTS request_transitions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id TEXT NOT NULL,
    from_status TEXT,
    to_status TEXT NOT NULL,
    reason_code TEXT NOT NULL,
    detail_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (request_id) REFERENCES requests(request_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_transitions_request
    ON request_transitions(request_id, id);

CREATE TABLE IF NOT EXISTS tenant_usage (
    tenant_id TEXT PRIMARY KEY,
    dispatched_count INTEGER NOT NULL DEFAULT 0,
    shed_count INTEGER NOT NULL DEFAULT 0,
    failed_count INTEGER NOT NULL DEFAULT 0,
    estimated_ms_consumed INTEGER NOT NULL DEFAULT 0,
    last_dispatch_seq INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS rounds (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    route_version INTEGER NOT NULL,
    budget_ms INTEGER NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    status TEXT NOT NULL,
    dispatched_count INTEGER NOT NULL DEFAULT 0,
    shed_count INTEGER NOT NULL DEFAULT 0,
    skipped_count INTEGER NOT NULL DEFAULT 0,
    estimated_used_ms INTEGER NOT NULL DEFAULT 0,
    decision_elapsed_ms REAL,
    trace_json TEXT
);

CREATE TABLE IF NOT EXISTS intents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    intent_uid TEXT NOT NULL UNIQUE,
    kind TEXT NOT NULL,
    request_id TEXT,
    phase TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    state TEXT NOT NULL,
    created_at TEXT NOT NULL,
    closed_at TEXT,
    close_reason TEXT
);

CREATE INDEX IF NOT EXISTS idx_intents_open ON intents(state, created_at);

CREATE TABLE IF NOT EXISTS admission_rejections (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id TEXT NOT NULL,
    code TEXT NOT NULL,
    reason TEXT NOT NULL,
    details_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    round_id INTEGER
);

CREATE TABLE IF NOT EXISTS recovery_reports (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    boot_id TEXT NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT NOT NULL,
    requeued_count INTEGER NOT NULL,
    aborted_count INTEGER NOT NULL,
    open_intent_count INTEGER NOT NULL,
    interrupted_round_count INTEGER NOT NULL,
    backend_rows_rebuilt INTEGER NOT NULL,
    facts_json TEXT NOT NULL
);
"""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _row_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
    return None if row is None else dict(row)


def _status_value(value: RequestStatus | str) -> str:
    return value.value if isinstance(value, RequestStatus) else str(value)


class Store:
    """Serialized SQLite access with explicit two-phase operation intents.

    Every resource-changing write is split into:
      1. persist an OPEN intent and commit;
      2. atomically change request state + backend counters + close the intent.

    A crash between (1) and (2) leaves the request in its previous fully-valid
    state.  A crash after (2) leaves a closed intent and fully-closed counters.
    """

    def __init__(self, config: GatewayConfig) -> None:
        self.config = config
        self.path = config.resolved_db_path
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            self.path,
            check_same_thread=False,
            isolation_level=None,
            timeout=10.0,
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=FULL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.execute("PRAGMA busy_timeout=10000")
        self._conn.executescript(SCHEMA)
        self._ensure_column("routes", "source_payload", "TEXT")
        self._set_meta_default("schema_version", "1")

    def _ensure_column(self, table: str, column: str, declaration: str) -> None:
        columns = {
            str(row["name"])
            for row in self._conn.execute(f"PRAGMA table_info({table})").fetchall()
        }
        if column not in columns:
            self._conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {declaration}")

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    @contextmanager
    def _write(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                yield self._conn
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
            else:
                self._conn.execute("COMMIT")

    @contextmanager
    def _read(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            self._conn.execute("BEGIN")
            try:
                yield self._conn
            finally:
                self._conn.execute("COMMIT")

    def _set_meta_default(self, key: str, value: str) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT OR IGNORE INTO meta(key, value) VALUES (?, ?)", (key, value)
            )

    def _get_meta_locked(self, key: str) -> str | None:
        row = self._conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
        return None if row is None else str(row["value"])

    def _set_meta_locked(self, key: str, value: str) -> None:
        self._conn.execute(
            "INSERT INTO meta(key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )

    @staticmethod
    def _open_intent_locked(
        conn: sqlite3.Connection,
        *,
        kind: str,
        request_id: str | None,
        phase: str,
        payload: dict[str, Any],
    ) -> str:
        intent_uid = uuid.uuid4().hex
        conn.execute(
            "INSERT INTO intents(intent_uid, kind, request_id, phase, payload_json, state, created_at) "
            "VALUES (?, ?, ?, ?, ?, 'OPEN', ?)",
            (intent_uid, kind, request_id, phase, canonical_json(payload), utc_now()),
        )
        return intent_uid

    @staticmethod
    def _close_intent_locked(
        conn: sqlite3.Connection, intent_uid: str, state: str, reason: str
    ) -> None:
        conn.execute(
            "UPDATE intents SET state = ?, closed_at = ?, close_reason = ? WHERE intent_uid = ?",
            (state, utc_now(), reason, intent_uid),
        )

    @staticmethod
    def _record_transition_locked(
        conn: sqlite3.Connection,
        request_id: str,
        from_status: RequestStatus | str | None,
        to_status: RequestStatus | str,
        reason_code: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        conn.execute(
            "INSERT INTO request_transitions(request_id, from_status, to_status, reason_code, detail_json, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (
                request_id,
                None if from_status is None else str(from_status),
                _status_value(to_status),
                reason_code,
                canonical_json(details or {}),
                utc_now(),
            ),
        )

    @staticmethod
    def _update_tenant_dispatch_locked(
        conn: sqlite3.Connection, tenant_id: str, estimated_ms: int, seq: int
    ) -> None:
        conn.execute(
            "INSERT INTO tenant_usage(tenant_id, dispatched_count, estimated_ms_consumed, last_dispatch_seq, updated_at) "
            "VALUES (?, 1, ?, ?, ?) "
            "ON CONFLICT(tenant_id) DO UPDATE SET "
            "dispatched_count = dispatched_count + 1, "
            "estimated_ms_consumed = estimated_ms_consumed + excluded.estimated_ms_consumed, "
            "last_dispatch_seq = excluded.last_dispatch_seq, "
            "updated_at = excluded.updated_at",
            (tenant_id, estimated_ms, seq, utc_now()),
        )

    @staticmethod
    def _update_tenant_shed_locked(conn: sqlite3.Connection, tenant_id: str) -> None:
        conn.execute(
            "INSERT INTO tenant_usage(tenant_id, shed_count, updated_at) VALUES (?, 1, ?) "
            "ON CONFLICT(tenant_id) DO UPDATE SET "
            "shed_count = shed_count + 1, updated_at = excluded.updated_at",
            (tenant_id, utc_now()),
        )

    def _request_dict_locked(self, row: sqlite3.Row) -> dict[str, Any]:
        item = dict(row)
        for key, output_key in (
            ("canonical_payload", "canonical_payload"),
            ("error_detail", "error_detail"),
            ("result_json", "result"),
        ):
            value = item.get(key)
            if value is None:
                item[output_key] = None
                if output_key != key:
                    item.pop(key, None)
                continue
            try:
                parsed = json.loads(value)
            except (TypeError, json.JSONDecodeError):
                parsed = value
            item[output_key] = parsed
            if output_key != key:
                item.pop(key, None)
        item["cancel_requested"] = bool(item["cancel_requested"])
        item["status"] = str(item["status"])
        return item

    def get_current_route(self) -> dict[str, Any] | None:
        with self._read() as conn:
            version = self._get_meta_locked("current_route_version")
            if version is None:
                return None
            row = conn.execute("SELECT * FROM routes WHERE version = ?", (int(version),)).fetchone()
            if row is None:
                return None
            payload = json.loads(row["payload"])
            payload["_committed_at"] = row["committed_at"]
            return payload

    def submit_route(
        self,
        route: RouteTable,
        persisted: dict[str, Any],
        checksum: str,
        source_document: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        with self._write() as conn:
            current_raw = self._get_meta_locked("current_route_version")
            current = int(current_raw) if current_raw is not None else 0
            if route.version <= current:
                raise GatewayError(
                    "E_STALE_ROUTE",
                    "Route version must be greater than the committed version",
                    http_status=409,
                    details={"submitted": route.version, "current": current},
                )
            now = utc_now()
            conn.execute(
                "INSERT INTO routes(version, checksum, generated_at, payload, source_payload, committed_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (
                    route.version,
                    checksum,
                    route.generated_at,
                    canonical_json(persisted),
                    canonical_json(source_document or persisted),
                    now,
                ),
            )
            for backend_id, backend in route.backends.items():
                conn.execute(
                    "INSERT INTO backend_runtime("
                    "route_version, backend_id, endpoint, vram_limit_mb, concurrency_limit, "
                    "vram_per_sample_mb, estimated_latency_ms, failure_threshold, updated_at"
                    ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        route.version,
                        backend_id,
                        backend.endpoint,
                        backend.vram_limit_mb,
                        backend.concurrency_limit,
                        backend.vram_per_sample_mb,
                        backend.estimated_latency_ms,
                        backend.failure_threshold,
                        now,
                    ),
                )
            self._set_meta_locked("current_route_version", str(route.version))
            return {
                "version": route.version,
                "checksum": checksum,
                "generated_at": route.generated_at,
                "committed_at": now,
                "previous_version": current or None,
                "models": list(route.models),
                "backends": list(route.backends),
            }

    def get_route(self, version: int) -> dict[str, Any] | None:
        with self._read() as conn:
            row = conn.execute("SELECT * FROM routes WHERE version = ?", (version,)).fetchone()
            if row is None:
                return None
            return json.loads(row["payload"])

    def list_routes(self) -> list[dict[str, Any]]:
        with self._read() as conn:
            rows = conn.execute(
                "SELECT version, checksum, generated_at, committed_at FROM routes ORDER BY version"
            ).fetchall()
            return [dict(row) for row in rows]

    def capacity_snapshot(self, route_version: int) -> dict[str, Any]:
        with self._read() as conn:
            route_row = conn.execute(
                "SELECT payload FROM routes WHERE version=?", (route_version,)
            ).fetchone()
            route_global = None
            if route_row is not None:
                route_global = json.loads(route_row["payload"]).get(
                    "global_concurrency_limit"
                )
            effective_global = self.config.global_concurrency_limit
            if route_global is not None:
                effective_global = min(effective_global, int(route_global))
            rows = conn.execute(
                "SELECT * FROM backend_runtime WHERE route_version = ? ORDER BY backend_id",
                (route_version,),
            ).fetchall()
            global_inflight = conn.execute(
                "SELECT COUNT(*) AS n FROM requests WHERE status = 'DISPATCHED'"
            ).fetchone()["n"]
            queued = conn.execute(
                "SELECT COUNT(*) AS n FROM requests WHERE status IN ('PENDING', 'QUEUED')"
            ).fetchone()["n"]
            backends = []
            for row in rows:
                item = dict(row)
                item["available_concurrency"] = max(
                    0, item["concurrency_limit"] - item["in_flight"]
                )
                item["available_vram_mb"] = round(
                    max(0.0, item["vram_limit_mb"] - item["vram_reserved_mb"]), 6
                )
                backends.append(item)
            return {
                "global_concurrency_limit": effective_global,
                "configured_global_concurrency_limit": self.config.global_concurrency_limit,
                "route_global_concurrency_limit": route_global,
                "global_in_flight": int(global_inflight),
                "global_available": max(0, effective_global - int(global_inflight)),
                "queue_limit": self.config.queue_limit,
                "queued": int(queued),
                "queue_available": max(0, self.config.queue_limit - int(queued)),
                "backends": backends,
            }

    def get_request(self, request_id: str) -> dict[str, Any] | None:
        with self._read() as conn:
            row = conn.execute(
                "SELECT * FROM requests WHERE request_id = ?", (request_id,)
            ).fetchone()
            return None if row is None else self._request_dict_locked(row)

    def get_request_transitions(self, request_id: str) -> list[dict[str, Any]]:
        with self._read() as conn:
            rows = conn.execute(
                "SELECT * FROM request_transitions WHERE request_id = ? ORDER BY id",
                (request_id,),
            ).fetchall()
            result = []
            for row in rows:
                item = dict(row)
                item["details"] = json.loads(item.pop("detail_json"))
                result.append(item)
            return result

    def list_requests(
        self, *, status: str | None = None, limit: int = 500
    ) -> list[dict[str, Any]]:
        with self._read() as conn:
            if status is None:
                rows = conn.execute(
                    "SELECT * FROM requests ORDER BY seq DESC LIMIT ?", (limit,)
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM requests WHERE status = ? ORDER BY seq DESC LIMIT ?",
                    (status, limit),
                ).fetchall()
            return [self._request_dict_locked(row) for row in rows]

    def record_admission_rejection(
        self, request_id: str, code: str, reason: str, details: dict[str, Any]
    ) -> None:
        with self._write() as conn:
            conn.execute(
                "INSERT INTO admission_rejections(request_id, code, reason, details_json, created_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (request_id, code, reason, canonical_json(details), utc_now()),
            )

    def create_request(
        self,
        spec: InferenceRequestSpec,
        route: RouteTable,
        *,
        strict_capacity: bool = False,
        capacity: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any], bool]:
        payload = spec.canonical_payload()
        digest = payload_digest(payload)
        # Idempotency precedes every current-capacity/circuit check: an already
        # admitted request must always reuse its durable outcome, even if its
        # backend has since degraded or the active route version changed.
        with self._read() as conn:
            existing = conn.execute(
                "SELECT * FROM requests WHERE request_id = ?", (spec.request_id,)
            ).fetchone()
            if existing is not None:
                if existing["payload_digest"] != digest:
                    raise GatewayError(
                        "E_CONFLICTING_REQUEST",
                        "The same request_id was already submitted with different content",
                        http_status=409,
                        details={"request_id": spec.request_id},
                    )
                return self._request_dict_locked(existing), True
        model = route.models.get(spec.model_id)
        if model is None:
            raise GatewayError(
                "E_ROUTE_INVALID",
                "Model is not present in the current route table",
                http_status=422,
                details={"model_id": spec.model_id, "route_version": route.version},
            )
        backend = route.backends[model.backend_id]
        demand = round(spec.sample_count * backend.vram_per_sample_mb, 6)
        estimated = model.estimated_latency_ms or backend.estimated_latency_ms
        capacity = capacity or self.capacity_snapshot(route.version)
        backend_capacity = next(
            (b for b in capacity["backends"] if b["backend_id"] == model.backend_id), None
        )
        if backend_capacity is None:
            raise GatewayError("E_ROUTE_INVALID", "Backend specification is unavailable", http_status=422)
        if backend_capacity["state"] == "DEGRADED":
            raise GatewayError(
                "E_BACKEND_DEGRADED",
                "Target backend is circuit-broken after repeated failures",
                http_status=503,
                details={"backend_id": model.backend_id, "route_version": route.version},
            )
        if demand > backend.vram_limit_mb + 1e-9:
            details = {
                "backend_id": model.backend_id,
                "required_vram_mb": demand,
                "vram_limit_mb": backend.vram_limit_mb,
                "required_concurrency": 1,
                "backend_concurrency_limit": backend.concurrency_limit,
                "global_available": capacity["global_available"],
            }
            self.record_admission_rejection(spec.request_id, "E_CAPACITY_EXCEEDED", "VRAM_TOO_LARGE", details)
            raise GatewayError(
                "E_CAPACITY_EXCEEDED",
                "Request VRAM demand exceeds the backend budget",
                http_status=429,
                details=details,
            )

        pending_error: GatewayError | None = None
        result: tuple[dict[str, Any], bool] | None = None
        with self._write() as conn:
            existing = conn.execute(
                "SELECT * FROM requests WHERE request_id = ?", (spec.request_id,)
            ).fetchone()
            if existing is not None:
                if existing["payload_digest"] != digest:
                    raise GatewayError(
                        "E_CONFLICTING_REQUEST",
                        "The same request_id was already submitted with different content",
                        http_status=409,
                        details={"request_id": spec.request_id},
                    )
                result = (self._request_dict_locked(existing), True)

            if result is None:
                current_raw = self._get_meta_locked("current_route_version")
                current = int(current_raw) if current_raw is not None else 0
                if current != route.version:
                    raise GatewayError(
                        "E_ROUTE_VERSION_CHANGED",
                        "Route changed during admission; retry against the new route",
                        http_status=409,
                        details={"expected": route.version, "current": current},
                    )

                queued = conn.execute(
                    "SELECT COUNT(*) AS n FROM requests WHERE status IN ('PENDING', 'QUEUED')"
                ).fetchone()["n"]
                if queued >= self.config.queue_limit:
                    details = {
                        "queue_limit": self.config.queue_limit,
                        "queued": int(queued),
                        "global_in_flight": capacity["global_in_flight"],
                        "global_available": capacity["global_available"],
                        "backend_available_concurrency": backend_capacity["available_concurrency"],
                        "backend_available_vram_mb": backend_capacity["available_vram_mb"],
                    }
                    conn.execute(
                        "INSERT INTO admission_rejections(request_id, code, reason, details_json, created_at) "
                        "VALUES (?, 'E_QUEUE_FULL', 'QUEUE_FULL', ?, ?)",
                        (spec.request_id, canonical_json(details), utc_now()),
                    )
                    pending_error = GatewayError(
                        "E_QUEUE_FULL",
                        "The bounded admission queue is full",
                        http_status=429,
                        details=details,
                    )

                if pending_error is None:
                    now = utc_now()
                    conn.execute(
                        "INSERT INTO requests("
                        "request_id, payload_digest, canonical_payload, model_id, sample_count, latency_cap_ms, "
                        "priority, tenant_id, fault_mode, route_version, backend_id, endpoint, vram_demand_mb, "
                        "estimated_latency_ms, route_global_concurrency_limit, status, created_at, updated_at"
                        ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', ?, ?)",
                        (
                            spec.request_id,
                            digest,
                            canonical_json(payload),
                            spec.model_id,
                            spec.sample_count,
                            spec.latency_cap_ms,
                            spec.priority,
                            spec.tenant_id,
                            spec.fault_mode,
                            route.version,
                            model.backend_id,
                            backend.endpoint,
                            demand,
                            estimated,
                            route.global_concurrency_limit,
                            now,
                            now,
                        ),
                    )
                    self._record_transition_locked(conn, spec.request_id, None, "PENDING", "ADMITTED", {})
                    if strict_capacity:
                        lacks = []
                        if capacity["global_available"] <= 0:
                            lacks.append("GLOBAL_CONCURRENCY")
                        if backend_capacity["available_concurrency"] <= 0:
                            lacks.append("BACKEND_CONCURRENCY")
                        if demand > backend_capacity["available_vram_mb"] + 1e-9:
                            lacks.append("VRAM_BUDGET")
                        if lacks:
                            details = {
                                "lacks": lacks,
                                "global_available": capacity["global_available"],
                                "backend_available_concurrency": backend_capacity["available_concurrency"],
                                "backend_available_vram_mb": backend_capacity["available_vram_mb"],
                                "required_vram_mb": demand,
                            }
                            conn.execute(
                                "UPDATE requests SET status='FAILED', error_code='E_CAPACITY_EXCEEDED', "
                                "error_detail=?, completed_at=?, updated_at=? WHERE request_id=?",
                                (canonical_json(details), now, now, spec.request_id),
                            )
                            self._record_transition_locked(
                                conn, spec.request_id, "PENDING", "FAILED", "E_CAPACITY_EXCEEDED", details
                            )
                            conn.execute(
                                "INSERT INTO admission_rejections(request_id, code, reason, details_json, created_at) "
                                "VALUES (?, 'E_CAPACITY_EXCEEDED', 'STRICT_ADMISSION', ?, ?)",
                                (spec.request_id, canonical_json(details), now),
                            )
                            pending_error = GatewayError(
                                "E_CAPACITY_EXCEEDED",
                                "Current capacity cannot satisfy strict admission",
                                http_status=429,
                                details=details,
                            )
                        else:
                            conn.execute(
                                "UPDATE requests SET status='QUEUED', updated_at=? WHERE request_id=?",
                                (now, spec.request_id),
                            )
                            self._record_transition_locked(
                                conn,
                                spec.request_id,
                                "PENDING",
                                "QUEUED",
                                "QUEUED",
                                {
                                    "priority": spec.priority,
                                    "tenant_id": spec.tenant_id,
                                    "global_available": capacity["global_available"],
                                    "backend_available_concurrency": backend_capacity["available_concurrency"],
                                    "backend_available_vram_mb": backend_capacity["available_vram_mb"],
                                },
                            )
                    else:
                        conn.execute(
                            "UPDATE requests SET status='QUEUED', updated_at=? WHERE request_id=?",
                            (now, spec.request_id),
                        )
                        self._record_transition_locked(
                            conn,
                            spec.request_id,
                            "PENDING",
                            "QUEUED",
                            "QUEUED",
                            {
                                "priority": spec.priority,
                                "tenant_id": spec.tenant_id,
                                "global_available": capacity["global_available"],
                                "backend_available_concurrency": backend_capacity["available_concurrency"],
                                "backend_available_vram_mb": backend_capacity["available_vram_mb"],
                            },
                        )
                    row = conn.execute(
                        "SELECT * FROM requests WHERE request_id=?", (spec.request_id,)
                    ).fetchone()
                    assert row is not None
                    result = (self._request_dict_locked(row), False)
        if pending_error is not None:
            raise pending_error
        assert result is not None
        return result

    def queued_requests(self) -> list[dict[str, Any]]:
        with self._read() as conn:
            rows = conn.execute(
                "SELECT * FROM requests WHERE status='QUEUED' ORDER BY priority DESC, seq ASC"
            ).fetchall()
            return [self._request_dict_locked(row) for row in rows]

    def tenant_usage(self) -> dict[str, dict[str, Any]]:
        with self._read() as conn:
            rows = conn.execute("SELECT * FROM tenant_usage").fetchall()
            return {str(row["tenant_id"]): dict(row) for row in rows}

    def create_round(
        self, budget_ms: int, route_version: int, requested_id: int | None = None
    ) -> dict[str, Any]:
        with self._write() as conn:
            now = utc_now()
            if requested_id is not None:
                existing = conn.execute(
                    "SELECT 1 FROM rounds WHERE id=?", (requested_id,)
                ).fetchone()
                if existing is not None:
                    raise GatewayError(
                        "E_ROUND_CONFLICT",
                        "Requested round_id already exists",
                        http_status=409,
                        details={"round_id": requested_id},
                    )
                cursor = conn.execute(
                    "INSERT INTO rounds(id, route_version, budget_ms, started_at, status) "
                    "VALUES (?, ?, ?, ?, 'RUNNING')",
                    (requested_id, route_version, budget_ms, now),
                )
            else:
                cursor = conn.execute(
                    "INSERT INTO rounds(route_version, budget_ms, started_at, status) "
                    "VALUES (?, ?, ?, 'RUNNING')",
                    (route_version, budget_ms, now),
                )
            return {"id": int(cursor.lastrowid), "budget_ms": budget_ms, "started_at": now}

    def _dispatch_precheck_locked(
        self,
        conn: sqlite3.Connection,
        request_id: str,
        global_limit: int,
        remaining_budget_ms: int,
    ) -> tuple[dict[str, Any] | None, str | None, dict[str, Any]]:
        row = conn.execute(
            "SELECT * FROM requests WHERE request_id=?", (request_id,)
        ).fetchone()
        if row is None:
            return None, "NOT_FOUND", {}
        request = self._request_dict_locked(row)
        if request["status"] != RequestStatus.QUEUED.value:
            return request, "NOT_QUEUED", {"status": request["status"]}
        backend = conn.execute(
            "SELECT * FROM backend_runtime WHERE route_version=? AND backend_id=?",
            (request["route_version"], request["backend_id"]),
        ).fetchone()
        if backend is None:
            return request, "BACKEND_MISSING", {}
        if backend["state"] == "DEGRADED":
            return request, "BACKEND_DEGRADED", dict(backend)
        if int(request["estimated_latency_ms"]) > remaining_budget_ms:
            return request, "TIME_BUDGET", {
                "estimated_latency_ms": int(request["estimated_latency_ms"]),
                "remaining_budget_ms": remaining_budget_ms,
            }
        effective_global_limit = global_limit
        request_global_limit = request.get("route_global_concurrency_limit")
        if request_global_limit is not None:
            effective_global_limit = min(effective_global_limit, int(request_global_limit))
        global_inflight = int(
            conn.execute(
                "SELECT COUNT(*) AS n FROM requests WHERE status='DISPATCHED'"
            ).fetchone()["n"]
        )
        if global_inflight >= effective_global_limit:
            return request, "GLOBAL_CONCURRENCY", {
                "global_in_flight": global_inflight,
                "global_concurrency_limit": effective_global_limit,
            }
        if int(backend["in_flight"]) >= int(backend["concurrency_limit"]):
            return request, "BACKEND_CONCURRENCY", {
                "in_flight": int(backend["in_flight"]),
                "concurrency_limit": int(backend["concurrency_limit"]),
            }
        projected_vram = float(backend["vram_reserved_mb"]) + float(request["vram_demand_mb"])
        if projected_vram > float(backend["vram_limit_mb"]) + 1e-9:
            return request, "VRAM_BUDGET", {
                "projected_vram_mb": round(projected_vram, 6),
                "vram_limit_mb": float(backend["vram_limit_mb"]),
                "required_vram_mb": float(request["vram_demand_mb"]),
            }
        return request, None, {}

    def try_dispatch(
        self,
        request_id: str,
        *,
        round_id: int,
        global_limit: int,
        remaining_budget_ms: int,
        dispatch_seq: int,
    ) -> dict[str, Any]:
        with self._lock:
            with self._read() as conn:
                request, reason, detail = self._dispatch_precheck_locked(
                    conn, request_id, global_limit, remaining_budget_ms
                )
            if reason is not None:
                return {
                    "ok": False,
                    "reason": reason,
                    "detail": detail,
                    "request": request,
                }

            with self._write() as conn:
                intent_uid = self._open_intent_locked(
                    conn,
                    kind="DISPATCH",
                    request_id=request_id,
                    phase="INTENT_COMMITTED",
                    payload={
                        "round_id": round_id,
                        "dispatch_seq": dispatch_seq,
                        "global_limit": global_limit,
                    },
                )

            with self._write() as conn:
                request, reason, detail = self._dispatch_precheck_locked(
                    conn, request_id, global_limit, remaining_budget_ms
                )
                if reason is not None:
                    self._close_intent_locked(conn, intent_uid, "ABORTED", reason)
                    return {
                        "ok": False,
                        "reason": reason,
                        "detail": detail,
                        "request": request,
                    }
                assert request is not None
                now = utc_now()
                conn.execute(
                    "UPDATE backend_runtime SET in_flight=in_flight+1, "
                    "vram_reserved_mb=vram_reserved_mb+?, updated_at=? "
                    "WHERE route_version=? AND backend_id=?",
                    (
                        float(request["vram_demand_mb"]),
                        now,
                        request["route_version"],
                        request["backend_id"],
                    ),
                )
                conn.execute(
                    "UPDATE requests SET status='DISPATCHED', vram_reserved_mb=?, round_id=?, "
                    "dispatch_seq=?, updated_at=? WHERE request_id=? AND status='QUEUED'",
                    (
                        float(request["vram_demand_mb"]),
                        round_id,
                        dispatch_seq,
                        now,
                        request_id,
                    ),
                )
                self._record_transition_locked(
                    conn,
                    request_id,
                    "QUEUED",
                    "DISPATCHED",
                    "DISPATCHED",
                    {
                        "round_id": round_id,
                        "dispatch_seq": dispatch_seq,
                        "backend_id": request["backend_id"],
                        "vram_reserved_mb": float(request["vram_demand_mb"]),
                        "estimated_latency_ms": int(request["estimated_latency_ms"]),
                    },
                )
                self._update_tenant_dispatch_locked(
                    conn,
                    str(request["tenant_id"]),
                    int(request["estimated_latency_ms"]),
                    int(request["seq"]),
                )
                self._close_intent_locked(conn, intent_uid, "DONE", "DISPATCHED")
                row = conn.execute(
                    "SELECT * FROM requests WHERE request_id=?", (request_id,)
                ).fetchone()
                assert row is not None
                return {"ok": True, "request": self._request_dict_locked(row), "detail": {}}

    def fail_backend_degraded(self, request_id: str, round_id: int) -> dict[str, Any]:
        with self._write() as conn:
            row = conn.execute(
                "SELECT * FROM requests WHERE request_id=?", (request_id,)
            ).fetchone()
            if row is None or row["status"] != "QUEUED":
                return {"ok": False, "reason": "NOT_QUEUED", "request": None if row is None else self._request_dict_locked(row)}
            now = utc_now()
            detail = {"code": "E_BACKEND_DEGRADED", "round_id": round_id}
            conn.execute(
                "UPDATE requests SET status='FAILED', error_code='E_BACKEND_DEGRADED', "
                "error_detail=?, completed_at=?, updated_at=? WHERE request_id=? AND status='QUEUED'",
                (canonical_json(detail), now, now, request_id),
            )
            self._record_transition_locked(
                conn, request_id, "QUEUED", "FAILED", "E_BACKEND_DEGRADED", detail
            )
            updated = conn.execute(
                "SELECT * FROM requests WHERE request_id=?", (request_id,)
            ).fetchone()
            assert updated is not None
            return {"ok": True, "request": self._request_dict_locked(updated), "detail": detail}

    def shed_request(
        self,
        request_id: str,
        *,
        reason_code: str,
        details: dict[str, Any],
        round_id: int | None = None,
    ) -> dict[str, Any] | None:
        with self._write() as conn:
            row = conn.execute(
                "SELECT * FROM requests WHERE request_id=?", (request_id,)
            ).fetchone()
            if row is None:
                return None
            status = RequestStatus(row["status"])
            if status is RequestStatus.SHED:
                return self._request_dict_locked(row)
            if status is not RequestStatus.QUEUED:
                return self._request_dict_locked(row)
            now = utc_now()
            payload = dict(details)
            payload.setdefault("round_id", round_id)
            conn.execute(
                "UPDATE requests SET status='SHED', error_code=?, error_detail=?, "
                "completed_at=?, updated_at=? WHERE request_id=? AND status='QUEUED'",
                (reason_code, canonical_json(payload), now, now, request_id),
            )
            self._record_transition_locked(
                conn, request_id, "QUEUED", "SHED", reason_code, payload
            )
            self._update_tenant_shed_locked(conn, str(row["tenant_id"]))
            updated = conn.execute(
                "SELECT * FROM requests WHERE request_id=?", (request_id,)
            ).fetchone()
            assert updated is not None
            return self._request_dict_locked(updated)

    def complete_dispatch(
        self,
        request_id: str,
        *,
        success: bool,
        result: dict[str, Any] | None,
        error_code: str | None,
        error_detail: dict[str, Any] | None,
        timed_out: bool = False,
    ) -> dict[str, Any] | None:
        with self._lock:
            with self._read() as conn:
                row = conn.execute(
                    "SELECT * FROM requests WHERE request_id=?", (request_id,)
                ).fetchone()
                if row is None or row["status"] != "DISPATCHED":
                    return None if row is None else self._request_dict_locked(row)
            with self._write() as conn:
                intent_uid = self._open_intent_locked(
                    conn,
                    kind="RELEASE",
                    request_id=request_id,
                    phase="INTENT_COMMITTED",
                    payload={"success": success, "timed_out": timed_out},
                )
            with self._write() as conn:
                row = conn.execute(
                    "SELECT * FROM requests WHERE request_id=?", (request_id,)
                ).fetchone()
                if row is None or row["status"] != "DISPATCHED":
                    self._close_intent_locked(conn, intent_uid, "ABORTED", "NOT_DISPATCHED")
                    return None if row is None else self._request_dict_locked(row)
                now = utc_now()
                target = RequestStatus.COMPLETED if success else RequestStatus.FAILED
                detail = error_detail or {}
                conn.execute(
                    "UPDATE backend_runtime SET "
                    "in_flight=MAX(0, in_flight-1), "
                    "vram_reserved_mb=MAX(0, vram_reserved_mb-?), "
                    "consecutive_failures=CASE WHEN ? THEN 0 ELSE consecutive_failures+1 END, "
                    "total_failures=total_failures+CASE WHEN ? THEN 0 ELSE 1 END, "
                    "timeout_count=timeout_count+CASE WHEN ? THEN 1 ELSE 0 END, "
                    "total_completed=total_completed+CASE WHEN ? THEN 1 ELSE 0 END, "
                    "state=CASE WHEN (CASE WHEN ? THEN 0 ELSE consecutive_failures+1 END) >= failure_threshold "
                    "THEN 'DEGRADED' ELSE state END, "
                    "updated_at=? "
                    "WHERE route_version=? AND backend_id=?",
                    (
                        float(row["vram_reserved_mb"]),
                        1 if success else 0,
                        1 if success else 0,
                        1 if timed_out else 0,
                        1 if success else 0,
                        1 if success else 0,
                        now,
                        row["route_version"],
                        row["backend_id"],
                    ),
                )
                conn.execute(
                    "UPDATE requests SET status=?, vram_reserved_mb=0, error_code=?, error_detail=?, "
                    "result_json=?, completed_at=?, updated_at=? WHERE request_id=? AND status='DISPATCHED'",
                    (
                        target.value,
                        error_code,
                        None if error_detail is None else canonical_json(error_detail),
                        None if result is None else canonical_json(result),
                        now,
                        now,
                        request_id,
                    ),
                )
                if not success:
                    conn.execute(
                        "INSERT INTO tenant_usage(tenant_id, failed_count, updated_at) VALUES (?, 1, ?) "
                        "ON CONFLICT(tenant_id) DO UPDATE SET "
                        "failed_count = failed_count + 1, updated_at = excluded.updated_at",
                        (row["tenant_id"], now),
                    )
                self._record_transition_locked(
                    conn,
                    request_id,
                    "DISPATCHED",
                    target,
                    error_code or "COMPLETED",
                    detail,
                )
                self._close_intent_locked(conn, intent_uid, "DONE", target.value)
                updated = conn.execute(
                    "SELECT * FROM requests WHERE request_id=?", (request_id,)
                ).fetchone()
                assert updated is not None
                return self._request_dict_locked(updated)

    def request_cancel(self, request_id: str, reason: str) -> tuple[dict[str, Any] | None, bool]:
        """Mark cancellation. Returns (request, needs_connection_close)."""
        with self._write() as conn:
            row = conn.execute(
                "SELECT * FROM requests WHERE request_id=?", (request_id,)
            ).fetchone()
            if row is None:
                return None, False
            request = self._request_dict_locked(row)
            status = RequestStatus(request["status"])
            if status in {RequestStatus.PENDING, RequestStatus.QUEUED}:
                now = utc_now()
                detail = {"reason": reason}
                conn.execute(
                    "UPDATE requests SET status='FAILED', error_code='E_REQUEST_CANCELLED', "
                    "error_detail=?, completed_at=?, updated_at=? WHERE request_id=?",
                    (canonical_json(detail), now, now, request_id),
                )
                self._record_transition_locked(
                    conn, request_id, status, "FAILED", "E_REQUEST_CANCELLED", detail
                )
                updated = conn.execute(
                    "SELECT * FROM requests WHERE request_id=?", (request_id,)
                ).fetchone()
                return self._request_dict_locked(updated), False
            if status is RequestStatus.DISPATCHED:
                conn.execute(
                    "UPDATE requests SET cancel_requested=1, updated_at=? WHERE request_id=?",
                    (utc_now(), request_id),
                )
                updated = conn.execute(
                    "SELECT * FROM requests WHERE request_id=?", (request_id,)
                ).fetchone()
                return self._request_dict_locked(updated), True
            return request, False

    def manual_transition(
        self,
        request_id: str,
        to_status: RequestStatus,
        reason_code: str,
        details: dict[str, Any],
    ) -> dict[str, Any]:
        with self._write() as conn:
            row = conn.execute(
                "SELECT * FROM requests WHERE request_id=?", (request_id,)
            ).fetchone()
            if row is None:
                raise GatewayError("E_NOT_FOUND", "Request not found", http_status=404)
            source = RequestStatus(row["status"])
            assert_legal_transition(source, to_status, recovery=False)
            if source is RequestStatus.DISPATCHED:
                raise GatewayError(
                    "E_INVALID_TRANSITION",
                    "DISPATCHED requests can only be finalized by the owning dispatch worker",
                    http_status=409,
                    details={"from": source.value, "to": to_status.value},
                )
            if to_status is RequestStatus.DISPATCHED:
                raise GatewayError(
                    "E_INVALID_TRANSITION",
                    "Manual transitions cannot reserve backend resources",
                    http_status=409,
                )
            now = utc_now()
            terminal = to_status in {
                RequestStatus.COMPLETED,
                RequestStatus.FAILED,
                RequestStatus.SHED,
                RequestStatus.ABORTED,
            }
            conn.execute(
                "UPDATE requests SET status=?, error_code=?, error_detail=?, completed_at=?, updated_at=? "
                "WHERE request_id=?",
                (
                    to_status.value,
                    None if to_status is RequestStatus.COMPLETED else reason_code,
                    canonical_json(details),
                    now if terminal else None,
                    now,
                    request_id,
                ),
            )
            self._record_transition_locked(
                conn, request_id, source, to_status, reason_code, details
            )
            updated = conn.execute(
                "SELECT * FROM requests WHERE request_id=?", (request_id,)
            ).fetchone()
            assert updated is not None
            return self._request_dict_locked(updated)

    def finish_round(
        self,
        round_id: int,
        *,
        trace: dict[str, Any],
        status: str = "COMPLETED",
    ) -> dict[str, Any]:
        with self._write() as conn:
            conn.execute(
                "UPDATE rounds SET finished_at=?, status=?, dispatched_count=?, shed_count=?, "
                "skipped_count=?, estimated_used_ms=?, decision_elapsed_ms=?, trace_json=? WHERE id=?",
                (
                    utc_now(),
                    status,
                    int(trace["summary"]["dispatched_count"]),
                    int(trace["summary"]["shed_count"]),
                    int(trace["summary"]["skipped_count"]),
                    int(trace["summary"]["estimated_used_ms"]),
                    float(trace["summary"]["decision_elapsed_ms"]),
                    canonical_json(trace),
                    round_id,
                ),
            )
            row = conn.execute("SELECT * FROM rounds WHERE id=?", (round_id,)).fetchone()
            assert row is not None
            return self._round_dict_locked(row)

    def _round_dict_locked(self, row: sqlite3.Row) -> dict[str, Any]:
        item = dict(row)
        raw = item.pop("trace_json", None)
        item["trace"] = None if raw is None else json.loads(raw)
        return item

    def get_round(self, round_id: int) -> dict[str, Any] | None:
        with self._read() as conn:
            row = conn.execute("SELECT * FROM rounds WHERE id=?", (round_id,)).fetchone()
            return None if row is None else self._round_dict_locked(row)

    def list_rounds(self, limit: int = 50) -> list[dict[str, Any]]:
        with self._read() as conn:
            rows = conn.execute(
                "SELECT * FROM rounds ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
            return [self._round_dict_locked(row) for row in rows]

    def attach_rejections(self, round_id: int) -> list[dict[str, Any]]:
        with self._write() as conn:
            rows = conn.execute(
                "SELECT * FROM admission_rejections WHERE round_id IS NULL ORDER BY id"
            ).fetchall()
            ids = [int(row["id"]) for row in rows]
            if ids:
                placeholders = ",".join("?" for _ in ids)
                conn.execute(
                    f"UPDATE admission_rejections SET round_id=? WHERE id IN ({placeholders})",
                    (round_id, *ids),
                )
            result = []
            for row in rows:
                item = dict(row)
                item["details"] = json.loads(item.pop("details_json"))
                result.append(item)
            return result

    def backend_status(
        self, *, route_version: int | None = None, include_retired: bool = False
    ) -> list[dict[str, Any]]:
        with self._read() as conn:
            if route_version is not None:
                rows = conn.execute(
                    "SELECT * FROM backend_runtime WHERE route_version=? ORDER BY backend_id",
                    (route_version,),
                ).fetchall()
            elif include_retired:
                rows = conn.execute(
                    "SELECT * FROM backend_runtime ORDER BY route_version DESC, backend_id"
                ).fetchall()
            else:
                current_raw = self._get_meta_locked("current_route_version")
                if current_raw is None:
                    return []
                rows = conn.execute(
                    "SELECT * FROM backend_runtime WHERE route_version=? ORDER BY backend_id",
                    (int(current_raw),),
                ).fetchall()
            return [dict(row) for row in rows]

    def reset_backend(self, route_version: int, backend_id: str) -> dict[str, Any]:
        with self._write() as conn:
            row = conn.execute(
                "SELECT * FROM backend_runtime WHERE route_version=? AND backend_id=?",
                (route_version, backend_id),
            ).fetchone()
            if row is None:
                raise GatewayError("E_NOT_FOUND", "Backend runtime not found", http_status=404)
            if int(row["in_flight"]) != 0:
                raise GatewayError(
                    "E_CAPACITY_EXCEEDED",
                    "Cannot reset a backend with in-flight requests",
                    http_status=409,
                    details={"in_flight": int(row["in_flight"])},
                )
            conn.execute(
                "UPDATE backend_runtime SET state='HEALTHY', consecutive_failures=0, "
                "total_failures=0, timeout_count=0, updated_at=? "
                "WHERE route_version=? AND backend_id=?",
                (utc_now(), route_version, backend_id),
            )
            updated = conn.execute(
                "SELECT * FROM backend_runtime WHERE route_version=? AND backend_id=?",
                (route_version, backend_id),
            ).fetchone()
            return dict(updated)

    def recover(self) -> dict[str, Any]:
        boot_id = uuid.uuid4().hex
        started = utc_now()
        requeued: list[dict[str, Any]] = []
        aborted: list[dict[str, Any]] = []
        with self._write() as conn:
            open_intents = conn.execute(
                "SELECT * FROM intents WHERE state='OPEN' ORDER BY id"
            ).fetchall()
            interrupted_rounds = conn.execute(
                "SELECT id FROM rounds WHERE status='RUNNING' ORDER BY id"
            ).fetchall()
            dispatched = conn.execute(
                "SELECT * FROM requests WHERE status='DISPATCHED' ORDER BY seq"
            ).fetchall()
            runtime_rows = conn.execute("SELECT * FROM backend_runtime").fetchall()

            for row in dispatched:
                request = dict(row)
                can_requeue = (
                    self.config.recovery_requeue
                    and int(request["recovery_count"]) < self.config.max_recovery_attempts
                    and not bool(request["cancel_requested"])
                    and conn.execute(
                        "SELECT 1 FROM routes WHERE version=?", (request["route_version"],)
                    ).fetchone()
                    is not None
                    and conn.execute(
                        "SELECT 1 FROM backend_runtime WHERE route_version=? AND backend_id=?",
                        (request["route_version"], request["backend_id"]),
                    ).fetchone()
                    is not None
                )
                now = utc_now()
                if can_requeue:
                    conn.execute(
                        "UPDATE requests SET status='QUEUED', vram_reserved_mb=0, round_id=NULL, "
                        "dispatch_seq=NULL, recovery_count=recovery_count+1, error_code=NULL, "
                        "error_detail=NULL, updated_at=? WHERE request_id=? AND status='DISPATCHED'",
                        (now, request["request_id"]),
                    )
                    self._record_transition_locked(
                        conn,
                        request["request_id"],
                        "DISPATCHED",
                        "QUEUED",
                        "RECOVERY_REQUEUE",
                        {
                            "boot_id": boot_id,
                            "previous_round_id": request["round_id"],
                            "previous_vram_reserved_mb": float(request["vram_reserved_mb"]),
                        },
                    )
                    requeued.append(
                        {
                            "request_id": request["request_id"],
                            "route_version": request["route_version"],
                            "backend_id": request["backend_id"],
                            "released_vram_mb": float(request["vram_reserved_mb"]),
                        }
                    )
                else:
                    conn.execute(
                        "UPDATE requests SET status='ABORTED', vram_reserved_mb=0, round_id=NULL, "
                        "dispatch_seq=NULL, error_code='E_RECOVERY_ABORTED', error_detail=?, "
                        "completed_at=?, updated_at=? WHERE request_id=? AND status='DISPATCHED'",
                        (
                            canonical_json({"boot_id": boot_id, "reason": "recovery_policy_aborted"}),
                            now,
                            now,
                            request["request_id"],
                        ),
                    )
                    self._record_transition_locked(
                        conn,
                        request["request_id"],
                        "DISPATCHED",
                        "ABORTED",
                        "E_RECOVERY_ABORTED",
                        {"boot_id": boot_id},
                    )
                    aborted.append(
                        {
                            "request_id": request["request_id"],
                            "released_vram_mb": float(request["vram_reserved_mb"]),
                        }
                    )

            open_intent_facts = []
            for row in open_intents:
                item = dict(row)
                open_intent_facts.append(
                    {
                        "intent_uid": item["intent_uid"],
                        "kind": item["kind"],
                        "request_id": item["request_id"],
                        "phase": item["phase"],
                    }
                )
                self._close_intent_locked(conn, item["intent_uid"], "ABORTED", "RECOVERY_SCAN")
            for row in interrupted_rounds:
                conn.execute(
                    "UPDATE rounds SET status='INTERRUPTED', finished_at=? WHERE id=?",
                    (started, int(row["id"])),
                )

            # Rebuild every counter from durable request facts.  Since all old
            # DISPATCHED rows were either requeued or aborted, this normally
            # zeros them; the recomputation form keeps the invariant explicit.
            for runtime in runtime_rows:
                in_flight = conn.execute(
                    "SELECT COUNT(*) AS n FROM requests WHERE status='DISPATCHED' "
                    "AND route_version=? AND backend_id=?",
                    (runtime["route_version"], runtime["backend_id"]),
                ).fetchone()["n"]
                reserved = conn.execute(
                    "SELECT COALESCE(SUM(vram_reserved_mb), 0) AS v FROM requests "
                    "WHERE status='DISPATCHED' AND route_version=? AND backend_id=?",
                    (runtime["route_version"], runtime["backend_id"]),
                ).fetchone()["v"]
                conn.execute(
                    "UPDATE backend_runtime SET in_flight=?, vram_reserved_mb=?, updated_at=? "
                    "WHERE route_version=? AND backend_id=?",
                    (
                        int(in_flight),
                        round(float(reserved), 6),
                        utc_now(),
                        runtime["route_version"],
                        runtime["backend_id"],
                    ),
                )

            facts = {
                "boot_id": boot_id,
                "source": "persisted_facts_only",
                "dispatched_found": len(dispatched),
                "open_intents": open_intent_facts,
                "interrupted_round_ids": [int(row["id"]) for row in interrupted_rounds],
                "requeued": requeued,
                "aborted": aborted,
                "backend_runtime_snapshot": [dict(row) for row in runtime_rows],
                "note": (
                    "Recovery describes only durable state found at startup; no graceful "
                    "shutdown report is implied for the killed process."
                ),
            }
            finished = utc_now()
            cursor = conn.execute(
                "INSERT INTO recovery_reports(boot_id, started_at, finished_at, requeued_count, "
                "aborted_count, open_intent_count, interrupted_round_count, backend_rows_rebuilt, facts_json) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    boot_id,
                    started,
                    finished,
                    len(requeued),
                    len(aborted),
                    len(open_intent_facts),
                    len(interrupted_rounds),
                    len(runtime_rows),
                    canonical_json(facts),
                ),
            )
            report = {
                "id": int(cursor.lastrowid),
                **facts,
                "started_at": started,
                "finished_at": finished,
                "requeued_count": len(requeued),
                "aborted_count": len(aborted),
                "open_intent_count": len(open_intent_facts),
                "interrupted_round_count": len(interrupted_rounds),
                "backend_rows_rebuilt": len(runtime_rows),
            }
            return report

    def latest_recovery_report(self) -> dict[str, Any] | None:
        with self._read() as conn:
            row = conn.execute(
                "SELECT * FROM recovery_reports ORDER BY id DESC LIMIT 1"
            ).fetchone()
            if row is None:
                return None
            item = dict(row)
            facts = json.loads(item.pop("facts_json"))
            item["facts"] = facts
            item.update(facts)
            return item

    def list_recovery_reports(self, limit: int = 20) -> list[dict[str, Any]]:
        with self._read() as conn:
            rows = conn.execute(
                "SELECT * FROM recovery_reports ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
            result = []
            for row in rows:
                item = dict(row)
                facts = json.loads(item.pop("facts_json"))
                item["facts"] = facts
                item.update(facts)
                result.append(item)
            return result

    def validation_snapshot(self) -> dict[str, Any]:
        with self._read() as conn:
            routes = [dict(row) for row in conn.execute("SELECT * FROM routes ORDER BY version")]
            runtimes = [dict(row) for row in conn.execute(
                "SELECT * FROM backend_runtime ORDER BY route_version, backend_id"
            )]
            requests = [self._request_dict_locked(row) for row in conn.execute(
                "SELECT * FROM requests ORDER BY seq"
            )]
            transitions = [dict(row) for row in conn.execute(
                "SELECT * FROM request_transitions ORDER BY id"
            )]
            rounds = [self._round_dict_locked(row) for row in conn.execute(
                "SELECT * FROM rounds ORDER BY id"
            )]
            open_intents = [dict(row) for row in conn.execute(
                "SELECT * FROM intents WHERE state='OPEN' ORDER BY id"
            )]
            latest_recovery = conn.execute(
                "SELECT * FROM recovery_reports ORDER BY id DESC LIMIT 1"
            ).fetchone()
            current_raw = self._get_meta_locked("current_route_version")
            return {
                "current_route_version": None if current_raw is None else int(current_raw),
                "routes": routes,
                "runtime": runtimes,
                "requests": requests,
                "transitions": transitions,
                "rounds": rounds,
                "open_intents": open_intents,
                "latest_recovery": None if latest_recovery is None else dict(latest_recovery),
                "config": {
                    "global_concurrency_limit": self.config.global_concurrency_limit,
                    "queue_limit": self.config.queue_limit,
                },
            }

