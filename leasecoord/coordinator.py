"""Lease coordinator: state machine, elections, failover, and recovery."""

from __future__ import annotations

import json
import os
import socket
import sqlite3
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from .config import CoordinatorConfig
from .election import ElectionResult, NoEligibleCandidateError, elect
from .errors import BusinessError, business_error
from .models import (
    EffectiveLease,
    LEGAL_NODE_TRANSITIONS,
    LeaseRecord,
    LeaseState,
    NodeRecord,
    NodeStatus,
    canonical_json,
    candidate_snapshot_digest,
    digest_json,
    node_status_transition_allowed,
    validate_node_id,
)
from .store import (
    Database,
    assert_lease_invariants,
    fetch_all_node_rows,
    fetch_lease_row,
    insert_event,
)


class SystemClock:
    def now_ms(self) -> int:
        return int(time.time() * 1000)


def _require_int(value: Any, field: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise business_error(
            "E_BAD_REQUEST",
            f"{field} must be an integer",
            field=field,
            value=value,
        )
    if value < minimum:
        raise business_error(
            "E_BAD_REQUEST",
            f"{field} must be >= {minimum}",
            field=field,
            value=value,
        )
    return value


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    if pid == os.getpid():
        return True
    if os.name == "nt":  # pragma: no cover - exercised on Windows hosts
        try:
            import ctypes

            PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
            STILL_ACTIVE = 259
            kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined]
            handle = kernel32.OpenProcess(
                PROCESS_QUERY_LIMITED_INFORMATION, False, pid
            )
            if not handle:
                return False
            exit_code = ctypes.c_ulong()
            try:
                if not kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code)):
                    return False
                return exit_code.value == STILL_ACTIVE
            finally:
                kernel32.CloseHandle(handle)
        except Exception:
            return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


@dataclass(slots=True)
class _LoadedState:
    lease: LeaseRecord
    nodes: dict[str, NodeRecord]


class Coordinator:
    """SQLite-backed single-writer coordinator.

    Each public mutating method creates its own short-lived SQLite connection.
    Lease-changing operations write a committed PENDING intent first, then apply
    the lease and node state in a second ``BEGIN IMMEDIATE`` transaction.
    """

    def __init__(
        self,
        db_path: str | Path,
        config: CoordinatorConfig | None = None,
        *,
        clock: Any | None = None,
        auto_recover: bool = True,
        crash_after_intent: bool = False,
    ) -> None:
        self.config = config or CoordinatorConfig()
        self.config.validate()
        self.clock = clock or SystemClock()
        self.db = Database(db_path, self.config)
        self.instance_id = uuid.uuid4().hex
        self.started_at_ms = self.clock.now_ms()
        self.hostname = socket.gethostname()
        self.crash_after_intent = crash_after_intent
        self._last_recovery_report: dict[str, Any] | None = None
        if auto_recover:
            self.recover()

    # ------------------------------------------------------------------
    # Clocks and serialization helpers
    # ------------------------------------------------------------------

    def _now(self, now_ms: int | None = None) -> int:
        if now_ms is None:
            return int(self.clock.now_ms())
        if isinstance(now_ms, bool) or not isinstance(now_ms, int) or now_ms < 0:
            raise business_error(
                "E_BAD_REQUEST",
                "now_ms must be a non-negative integer",
                value=now_ms,
            )
        return now_ms

    def _coerce_expected_version(self, value: Any) -> int | None:
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise business_error(
                "E_STALE_LEASE",
                "expected_lease_version must be a non-negative integer",
                expected_lease_version=value,
            )
        return value

    @staticmethod
    def _load_state(conn: sqlite3.Connection) -> _LoadedState:
        lease = LeaseRecord.from_row(fetch_lease_row(conn))
        nodes = {
            row["id"]: NodeRecord.from_row(row) for row in fetch_all_node_rows(conn)
        }
        return _LoadedState(lease=lease, nodes=nodes)

    def _effective_lease(
        self,
        lease: LeaseRecord,
        nodes: dict[str, NodeRecord],
        now_ms: int,
    ) -> EffectiveLease:
        holder = nodes.get(lease.holder_id) if lease.holder_id else None
        ttl_expired = lease.state is LeaseState.GRANTED and now_ms >= lease.expires_at_ms
        holder_fresh = bool(
            holder is not None
            and holder.heartbeat_fresh(now_ms, self.config.heartbeat_timeout_ms)
        )
        valid = (
            lease.state is LeaseState.GRANTED
            and not ttl_expired
            and holder is not None
            and holder_fresh
        )
        invalidation_reason: str | None = None
        if lease.state is LeaseState.FREE:
            effective_state = LeaseState.FREE
        elif lease.state is LeaseState.EXPIRED:
            effective_state = LeaseState.EXPIRED
            invalidation_reason = "lease_record_expired"
        elif ttl_expired:
            effective_state = LeaseState.EXPIRED
            invalidation_reason = "lease_ttl_expired"
        elif holder is None:
            effective_state = LeaseState.EXPIRED
            invalidation_reason = "lease_holder_missing"
        elif not holder_fresh:
            effective_state = LeaseState.EXPIRED
            invalidation_reason = "leader_heartbeat_timeout"
        else:
            effective_state = LeaseState.GRANTED

        holder_status: NodeStatus | None = None
        if holder is not None:
            if not holder_fresh:
                holder_status = NodeStatus.SUSPECT
            elif valid:
                holder_status = NodeStatus.LEADER
            else:
                holder_status = NodeStatus.FOLLOWER
        return EffectiveLease(
            record=lease,
            effective_state=effective_state,
            valid=valid,
            expired_by_ttl=ttl_expired,
            holder_heartbeat_fresh=holder_fresh,
            holder_effective_status=holder_status,
            invalidation_reason=invalidation_reason,
        )

    def _node_view(
        self,
        node: NodeRecord,
        effective: EffectiveLease,
        now_ms: int,
    ) -> dict[str, Any]:
        effective_status = node.effective_status(
            now_ms,
            self.config.heartbeat_timeout_ms,
            effective.record.holder_id,
            effective.valid,
        )
        return {
            "id": node.id,
            "priority": node.priority,
            "lease_ttl_ms": node.lease_ttl_ms,
            "last_heartbeat_ms": node.last_heartbeat_ms,
            "heartbeat_age_ms": node.heartbeat_age_ms(now_ms),
            "heartbeat_fresh": node.heartbeat_fresh(
                now_ms, self.config.heartbeat_timeout_ms
            ),
            "status": effective_status.value,
            "stored_status": node.status.value,
            "leader_lease_version": node.leader_lease_version,
            "registered_version": node.registered_version,
            "updated_at_ms": node.updated_at_ms,
        }

    def _lease_view(
        self,
        lease: LeaseRecord,
        nodes: dict[str, NodeRecord],
        now_ms: int,
    ) -> dict[str, Any]:
        effective = self._effective_lease(lease, nodes, now_ms)
        holder = nodes.get(lease.holder_id) if lease.holder_id else None
        return {
            "state": effective.effective_state.value,
            "stored_state": lease.state.value,
            "valid": effective.valid,
            "holder_id": lease.holder_id,
            "holder_status": (
                effective.holder_effective_status.value
                if effective.holder_effective_status is not None
                else None
            ),
            "version": lease.version,
            "expires_at_ms": lease.expires_at_ms,
            "remaining_ms": max(0, lease.expires_at_ms - now_ms),
            "term": lease.term,
            "updated_at_ms": lease.updated_at_ms,
            "expired_by_ttl": effective.expired_by_ttl,
            "holder_heartbeat_fresh": effective.holder_heartbeat_fresh,
            "invalidation_reason": effective.invalidation_reason,
            "holder_leader_lease_version": (
                holder.leader_lease_version if holder is not None else None
            ),
        }

    def _candidate_validation_error(
        self,
        node: NodeRecord | None,
        now_ms: int,
        *,
        require_fresh: bool = True,
        require_not_suspect: bool = True,
    ) -> BusinessError | None:
        if node is None:
            return business_error(
                "E_NODE_INVALID",
                "node is not registered",
                reason="node_not_registered",
            )
        if not (self.config.priority_min <= node.priority <= self.config.priority_max):
            return business_error(
                "E_NODE_INVALID",
                "node priority is outside the configured range",
                node_id=node.id,
                reason="priority_out_of_range",
                priority=node.priority,
                allowed_range=[
                    self.config.priority_min,
                    self.config.priority_max,
                ],
            )
        if require_fresh and not node.heartbeat_fresh(
            now_ms, self.config.heartbeat_timeout_ms
        ):
            return business_error(
                "E_NODE_INVALID",
                "node heartbeat is stale",
                node_id=node.id,
                reason="heartbeat_timeout",
                heartbeat_age_ms=node.heartbeat_age_ms(now_ms),
                heartbeat_timeout_ms=self.config.heartbeat_timeout_ms,
            )
        if require_not_suspect and node.status is NodeStatus.SUSPECT:
            return business_error(
                "E_NODE_INVALID",
                "node is marked SUSPECT and must refresh its heartbeat",
                node_id=node.id,
                reason="node_suspect",
            )
        return None

    def _mark_stale_nodes(
        self,
        conn: sqlite3.Connection,
        nodes: dict[str, NodeRecord],
        now_ms: int,
        *,
        skip_node_id: str | None = None,
    ) -> list[str]:
        changed: list[str] = []
        for node in nodes.values():
            if node.id == skip_node_id:
                continue
            if not node.heartbeat_fresh(now_ms, self.config.heartbeat_timeout_ms):
                if node.status is not NodeStatus.SUSPECT or node.leader_lease_version:
                    conn.execute(
                        """
                        UPDATE nodes
                        SET status = 'SUSPECT', leader_lease_version = 0, updated_at_ms = ?
                        WHERE id = ?
                        """,
                        (now_ms, node.id),
                    )
                    changed.append(node.id)
        return changed

    def _insert_event(
        self,
        conn: sqlite3.Connection,
        *,
        now_ms: int,
        operation: str,
        old: LeaseRecord,
        new_version: int,
        new_state: LeaseState,
        holder_id: str | None,
        term: int,
        details: dict[str, Any],
    ) -> None:
        insert_event(
            conn,
            at_ms=now_ms,
            operation=operation,
            old_version=old.version,
            new_version=new_version,
            old_state=old.state.value,
            new_state=new_state.value,
            holder_id=holder_id,
            term=term,
            details_json=canonical_json(details),
        )

    # ------------------------------------------------------------------
    # Read paths
    # ------------------------------------------------------------------

    def get_leader(self, *, now_ms: int | None = None) -> dict[str, Any]:
        now = self._now(now_ms)

        def query(conn: sqlite3.Connection) -> dict[str, Any]:
            state = self._load_state(conn)
            effective = self._effective_lease(state.lease, state.nodes, now)
            lease_view = self._lease_view(state.lease, state.nodes, now)
            holder = (
                state.nodes.get(state.lease.holder_id)
                if state.lease.holder_id
                else None
            )
            has_leader = bool(effective.valid and holder is not None)
            return {
                "has_leader": has_leader,
                "leader": (
                    self._node_view(holder, effective, now)
                    if has_leader and holder is not None
                    else None
                ),
                "lease_version": state.lease.version,
                "term": state.lease.term,
                "lease": lease_view,
            }

        return self.db.read(query)

    def get_lease(self, *, now_ms: int | None = None) -> dict[str, Any]:
        now = self._now(now_ms)

        def query(conn: sqlite3.Connection) -> dict[str, Any]:
            state = self._load_state(conn)
            effective = self._effective_lease(state.lease, state.nodes, now)
            holder = (
                state.nodes.get(state.lease.holder_id)
                if state.lease.holder_id
                else None
            )
            return {
                "lease": self._lease_view(state.lease, state.nodes, now),
                "leader": (
                    self._node_view(holder, effective, now)
                    if effective.valid and holder is not None
                    else None
                ),
                "lease_version": state.lease.version,
            }

        return self.db.read(query)

    def get_node(self, node_id: str, *, now_ms: int | None = None) -> dict[str, Any]:
        now = self._now(now_ms)
        try:
            validate_node_id(node_id)
        except ValueError as exc:
            raise business_error(
                "E_NODE_INVALID", str(exc), node_id=node_id
            ) from exc

        def query(conn: sqlite3.Connection) -> dict[str, Any]:
            state = self._load_state(conn)
            node = state.nodes.get(node_id)
            if node is None:
                raise business_error(
                    "E_NODE_INVALID",
                    "node is not registered",
                    node_id=node_id,
                    reason="node_not_registered",
                )
            effective = self._effective_lease(state.lease, state.nodes, now)
            return self._node_view(node, effective, now)

        return self.db.read(query)

    def list_nodes(self, *, now_ms: int | None = None) -> list[dict[str, Any]]:
        now = self._now(now_ms)

        def query(conn: sqlite3.Connection) -> list[dict[str, Any]]:
            state = self._load_state(conn)
            effective = self._effective_lease(state.lease, state.nodes, now)
            return [
                self._node_view(node, effective, now)
                for node in sorted(state.nodes.values(), key=lambda item: item.id)
            ]

        return self.db.read(query)

    def get_config(self) -> dict[str, Any]:
        return self.config.to_public_dict()

    def get_events(self, limit: int = 100) -> list[dict[str, Any]]:
        if isinstance(limit, bool) or not isinstance(limit, int):
            raise business_error("E_BAD_REQUEST", "limit must be an integer")
        limit = max(1, min(limit, 1_000))

        def query(conn: sqlite3.Connection) -> list[dict[str, Any]]:
            rows = conn.execute(
                "SELECT * FROM lease_events ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
            return [
                {
                    "event_id": row["id"],
                    "at_ms": row["at_ms"],
                    "operation": row["operation"],
                    "old_version": row["old_version"],
                    "new_version": row["new_version"],
                    "old_state": row["old_state"],
                    "new_state": row["new_state"],
                    "holder_id": row["holder_id"],
                    "term": row["term"],
                    "details": json.loads(row["details_json"]),
                }
                for row in rows
            ]

        return self.db.read(query)

    # ------------------------------------------------------------------
    # Registration and heartbeat
    # ------------------------------------------------------------------

    def register_node(
        self,
        node_id: str,
        priority: int,
        lease_ttl_ms: int,
        *,
        last_heartbeat_ms: int | None = None,
        status: str | NodeStatus | None = None,
        now_ms: int | None = None,
    ) -> dict[str, Any]:
        now = self._now(now_ms)
        try:
            validate_node_id(node_id)
        except ValueError as exc:
            raise business_error(
                "E_NODE_INVALID", str(exc), node_id=node_id
            ) from exc
        if isinstance(priority, bool) or not isinstance(priority, int):
            raise business_error(
                "E_NODE_INVALID",
                "priority must be an integer",
                node_id=node_id,
                priority=priority,
            )
        if not (self.config.priority_min <= priority <= self.config.priority_max):
            raise business_error(
                "E_NODE_INVALID",
                "priority is outside the configured range",
                node_id=node_id,
                priority=priority,
                allowed_range=[
                    self.config.priority_min,
                    self.config.priority_max,
                ],
            )
        if (
            isinstance(lease_ttl_ms, bool)
            or not isinstance(lease_ttl_ms, int)
            or not (
                self.config.min_lease_ttl_ms
                <= lease_ttl_ms
                <= self.config.max_lease_ttl_ms
            )
        ):
            raise business_error(
                "E_NODE_INVALID",
                "lease_ttl_ms is outside the configured range",
                node_id=node_id,
                lease_ttl_ms=lease_ttl_ms,
                allowed_range_ms=[
                    self.config.min_lease_ttl_ms,
                    self.config.max_lease_ttl_ms,
                ],
            )
        if last_heartbeat_ms is not None and (
            isinstance(last_heartbeat_ms, bool)
            or not isinstance(last_heartbeat_ms, int)
            or last_heartbeat_ms < 0
        ):
            raise business_error(
                "E_NODE_INVALID",
                "last_heartbeat_ms must be a non-negative integer",
                node_id=node_id,
            )
        requested_heartbeat = min(
            now, last_heartbeat_ms if last_heartbeat_ms is not None else now
        )
        requested_status: NodeStatus | None = None
        if status is not None:
            try:
                requested_status = NodeStatus(status)
            except ValueError as exc:
                raise business_error(
                    "E_NODE_INVALID",
                    "status is not recognized",
                    node_id=node_id,
                    status=status,
                ) from exc
            if requested_status not in {NodeStatus.ACTIVE, NodeStatus.FOLLOWER}:
                raise business_error(
                    "E_INVALID_TRANSITION",
                    "new registration may only start ACTIVE or FOLLOWER",
                    node_id=node_id,
                    requested_status=requested_status.value,
                )

        def operation(conn: sqlite3.Connection) -> dict[str, Any]:
            state = self._load_state(conn)
            existing = state.nodes.get(node_id)
            if existing is None:
                initial_status = requested_status or NodeStatus.ACTIVE
                conn.execute(
                    """
                    INSERT INTO nodes
                        (id, priority, lease_ttl_ms, last_heartbeat_ms, status,
                         leader_lease_version, registered_version, updated_at_ms)
                    VALUES (?, ?, ?, ?, ?, 0, 1, ?)
                    """,
                    (
                        node_id,
                        priority,
                        lease_ttl_ms,
                        requested_heartbeat,
                        initial_status.value,
                        now,
                    ),
                )
                registered_version = 1
            else:
                heartbeat = max(existing.last_heartbeat_ms, requested_heartbeat)
                next_status = existing.status
                if (
                    existing.status is NodeStatus.SUSPECT
                    and heartbeat >= now - self.config.heartbeat_timeout_ms
                ):
                    next_status = NodeStatus.ACTIVE
                conn.execute(
                    """
                    UPDATE nodes
                    SET priority = ?, lease_ttl_ms = ?, last_heartbeat_ms = ?,
                        status = ?, updated_at_ms = ?, registered_version = registered_version + 1
                    WHERE id = ?
                    """,
                    (
                        priority,
                        lease_ttl_ms,
                        heartbeat,
                        next_status.value,
                        now,
                        node_id,
                    ),
                )
                registered_version = existing.registered_version + 1
            assert_lease_invariants(conn)
            updated_state = self._load_state(conn)
            effective = self._effective_lease(updated_state.lease, updated_state.nodes, now)
            node = updated_state.nodes[node_id]
            return {
                "node": self._node_view(node, effective, now),
                "registered_version": registered_version,
                "lease_version": updated_state.lease.version,
                "operation": "created" if existing is None else "updated",
            }

        return self.db.write(operation)

    def heartbeat(
        self,
        node_id: str,
        *,
        heartbeat_ms: int | None = None,
        now_ms: int | None = None,
    ) -> dict[str, Any]:
        now = self._now(now_ms)
        try:
            validate_node_id(node_id)
        except ValueError as exc:
            raise business_error(
                "E_NODE_INVALID", str(exc), node_id=node_id
            ) from exc
        if heartbeat_ms is not None and (
            isinstance(heartbeat_ms, bool)
            or not isinstance(heartbeat_ms, int)
            or heartbeat_ms < 0
        ):
            raise business_error(
                "E_NODE_INVALID",
                "heartbeat_ms must be a non-negative integer",
                node_id=node_id,
            )
        requested = min(now, heartbeat_ms if heartbeat_ms is not None else now)

        def operation(conn: sqlite3.Connection) -> dict[str, Any]:
            state = self._load_state(conn)
            node = state.nodes.get(node_id)
            if node is None:
                raise business_error(
                    "E_NODE_INVALID",
                    "node is not registered",
                    node_id=node_id,
                    reason="node_not_registered",
                )
            heartbeat_value = max(node.last_heartbeat_ms, requested)
            lease = state.lease
            lease_expired = False
            if lease.holder_id == node_id and lease.state is LeaseState.GRANTED:
                if now >= lease.expires_at_ms:
                    lease_expired = True
                    new_version = lease.version + 1
                    conn.execute(
                        """
                        UPDATE lease
                        SET state = 'EXPIRED', version = ?, updated_at_ms = ?
                        WHERE singleton = 1
                        """,
                        (new_version, now),
                    )
                    self._insert_event(
                        conn,
                        now_ms=now,
                        operation="HEARTBEAT_EXPIRE",
                        old=lease,
                        new_version=new_version,
                        new_state=LeaseState.EXPIRED,
                        holder_id=lease.holder_id,
                        term=lease.term,
                        details={"node_id": node_id, "reason": "lease_ttl_expired"},
                    )
                    next_status = NodeStatus.ACTIVE
                    leader_version = 0
                else:
                    next_status = NodeStatus.LEADER
                    leader_version = lease.version
            elif node.status is NodeStatus.SUSPECT:
                next_status = NodeStatus.ACTIVE
                leader_version = 0
            elif node.status is NodeStatus.LEADER:
                next_status = NodeStatus.FOLLOWER
                leader_version = 0
            else:
                next_status = node.status
                leader_version = node.leader_lease_version

            conn.execute(
                """
                UPDATE nodes
                SET last_heartbeat_ms = ?, status = ?, leader_lease_version = ?,
                    updated_at_ms = ?
                WHERE id = ?
                """,
                (heartbeat_value, next_status.value, leader_version, now, node_id),
            )
            assert_lease_invariants(conn)
            updated = self._load_state(conn)
            effective = self._effective_lease(updated.lease, updated.nodes, now)
            return {
                "node": self._node_view(updated.nodes[node_id], effective, now),
                "heartbeat_advanced": heartbeat_value > node.last_heartbeat_ms,
                "lease_version": updated.lease.version,
                "lease": self._lease_view(updated.lease, updated.nodes, now),
                "lease_expired_by_this_update": lease_expired,
            }

        return self.db.write(operation)

    # ------------------------------------------------------------------
    # Intent protocol
    # ------------------------------------------------------------------

    @staticmethod
    def _intent_fingerprint(
        kind: str,
        node_id: str | None,
        expected_version: int | None,
        payload: dict[str, Any],
    ) -> str:
        return digest_json(
            {
                "kind": kind,
                "node_id": node_id,
                "expected_lease_version": expected_version,
                "payload": payload,
            }
        )

    def _replay_intent(self, row: sqlite3.Row, fingerprint: str) -> dict[str, Any]:
        if row["fingerprint"] != fingerprint:
            raise business_error(
                "E_CONFLICTING_REQUEST",
                "request_id was already used with a different request",
                request_id=row["request_id"],
                existing_intent_id=row["id"],
                existing_kind=row["kind"],
            )
        if row["status"] == "COMMITTED":
            result = json.loads(row["result_json"])
            result["replayed"] = True
            result["intent_id"] = row["id"]
            return result
        if row["status"] == "ABORTED":
            error_json = row["error_json"]
            if error_json:
                error = json.loads(error_json)
                context = dict(error.get("context") or {})
                context.update(
                    {
                        "replayed_aborted_intent": True,
                        "intent_id": row["id"],
                    }
                )
                raise BusinessError(
                    code=error["code"],
                    message=error["message"],
                    context=context,
                    http_status=int(error.get("http_status", 409)),
                )
            raise business_error(
                "E_CONFLICTING_REQUEST",
                "request_id points to an aborted intent",
                request_id=row["request_id"],
                intent_id=row["id"],
            )
        raise business_error(
            "E_CONFLICTING_REQUEST",
            "an identical request is still pending",
            request_id=row["request_id"],
            intent_id=row["id"],
            intent_status=row["status"],
        )

    def _find_request_intent(self, request_id: str) -> sqlite3.Row | None:
        def query(conn: sqlite3.Connection) -> sqlite3.Row | None:
            return conn.execute(
                "SELECT * FROM intents WHERE request_id = ?", (request_id,)
            ).fetchone()

        return self.db.read(query)

    def _insert_intent(
        self,
        *,
        intent_id: str,
        request_id: str | None,
        kind: str,
        node_id: str | None,
        expected_version: int | None,
        fingerprint: str,
        payload: dict[str, Any],
        now_ms: int,
    ) -> None:
        def operation(conn: sqlite3.Connection) -> None:
            conn.execute(
                """
                INSERT INTO intents
                    (id, request_id, kind, node_id, expected_lease_version,
                     fingerprint, payload_json, status, owner_pid, owner_instance,
                     owner_host, created_at_ms)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'PENDING', ?, ?, ?, ?)
                """,
                (
                    intent_id,
                    request_id,
                    kind,
                    node_id,
                    expected_version,
                    fingerprint,
                    canonical_json(payload),
                    os.getpid(),
                    self.instance_id,
                    self.hostname,
                    now_ms,
                ),
            )

        try:
            self.db.write(operation)
        except sqlite3.IntegrityError as exc:
            if request_id:
                raise business_error(
                    "E_CONFLICTING_REQUEST",
                    "request_id was concurrently claimed",
                    request_id=request_id,
                ) from exc
            raise

    def _abort_intent(
        self,
        intent_id: str,
        error: BusinessError,
        *,
        now_ms: int,
        note: str = "operation rejected; lease transaction rolled back",
    ) -> None:
        error_payload = error.to_dict()
        error_payload["http_status"] = error.http_status

        def operation(conn: sqlite3.Connection) -> None:
            conn.execute(
                """
                UPDATE intents
                SET status = 'ABORTED', completed_at_ms = ?, error_json = ?, note = ?
                WHERE id = ? AND status = 'PENDING'
                """,
                (now_ms, canonical_json(error_payload), note, intent_id),
            )

        try:
            self.db.write(operation)
        except Exception:
            # The pending row is deliberately left for startup recovery if this
            # secondary bookkeeping write cannot complete.
            pass

    def _run_lease_intent(
        self,
        *,
        kind: str,
        node_id: str | None,
        expected_version: int | None,
        payload: dict[str, Any],
        now_ms: int,
        operation: Callable[[sqlite3.Connection], dict[str, Any]],
        request_id: str | None = None,
    ) -> dict[str, Any]:
        if request_id is not None:
            if not isinstance(request_id, str) or not request_id.strip():
                raise business_error(
                    "E_BAD_REQUEST",
                    "request_id must be a non-empty string",
                    request_id=request_id,
                )
            if len(request_id) > 256:
                raise business_error(
                    "E_BAD_REQUEST",
                    "request_id must be at most 256 characters",
                    request_id=request_id,
                )
        fingerprint = self._intent_fingerprint(
            kind, node_id, expected_version, payload
        )
        if request_id:
            existing = self._find_request_intent(request_id)
            if existing is not None:
                return self._replay_intent(existing, fingerprint)
        intent_id = uuid.uuid4().hex
        self._insert_intent(
            intent_id=intent_id,
            request_id=request_id,
            kind=kind,
            node_id=node_id,
            expected_version=expected_version,
            fingerprint=fingerprint,
            payload=payload,
            now_ms=now_ms,
        )
        if self.crash_after_intent:
            os._exit(91)

        def apply(conn: sqlite3.Connection) -> dict[str, Any]:
            result = operation(conn)
            assert_lease_invariants(conn)
            result = dict(result)
            result["intent_id"] = intent_id
            conn.execute(
                """
                UPDATE intents
                SET status = 'COMMITTED', completed_at_ms = ?, result_json = ?,
                    note = 'lease mutation committed atomically'
                WHERE id = ? AND status = 'PENDING'
                """,
                (now_ms, canonical_json(result), intent_id),
            )
            if conn.execute(
                "SELECT status FROM intents WHERE id = ?", (intent_id,)
            ).fetchone()["status"] != "COMMITTED":
                raise RuntimeError("intent transition to COMMITTED failed")
            return result

        try:
            return self.db.write(apply)
        except BusinessError as exc:
            self._abort_intent(intent_id, exc, now_ms=now_ms)
            raise
        except Exception as exc:
            internal = business_error(
                "E_INTERNAL",
                "lease transaction failed before commit",
                intent_id=intent_id,
                exception_type=type(exc).__name__,
            )
            self._abort_intent(intent_id, internal, now_ms=now_ms)
            raise internal from exc

    # ------------------------------------------------------------------
    # Lease operations
    # ------------------------------------------------------------------

    def campaign(
        self,
        node_id: str,
        *,
        expected_lease_version: int | None = None,
        now_ms: int | None = None,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        now = self._now(now_ms)
        expected = self._coerce_expected_version(expected_lease_version)
        try:
            validate_node_id(node_id)
        except ValueError as exc:
            raise business_error(
                "E_NODE_INVALID", str(exc), node_id=node_id
            ) from exc
        if request_id is not None and not isinstance(request_id, str):
            raise business_error("E_BAD_REQUEST", "request_id must be a string")

        def operation(conn: sqlite3.Connection) -> dict[str, Any]:
            state = self._load_state(conn)
            node = state.nodes.get(node_id)
            validation = self._candidate_validation_error(node, now)
            if validation is not None:
                raise validation
            assert node is not None
            effective = self._effective_lease(state.lease, state.nodes, now)
            if effective.valid:
                raise business_error(
                    "E_LEASE_HELD",
                    "a valid lease is already held",
                    holder_id=state.lease.holder_id,
                    expires_at_ms=state.lease.expires_at_ms,
                    lease_version=state.lease.version,
                    term=state.lease.term,
                )
            if expected is not None and expected != state.lease.version:
                raise business_error(
                    "E_STALE_LEASE",
                    "expected lease version does not match committed state",
                    expected_lease_version=expected,
                    current_lease_version=state.lease.version,
                    holder_id=state.lease.holder_id,
                    expires_at_ms=state.lease.expires_at_ms,
                )
            trigger_reason = self._grant_trigger_reason(
                state.lease, state.nodes, now
            )
            try:
                election = elect(
                    list(state.nodes.values()),
                    now_ms=now,
                    config=self.config,
                    seed_extra={
                        "lease_version": state.lease.version,
                        "term": state.lease.term,
                        "operation": "campaign",
                    },
                )
            except NoEligibleCandidateError as exc:
                raise business_error(
                    "E_NO_ELIGIBLE_CANDIDATE",
                    "no heartbeat-fresh candidate is eligible",
                    candidate_count=exc.candidate_count,
                    preserved_lease_version=state.lease.version,
                    trigger_reason=trigger_reason,
                    candidate_set_digest=candidate_snapshot_digest(
                        state.nodes.values()
                    ),
                ) from exc
            return self._grant_lease(
                conn,
                old_lease=state.lease,
                old_nodes=state.nodes,
                election=election,
                now_ms=now,
                trigger_reason=trigger_reason,
                operation="CAMPAIGN_GRANT",
                extra={"campaigning_node_id": node.id},
            )

        return self._run_lease_intent(
            kind="CAMPAIGN",
            node_id=node_id,
            expected_version=expected,
            payload={
                "node_id": node_id,
                "expected_lease_version": expected,
            },
            now_ms=now,
            operation=operation,
            request_id=request_id,
        )

    def _grant_trigger_reason(
        self, lease: LeaseRecord, nodes: dict[str, NodeRecord], now_ms: int
    ) -> str:
        if lease.state is LeaseState.FREE:
            return "lease_free"
        if lease.state is LeaseState.EXPIRED:
            return "lease_record_expired"
        if now_ms >= lease.expires_at_ms:
            return "lease_ttl_expired"
        holder = nodes.get(lease.holder_id) if lease.holder_id else None
        if holder is None:
            return "lease_holder_missing"
        if not holder.heartbeat_fresh(now_ms, self.config.heartbeat_timeout_ms):
            return "leader_heartbeat_timeout"
        return "lease_invalid"

    def _grant_lease(
        self,
        conn: sqlite3.Connection,
        *,
        old_lease: LeaseRecord,
        old_nodes: dict[str, NodeRecord],
        election: ElectionResult,
        now_ms: int,
        trigger_reason: str,
        operation: str,
        extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        winner = election.winner
        new_version = old_lease.version + 1
        new_term = old_lease.term + 1
        new_expiry = now_ms + winner.lease_ttl_ms
        if new_expiry <= now_ms:
            new_expiry = now_ms + 1
        conn.execute(
            """
            UPDATE lease
            SET holder_id = ?, version = ?, expires_at_ms = ?, term = ?,
                state = 'GRANTED', updated_at_ms = ?
            WHERE singleton = 1
            """,
            (winner.id, new_version, new_expiry, new_term, now_ms),
        )
        for node in old_nodes.values():
            if node.id == winner.id:
                next_status = NodeStatus.LEADER
                leader_version = new_version
            elif not node.heartbeat_fresh(now_ms, self.config.heartbeat_timeout_ms):
                next_status = NodeStatus.SUSPECT
                leader_version = 0
            else:
                next_status = NodeStatus.FOLLOWER
                leader_version = 0
            conn.execute(
                """
                UPDATE nodes
                SET status = ?, leader_lease_version = ?, updated_at_ms = ?
                WHERE id = ?
                """,
                (next_status.value, leader_version, now_ms, node.id),
            )
        details = {
            "trigger_reason": trigger_reason,
            "winner_id": winner.id,
            "election_algorithm": election.metadata.get("algorithm"),
            "election_estimated": election.metadata.get("estimated", False),
            "candidate_set_digest": election.metadata.get("candidate_set_digest"),
        }
        if extra:
            details.update(extra)
        self._insert_event(
            conn,
            now_ms=now_ms,
            operation=operation,
            old=old_lease,
            new_version=new_version,
            new_state=LeaseState.GRANTED,
            holder_id=winner.id,
            term=new_term,
            details=details,
        )
        updated = self._load_state(conn)
        effective = self._effective_lease(updated.lease, updated.nodes, now_ms)
        leader_view = self._node_view(updated.nodes[winner.id], effective, now_ms)
        return {
            "lease_version": new_version,
            "term": new_term,
            "leader": leader_view,
            "lease": self._lease_view(updated.lease, updated.nodes, now_ms),
            "election": election.metadata,
            "election_basis": election.metadata,
            "previous_lease": {
                "holder_id": old_lease.holder_id,
                "version": old_lease.version,
                "term": old_lease.term,
                "state": old_lease.state.value,
                "expires_at_ms": old_lease.expires_at_ms,
            },
            "trigger_reason": trigger_reason,
            "candidate_set_summary": {
                "candidate_count": election.metadata.get("candidate_count"),
                "candidate_set_digest": election.metadata.get("candidate_set_digest"),
                "winner_id": winner.id,
                "algorithm": election.metadata.get("algorithm"),
                "estimated": election.metadata.get("estimated", False),
            },
        }

    def renew(
        self,
        node_id: str,
        *,
        expected_lease_version: int | None = None,
        now_ms: int | None = None,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        now = self._now(now_ms)
        expected = self._coerce_expected_version(expected_lease_version)
        try:
            validate_node_id(node_id)
        except ValueError as exc:
            raise business_error(
                "E_NODE_INVALID", str(exc), node_id=node_id
            ) from exc

        def operation(conn: sqlite3.Connection) -> dict[str, Any]:
            state = self._load_state(conn)
            node = state.nodes.get(node_id)
            validation = self._candidate_validation_error(node, now)
            if validation is not None:
                raise validation
            assert node is not None
            lease = state.lease
            if lease.state is LeaseState.GRANTED and lease.holder_id != node_id:
                raise business_error(
                    "E_LEASE_HELD",
                    "a different node holds the lease",
                    holder_id=lease.holder_id,
                    expires_at_ms=lease.expires_at_ms,
                    lease_version=lease.version,
                )
            if lease.state is not LeaseState.GRANTED or lease.holder_id != node_id:
                raise business_error(
                    "E_INVALID_TRANSITION",
                    "only the current lease holder may renew",
                    node_id=node_id,
                    lease_state=lease.state.value,
                    holder_id=lease.holder_id,
                    lease_version=lease.version,
                )
            if expected is not None and expected != lease.version:
                raise business_error(
                    "E_STALE_LEASE",
                    "expected lease version is stale",
                    expected_lease_version=expected,
                    current_lease_version=lease.version,
                )
            if now >= lease.expires_at_ms:
                raise business_error(
                    "E_STALE_LEASE",
                    "lease has already expired and cannot be renewed",
                    node_id=node_id,
                    current_lease_version=lease.version,
                    expires_at_ms=lease.expires_at_ms,
                    now_ms=now,
                )
            new_version = lease.version + 1
            new_expiry = max(lease.expires_at_ms + 1, now + node.lease_ttl_ms)
            conn.execute(
                """
                UPDATE lease
                SET expires_at_ms = ?, version = ?, updated_at_ms = ?
                WHERE singleton = 1
                """,
                (new_expiry, new_version, now),
            )
            conn.execute(
                """
                UPDATE nodes
                SET status = 'LEADER', leader_lease_version = ?, updated_at_ms = ?
                WHERE id = ?
                """,
                (new_version, now, node_id),
            )
            stale_marked = self._mark_stale_nodes(
                conn, state.nodes, now, skip_node_id=node_id
            )
            self._insert_event(
                conn,
                now_ms=now,
                operation="RENEW",
                old=lease,
                new_version=new_version,
                new_state=LeaseState.GRANTED,
                holder_id=node_id,
                term=lease.term,
                details={
                    "node_id": node_id,
                    "expires_at_ms": new_expiry,
                    "stale_nodes_marked": stale_marked,
                },
            )
            updated = self._load_state(conn)
            effective = self._effective_lease(updated.lease, updated.nodes, now)
            return {
                "lease_version": new_version,
                "term": updated.lease.term,
                "lease": self._lease_view(updated.lease, updated.nodes, now),
                "leader": self._node_view(updated.nodes[node_id], effective, now),
                "stale_nodes_marked": stale_marked,
            }

        return self._run_lease_intent(
            kind="RENEW",
            node_id=node_id,
            expected_version=expected,
            payload={
                "node_id": node_id,
                "expected_lease_version": expected,
            },
            now_ms=now,
            operation=operation,
            request_id=request_id,
        )

    def resign(
        self,
        node_id: str,
        *,
        expected_lease_version: int | None = None,
        now_ms: int | None = None,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        now = self._now(now_ms)
        expected = self._coerce_expected_version(expected_lease_version)
        try:
            validate_node_id(node_id)
        except ValueError as exc:
            raise business_error(
                "E_NODE_INVALID", str(exc), node_id=node_id
            ) from exc

        def operation(conn: sqlite3.Connection) -> dict[str, Any]:
            state = self._load_state(conn)
            node = state.nodes.get(node_id)
            if node is None:
                raise business_error(
                    "E_NODE_INVALID",
                    "node is not registered",
                    node_id=node_id,
                    reason="node_not_registered",
                )
            lease = state.lease
            if lease.state is LeaseState.GRANTED and lease.holder_id != node_id:
                raise business_error(
                    "E_LEASE_HELD",
                    "a different node holds the lease",
                    holder_id=lease.holder_id,
                    expires_at_ms=lease.expires_at_ms,
                    lease_version=lease.version,
                )
            if lease.state is not LeaseState.GRANTED or lease.holder_id != node_id:
                raise business_error(
                    "E_INVALID_TRANSITION",
                    "node is not the current lease holder",
                    node_id=node_id,
                    lease_state=lease.state.value,
                    holder_id=lease.holder_id,
                    lease_version=lease.version,
                )
            if expected is not None and expected != lease.version:
                raise business_error(
                    "E_STALE_LEASE",
                    "expected lease version is stale",
                    expected_lease_version=expected,
                    current_lease_version=lease.version,
                )
            if now >= lease.expires_at_ms:
                raise business_error(
                    "E_STALE_LEASE",
                    "expired lease cannot be resigned; run an election or failover",
                    current_lease_version=lease.version,
                    expires_at_ms=lease.expires_at_ms,
                )
            new_version = lease.version + 1
            conn.execute(
                """
                UPDATE lease
                SET holder_id = NULL, version = ?, expires_at_ms = 0,
                    state = 'FREE', updated_at_ms = ?
                WHERE singleton = 1
                """,
                (new_version, now),
            )
            conn.execute(
                """
                UPDATE nodes
                SET status = 'FOLLOWER', leader_lease_version = 0, updated_at_ms = ?
                WHERE id = ?
                """,
                (now, node_id),
            )
            stale_marked = self._mark_stale_nodes(
                conn, state.nodes, now, skip_node_id=node_id
            )
            self._insert_event(
                conn,
                now_ms=now,
                operation="RESIGN",
                old=lease,
                new_version=new_version,
                new_state=LeaseState.FREE,
                holder_id=None,
                term=lease.term,
                details={"node_id": node_id, "stale_nodes_marked": stale_marked},
            )
            updated = self._load_state(conn)
            return {
                "lease_version": new_version,
                "term": updated.lease.term,
                "lease": self._lease_view(updated.lease, updated.nodes, now),
                "leader": None,
                "stale_nodes_marked": stale_marked,
                "resigned_node": self._node_view(
                    updated.nodes[node_id],
                    self._effective_lease(updated.lease, updated.nodes, now),
                    now,
                ),
            }

        return self._run_lease_intent(
            kind="RESIGN",
            node_id=node_id,
            expected_version=expected,
            payload={
                "node_id": node_id,
                "expected_lease_version": expected,
            },
            now_ms=now,
            operation=operation,
            request_id=request_id,
        )

    def failover(
        self,
        *,
        reason: str | None = None,
        expected_lease_version: int | None = None,
        force: bool = False,
        now_ms: int | None = None,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        now = self._now(now_ms)
        expected = self._coerce_expected_version(expected_lease_version)
        if reason is not None and not isinstance(reason, str):
            raise business_error("E_BAD_REQUEST", "reason must be a string")
        if reason is not None:
            reason = reason[:256]
        if not isinstance(force, bool):
            raise business_error("E_BAD_REQUEST", "force must be a boolean")

        def operation(conn: sqlite3.Connection) -> dict[str, Any]:
            state = self._load_state(conn)
            lease = state.lease
            if lease.state is not LeaseState.GRANTED:
                raise business_error(
                    "E_INVALID_TRANSITION",
                    "failover requires a GRANTED lease",
                    lease_state=lease.state.value,
                    lease_version=lease.version,
                )
            if expected is not None and expected != lease.version:
                raise business_error(
                    "E_STALE_LEASE",
                    "expected lease version is stale",
                    expected_lease_version=expected,
                    current_lease_version=lease.version,
                )
            holder = state.nodes.get(lease.holder_id) if lease.holder_id else None
            ttl_expired = now >= lease.expires_at_ms
            holder_fresh = bool(
                holder is not None
                and holder.heartbeat_fresh(now, self.config.heartbeat_timeout_ms)
            )
            effective = self._effective_lease(lease, state.nodes, now)
            if not force and effective.valid:
                raise business_error(
                    "E_INVALID_TRANSITION",
                    "leader is healthy; failover would be an invalid transition",
                    holder_id=lease.holder_id,
                    lease_version=lease.version,
                    expires_at_ms=lease.expires_at_ms,
                    hint="pass force=true only for an explicit local test fault",
                )
            if reason:
                trigger_reason = reason
            elif ttl_expired:
                trigger_reason = "lease_ttl_expired"
            elif not holder_fresh:
                trigger_reason = "leader_heartbeat_timeout"
            elif force:
                trigger_reason = "forced_failover"
            else:
                trigger_reason = "leader_unavailable"

            exclude_holder = bool(
                lease.holder_id and (force or trigger_reason == "leader_heartbeat_timeout")
            )
            pool = [
                node
                for node in state.nodes.values()
                if not (exclude_holder and node.id == lease.holder_id)
            ]
            if not pool:
                raise business_error(
                    "E_NO_ELIGIBLE_CANDIDATE",
                    "candidate pool is empty after excluding the failed leader",
                    candidate_count=0,
                    preserved_lease_version=lease.version,
                    trigger_reason=trigger_reason,
                    excluded_holder_id=lease.holder_id,
                    candidate_set_digest=candidate_snapshot_digest([]),
                    registered_candidate_count=len(state.nodes),
                )
            try:
                election = elect(
                    pool,
                    now_ms=now,
                    config=self.config,
                    seed_extra={
                        "lease_version": lease.version,
                        "term": lease.term,
                        "operation": "failover",
                        "trigger_reason": trigger_reason,
                    },
                )
            except NoEligibleCandidateError as exc:
                raise business_error(
                    "E_NO_ELIGIBLE_CANDIDATE",
                    "no heartbeat-fresh candidate is eligible for failover",
                    candidate_count=exc.candidate_count,
                    preserved_lease_version=lease.version,
                    trigger_reason=trigger_reason,
                    excluded_holder_id=lease.holder_id if exclude_holder else None,
                    candidate_set_digest=candidate_snapshot_digest(pool),
                    registered_candidate_count=len(state.nodes),
                ) from exc
            result = self._grant_lease(
                conn,
                old_lease=lease,
                old_nodes=state.nodes,
                election=election,
                now_ms=now,
                trigger_reason=trigger_reason,
                operation="FAILOVER_GRANT",
                extra={
                    "failed_holder_id": lease.holder_id,
                    "force": force,
                    "excluded_holder": exclude_holder,
                },
            )
            result["failover"] = {
                "trigger_reason": trigger_reason,
                "failed_holder_id": lease.holder_id,
                "holder_excluded": exclude_holder,
                "candidate_pool_count": len(pool),
                "candidate_set_digest": election.metadata.get(
                    "candidate_set_digest"
                ),
                "preserved_version_on_no_candidate": True,
            }
            return result

        return self._run_lease_intent(
            kind="FAILOVER",
            node_id=None,
            expected_version=expected,
            payload={
                "reason": reason,
                "expected_lease_version": expected,
                "force": force,
            },
            now_ms=now,
            operation=operation,
            request_id=request_id,
        )

    def admin_transition(
        self,
        node_id: str,
        target_status: str | NodeStatus,
        *,
        expected_lease_version: int | None = None,
        now_ms: int | None = None,
    ) -> dict[str, Any]:
        now = self._now(now_ms)
        expected = self._coerce_expected_version(expected_lease_version)
        try:
            validate_node_id(node_id)
        except ValueError as exc:
            raise business_error(
                "E_NODE_INVALID", str(exc), node_id=node_id
            ) from exc
        try:
            target = NodeStatus(target_status)
        except ValueError as exc:
            raise business_error(
                "E_INVALID_TRANSITION",
                "target status is not recognized",
                node_id=node_id,
                target_status=str(target_status),
            ) from exc

        def operation(conn: sqlite3.Connection) -> dict[str, Any]:
            state = self._load_state(conn)
            node = state.nodes.get(node_id)
            if node is None:
                raise business_error(
                    "E_NODE_INVALID",
                    "node is not registered",
                    node_id=node_id,
                    reason="node_not_registered",
                )
            if expected is not None and expected != state.lease.version:
                raise business_error(
                    "E_STALE_LEASE",
                    "expected lease version is stale",
                    expected_lease_version=expected,
                    current_lease_version=state.lease.version,
                )
            effective = self._effective_lease(state.lease, state.nodes, now)
            if target is NodeStatus.LEADER:
                raise business_error(
                    "E_INVALID_TRANSITION",
                    "LEADER may only be entered by an atomic election commit",
                    node_id=node_id,
                    current_status=node.status.value,
                    target_status=target.value,
                )
            if node.status is NodeStatus.LEADER:
                raise business_error(
                    "E_INVALID_TRANSITION",
                    "LEADER may only leave through renew, resign, expiry, or failover",
                    node_id=node_id,
                    current_status=node.status.value,
                    target_status=target.value,
                    lease_version=state.lease.version,
                )
            if effective.valid and state.lease.holder_id == node_id:
                raise business_error(
                    "E_INVALID_TRANSITION",
                    "active lease holder must resign or be failed over",
                    node_id=node_id,
                    current_status=node.status.value,
                    target_status=target.value,
                    lease_version=state.lease.version,
                )
            if not node_status_transition_allowed(node.status, target):
                raise business_error(
                    "E_INVALID_TRANSITION",
                    "node status transition is not legal",
                    node_id=node_id,
                    current_status=node.status.value,
                    target_status=target.value,
                    legal_targets=sorted(
                        status.value for status in LEGAL_NODE_TRANSITIONS[node.status]
                    ),
                )
            conn.execute(
                """
                UPDATE nodes
                SET status = ?, leader_lease_version = 0, updated_at_ms = ?
                WHERE id = ?
                """,
                (target.value, now, node_id),
            )
            assert_lease_invariants(conn)
            updated = self._load_state(conn)
            return {
                "node": self._node_view(
                    updated.nodes[node_id],
                    self._effective_lease(updated.lease, updated.nodes, now),
                    now,
                ),
                "lease_version": updated.lease.version,
            }

        return self.db.write(operation)

    # ------------------------------------------------------------------
    # Recovery
    # ------------------------------------------------------------------

    def _intent_is_orphaned(self, row: sqlite3.Row, now_ms: int) -> bool:
        owner_pid = int(row["owner_pid"])
        owner_host = row["owner_host"]
        if owner_host == self.hostname and _pid_alive(owner_pid):
            return False
        if owner_host != self.hostname:
            age = now_ms - int(row["created_at_ms"])
            return age > self.config.recovery_remote_stale_after_ms
        return True

    def recover(self, *, now_ms: int | None = None) -> dict[str, Any]:
        now = self._now(now_ms)

        def pending_query(conn: sqlite3.Connection) -> list[sqlite3.Row]:
            return list(
                conn.execute(
                    "SELECT * FROM intents WHERE status = 'PENDING' ORDER BY created_at_ms, id"
                )
            )

        pending = self.db.read(pending_query)
        pending_report: list[dict[str, Any]] = []
        aborted_count = 0
        for row in pending:
            orphaned = self._intent_is_orphaned(row, now)
            action = "pending_owner_alive_skipped"
            if orphaned:
                error = business_error(
                    "E_INTERNAL",
                    "coordinator process terminated after intent was persisted and before lease commit",
                    intent_id=row["id"],
                    kind=row["kind"],
                    rollback="sqlite_uncommitted_lease_transaction_rolled_back",
                )

                def abort(conn: sqlite3.Connection) -> bool:
                    current = conn.execute(
                        "SELECT status FROM intents WHERE id = ?", (row["id"],)
                    ).fetchone()
                    if current is None or current["status"] != "PENDING":
                        return False
                    conn.execute(
                        """
                        UPDATE intents
                        SET status = 'ABORTED', completed_at_ms = ?, error_json = ?, note = ?
                        WHERE id = ? AND status = 'PENDING'
                        """,
                        (
                            now,
                            canonical_json(
                                {
                                    **error.to_dict(),
                                    "http_status": error.http_status,
                                }
                            ),
                            "startup recovery aborted an orphaned intent; no lease mutation was committed",
                            row["id"],
                        ),
                    )
                    return True

                did_abort = self.db.write(abort)
                if did_abort:
                    aborted_count += 1
                    action = "aborted_orphaned_intent_lease_version_unchanged"
                else:
                    action = "already_completed_elsewhere"
            pending_report.append(
                {
                    "intent_id": row["id"],
                    "kind": row["kind"],
                    "node_id": row["node_id"],
                    "expected_lease_version": row["expected_lease_version"],
                    "owner_pid": row["owner_pid"],
                    "owner_host": row["owner_host"],
                    "created_at_ms": row["created_at_ms"],
                    "action": action,
                }
            )

        before_state = self.db.read(self._load_state)
        invalid_nodes: list[dict[str, Any]] = []
        status_updates: list[dict[str, Any]] = []
        stale_holder_ids: list[str] = []
        requires_failover = False
        for node in sorted(before_state.nodes.values(), key=lambda item: item.id):
            if node.heartbeat_fresh(now, self.config.heartbeat_timeout_ms):
                continue
            invalid_nodes.append(
                {
                    "id": node.id,
                    "last_heartbeat_ms": node.last_heartbeat_ms,
                    "heartbeat_age_ms": node.heartbeat_age_ms(now),
                    "persisted_status": node.status.value,
                }
            )
            if (
                before_state.lease.state is LeaseState.GRANTED
                and before_state.lease.holder_id == node.id
            ):
                stale_holder_ids.append(node.id)
                requires_failover = True

        def recheck_health(conn: sqlite3.Connection) -> list[dict[str, Any]]:
            state = self._load_state(conn)
            changes: list[dict[str, Any]] = []
            for node in state.nodes.values():
                if (
                    state.lease.state is LeaseState.GRANTED
                    and state.lease.holder_id == node.id
                ):
                    # A stale leader cannot be persisted as SUSPECT while its
                    # GRANTED lease remains visible. Failover performs that
                    # atomic transition.
                    continue
                fresh = node.heartbeat_fresh(
                    now, self.config.heartbeat_timeout_ms
                )
                if not fresh and node.status is not NodeStatus.SUSPECT:
                    conn.execute(
                        """
                        UPDATE nodes
                        SET status = 'SUSPECT', leader_lease_version = 0,
                            updated_at_ms = ?
                        WHERE id = ?
                        """,
                        (now, node.id),
                    )
                    changes.append(
                        {
                            "node_id": node.id,
                            "from": node.status.value,
                            "to": "SUSPECT",
                            "reason": "heartbeat_timeout_on_recovery",
                        }
                    )
                elif fresh and node.status is NodeStatus.SUSPECT:
                    conn.execute(
                        """
                        UPDATE nodes
                        SET status = 'ACTIVE', leader_lease_version = 0,
                            updated_at_ms = ?
                        WHERE id = ?
                        """,
                        (now, node.id),
                    )
                    changes.append(
                        {
                            "node_id": node.id,
                            "from": "SUSPECT",
                            "to": "ACTIVE",
                            "reason": "heartbeat_fresh_on_recovery",
                        }
                    )
            assert_lease_invariants(conn)
            return changes

        status_updates = self.db.write(recheck_health)
        after_state = self.db.read(self._load_state)
        report_id = f"rec-{uuid.uuid4().hex}"
        report: dict[str, Any] = {
            "report_id": report_id,
            "instance_id": self.instance_id,
            "generated_at_ms": now,
            "process_started_at_ms": self.started_at_ms,
            "facts_source": "sqlite_committed_state_only",
            "pending_intents_found": pending_report,
            "pending_intents_aborted_count": aborted_count,
            "lease": self._lease_view(after_state.lease, after_state.nodes, now),
            "last_complete_lease": {
                "holder_id": after_state.lease.holder_id,
                "version": after_state.lease.version,
                "expires_at_ms": after_state.lease.expires_at_ms,
                "term": after_state.lease.term,
                "state": after_state.lease.state.value,
                "updated_at_ms": after_state.lease.updated_at_ms,
            },
            "heartbeat_recheck": {
                "evaluated_node_count": len(after_state.nodes),
                "heartbeat_timeout_ms": self.config.heartbeat_timeout_ms,
                "invalid_node_ids": [item["id"] for item in invalid_nodes],
                "invalid_nodes": invalid_nodes,
                "stale_lease_holder_ids": stale_holder_ids,
                "requires_failover": requires_failover,
                "status_updates": status_updates,
            },
            "orphaned_intent_rollback": {
                "strategy": "last_committed_lease_version_preserved",
                "committed_lease_version": after_state.lease.version,
            },
            "limitations": [
                "A forced process termination cannot produce a complete graceful shutdown report.",
                "This report is derived only from SQLite rows committed before restart and does not invent in-memory facts.",
                "A stale GRANTED holder is reported as invalid and requires an explicit or subsequent atomic failover; its lease version is preserved until that transaction commits.",
            ],
        }

        def persist_report(conn: sqlite3.Connection) -> None:
            conn.execute(
                """
                INSERT INTO recovery_reports (instance_id, created_at_ms, report_json)
                VALUES (?, ?, ?)
                """,
                (self.instance_id, now, canonical_json(report)),
            )

        self.db.write(persist_report)
        self._last_recovery_report = report
        return report

    def get_recovery_report(self) -> dict[str, Any]:
        def query(conn: sqlite3.Connection) -> sqlite3.Row | None:
            return conn.execute(
                "SELECT * FROM recovery_reports ORDER BY id DESC LIMIT 1"
            ).fetchone()

        row = self.db.read(query)
        if row is None:
            return {
                "available": False,
                "facts_source": "sqlite_committed_state_only",
                "message": "no persisted recovery report",
            }
        report = json.loads(row["report_json"])
        report["available"] = True
        report["report_row_id"] = row["id"]
        return report

    def close(self) -> None:
        """Connections are operation-scoped; retained for host integration."""

