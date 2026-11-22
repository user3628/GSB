"""Derived quota accounting and consistency invariants."""

from __future__ import annotations

import sqlite3
from typing import Any

from .errors import E_QUOTA_INCONSISTENT, SchedulerError
from .state_machine import ACTIVE_RESERVATION_STATES


_PLACEHOLDERS = ",".join("?" for _ in ACTIVE_RESERVATION_STATES)


def group_usage(conn: sqlite3.Connection, group_id: str) -> dict[str, int]:
    node_row = conn.execute(
        "SELECT COALESCE(SUM(occupied_memory_bytes), 0) AS memory_bytes, "
        "COALESCE(SUM(occupied_compute_millis), 0) AS compute_millis "
        "FROM nodes WHERE quota_group = ?",
        (group_id,),
    ).fetchone()
    reserve_row = conn.execute(
        "SELECT COALESCE(SUM(memory_bytes), 0) AS memory_bytes, "
        "COALESCE(SUM(compute_millis), 0) AS compute_millis "
        "FROM reservations WHERE quota_group = ? AND active = 1",
        (group_id,),
    ).fetchone()
    group = conn.execute(
        "SELECT memory_limit_bytes, compute_limit_millis FROM quota_groups "
        "WHERE group_id = ?",
        (group_id,),
    ).fetchone()
    if group is None:
        raise SchedulerError(
            "E_QUOTA_GROUP_UNKNOWN",
            f"quota group {group_id!r} does not exist",
            details={"quota_group": group_id},
        )
    node_memory = int(node_row["memory_bytes"])
    node_compute = int(node_row["compute_millis"])
    reserved_memory = int(reserve_row["memory_bytes"])
    reserved_compute = int(reserve_row["compute_millis"])
    return {
        "memory_limit_bytes": int(group["memory_limit_bytes"]),
        "compute_limit_millis": int(group["compute_limit_millis"]),
        "node_occupancy_bytes": node_memory,
        "node_occupancy_compute_millis": node_compute,
        "reserved_bytes": reserved_memory,
        "reserved_compute_millis": reserved_compute,
        "memory_used_bytes": node_memory + reserved_memory,
        "compute_used_millis": node_compute + reserved_compute,
        "memory_remaining_bytes": int(group["memory_limit_bytes"])
        - node_memory
        - reserved_memory,
        "compute_remaining_millis": int(group["compute_limit_millis"])
        - node_compute
        - reserved_compute,
    }


def group_usage_map(conn: sqlite3.Connection) -> dict[str, dict[str, int]]:
    rows = conn.execute("SELECT group_id FROM quota_groups ORDER BY group_id").fetchall()
    return {str(row["group_id"]): group_usage(conn, str(row["group_id"])) for row in rows}


def node_usage(conn: sqlite3.Connection, node_id: str) -> dict[str, Any]:
    node = conn.execute(
        "SELECT node_id, total_memory_bytes, available_compute_millis, "
        "occupied_memory_bytes, occupied_compute_millis, quota_group, healthy "
        "FROM nodes WHERE node_id = ?",
        (node_id,),
    ).fetchone()
    if node is None:
        raise SchedulerError(
            "E_NODE_UNKNOWN",
            f"node {node_id!r} does not exist",
            details={"node_id": node_id},
        )
    allocated = conn.execute(
        "SELECT COALESCE(SUM(memory_bytes), 0) AS memory_bytes, "
        "COALESCE(SUM(compute_millis), 0) AS compute_millis "
        "FROM node_allocations WHERE node_id = ? AND active = 1",
        (node_id,),
    ).fetchone()
    allocated_memory = int(allocated["memory_bytes"])
    allocated_compute = int(allocated["compute_millis"])
    return {
        "node_id": str(node["node_id"]),
        "quota_group": str(node["quota_group"]),
        "healthy": bool(node["healthy"]),
        "total_memory_bytes": int(node["total_memory_bytes"]),
        "available_compute_millis": int(node["available_compute_millis"]),
        "occupied_memory_bytes": int(node["occupied_memory_bytes"]),
        "occupied_compute_millis": int(node["occupied_compute_millis"]),
        "allocated_bytes": allocated_memory,
        "allocated_compute_millis": allocated_compute,
        "used_memory_bytes": int(node["occupied_memory_bytes"]) + allocated_memory,
        "used_compute_millis": int(node["occupied_compute_millis"])
        + allocated_compute,
        "remaining_memory_bytes": int(node["total_memory_bytes"])
        - int(node["occupied_memory_bytes"])
        - allocated_memory,
        "remaining_compute_millis": int(node["available_compute_millis"])
        - int(node["occupied_compute_millis"])
        - allocated_compute,
    }


def node_usage_map(conn: sqlite3.Connection) -> dict[str, dict[str, Any]]:
    rows = conn.execute("SELECT node_id FROM nodes ORDER BY node_id").fetchall()
    return {str(row["node_id"]): node_usage(conn, str(row["node_id"])) for row in rows}


def find_inconsistencies(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []

    def issue(code: str, message: str, **details: Any) -> None:
        issues.append({"code": code, "message": message, "details": details})

    for group in conn.execute(
        "SELECT group_id FROM quota_groups ORDER BY group_id"
    ).fetchall():
        usage = group_usage(conn, str(group["group_id"]))
        if usage["memory_used_bytes"] > usage["memory_limit_bytes"]:
            issue(
                "GROUP_MEMORY_OVER_LIMIT",
                "group memory usage exceeds its limit",
                **usage,
            )
        if usage["compute_used_millis"] > usage["compute_limit_millis"]:
            issue(
                "GROUP_COMPUTE_OVER_LIMIT",
                "group compute usage exceeds its limit",
                **usage,
            )

    for node in conn.execute("SELECT node_id FROM nodes ORDER BY node_id").fetchall():
        usage = node_usage(conn, str(node["node_id"]))
        if usage["used_memory_bytes"] > usage["total_memory_bytes"]:
            issue("NODE_MEMORY_OVER_CAPACITY", "node memory is overallocated", **usage)
        if usage["used_compute_millis"] > usage["available_compute_millis"]:
            issue(
                "NODE_COMPUTE_OVER_CAPACITY",
                "node compute is overallocated",
                **usage,
            )

    active_status_placeholders = ",".join("?" for _ in ACTIVE_RESERVATION_STATES)
    bad_reservations = conn.execute(
        "SELECT r.request_id, r.active, q.status FROM reservations r "
        "JOIN requests q ON q.request_id = r.request_id "
        f"WHERE (r.active = 1 AND q.status NOT IN ({active_status_placeholders})) "
        f"OR (r.active = 0 AND q.status IN ({active_status_placeholders}))",
        (*sorted(ACTIVE_RESERVATION_STATES), *sorted(ACTIVE_RESERVATION_STATES)),
    ).fetchall()
    for row in bad_reservations:
        issue(
            "REQUEST_RESERVATION_STATE_MISMATCH",
            "active reservation and request status disagree",
            request_id=row["request_id"],
            reservation_active=bool(row["active"]),
            request_status=row["status"],
        )

    active_requests_without_reservation = conn.execute(
        "SELECT q.request_id, q.status FROM requests q "
        "LEFT JOIN reservations r ON r.request_id = q.request_id "
        f"WHERE q.status IN ({active_status_placeholders}) AND "
        "(r.request_id IS NULL OR r.active != 1)",
        tuple(sorted(ACTIVE_RESERVATION_STATES)),
    ).fetchall()
    for row in active_requests_without_reservation:
        issue(
            "ACTIVE_REQUEST_WITHOUT_RESERVATION",
            "active request has no active reservation",
            request_id=row["request_id"],
            request_status=row["status"],
        )

    bad_allocations = conn.execute(
        "SELECT a.allocation_id, a.batch_id, b.status FROM node_allocations a "
        "JOIN batches b ON b.batch_id = a.batch_id "
        "WHERE a.active = 1 AND b.status NOT IN ('PLANNED', 'QUEUED', 'RUNNING')"
    ).fetchall()
    for row in bad_allocations:
        issue(
            "TERMINAL_BATCH_WITH_ACTIVE_ALLOCATION",
            "terminal batch still owns node allocations",
            allocation_id=row["allocation_id"],
            batch_id=row["batch_id"],
            batch_status=row["status"],
        )

    allocation_mismatches = conn.execute(
        "SELECT b.batch_id, b.node_id AS batch_node, b.memory_bytes AS batch_memory, "
        "b.compute_millis AS batch_compute, "
        "COALESCE(SUM(CASE WHEN a.active = 1 THEN a.memory_bytes ELSE 0 END), 0) "
        "AS active_memory, "
        "COALESCE(SUM(CASE WHEN a.active = 1 THEN a.compute_millis ELSE 0 END), 0) "
        "AS active_compute, "
        "COUNT(DISTINCT CASE WHEN a.active = 1 THEN a.node_id END) AS active_node_count, "
        "MIN(CASE WHEN a.active = 1 THEN a.node_id END) AS active_node "
        "FROM batches b LEFT JOIN node_allocations a ON a.batch_id = b.batch_id "
        "WHERE b.status IN ('PLANNED', 'QUEUED', 'RUNNING') GROUP BY b.batch_id "
        "HAVING active_memory != b.memory_bytes OR active_compute != b.compute_millis "
        "OR active_node_count != 1 OR active_node != b.node_id"
    ).fetchall()
    for row in allocation_mismatches:
        issue(
            "BATCH_ALLOCATION_MISMATCH",
            "active node allocations do not match their active batch",
            batch_id=row["batch_id"],
            batch_node=row["batch_node"],
            active_node=row["active_node"],
            batch_memory_bytes=row["batch_memory"],
            active_memory_bytes=row["active_memory"],
            batch_compute_millis=row["batch_compute"],
            active_compute_millis=row["active_compute"],
        )

    reservation_mismatches = conn.execute(
        "SELECT r.request_id, r.active, r.memory_bytes AS reserved_memory, "
        "r.compute_millis AS reserved_compute, r.quota_group AS reserved_group, "
        "q.memory_bytes AS request_memory, q.compute_millis AS request_compute, "
        "q.quota_group AS request_group "
        "FROM reservations r JOIN requests q ON q.request_id = r.request_id "
        "WHERE r.active = 1 AND (r.memory_bytes != q.memory_bytes "
        "OR r.compute_millis != q.compute_millis "
        "OR r.quota_group != q.quota_group)"
    ).fetchall()
    for row in reservation_mismatches:
        issue(
            "RESERVATION_REQUEST_MISMATCH",
            "active reservation differs from its request's normalized demand",
            request_id=row["request_id"],
            reserved_memory_bytes=row["reserved_memory"],
            request_memory_bytes=row["request_memory"],
            reserved_compute_millis=row["reserved_compute"],
            request_compute_millis=row["request_compute"],
            reserved_group=row["reserved_group"],
            request_group=row["request_group"],
        )

    node_group_mismatches = conn.execute(
        "SELECT q.request_id, q.node_id, q.quota_group AS request_group, "
        "n.quota_group AS node_group FROM requests q JOIN nodes n "
        "ON n.node_id = q.node_id WHERE q.node_id IS NOT NULL "
        "AND q.quota_group != n.quota_group"
    ).fetchall()
    for row in node_group_mismatches:
        issue(
            "REQUEST_NODE_GROUP_MISMATCH",
            "request quota group differs from its selected node",
            request_id=row["request_id"],
            node_id=row["node_id"],
            request_group=row["request_group"],
            node_group=row["node_group"],
        )

    bad_node_groups = conn.execute(
        "SELECT b.batch_id, b.quota_group AS batch_group, n.quota_group AS node_group "
        "FROM batches b JOIN nodes n ON n.node_id = b.node_id "
        "WHERE b.quota_group != n.quota_group"
    ).fetchall()
    for row in bad_node_groups:
        issue(
            "BATCH_NODE_GROUP_MISMATCH",
            "batch quota group does not match node quota group",
            batch_id=row["batch_id"],
            batch_group=row["batch_group"],
            node_group=row["node_group"],
        )

    batches = conn.execute(
        "WITH member_agg AS ("
        "  SELECT batch_id, COUNT(*) AS member_count, "
        "         COALESCE(SUM(samples), 0) AS member_samples, "
        "         COALESCE(SUM(memory_bytes), 0) AS member_memory, "
        "         COALESCE(SUM(compute_millis), 0) AS member_compute "
        "  FROM batch_members GROUP BY batch_id"
        "), allocation_agg AS ("
        "  SELECT batch_id, "
        "         COALESCE(SUM(CASE WHEN active = 1 THEN memory_bytes ELSE 0 END), 0) "
        "           AS active_allocation_memory, "
        "         COALESCE(SUM(CASE WHEN active = 1 THEN compute_millis ELSE 0 END), 0) "
        "           AS active_allocation_compute "
        "  FROM node_allocations GROUP BY batch_id"
        ") "
        "SELECT b.batch_id, b.status, b.total_samples, b.memory_bytes, b.compute_millis, "
        "b.estimated_latency_millis, b.node_id, m.max_batch_samples, "
        "m.unit_sample_cost_millis, "
        "COALESCE(ma.member_count, 0) AS member_count, "
        "COALESCE(ma.member_samples, 0) AS member_samples, "
        "COALESCE(ma.member_memory, 0) AS member_memory, "
        "COALESCE(ma.member_compute, 0) AS member_compute, "
        "COALESCE(aa.active_allocation_memory, 0) AS active_allocation_memory, "
        "COALESCE(aa.active_allocation_compute, 0) AS active_allocation_compute "
        "FROM batches b JOIN models m ON m.model_id = b.model_id "
        "LEFT JOIN member_agg ma ON ma.batch_id = b.batch_id "
        "LEFT JOIN allocation_agg aa ON aa.batch_id = b.batch_id"
    ).fetchall()
    for row in batches:
        batch_id = row["batch_id"]
        if int(row["member_count"]) == 0:
            issue(
                "EMPTY_BATCH",
                "batch has no members",
                batch_id=batch_id,
            )
        if int(row["member_samples"]) != int(row["total_samples"]):
            issue(
                "BATCH_SAMPLE_TOTAL_MISMATCH",
                "batch member samples do not equal batch total",
                batch_id=batch_id,
                member_samples=row["member_samples"],
                total_samples=row["total_samples"],
            )
        if int(row["member_memory"]) != int(row["memory_bytes"]):
            issue(
                "BATCH_MEMORY_TOTAL_MISMATCH",
                "batch member memory does not equal batch total",
                batch_id=batch_id,
            )
        if int(row["member_compute"]) != int(row["compute_millis"]):
            issue(
                "BATCH_COMPUTE_TOTAL_MISMATCH",
                "batch member compute does not equal batch total",
                batch_id=batch_id,
            )
        if int(row["total_samples"]) > int(row["max_batch_samples"]):
            issue(
                "BATCH_MODEL_LIMIT_EXCEEDED",
                "batch exceeds model maximum samples",
                batch_id=batch_id,
                total_samples=row["total_samples"],
                max_batch_samples=row["max_batch_samples"],
            )
        if int(row["estimated_latency_millis"]) != (
            int(row["total_samples"]) * int(row["unit_sample_cost_millis"])
        ):
            issue(
                "BATCH_LATENCY_FORMULA_MISMATCH",
                "batch latency does not equal samples multiplied by unit cost",
                batch_id=batch_id,
            )
        if int(row["active_allocation_memory"]) != int(row["memory_bytes"]) and (
            row["status"] not in {"COMPLETED", "PREEMPTED", "ABORTED"}
        ):
            issue(
                "ACTIVE_BATCH_ALLOCATION_MISMATCH",
                "active batch's node allocation does not match batch memory",
                batch_id=batch_id,
            )

    request_overallocations = conn.execute(
        "SELECT r.request_id, r.memory_bytes, r.compute_millis, "
        "COALESCE(SUM(a.memory_bytes), 0) AS allocated_memory, "
        "COALESCE(SUM(a.compute_millis), 0) AS allocated_compute "
        "FROM reservations r LEFT JOIN node_allocations a "
        "ON a.request_id = r.request_id AND a.active = 1 "
        "WHERE r.active = 1 GROUP BY r.request_id "
        "HAVING allocated_memory > r.memory_bytes OR allocated_compute > r.compute_millis"
    ).fetchall()
    for row in request_overallocations:
        issue(
            "REQUEST_NODE_OVERALLOCATION",
            "request node allocations exceed its reservation",
            request_id=row["request_id"],
        )

    active_without_batches = conn.execute(
        "SELECT q.request_id, q.status FROM requests q "
        "WHERE q.status IN ('BATCHED', 'RUNNING') AND NOT EXISTS ("
        "  SELECT 1 FROM batch_members bm JOIN batches b ON b.batch_id = bm.batch_id "
        "  WHERE bm.request_id = q.request_id "
        "  AND b.status IN ('PLANNED', 'QUEUED', 'RUNNING')"
        ")"
    ).fetchall()
    for row in active_without_batches:
        issue(
            "BATCHED_REQUEST_WITHOUT_ACTIVE_BATCH",
            "BATCHED/RUNNING request has no non-terminal batch",
            request_id=row["request_id"],
            request_status=row["status"],
        )

    return issues


def assert_consistent(conn: sqlite3.Connection, *, context: str) -> None:
    issues = find_inconsistencies(conn)
    if issues:
        raise SchedulerError(
            E_QUOTA_INCONSISTENT,
            "quota accounting is inconsistent",
            details={"context": context, "issues": issues},
        )

