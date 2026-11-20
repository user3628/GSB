from __future__ import annotations

import hashlib
import uuid
from typing import Any

from app.digest import policy_checksum, trace_checksum
from app.engine import match_route, simulate
from app.errors import E_BATCH_INCOMPLETE, E_INVALID_ARGUMENT, E_ROUTE_AMBIGUOUS, BusinessError
from app.models import BatchStatus, PolicySnapshot, TraceSample
from app.storage import Storage


def _order_id(batch_id: str, route_id: str, difference_type: str) -> str:
    seed = f"{batch_id}|{route_id}|{difference_type}".encode("utf-8")
    return "ord_" + hashlib.sha256(seed).hexdigest()[:20]


class WorkbenchService:
    def __init__(self, storage: Storage) -> None:
        self.storage = storage

    def submit_snapshot(self, snapshot: PolicySnapshot) -> dict[str, Any]:
        expected_checksum = policy_checksum(snapshot)
        return self.storage.commit_snapshot(snapshot, expected_checksum)

    def current_snapshot(self) -> dict[str, Any]:
        snapshot = self.storage.get_current_snapshot()
        return {
            "version": snapshot.version,
            "generated_at": snapshot.generated_at,
            "checksum": snapshot.checksum,
            "snapshot": snapshot.model_dump(mode="json"),
        }

    def get_snapshot(self, version: int) -> dict[str, Any]:
        snapshot = self.storage.get_snapshot(version)
        return {
            "version": snapshot.version,
            "generated_at": snapshot.generated_at,
            "checksum": snapshot.checksum,
            "snapshot": snapshot.model_dump(mode="json"),
        }

    def prepare_batch(self, snapshot_version: int, trace: TraceSample) -> dict[str, Any]:
        snapshot = self.storage.get_snapshot(snapshot_version)
        batch_id = "batch_" + uuid.uuid4().hex
        return self.storage.create_batch(
            batch_id=batch_id,
            snapshot=snapshot,
            trace=trace,
            trace_sha256=trace_checksum(trace),
        )

    def _assign_order_ids(self, batch_id: str, orders: list[dict[str, Any]]) -> None:
        for order in orders:
            order["order_id"] = _order_id(
                batch_id,
                order["route_id"],
                order["difference_type"],
            )

    def execute_prepared_batch(
        self,
        batch_id: str,
        *,
        stage_limit: int | None = None,
        finalize: bool = True,
    ) -> dict[str, Any]:
        context = self.storage.get_batch_context(batch_id)
        if context["status"] != BatchStatus.PENDING.value:
            raise BusinessError(
                E_INVALID_ARGUMENT,
                "only a PENDING batch can be executed",
                status_code=409,
                details={"batch_id": batch_id, "status": context["status"]},
            )
        self.storage.start_batch(batch_id)
        try:
            snapshot = self.storage.get_snapshot(int(context["snapshot_version"]))
            simulation = simulate(snapshot, context["trace"])
            decisions = simulation["decisions"]
            if stage_limit is not None:
                if stage_limit <= 0:
                    raise BusinessError(
                        E_INVALID_ARGUMENT,
                        "stage_limit must be positive",
                        details={"stage_limit": stage_limit},
                    )
                decisions = decisions[:stage_limit]
            staged_count = self.storage.stage_decisions(batch_id, decisions)
            if not finalize:
                return {
                    "batch": self.storage.get_batch_summary(batch_id),
                    "staged_count": staged_count,
                    "simulation": simulation,
                }
            self._assign_order_ids(batch_id, simulation["orders"])
            summary = self.storage.finalize_batch(
                batch_id,
                orders=simulation["orders"],
                stats=simulation["stats"],
                conflicts=simulation["conflicts"],
            )
            return {
                "batch": summary,
                "decisions": self.storage.get_decisions(batch_id),
                "orders": self.storage.get_orders(batch_id),
            }
        except BaseException:
            try:
                current = self.storage.get_batch_summary(batch_id)
                if current["status"] in (BatchStatus.PENDING.value, BatchStatus.RUNNING.value):
                    self.storage.transition_batch(batch_id, BatchStatus.ABORTED)
            except Exception:
                pass
            raise

    def run_batch(self, snapshot_version: int, trace: TraceSample) -> dict[str, Any]:
        prepared = self.prepare_batch(snapshot_version, trace)
        return self.execute_prepared_batch(prepared["batch_id"])

    def get_batch(self, batch_id: str) -> dict[str, Any]:
        summary = self.storage.get_batch_summary(batch_id)
        result = {"batch": summary}
        if summary["status"] == BatchStatus.COMPLETED.value:
            result["decisions"] = self.storage.get_decisions(batch_id)
            result["orders"] = self.storage.get_orders(batch_id)
        return result

    def get_decisions(self, batch_id: str) -> list[dict[str, Any]]:
        return self.storage.get_decisions(batch_id)

    def get_orders(self, batch_id: str) -> list[dict[str, Any]]:
        return self.storage.get_orders(batch_id)

    def get_order(self, order_id: str) -> dict[str, Any]:
        return self.storage.get_order(order_id)

    def resolve_route(
        self,
        snapshot_version: int,
        path: str,
        headers: dict[str, str],
        *,
        strict: bool = False,
    ) -> dict[str, Any]:
        snapshot = self.storage.get_snapshot(snapshot_version)
        resolution = match_route(snapshot, path, headers)
        if strict and resolution["tied_at_top_precedence"]:
            raise BusinessError(
                E_ROUTE_AMBIGUOUS,
                "multiple routes tie at top precedence in strict resolution mode",
                status_code=409,
                details={
                    "snapshot_version": snapshot_version,
                    "path": path,
                    "strategy": resolution["strategy"],
                    "candidates": resolution["candidates"],
                },
            )
        return {"snapshot_version": snapshot_version, **resolution}

    def compare_batches(self, left_batch_id: str, right_batch_id: str) -> dict[str, Any]:
        left_summary = self.storage.get_batch_summary(left_batch_id)
        right_summary = self.storage.get_batch_summary(right_batch_id)
        if (
            left_summary["status"] != BatchStatus.COMPLETED.value
            or right_summary["status"] != BatchStatus.COMPLETED.value
        ):
            raise BusinessError(
                E_BATCH_INCOMPLETE,
                "both batches must be COMPLETED before comparison",
                status_code=409,
                details={
                    "left": {"batch_id": left_batch_id, "status": left_summary["status"]},
                    "right": {"batch_id": right_batch_id, "status": right_summary["status"]},
                },
            )
        if left_summary["trace_checksum"] != right_summary["trace_checksum"]:
            raise BusinessError(
                E_INVALID_ARGUMENT,
                "batches must reference the identical trace checksum",
                details={
                    "left_trace_checksum": left_summary["trace_checksum"],
                    "right_trace_checksum": right_summary["trace_checksum"],
                },
            )

        left_decisions = self.storage.get_decisions(left_batch_id)
        right_decisions = self.storage.get_decisions(right_batch_id)
        left_by_request = {item["request_id"]: item for item in left_decisions}
        right_by_request = {item["request_id"]: item for item in right_decisions}
        compared_fields = (
            "route_id",
            "matched_rule_ids",
            "decision_type",
            "trigger_parameters",
            "response_status",
            "response_latency_ms",
            "parameter_conflicts",
        )
        decision_changes = []
        for request_id in sorted(set(left_by_request) | set(right_by_request)):
            left = left_by_request.get(request_id)
            right = right_by_request.get(request_id)
            if left is None or right is None:
                decision_changes.append(
                    {
                        "request_id": request_id,
                        "changed_fields": ["presence"],
                        "left_present": left is not None,
                        "right_present": right is not None,
                    }
                )
                continue
            changed_fields = [field for field in compared_fields if left.get(field) != right.get(field)]
            if changed_fields:
                decision_changes.append(
                    {
                        "request_id": request_id,
                        "changed_fields": changed_fields,
                        "left": {field: left.get(field) for field in changed_fields},
                        "right": {field: right.get(field) for field in changed_fields},
                    }
                )

        left_conflicts = {item["route_id"]: item for item in (left_summary["conflicts"] or [])}
        right_conflicts = {item["route_id"]: item for item in (right_summary["conflicts"] or [])}
        route_conflicts = []
        for route_id in sorted(set(left_conflicts) | set(right_conflicts)):
            left_conflict = left_conflicts.get(route_id)
            right_conflict = right_conflicts.get(route_id)
            if left_conflict is None:
                status = "ADDED"
            elif right_conflict is None:
                status = "REMOVED"
            elif left_conflict == right_conflict:
                status = "UNCHANGED"
            else:
                status = "CHANGED"
            route_conflicts.append(
                {
                    "route_id": route_id,
                    "status": status,
                    "left": left_conflict,
                    "right": right_conflict,
                }
            )

        return {
            "left_batch_id": left_batch_id,
            "right_batch_id": right_batch_id,
            "trace_checksum": left_summary["trace_checksum"],
            "left_snapshot_version": left_summary["snapshot_version"],
            "right_snapshot_version": right_summary["snapshot_version"],
            "decision_change_count": len(decision_changes),
            "unchanged_decision_count": len(left_decisions) - len(decision_changes),
            "decision_changes": decision_changes,
            "route_conflict_comparison": route_conflicts,
        }

    def recover_incomplete_batches(self) -> dict[str, Any]:
        reports = self.storage.recover_incomplete_batches()
        return {"recovered_count": len(reports), "reports": reports}

    def list_recovery_reports(self) -> list[dict[str, Any]]:
        return self.storage.list_recovery_reports()

    def get_recovery_report_for_batch(self, batch_id: str) -> dict[str, Any]:
        return self.storage.get_recovery_report_for_batch(batch_id)
