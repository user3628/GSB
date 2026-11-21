from __future__ import annotations

import base64
import copy
import os
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from .codec import reconstruct_shard
from .domain import Layout, Trace
from .errors import BusinessError, SimulatedCrash, fail
from .health import evaluate_object_health
from .layout import layout_summary, parse_layout
from .planner import (
    PlanningResult,
    ReusableShard,
    TaskSpec,
    canonical_decision_digest,
    plan_read_repair,
)
from .store import Store, utc_now
from .trace import parse_trace


class CrashController:
    """One-shot fault injection for an actual process exit or a test exception."""

    def __init__(self, point: str | None = None, mode: str = "raise") -> None:
        self.point = point
        self.mode = mode
        self._lock = threading.Lock()
        self._fired = False

    def arm(self, point: str = "after_task_start", mode: str = "raise") -> None:
        with self._lock:
            self.point = point
            self.mode = mode
            self._fired = False

    def disarm(self) -> None:
        with self._lock:
            self.point = None
            self._fired = False

    def maybe_crash(self, point: str) -> None:
        with self._lock:
            if self._fired or self.point != point:
                return
            self._fired = True
            mode = self.mode
            self.point = None
        if mode == "exit":
            os._exit(42)
        raise SimulatedCrash(f"injected crash at {point}")


class RepairEngine:
    def __init__(self, store: Store, crash_controller: CrashController | None = None) -> None:
        self.store = store
        self.crash_controller = crash_controller or CrashController()
        self._write_lock = threading.RLock()

    def recover(self) -> dict[str, Any]:
        with self._write_lock:
            return self.store.recover()

    def load_layout(self, document: Any) -> dict[str, Any]:
        layout = parse_layout(document)
        with self._write_lock:
            created = self.store.save_layout(layout.layout_version, layout.document_hash, layout.raw_document)
        summary = layout_summary(layout)
        summary["created"] = created
        if not created:
            summary["idempotent_reuse"] = True
        return summary

    def get_layout(self, layout_version: str | None = None) -> Layout:
        document = (
            self.store.get_layout_document(layout_version)
            if layout_version
            else self.store.get_latest_layout_document()
        )
        if document is None:
            fail(
                "E_LAYOUT_NOT_FOUND",
                "布局版本不存在",
                status_code=404,
                details={"layout_version": layout_version},
            )
        return parse_layout(document)

    def list_layouts(self) -> list[dict[str, Any]]:
        rows = self.store.list_layouts()
        for row in rows:
            document = self.store.get_layout_document(row["layout_version"])
            if document is not None:
                summary = layout_summary(parse_layout(document))
                row["object_count"] = summary["object_count"]
                row["max_k_plus_m"] = summary["max_k_plus_m"]
        return rows

    def get_layout_summary(self, layout_version: str | None = None) -> dict[str, Any]:
        return layout_summary(self.get_layout(layout_version))

    def _reusable_lookup_factory(self, layout: Layout):
        def lookup(object_id: str, shard_index: int, expected_digest: str) -> ReusableShard | None:
            row = self.store.get_reusable_shard(
                layout.layout_version, object_id, shard_index, expected_digest
            )
            if not row:
                return None
            return ReusableShard(
                object_id=object_id,
                shard_index=shard_index,
                digest=row["digest"],
                task_id=row["task_id"],
                round_id=row["round_id"],
            )

        return lookup

    def submit_trace(self, document: Any) -> dict[str, Any]:
        if not isinstance(document, dict):
            fail("E_TRACE_INVALID", "轨迹文档必须是 JSON 对象")
        requested_version = document.get("layout_version")
        layout = self.get_layout(requested_version if isinstance(requested_version, str) else None)
        trace = parse_trace(document, layout)

        with self._write_lock:
            existing = self.store.get_existing_requests([request.request_id for request in trace.requests])
            conflicts = [
                request.request_id
                for request in trace.requests
                if request.request_id in existing
                and existing[request.request_id]["content_hash"] != request.content_hash
            ]
            if conflicts:
                fail(
                    "E_CONFLICTING_REQUEST",
                    "同一 request_id 已以不同内容裁决，拒绝覆盖",
                    status_code=409,
                    details={"request_ids": conflicts},
                )
            round_seq = self.store.next_round_sequence()
            round_id = f"rnd-{round_seq:06d}-{uuid.uuid4().hex[:8]}"
            trace_document = {
                "trace_id": trace.trace_id,
                "layout_version": trace.layout_version,
                "requests": [request.canonical() for request in trace.requests],
                "options": trace.options.to_dict(),
            }
            self.store.create_round(
                {
                    "round_id": round_id,
                    "round_seq": round_seq,
                    "trace_id": trace.trace_id,
                    "layout_version": trace.layout_version,
                    "trace_document": trace_document,
                    "trace_hash": trace.trace_hash,
                    "options": trace.options.to_dict(),
                }
            )
            try:
                return self._run_round(round_id=round_id, layout=layout, trace=trace, existing=existing)
            except SimulatedCrash:
                raise
            except BusinessError as exc:
                self.store.abort_round(round_id, f"{exc.code}: {exc.message}")
                raise
            except BaseException as exc:
                self.store.abort_round(round_id, f"INTERNAL: {type(exc).__name__}")
                raise

    def _run_round(
        self,
        *,
        round_id: str,
        layout: Layout,
        trace: Trace,
        existing: dict[str, dict[str, Any]],
    ) -> dict[str, Any]:
        new_requests = tuple(request for request in trace.requests if request.request_id not in existing)
        planning_trace = Trace(
            trace_id=trace.trace_id,
            layout_version=trace.layout_version,
            requests=new_requests,
            options=trace.options,
            trace_hash=trace.trace_hash,
        )
        planning = plan_read_repair(
            layout=layout,
            trace=planning_trace,
            reusable_lookup=self._reusable_lookup_factory(layout),
        )
        new_decisions = {decision["request_id"]: decision for decision in planning.decisions}
        assembled_decisions: list[dict[str, Any]] = []
        reused_ids: list[str] = []
        request_records: list[dict[str, Any]] = []
        for request in trace.requests:
            if request.request_id in existing:
                decision = copy.deepcopy(existing[request.request_id]["decision"])
                decision["seq"] = request.seq
                assembled_decisions.append(
                    {
                        "request_id": request.request_id,
                        "seq": request.seq,
                        "reused": True,
                        "reused_from_round_id": existing[request.request_id]["first_round_id"],
                        "decision": decision,
                    }
                )
                reused_ids.append(request.request_id)
            else:
                decision = new_decisions[request.request_id]
                assembled_decisions.append(
                    {
                        "request_id": request.request_id,
                        "seq": request.seq,
                        "reused": False,
                        "decision": decision,
                    }
                )
                request_records.append(
                    {
                        "request_id": request.request_id,
                        "content_hash": request.content_hash,
                        "decision": decision,
                    }
                )

        if planning.tasks:
            self.store.plan_round(round_id, [task.to_store() for task in planning.tasks])
        executed = self._execute_tasks(
            round_id=round_id,
            layout=layout,
            trace=planning_trace,
            tasks=planning.tasks,
        )
        self._finalize_ec_decisions(assembled_decisions, executed, layout)
        decisions_core = [item["decision"] for item in assembled_decisions]
        decisions_core.sort(key=lambda item: (item["seq"], item["request_id"]))
        decision_digest = canonical_decision_digest(layout.layout_version, decisions_core)
        budget = self._budget_report(
            config=trace.options.to_dict(),
            planning=planning,
            executed=executed,
            decisions=decisions_core,
        )
        summary = self._summary(decisions_core, budget)
        read_results = [
            {
                "request_id": decision["request_id"],
                "seq": decision["seq"],
                "outcome": decision["outcome"],
                **decision["read_result"],
            }
            for decision in decisions_core
        ]
        round_row = self.store.get_round_record(round_id, include_uncommitted=True)
        result = {
            "round_id": round_id,
            "round_seq": round_row["round_seq"],
            "trace_id": trace.trace_id,
            "trace_hash": trace.trace_hash,
            "layout_version": layout.layout_version,
            "layout_document_hash": layout.document_hash,
            "state": "COMMITTED",
            "options": trace.options.to_dict(),
            "summary": summary,
            "decisions": decisions_core,
            "read_results": read_results,
            "decision_digest": decision_digest,
            "reused_request_ids": reused_ids,
            "budget": budget,
        }
        self.store.commit_round(
            round_id=round_id,
            result=result,
            decisions=assembled_decisions,
            request_records=request_records,
            decision_digest=decision_digest,
        )
        return result

    def _execute_tasks(
        self,
        *,
        round_id: str,
        layout: Layout,
        trace: Trace,
        tasks: list[TaskSpec],
    ) -> dict[str, dict[str, Any]]:
        outcomes: dict[str, dict[str, Any]] = {}
        if not tasks:
            return outcomes
        health_by_object = {
            object_id: evaluate_object_health(layout, obj) for object_id, obj in layout.objects.items()
        }
        waves: dict[int, list[TaskSpec]] = {}
        for task in tasks:
            waves.setdefault(task.wave, []).append(task)
        for wave_number in sorted(waves):
            wave_tasks = sorted(
                waves[wave_number],
                key=lambda task: (task.estimated_start_us, -task.priority, task.arrival_ms, task.task_id),
            )
            for task in wave_tasks:
                self.store.mark_task_running(round_id, task.task_id)
                self.crash_controller.maybe_crash("after_task_start")

            def reconstruct(task: TaskSpec) -> tuple[TaskSpec, bytes | None, dict[str, Any] | None, dict[str, Any] | None]:
                obj = layout.objects[task.object_id]
                health = health_by_object[task.object_id]
                sources = {
                    shard.shard_index: shard.content
                    for shard in health.shards
                    if shard.readable and shard.content is not None
                }
                try:
                    content, metadata = reconstruct_shard(
                        layout=layout,
                        obj=obj,
                        target_shard=task.shard_index,
                        source_contents=sources,
                    )
                    return task, content, metadata, None
                except BusinessError as exc:
                    return task, None, None, exc.as_dict()["error"]
                except Exception as exc:  # pragma: no cover - defensive
                    return task, None, None, {
                        "code": "E_REBUILD_INTERNAL",
                        "message": "重建任务内部错误",
                        "details": {"type": type(exc).__name__, "task_id": task.task_id},
                    }

            with ThreadPoolExecutor(max_workers=min(len(wave_tasks), trace.options.rebuild_concurrency)) as executor:
                futures = [executor.submit(reconstruct, task) for task in wave_tasks]
            for task, future in zip(wave_tasks, futures):
                try:
                    _, content, metadata, error = future.result()
                except Exception as exc:  # pragma: no cover - defensive
                    content, metadata = None, None
                    error = {
                        "code": "E_REBUILD_INTERNAL",
                        "message": "重建执行失败",
                        "details": {"type": type(exc).__name__, "task_id": task.task_id},
                    }
                obj = layout.objects[task.object_id]
                if error is not None:
                    self.store.commit_task_failure(
                        round_id=round_id,
                        task=task.to_store(),
                        error=error,
                        budget_consumed=True,
                    )
                    outcomes[task.task_id] = {
                        "state": "FAILED",
                        "bytes": task.bytes,
                        "error": error,
                        "digest": None,
                        "task": task,
                    }
                else:
                    assert content is not None and metadata is not None
                    digest = metadata["actual_digest"]
                    self.store.commit_task_success(
                        round_id=round_id,
                        task=task.to_store(),
                        content_b64=base64.b64encode(content).decode("ascii"),
                        digest=digest,
                        layout_version=layout.layout_version,
                        expected_digest=obj.shard(task.shard_index).digest,
                        metadata=metadata,
                    )
                    outcomes[task.task_id] = {
                        "state": "COMMITTED",
                        "bytes": task.bytes,
                        "error": None,
                        "digest": digest,
                        "task": task,
                        "metadata": metadata,
                    }
                self.crash_controller.maybe_crash("after_task_commit")
        return outcomes

    def _finalize_ec_decisions(
        self,
        assembled_decisions: list[dict[str, Any]],
        executed: dict[str, dict[str, Any]],
        layout: Layout,
    ) -> None:
        for envelope in assembled_decisions:
            if envelope.get("reused"):
                continue
            decision = envelope["decision"]
            obj = layout.objects[decision["object_id"]]
            if decision.get("strategy") == "EC_REBUILD":
                missing = set(decision.get("rebuilt_shards", []))
                task_outcomes = [
                    outcome
                    for outcome in executed.values()
                    if outcome["task"].object_id == obj.object_id
                    and outcome["task"].shard_index in missing
                ]
                failed = [outcome for outcome in task_outcomes if outcome["state"] == "FAILED"]
                if failed:
                    decision["outcome"] = "FAILED"
                    decision["error"] = failed[0]["error"]
                    decision["basis"]["task_errors"] = [
                        {
                            "task_id": outcome["task"].task_id,
                            "shard_index": outcome["task"].shard_index,
                            "error": outcome["error"],
                        }
                        for outcome in failed
                    ]
                else:
                    decision["outcome"] = "SUCCESS"
                decision["successfully_rebuilt_shards"] = [
                    outcome["task"].shard_index
                    for outcome in task_outcomes
                    if outcome["state"] == "COMMITTED"
                ]
                decision["successfully_rebuilt_shards"].sort()
            self._attach_read_result(decision, obj)

    def _attach_read_result(self, decision: dict[str, Any], obj) -> None:
        """Produce a stable per-shard merge plan, without exposing shard bytes."""

        successful = set(decision.get("successfully_rebuilt_shards", []))
        reused = set(decision.get("reused_reconstructed_shards", []))
        health_by_index = {item["shard_index"]: item for item in decision["shard_health"]}
        served = []
        failed = []
        for shard_index in sorted(decision["requested_shards"]):
            health = health_by_index[shard_index]
            source = None
            if health["readable"]:
                primary_id = obj.shard(shard_index).replicas[0].replica_id
                source = "PRIMARY_READ" if health["selected_replica_id"] == primary_id else "REPLICA_FILL"
            elif shard_index in successful:
                source = "EC_REBUILD"
            elif shard_index in reused:
                source = "PERSISTED_REBUILD_REUSE"
            if source is not None:
                served.append(
                    {
                        "shard_index": shard_index,
                        "source": source,
                        "replica_id": health.get("selected_replica_id"),
                    }
                )
            else:
                failed.append(
                    {
                        "shard_index": shard_index,
                        "reason_codes": health.get("reason_codes", []),
                    }
                )
        decision["read_result"] = {
            "status": "COMPLETE" if decision["outcome"] == "SUCCESS" and not failed else "FAILED",
            "stable_order": sorted(decision["requested_shards"]),
            "served_shards": served,
            "failed_shards": failed,
        }

    def _budget_report(
        self,
        *,
        config: dict[str, int],
        planning: PlanningResult,
        executed: dict[str, dict[str, Any]],
        decisions: list[dict[str, Any]],
    ) -> dict[str, Any]:
        task_rows = []
        for task in sorted(
            planning.tasks,
            key=lambda item: (item.wave, item.estimated_start_us, -item.priority, item.arrival_ms, item.task_id),
        ):
            outcome = executed.get(task.task_id)
            task_rows.append(
                {
                    "task_id": task.task_id,
                    "request_id": task.request_id,
                    "request_ids": sorted(task.request_ids),
                    "object_id": task.object_id,
                    "shard_index": task.shard_index,
                    "bytes": task.bytes,
                    "bytes_charged": task.bytes,
                    "priority": task.priority,
                    "arrival_ms": task.arrival_ms,
                    "wave": task.wave,
                    "start_us": task.estimated_start_us,
                    "end_us": task.estimated_end_us,
                    "duration_us": task.duration_us,
                    "state": outcome["state"] if outcome else "UNKNOWN",
                    "digest": outcome["digest"] if outcome else None,
                    "error": outcome["error"] if outcome else None,
                }
            )
        consumed = sum(row["bytes_charged"] for row in task_rows)
        shed_requests = [
            {
                "request_id": decision["request_id"],
                "code": decision["error"]["code"],
                "remaining_bytes_at_decision": decision["basis"].get("remaining_byte_budget_before"),
                "required_bytes": decision["basis"].get("rebuild_bytes"),
            }
            for decision in decisions
            if decision.get("error") and decision["error"]["code"] in {"E_REBUILD_SHED", "E_REBUILD_QUEUE_FULL"}
        ]
        byte_budget = int(config["round_byte_budget"])
        return {
            "byte_budget": byte_budget,
            "consumed_bytes": consumed,
            "remaining_bytes": max(byte_budget - consumed, 0),
            "byte_utilization_bps": (consumed * 10_000 // byte_budget) if byte_budget else 0,
            "time_budget_us": int(config["round_time_budget_us"]),
            "estimated_makespan_us": planning.estimated_makespan_us,
            "concurrency_limit": int(config["rebuild_concurrency"]),
            "queue_capacity": int(config["rebuild_queue_capacity"]),
            "task_count": len(task_rows),
            "committed_tasks": sum(1 for row in task_rows if row["state"] == "COMMITTED"),
            "failed_tasks": sum(1 for row in task_rows if row["state"] == "FAILED"),
            "shed_requests": shed_requests,
            "tasks": task_rows,
            "reservation_lifecycle": "RESERVED at round plan; RESERVED->COMMITTED atomically with shard result commit; uncommitted reservations released during recovery.",
        }

    def _summary(self, decisions: list[dict[str, Any]], budget: dict[str, Any]) -> dict[str, Any]:
        strategies: dict[str, int] = {}
        error_codes: dict[str, int] = {}
        for decision in decisions:
            strategy = decision.get("strategy") or "UNKNOWN"
            strategies[strategy] = strategies.get(strategy, 0) + 1
            error = decision.get("error")
            if error:
                error_codes[error["code"]] = error_codes.get(error["code"], 0) + 1
        return {
            "request_count": len(decisions),
            "success_count": sum(1 for decision in decisions if decision.get("outcome") == "SUCCESS"),
            "failed_count": sum(1 for decision in decisions if decision.get("outcome") == "FAILED"),
            "pending_count": sum(1 for decision in decisions if decision.get("outcome") == "PENDING"),
            "strategy_counts": dict(sorted(strategies.items())),
            "error_counts": dict(sorted(error_codes.items())),
            "consumed_bytes": budget["consumed_bytes"],
            "estimated_makespan_us": budget["estimated_makespan_us"],
        }

    def get_round(self, round_id: str) -> dict[str, Any]:
        record = self.store.get_round_record(round_id)
        if not record:
            uncommitted = self.store.get_round_record(round_id, include_uncommitted=True)
            if uncommitted:
                fail(
                    "E_ROUND_NOT_COMMITTED",
                    "轮次未提交，不能读取中间状态",
                    status_code=409,
                    details={"round_id": round_id, "state": uncommitted["state"]},
                )
            fail("E_ROUND_NOT_FOUND", "轮次不存在", status_code=404, details={"round_id": round_id})
        import json

        return json.loads(record["result_json"])

    def get_round_decisions(self, round_id: str) -> list[dict[str, Any]]:
        self.get_round(round_id)
        return self.store.get_round_decisions(round_id)

    def get_round_budget(self, round_id: str) -> dict[str, Any]:
        result = self.get_round(round_id)
        return {
            "round_id": round_id,
            "committed_budget_summary": result["budget"],
            "persisted_ledger_facts": self.store.get_budget_facts(round_id),
        }

    def list_rounds(self, limit: int = 50) -> list[dict[str, Any]]:
        return self.store.list_rounds(limit)

    def latest_recovery_report(self) -> dict[str, Any] | None:
        return self.store.get_latest_recovery_report()

    def recovery_report(self, report_id: str) -> dict[str, Any] | None:
        return self.store.get_recovery_report(report_id)

    def arm_crash(self, point: str = "after_task_start", mode: str = "exit") -> dict[str, Any]:
        if point not in {"after_task_start", "after_task_commit"}:
            fail("E_REQUEST_INVALID", "crash point 非法", status_code=422, details={"point": point})
        if mode not in {"exit", "raise"}:
            fail("E_REQUEST_INVALID", "crash mode 非法", status_code=422, details={"mode": mode})
        self.crash_controller.arm(point=point, mode=mode)
        return {"armed": True, "point": point, "mode": mode, "boundary": "127.0.0.1"}

    def replay_signature(self, round_id: str) -> dict[str, Any]:
        result = self.get_round(round_id)
        return {
            "round_id": round_id,
            "decision_digest": result["decision_digest"],
            "decisions": result["decisions"],
        }

    def health(self) -> dict[str, Any]:
        return {
            "status": "ok",
            "time": utc_now(),
            "db_path": self.store.db_path,
            "layout_count": len(self.store.list_layouts()),
            "latest_recovery_report_id": (self.latest_recovery_report() or {}).get("report_id"),
        }
