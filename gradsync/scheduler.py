from __future__ import annotations

import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlparse

import httpx
import numpy as np

from .aggregation import GradientBlock, aggregate_blocks, gradient_content_hash
from .domain import FinalizationPolicy
from .errors import ErrorCode, GradSyncError
from .store import SQLiteStore


def validate_loopback_url(url: str, *, field: str = "url") -> None:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        raise GradSyncError(
            ErrorCode.E_VALIDATION_ERROR,
            "仅允许 http/https 回环地址",
            details={field: url, "reason": "scheme"},
        )
    if parsed.hostname not in {"127.0.0.1", "localhost", "::1"}:
        raise GradSyncError(
            ErrorCode.E_VALIDATION_ERROR,
            "仅允许回环地址，拒绝外部网络目标",
            details={field: url, "reason": "non_loopback_host"},
        )


class GradientScheduler:
    def __init__(
        self,
        store: SQLiteStore,
        *,
        clock: Callable[[], float] = time.time,
        crash_hook: Callable[[str, dict[str, Any]], None] | None = None,
        allow_process_crash: bool = True,
    ) -> None:
        self.store = store
        self.clock = clock
        self.crash_hook = crash_hook
        self.allow_process_crash = allow_process_crash
        self._lock_guard = threading.Lock()
        self._round_locks: dict[str, threading.RLock] = {}

    def _round_lock(self, round_id: str) -> threading.RLock:
        with self._lock_guard:
            return self._round_locks.setdefault(round_id, threading.RLock())

    def create_round(self, spec: dict[str, Any]) -> dict[str, Any]:
        round_id = str(spec.get("round_id", "")).strip()
        if not round_id:
            raise GradSyncError(ErrorCode.E_VALIDATION_ERROR, "round_id 不能为空")
        workers = list(spec.get("worker_ids") or [])
        if not workers or len(set(workers)) != len(workers):
            raise GradSyncError(
                ErrorCode.E_VALIDATION_ERROR,
                "worker_ids 必须非空且不能重复",
                round_id=round_id,
            )
        if int(spec.get("shard_count", 0)) <= 0 or int(spec.get("shard_dim", 0)) <= 0:
            raise GradSyncError(
                ErrorCode.E_VALIDATION_ERROR,
                "shard_count 与 shard_dim 必须为正整数",
                round_id=round_id,
            )
        sample_counts = spec.get("sample_counts") or {}
        if set(sample_counts) != set(workers):
            raise GradSyncError(
                ErrorCode.E_VALIDATION_ERROR,
                "sample_counts 必须与 worker_ids 完全一致",
                round_id=round_id,
            )
        for node_id, count in sample_counts.items():
            if not isinstance(count, int) or count < 0:
                raise GradSyncError(
                    ErrorCode.E_VALIDATION_ERROR,
                    "样本数必须为非负整数",
                    round_id=round_id,
                    details={"node_id": node_id, "sample_count": count},
                )
        if float(spec.get("collect_seconds", 0)) <= 0:
            raise GradSyncError(
                ErrorCode.E_VALIDATION_ERROR,
                "collect_seconds 必须大于 0",
                round_id=round_id,
            )
        if float(spec.get("max_wait_seconds", -1)) < 0:
            raise GradSyncError(
                ErrorCode.E_VALIDATION_ERROR,
                "max_wait_seconds 不能为负数",
                round_id=round_id,
            )
        try:
            policy = FinalizationPolicy(spec.get("policy", FinalizationPolicy.STRICT.value))
        except ValueError as exc:
            raise GradSyncError(
                ErrorCode.E_VALIDATION_ERROR,
                "policy 必须为 STRICT 或 DEGRADED",
                round_id=round_id,
            ) from exc
        endpoints = dict(spec.get("worker_endpoints") or {})
        unknown_endpoint_nodes = set(endpoints) - set(workers)
        if unknown_endpoint_nodes:
            raise GradSyncError(
                ErrorCode.E_VALIDATION_ERROR,
                "worker_endpoints 包含未注册节点",
                round_id=round_id,
                details={"node_ids": sorted(unknown_endpoint_nodes)},
            )
        for node_id, endpoint in endpoints.items():
            validate_loopback_url(str(endpoint), field=f"worker_endpoints.{node_id}")
        normalized = {
            "round_id": round_id,
            "seed": int(spec.get("seed", 0)),
            "shard_count": int(spec["shard_count"]),
            "shard_dim": int(spec["shard_dim"]),
            "worker_ids": workers,
            "sample_counts": {node_id: int(sample_counts[node_id]) for node_id in workers},
            "worker_endpoints": {node_id: str(endpoint).rstrip("/") for node_id, endpoint in endpoints.items()},
            "policy": policy.value,
            "collect_seconds": float(spec["collect_seconds"]),
            "max_wait_seconds": float(spec["max_wait_seconds"]),
        }
        return self.store.create_round(normalized, now=self.clock())

    def submit(
        self,
        *,
        round_id: str,
        node_id: str,
        shard_id: int,
        sample_count: int,
        gradient: list[float],
    ) -> dict[str, Any]:
        with self._round_lock(round_id):
            round_data = self.store.require_round(round_id)
            if len(gradient) != int(round_data["shard_dim"]):
                raise GradSyncError(
                    ErrorCode.E_INVALID_SUBMISSION,
                    "梯度块维度不符合轮次元数据",
                    round_id=round_id,
                    details={
                        "node_id": node_id,
                        "shard_id": shard_id,
                        "expected_dim": int(round_data["shard_dim"]),
                        "actual_dim": len(gradient),
                    },
                )
            array = np.asarray(gradient, dtype="<f8", order="C")
            content_hash = gradient_content_hash(sample_count, array)
            result = self.store.record_submission(
                round_id=round_id,
                node_id=node_id,
                shard_id=int(shard_id),
                sample_count=int(sample_count),
                gradient_blob=array.tobytes(order="C"),
                content_hash=content_hash,
                received_at=self.clock(),
            )
            if result["outcome"] == "CONFLICT":
                raise GradSyncError(
                    ErrorCode.E_CONFLICTING_REPORT,
                    "同一节点、轮次、分片已被首次提交占用，冲突内容未写入",
                    round_id=round_id,
                    details={
                        "node_id": node_id,
                        "shard_id": shard_id,
                        "first_content_hash": result["first_content_hash"],
                        "incoming_content_hash": result["incoming_content_hash"],
                    },
                )
            return {"round_id": round_id, "node_id": node_id, "shard_id": shard_id, **result}

    def finalize(
        self,
        *,
        round_id: str,
        policy: FinalizationPolicy | str | None = None,
    ) -> dict[str, Any]:
        requested_policy = FinalizationPolicy(policy) if policy is not None else None
        with self._round_lock(round_id):
            self.store.require_round(round_id)
            existing = self.store.get_aggregate(round_id)
            if existing is not None:
                return self._serialize_result(round_id, existing)

            prepared = self.store.prepare_aggregation(
                round_id=round_id,
                requested_policy=requested_policy,
                now=self.clock(),
            )
            if prepared.get("cached"):
                existing = self.store.get_aggregate(round_id)
                if existing is None:
                    raise GradSyncError(
                        ErrorCode.E_INTERNAL,
                        "轮次状态与聚合结果不一致",
                        round_id=round_id,
                    )
                return self._serialize_result(round_id, existing)

            snapshot = prepared["snapshot"]
            intent_id = prepared["intent_id"]
            round_data = self.store.require_round(round_id)
            try:
                aggregate = self._aggregate_snapshot(round_data, snapshot)
            except GradSyncError as exc:
                self.store.fail_intent(
                    round_id=round_id,
                    intent_id=intent_id,
                    code=exc.code,
                    message=exc.message,
                    at=self.clock(),
                )
                raise

            tensor = aggregate.tensor
            tensor_blob = np.asarray(tensor, dtype="<f8", order="C").tobytes(order="C")
            self._maybe_crash(
                "before_aggregated_commit",
                {
                    "round_id": round_id,
                    "intent_id": intent_id,
                    "snapshot_hash": prepared["snapshot_hash"],
                    "digest_sha256": aggregate.digest,
                },
            )
            self.store.commit_aggregation(
                round_id=round_id,
                intent_id=intent_id,
                tensor_blob=tensor_blob,
                shard_count=int(round_data["shard_count"]),
                shard_dim=int(round_data["shard_dim"]),
                digest=aggregate.digest,
                element_sum=aggregate.element_sum,
                element_sum_text=aggregate.element_sum_text,
                missing_nodes=list(snapshot.get("missing_nodes", [])),
                missing_blocks=list(snapshot.get("missing_blocks", [])),
                degraded=bool(snapshot.get("missing_blocks")),
                policy=FinalizationPolicy(snapshot["mode"]),
                snapshot_hash=prepared["snapshot_hash"],
                computed_at=self.clock(),
            )
            result = self.store.get_aggregate(round_id)
            if result is None:
                raise GradSyncError(
                    ErrorCode.E_INTERNAL,
                    "聚合提交后未读取到结果",
                    round_id=round_id,
                )
            return self._serialize_result(round_id, result)

    def _aggregate_snapshot(
        self, round_data: dict[str, Any], snapshot: dict[str, Any]
    ) -> Any:
        blocks: list[GradientBlock] = []
        for item in snapshot.get("included", []):
            stored = self.store.get_submission(
                round_data["round_id"], item["node_id"], int(item["shard_id"])
            )
            if stored is None:
                raise GradSyncError(
                    ErrorCode.E_INTERNAL,
                    "快照引用的分片提交不存在",
                    round_id=round_data["round_id"],
                    details={"node_id": item["node_id"], "shard_id": item["shard_id"]},
                )
            if stored["content_hash"] != item["content_hash"]:
                raise GradSyncError(
                    ErrorCode.E_CONFLICTING_REPORT,
                    "快照中的分片内容摘要与持久化记录不一致",
                    round_id=round_data["round_id"],
                    details={"node_id": item["node_id"], "shard_id": item["shard_id"]},
                )
            array = np.frombuffer(stored["gradient_blob"], dtype="<f8").copy()
            blocks.append(
                GradientBlock(
                    node_id=item["node_id"],
                    shard_id=int(item["shard_id"]),
                    sample_count=int(item["sample_count"]),
                    gradient=array,
                    content_hash=item["content_hash"],
                )
            )
        return aggregate_blocks(
            round_id=round_data["round_id"],
            shard_count=int(round_data["shard_count"]),
            shard_dim=int(round_data["shard_dim"]),
            blocks=blocks,
        )

    def _serialize_result(self, round_id: str, result: dict[str, Any]) -> dict[str, Any]:
        round_data = self.store.require_round(round_id)
        degraded = bool(result["degraded"])
        return {
            "ok": True,
            "code": ErrorCode.E_DEGRADED_ROUND.value if degraded else ErrorCode.OK.value,
            "round_id": round_id,
            "state": round_data["status"],
            "degraded": degraded,
            "policy": result["policy"],
            "missing_nodes": result["missing_nodes"],
            "missing_blocks": result["missing_blocks"],
            "weights": round_data["sample_counts"],
            "shape": [int(result["shard_count"]), int(result["shard_dim"])],
            "digest_sha256": result["digest_sha256"],
            "element_sum": result["element_sum_text"],
            "tensor_preview": result["tensor_preview"],
            "tensor_float64_le_b64": result["tensor_b64"],
            "snapshot_hash": result["snapshot_hash"],
            "computed_at": float(result["computed_at"]),
        }

    def get_round_status(self, round_id: str) -> dict[str, Any]:
        round_data = self.store.require_round(round_id)
        submissions = self.store.list_submissions(round_id)
        result = self.store.get_aggregate(round_id)
        expected_blocks = len(round_data["worker_ids"]) * int(round_data["shard_count"])
        present = {(item["shard_id"], item["node_id"]) for item in submissions}
        missing_blocks = [
            {"shard_id": shard_id, "node_id": node_id}
            for shard_id in range(int(round_data["shard_count"]))
            for node_id in round_data["worker_ids"]
            if (shard_id, node_id) not in present
        ]
        late_blocks = [
            {"shard_id": item["shard_id"], "node_id": item["node_id"], "received_at": item["received_at"]}
            for item in submissions
            if item["late"]
        ]
        return {
            "round_id": round_id,
            "state": round_data["status"],
            "policy": round_data["policy"],
            "finalization_policy": round_data["finalization_policy"],
            "seed": round_data["seed"],
            "shard_count": round_data["shard_count"],
            "shard_dim": round_data["shard_dim"],
            "worker_ids": round_data["worker_ids"],
            "sample_counts": round_data["sample_counts"],
            "collect_deadline_at": round_data["collect_deadline_at"],
            "max_wait_deadline_at": round_data["max_wait_deadline_at"],
            "expected_blocks": expected_blocks,
            "received_blocks": len(submissions),
            "on_time_blocks": sum(1 for item in submissions if not item["late"]),
            "late_blocks": late_blocks,
            "missing_blocks": missing_blocks,
            "missing_nodes": sorted({item["node_id"] for item in missing_blocks}),
            "snapshot_hash": round_data["snapshot_hash"],
            "last_error_code": round_data["last_error_code"],
            "submissions": submissions,
            "result": self._serialize_result(round_id, result) if result is not None else None,
            "events": self.store.get_events(round_id),
        }

    def get_result(self, round_id: str) -> dict[str, Any]:
        result = self.store.get_aggregate(round_id)
        if result is None:
            raise GradSyncError(
                ErrorCode.E_RESULT_NOT_FOUND,
                "轮次尚无聚合结果",
                round_id=round_id,
            )
        return self._serialize_result(round_id, result)

    def abort(self, round_id: str) -> dict[str, Any]:
        with self._round_lock(round_id):
            return self.store.abort_round(round_id, now=self.clock())

    def get_events(self, round_id: str) -> list[dict[str, Any]]:
        return self.store.get_events(round_id)

    def _maybe_crash(self, point: str, facts: dict[str, Any]) -> None:
        if self.crash_hook is not None:
            self.crash_hook(point, facts)
        if not self.allow_process_crash:
            return
        if os.environ.get("GRADSYNC_CRASH_AT") != point:
            return
        marker = os.environ.get("GRADSYNC_CRASH_MARKER")
        if marker:
            path = Path(marker)
            path.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                "point": point,
                "at": self.clock(),
                "facts": facts,
                "note": "intent committed; AGGREGATED result not committed",
            }
            with path.open("w", encoding="utf-8", newline="\n") as handle:
                handle.write(json.dumps(payload, indent=2, sort_keys=True) + "\n")
                handle.flush()
                os.fsync(handle.fileno())
        os._exit(91)

    def recover_pending_intents(self) -> dict[str, Any]:
        started_at = self.clock()
        pending = self.store.get_pending_intents()
        entries: list[dict[str, Any]] = []
        seen_rounds: set[str] = set()
        for intent in pending:
            round_id = intent["round_id"]
            if round_id in seen_rounds:
                continue
            seen_rounds.add(round_id)
            with self._round_lock(round_id):
                latest = self.store.get_intent(intent["intent_id"])
                if latest is None or latest["status"] not in {"PENDING", "IN_PROGRESS"}:
                    continue
                round_before = self.store.require_round(round_id)
                entry: dict[str, Any] = {
                    "intent_id": intent["intent_id"],
                    "round_id": round_id,
                    "round_state_before": round_before["status"],
                    "intent_status_before": latest["status"],
                    "snapshot_hash": latest["snapshot_hash"],
                }
                try:
                    marker = self.store.mark_recovery_started(
                        round_id=round_id,
                        intent_id=intent["intent_id"],
                        at=self.clock(),
                    )
                    if marker.get("cancelled"):
                        entry.update({"outcome": "CANCELLED", "code": "ABORTED_ROUND"})
                        entries.append(entry)
                        continue
                    existing = self.store.get_aggregate(round_id)
                    if existing is not None:
                        self.store.mark_intent_committed(
                            intent_id=intent["intent_id"], at=self.clock()
                        )
                        entry.update(
                            {
                                "outcome": "ALREADY_COMMITTED",
                                "digest_sha256": existing["digest_sha256"],
                                "element_sum": existing["element_sum_text"],
                                "round_state_after": "AGGREGATED",
                            }
                        )
                        entries.append(entry)
                        continue
                    snapshot = latest["snapshot"]
                    aggregate = self._aggregate_snapshot(round_before, snapshot)
                    tensor_blob = np.asarray(
                        aggregate.tensor, dtype="<f8", order="C"
                    ).tobytes(order="C")
                    self.store.commit_aggregation(
                        round_id=round_id,
                        intent_id=intent["intent_id"],
                        tensor_blob=tensor_blob,
                        shard_count=int(round_before["shard_count"]),
                        shard_dim=int(round_before["shard_dim"]),
                        digest=aggregate.digest,
                        element_sum=aggregate.element_sum,
                        element_sum_text=aggregate.element_sum_text,
                        missing_nodes=list(snapshot.get("missing_nodes", [])),
                        missing_blocks=list(snapshot.get("missing_blocks", [])),
                        degraded=bool(snapshot.get("missing_blocks")),
                        policy=FinalizationPolicy(snapshot["mode"]),
                        snapshot_hash=latest["snapshot_hash"],
                        computed_at=self.clock(),
                        recovered=True,
                    )
                    entry.update(
                        {
                            "outcome": "RECOVERED",
                            "digest_sha256": aggregate.digest,
                            "element_sum": aggregate.element_sum_text,
                            "round_state_after": "AGGREGATED",
                        }
                    )
                except GradSyncError as exc:
                    self.store.fail_intent(
                        round_id=round_id,
                        intent_id=intent["intent_id"],
                        code=exc.code,
                        message=exc.message,
                        at=self.clock(),
                    )
                    entry.update(
                        {
                            "outcome": "NEEDS_RERUN",
                            "code": exc.code.value,
                            "message": exc.message,
                            "round_state_after": self.store.require_round(round_id)["status"],
                        }
                    )
                except Exception as exc:  # recovery must report, not hide the failure
                    self.store.fail_intent(
                        round_id=round_id,
                        intent_id=intent["intent_id"],
                        code=ErrorCode.E_INTERNAL,
                        message=f"{type(exc).__name__}: {exc}",
                        at=self.clock(),
                    )
                    entry.update(
                        {
                            "outcome": "NEEDS_RERUN",
                            "code": ErrorCode.E_INTERNAL.value,
                            "message": f"{type(exc).__name__}: {exc}",
                            "round_state_after": self.store.require_round(round_id)["status"],
                        }
                    )
                entries.append(entry)
        finished_at = self.clock()
        report = {
            "ok": True,
            "source": "startup_sqlite_scan",
            "report_scope": "仅包含 SQLite 启动扫描时仍存在的事实；不承诺崩溃进程的完整退出报告",
            "started_at": started_at,
            "finished_at": finished_at,
            "pending_intents_found": len(pending),
            "recovered_rounds": len(entries),
            "entries": entries,
        }
        self.store.record_recovery_report(report, started_at=started_at, finished_at=finished_at)
        return report

    def get_recovery_report(self) -> dict[str, Any]:
        latest = self.store.latest_recovery_report()
        if latest is None:
            return {
                "ok": True,
                "source": "startup_sqlite_scan",
                "report_scope": "尚无启动恢复扫描记录",
                "entries": [],
            }
        return latest["report"]

    def deliver(self, round_id: str) -> dict[str, Any]:
        round_data = self.store.require_round(round_id)
        result = self.get_result(round_id)
        endpoints = round_data["worker_endpoints"]
        if not endpoints:
            return {
                "ok": True,
                "round_id": round_id,
                "deliveries": [],
                "skipped": "轮次未注册工作节点结果端点",
            }

        attempts: list[dict[str, Any]] = []

        def send(node_id: str, endpoint: str) -> dict[str, Any]:
            target = endpoint.rstrip("/") + "/results"
            attempted_at = self.clock()
            try:
                response = httpx.post(target, json=result, timeout=3.0)
                status = "DELIVERED" if response.is_success else "FAILED"
                error = None if response.is_success else response.text[:500]
                http_status = response.status_code
            except Exception as exc:
                status = "FAILED"
                error = f"{type(exc).__name__}: {exc}"
                http_status = None
            self.store.record_delivery(
                round_id=round_id,
                node_id=node_id,
                endpoint=endpoint,
                status=status,
                http_status=http_status,
                error=error,
                attempted_at=attempted_at,
            )
            return {
                "node_id": node_id,
                "endpoint": endpoint,
                "status": status,
                "http_status": http_status,
                "error": error,
                "attempted_at": attempted_at,
            }

        with ThreadPoolExecutor(max_workers=min(8, len(endpoints))) as executor:
            futures = {
                executor.submit(send, node_id, endpoint): node_id
                for node_id, endpoint in endpoints.items()
            }
            for future in as_completed(futures):
                attempts.append(future.result())
        attempts.sort(key=lambda item: item["node_id"])
        return {
            "ok": all(item["status"] == "DELIVERED" for item in attempts),
            "round_id": round_id,
            "deliveries": attempts,
        }

    def health(self) -> dict[str, Any]:
        return {"ok": True, **self.store.health()}

