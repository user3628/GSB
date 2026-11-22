"""应用服务层：提交、级联重建、诊断查询与中断恢复。

一致性边界（对应需求 5、7、8）：

1. 所有写路径在 ``storage.write_lock`` 下串行，等价于单写者；
2. 每个提交先在独立事务写入 PENDING 意图并 fsync 落盘，再在第二个
   ``BEGIN IMMEDIATE`` 事务内整体写入新版本的资源、依赖边与全部派生结构；
3. 派生结构在提交线程内一次性全量重算（级联失效 + 原子重建），
   读者只能看到旧完整版本或新完整版本，不可能读到混合版本；
4. 进程被 kill 后，未 commit 的第二阶段事务由 SQLite 自动回滚，
   重启时 :meth:`BenchService.recover` 扫描 PENDING 意图并标记 ABORTED，
   当前版本指针保持指向最后一个完整版本。
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from . import canonical
from .errors import ConflictingRequest, IntentNotFound, NoSnapshot, SpecInvalid, StaleBase
from .graph import compute_graph_view, filter_impact_with_budget
from .models import Increment, Resource, Snapshot
from .storage import INTENT_ABORTED, INTENT_COMMITTED, INTENT_PENDING, Storage, utc_now_iso
from .validation import validate_removals, validate_resources, validate_snapshot_envelope

RECOVERY_ABORT_CODE = "ABORTED_AFTER_CRASH"


class BenchService:
    def __init__(self, storage: Storage) -> None:
        self.storage = storage

    # ---------- 启动恢复 ----------

    def recover(self) -> dict[str, Any]:
        """扫描 PENDING 意图并恢复到最后一个完整版本。

        报告内容全部来自数据库已落盘事实，不推测崩溃前的内存状态。
        """

        pending = self.storage.scan_pending_intents()
        current_before = self.storage.get_current_version()
        available_versions = self._available_versions()
        aborted: list[dict[str, Any]] = []

        for intent in pending:
            target = intent["target_version"]
            intact = target is not None and target in available_versions
            if intact:
                # 理论上不会出现：意图 PENDING 而版本完整。保守按已提交补齐。
                with self.storage.write_lock:
                    connection = self.storage._connect()
                    try:
                        connection.execute("BEGIN IMMEDIATE")
                        self.storage.mark_intent(
                            connection, intent["intent_id"], INTENT_COMMITTED, target_version=target
                        )
                        connection.commit()
                    finally:
                        connection.close()
                aborted.append(
                    {"intent_id": intent["intent_id"], "action": "marked_committed", "target_version": target}
                )
                continue

            self.storage.abort_intent(intent["intent_id"], RECOVERY_ABORT_CODE)
            aborted.append(
                {
                    "intent_id": intent["intent_id"],
                    "kind": intent["kind"],
                    "base_version": intent["base_version"],
                    "action": "marked_aborted",
                    "reason": "intent remained PENDING while no complete target version was durable",
                }
            )

        current_after = self.storage.get_current_version()
        report = {
            "recovered_at": utc_now_iso(),
            "db_path": str(self.storage.db_path),
            "pending_intent_count": len(pending),
            "actions": aborted,
            "current_version_before": current_before,
            "current_version_after": current_after,
            "durable_versions": available_versions,
            "note": "报告仅包含 SQLite 中已落盘的事实；未提交事务已由 SQLite 回滚。",
        }
        self.storage.save_recovery_report(report)
        self._append_recovery_log(report)
        return report

    def _available_versions(self) -> list[int]:
        with self.storage.read_transaction() as connection:
            rows = connection.execute("SELECT version FROM snapshots ORDER BY version").fetchall()
            return [row["version"] for row in rows]

    def _append_recovery_log(self, report: dict[str, Any]) -> None:
        log_dir = Path(self.storage.db_path).parent.parent / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        with (log_dir / "recovery.log").open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(report, ensure_ascii=False, sort_keys=True) + "\n")

    # ---------- 整份快照提交 ----------

    def submit_snapshot(self, payload: Any, request_id: str) -> dict[str, Any]:
        try:
            snapshot = Snapshot.from_dict(payload)
        except TypeError as exc:
            raise SpecInvalid(str(exc), {"at": "snapshot"}) from exc
        validate_resources(list(snapshot.resources))

        request_hash = canonical.request_fingerprint("snapshot", payload)
        with self.storage.write_lock:
            reused = self._reuse_or_conflict(request_id, request_hash)
            if reused is not None:
                return reused

            current_version = self.storage.get_current_version()
            validate_snapshot_envelope(snapshot, current_version)
            resources = [item.to_dict() for item in snapshot.resources]
            view = compute_graph_view(list(snapshot.resources))

            self.storage.insert_pending_intent(
                intent_id=request_id,
                kind="snapshot",
                request_hash=request_hash,
                base_version=current_version,
                detail={"version": snapshot.version, "resource_count": len(resources)},
            )
            self.storage.commit_version(
                intent_id=request_id,
                version=snapshot.version,
                generated_at=snapshot.generated_at,
                checksum=snapshot.checksum,
                supplied_checksum=snapshot.supplied_checksum,
                resources=resources,
                view=view,
                kind="snapshot",
            )
            return self._commit_result(request_id, snapshot.version, view, resources, replaced=current_version)

    # ---------- 增量片段提交 ----------

    def submit_increment(self, payload: Any, request_id: str) -> dict[str, Any]:
        try:
            increment = Increment.from_dict(payload)
        except TypeError as exc:
            raise SpecInvalid(str(exc), {"at": "increment"}) from exc

        request_hash = canonical.request_fingerprint("increment", payload)
        with self.storage.write_lock:
            reused = self._reuse_or_conflict(request_id, request_hash)
            if reused is not None:
                return reused

            current_version = self.storage.get_current_version()
            if current_version is None:
                raise SpecInvalid("当前没有任何已提交快照，无法应用增量", {"at": "increment"})
            if increment.base_version != current_version:
                raise StaleBase(
                    "增量基础版本与当前版本不一致，状态未修改",
                    {"base_version": increment.base_version, "current_version": current_version},
                )

            current_rows = self.storage.load_snapshot_rows(current_version)
            assert current_rows is not None
            current_map = {item["id"]: Resource.from_dict(item) for item in current_rows["resources"]}
            validate_removals(increment.removals, current_map)

            new_map = dict(current_map)
            for resource in increment.upserts:
                new_map[resource.id] = resource
            for removed in increment.removals:
                new_map.pop(removed, None)

            retained_ids = set(new_map)
            validate_resources(list(new_map.values()), known_ids=retained_ids)

            new_version = current_version + 1
            generated_at = increment.generated_at or f"increment-derived-at-v{new_version}"
            ordered_resources = sorted(new_map.values(), key=lambda item: item.id)
            resource_dicts = [item.to_dict() for item in ordered_resources]
            checksum = canonical.snapshot_digest(new_version, generated_at, resource_dicts)
            view = compute_graph_view(ordered_resources)

            old_view = self.storage.load_view_rows(current_version)
            invalidated = self._cascade_set(increment.touched_ids(), old_view)

            self.storage.insert_pending_intent(
                intent_id=request_id,
                kind="increment",
                request_hash=request_hash,
                base_version=current_version,
                detail={
                    "touched": sorted(increment.touched_ids()),
                    "upsert_count": len(increment.upserts),
                    "removal_count": len(increment.removals),
                },
            )
            self.storage.commit_version(
                intent_id=request_id,
                version=new_version,
                generated_at=generated_at,
                checksum=checksum,
                supplied_checksum=None,
                resources=resource_dicts,
                view=view,
                kind="increment",
            )
            result = self._commit_result(request_id, new_version, view, resource_dicts, replaced=current_version)
            result["increment"] = {
                "base_version": current_version,
                "new_version": new_version,
                "touched_resources": sorted(increment.touched_ids()),
                "cascade_invalidated": sorted(invalidated),
            }
            return result

    def _cascade_set(self, touched: set[str], old_view: dict[str, Any] | None) -> set[str]:
        """级联失效集合：被改动资源本身 + 旧版本中其全部下游。"""

        invalidated = set(touched)
        if old_view is None:
            return invalidated
        for rid in touched:
            entry = old_view["resources"].get(rid)
            if entry:
                invalidated.update(entry["downstream"])
        return invalidated

    def _reuse_or_conflict(self, request_id: str, request_hash: str) -> dict[str, Any] | None:
        """幂等判定：同一 request_id 已落盘时复用或报冲突。"""

        existing = self.storage.get_intent(request_id)
        if existing is None:
            return None
        if existing["request_hash"] != request_hash:
            raise ConflictingRequest(
                "同一提交标识对应了不同的请求内容",
                {"intent_id": request_id, "stored_hash": existing["request_hash"], "incoming_hash": request_hash},
            )
        if existing["status"] == INTENT_COMMITTED:
            version = existing["target_version"]
            rows = self.storage.load_snapshot_rows(version)
            view = self.storage.load_view_rows(version)
            result = self._commit_result(request_id, version, view, rows["resources"], replaced=None)
            result["idempotent_replay"] = True
            return result
        if existing["status"] == INTENT_ABORTED:
            # 同内容重试：把旧意图重置为 PENDING 重新执行；不同内容上面已报冲突。
            self.storage.rearm_intent(request_id)
            return None
        return None

    def _commit_result(
        self,
        request_id: str,
        version: int,
        view: dict[str, Any],
        resources: list[dict[str, Any]],
        *,
        replaced: int | None,
    ) -> dict[str, Any]:
        return {
            "ok": True,
            "request_id": request_id,
            "snapshot_version": version,
            "replaced_version": replaced,
            "resource_count": len(resources),
            "is_acyclic": view["is_acyclic"],
            "cycles": view["cycles"],
            "blocked_resources": view["blocked_resources"],
        }

    # ---------- 诊断读路径 ----------

    def _require_version(self, version: int | None) -> int:
        current = self.storage.get_current_version()
        if current is None:
            raise NoSnapshot("尚无已提交快照")
        if version is None:
            return current
        if version > current or self.storage.load_snapshot_rows(version) is None:
            raise NoSnapshot(f"版本 {version} 不存在", {"requested_version": version, "current_version": current})
        return version

    def get_snapshot(self, version: int | None = None) -> dict[str, Any]:
        resolved = self._require_version(version)
        rows = self.storage.load_snapshot_rows(resolved)
        rows["resources"].sort(key=lambda item: item["id"])
        return {"snapshot_version": resolved, "snapshot": rows}

    def get_resource(self, resource_id: str, version: int | None = None) -> dict[str, Any]:
        resolved = self._require_version(version)
        view = self.storage.load_view_rows(resolved)
        if resource_id not in view["resources"]:
            raise NoSnapshot(f"资源 {resource_id} 在版本 {resolved} 不存在", {"resource_id": resource_id})
        rows = self.storage.load_snapshot_rows(resolved)
        spec = next((item for item in rows["resources"] if item["id"] == resource_id), None)
        return {"snapshot_version": resolved, "spec": spec, "derived": view["resources"][resource_id]}

    def get_layers(self, version: int | None = None) -> dict[str, Any]:
        resolved = self._require_version(version)
        view = self.storage.load_view_rows(resolved)
        return {
            "snapshot_version": resolved,
            "is_acyclic": view["is_acyclic"],
            "layers": view["layers"],
            "blocked_resources": view["blocked_resources"],
            "cycles": view["cycles"],
        }

    def get_batches(self, version: int | None = None, budget_ms: float | None = None) -> dict[str, Any]:
        resolved = self._require_version(version)
        view = self.storage.load_view_rows(resolved)
        deadline = None if budget_ms is None else time.monotonic() + budget_ms / 1000.0
        batches: list[dict[str, Any]] = []
        completed = True
        for batch in view["batches"]:
            if deadline is not None and time.monotonic() >= deadline:
                completed = False
                break
            batches.append(batch)
        remaining = view["batches"][len(batches):]
        return {
            "snapshot_version": resolved,
            "completed": completed,
            "batches": batches,
            "remaining_batches": remaining,
            "budget_ms": budget_ms,
            "is_acyclic": view["is_acyclic"],
            "cycles": view["cycles"],
            "blocked_resources": view["blocked_resources"],
        }

    def get_impact(
        self,
        resource_id: str,
        version: int | None = None,
        budget_ms: float | None = None,
        delay_per_item_ms: float = 0.0,
    ) -> dict[str, Any]:
        resolved = self._require_version(version)
        view = self.storage.load_view_rows(resolved)
        if resource_id not in view["resources"]:
            raise NoSnapshot(f"资源 {resource_id} 在版本 {resolved} 不存在", {"resource_id": resource_id})
        entry = view["resources"][resource_id]
        deadline = None if budget_ms is None else time.monotonic() + budget_ms / 1000.0
        hook = None
        if delay_per_item_ms > 0:
            pause = delay_per_item_ms / 1000.0

            def hook() -> None:
                time.sleep(pause)

        progress = filter_impact_with_budget(
            entry["downstream"], budget_ms=budget_ms, deadline=deadline, sleep_hook=hook
        )
        return {
            "snapshot_version": resolved,
            "resource_id": resource_id,
            "completed": progress["completed"],
            "downstream": progress["items"],
            "remaining": progress.get("remaining", []),
            "budget_ms": budget_ms,
            "status": entry["status"],
        }

    def get_intent(self, request_id: str) -> dict[str, Any]:
        intent = self.storage.get_intent(request_id)
        if intent is None:
            raise IntentNotFound(f"提交标识 {request_id} 不存在", {"intent_id": request_id})
        intent["detail"] = json.loads(intent.pop("detail_json"))
        return intent

    def list_intents(self) -> list[dict[str, Any]]:
        intents = self.storage.list_intents()
        for intent in intents:
            intent["detail"] = json.loads(intent.pop("detail_json"))
        return intents
