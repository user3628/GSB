"""分发协调器：意图先行、单写者串行、状态机驱动与失败收敛。"""

from __future__ import annotations

import threading
from typing import Any

from .errors import (
    DispatchError,
    SnapshotNotFoundError,
    StaleSnapshotError,
    InvalidSnapshotError,
)
from .models import Snapshot, compute_checksum, load_declarations
from .planner import build_plan
from .push_client import PushClient
from .repository import Repository
from .state_machine import APPLIED, DISPATCHED, FAILED, PENDING, ROLLED_BACK

TERMINAL_RUN_STATUSES = frozenset(
    {"COMPLETED", "PARTIAL", "ROLLED_BACK", "ABORTED"}
)
ACTIVE_RUN_STATUSES = frozenset({"PENDING", "RUNNING"})
POLICIES = frozenset({"continue", "rollback"})


class Coordinator:
    def __init__(self, repository: Repository, push_client: PushClient | None = None):
        self.repo = repository
        self.client = push_client or PushClient()
        # 全局单写者闸门：任意时刻只有一个分发执行，跨快照版本同样串行；
        # 新版本快照的分发因此自然排在当前分发之后。
        self._writer_gate = threading.RLock()
        self._conditions: dict[int, threading.Condition] = {}
        self._condition_lock = threading.Lock()

    def _condition_for(self, version: int) -> threading.Condition:
        with self._condition_lock:
            condition = self._conditions.get(version)
            if condition is None:
                condition = threading.Condition()
                self._conditions[version] = condition
            return condition

    # ------------------------------------------------------------ 快照提交
    def submit_snapshot(self, raw_snapshot: dict[str, Any]) -> Snapshot:
        try:
            snapshot = Snapshot.from_dict(raw_snapshot)
        except (ValueError, TypeError) as exc:
            raise InvalidSnapshotError(str(exc), details={"raw": raw_snapshot}) from exc
        if not snapshot.checksum_valid():
            raise InvalidSnapshotError(
                "校验摘要不匹配",
                details={
                    "version": snapshot.version,
                    "expected": snapshot.checksum,
                    "actual": compute_checksum(snapshot.config_items),
                },
            )
        latest = self.repo.latest_version()
        if latest is not None and snapshot.version <= latest:
            raise StaleSnapshotError(
                f"快照版本 {snapshot.version} 不大于当前版本 {latest}",
                details={"submitted": snapshot.version, "current": latest},
            )
        self.repo.insert_snapshot(snapshot)
        return snapshot

    def register_declarations(self, raw_declarations: list[dict[str, Any]]) -> dict:
        declarations = load_declarations(raw_declarations)
        self.repo.replace_declarations(declarations)
        return declarations

    # ------------------------------------------------------------ 计划生成
    def ensure_plan(self, version: int) -> dict:
        existing = self.repo.get_plan(version)
        if existing is not None:
            return existing
        snapshot = self.repo.get_snapshot(version)
        if snapshot is None:
            raise SnapshotNotFoundError(
                f"快照版本 {version} 不存在", details={"version": version}
            )
        declarations = self.repo.load_declarations()
        previous = self.repo.previous_snapshot(version)
        previous_items = previous.config_items if previous else None
        plan = build_plan(version, snapshot.config_items, previous_items, declarations)
        payload = plan.to_dict()
        self.repo.save_plan(version, payload)
        return payload

    # ------------------------------------------------------------ 分发入口
    def dispatch(self, version: int, failure_policy: str = "rollback") -> dict:
        if failure_policy not in POLICIES:
            raise ValueError(f"未知失败策略: {failure_policy}")
        condition = self._condition_for(version)
        with condition:
            while True:
                run = self.repo.get_run(version)
                if run is not None and run["status"] in TERMINAL_RUN_STATUSES:
                    return run
                if run is not None and run["status"] in ACTIVE_RUN_STATUSES:
                    # 同版本已有执行者：等待并复用其结论
                    condition.wait()
                    continue
                break
            try:
                self._run_dispatch(version, failure_policy)
            finally:
                condition.notify_all()
            return self.repo.get_run(version)

    def _run_dispatch(self, version: int, failure_policy: str) -> None:
        snapshot = self.repo.get_snapshot(version)
        if snapshot is None:
            raise SnapshotNotFoundError(
                f"快照版本 {version} 不存在", details={"version": version}
            )
        self.ensure_plan(version)
        # 先落意图记录（PENDING），后续执行崩溃也能被恢复扫描发现
        self.repo.create_run_intent(
            version,
            idempotency_key=f"dispatch-v{version}",
            failure_policy=failure_policy,
        )
        with self._writer_gate:
            self._execute(version, failure_policy)

    def _execute(self, version: int, failure_policy: str) -> None:
        self.repo.update_run_status(version, "RUNNING")
        plan = self.repo.get_plan(version)
        snapshot = self.repo.get_snapshot(version)
        declarations = self.repo.load_declarations()
        applied: list[str] = []
        failures: list[dict[str, str]] = []

        for component_id in plan["ordered_components"]:
            current = self.repo.ensure_component_state(
                version, component_id, f"apply-v{version}-{component_id}"
            )
            if current == APPLIED:
                # 恢复后重跑：已确认应用的组件凭幂等键跳过，不重复推送
                applied.append(component_id)
                continue
            if current == ROLLED_BACK:
                continue
            declaration = declarations[component_id]
            self.repo.transition_component_state(
                version, component_id, DISPATCHED, reason="push_start"
            )
            payload = {
                "component_id": component_id,
                "version": version,
                "config_items": snapshot.config_items,
                "idempotency_key": f"apply-v{version}-{component_id}",
            }
            try:
                self.client.post(declaration.endpoint, "/apply", payload)
            except DispatchError as exc:
                self.repo.transition_component_state(
                    version,
                    component_id,
                    FAILED,
                    reason="push_failed",
                    error_code=exc.code,
                    error_message=exc.message,
                )
                failures.append(
                    {"component_id": component_id, "error_code": exc.code}
                )
                if failure_policy == "rollback":
                    self._rollback_applied(version, applied, declarations)
                    self.repo.update_run_status(
                        version,
                        "ROLLED_BACK",
                        error_code=exc.code,
                        error_message=(
                            f"组件 {component_id} 失败触发回滚: {exc.message}"
                        ),
                    )
                    return
                continue
            self.repo.transition_component_state(
                version, component_id, APPLIED, reason="push_succeeded"
            )
            applied.append(component_id)

        if failures:
            self.repo.update_run_status(
                version,
                "PARTIAL",
                error_code="E_PARTIAL_APPLY",
                error_message="; ".join(
                    f"{item['component_id']}={item['error_code']}" for item in failures
                ),
            )
        else:
            self.repo.update_run_status(version, "COMPLETED")

    def _rollback_applied(
        self, version: int, applied: list[str], declarations: dict
    ) -> list[dict[str, str]]:
        rollback_errors: list[dict[str, str]] = []
        for component_id in reversed(applied):
            idem = f"rollback-v{version}-{component_id}"
            try:
                self.client.post(
                    declarations[component_id].endpoint,
                    "/rollback",
                    {
                        "component_id": component_id,
                        "version": version,
                        "idempotency_key": idem,
                    },
                )
            except DispatchError as exc:
                # 回滚指令无法确认：依据事实标记 FAILED 并继续其余组件
                self.repo.transition_component_state(
                    version,
                    component_id,
                    FAILED,
                    reason="rollback_failed",
                    error_code=exc.code,
                    error_message=exc.message,
                )
                rollback_errors.append(
                    {"component_id": component_id, "error_code": exc.code}
                )
                continue
            self.repo.transition_component_state(
                version, component_id, ROLLED_BACK, reason="rollback_succeeded"
            )
        return rollback_errors

    # ------------------------------------------------------------ 查询
    def get_status(self, version: int) -> dict:
        run = self.repo.get_run(version)
        if run is None:
            raise SnapshotNotFoundError(
                f"快照版本 {version} 不存在", details={"version": version}
            )
        return {
            "run": run,
            "plan": self.repo.get_plan(version),
            "components": self.repo.list_component_states(version),
        }

    def get_trajectory(self, version: int) -> dict[str, list[dict]]:
        return self.repo.all_trajectories(version)
