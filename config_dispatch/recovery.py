"""进程重启后的恢复：扫描未完成意图，仅依据持久化事实收敛。

恢复报告只陈述数据库中已持久化的事实与据此采取的动作；强制终止发生在
进程内部、没有任何持久化退出报告可恢复，因此不承诺生成完整退出报告。
"""

from __future__ import annotations

from .coordinator import Coordinator
from .repository import Repository
from .state_machine import APPLIED, DISPATCHED, PENDING, ROLLED_BACK

ACTIVE_STATUSES = ("PENDING", "RUNNING")


class RecoveryService:
    def __init__(self, coordinator: Coordinator):
        self.coordinator = coordinator
        self.repo: Repository = coordinator.repo

    def recover(self) -> dict:
        unfinished = self.repo.list_unfinished_runs()
        report: dict = {
            "scanned_unfinished_intents": len(unfinished),
            "runs": [],
            "note": (
                "恢复仅基于 SQLite 中已持久化的状态事实；强制终止时进程内"
                "未提交的退出报告无法恢复。"
            ),
        }
        for run in unfinished:
            version = run["version"]
            states = self.repo.list_component_states(version)
            facts_before = [
                {
                    "component_id": row["component_id"],
                    "state": row["state"],
                    "error_code": row["error_code"],
                }
                for row in states
            ]
            entry: dict = {
                "version": version,
                "failure_policy": run["failure_policy"],
                "persisted_facts": facts_before,
                "actions": [],
            }
            policy = run["failure_policy"]
            applied = [row["component_id"] for row in states if row["state"] == APPLIED]
            in_flight = [
                row["component_id"] for row in states if row["state"] == DISPATCHED
            ]
            self.repo.update_run_status(
                version,
                "ABORTED",
                error_code="E_DISPATCH_ABORTED",
                error_message="进程在分发过程中终止，运行已标记中止",
            )
            entry["actions"].append("run_marked_aborted")
            if policy == "rollback" or applied:
                declarations = self.repo.load_declarations()
                for component_id in in_flight:
                    # DISPATCHED 仅代表请求已发出，无应用确认事实，按未应用收敛
                    self.repo.transition_component_state(
                        version, component_id, ROLLED_BACK, reason="recovery_inflight_rollback"
                    )
                    entry["actions"].append(f"{component_id}:inflight_marked_rolled_back")
                rollback_errors = self.coordinator._rollback_applied(
                    version, applied, declarations
                )
                entry["actions"].extend(
                    f"{component_id}:rolled_back_from_persisted_applied"
                    for component_id in reversed(applied)
                )
                entry["rollback_errors"] = rollback_errors
                self.repo.update_run_status(
                    version,
                    "ROLLED_BACK",
                    error_code="E_DISPATCH_ABORTED",
                    error_message="恢复流程依据持久化事实完成回滚",
                )
                entry["final_status"] = "ROLLED_BACK"
            else:
                # continue 策略且无任何已应用事实：未完成组件保留/回归 PENDING 并重新排队
                for row in states:
                    if row["state"] == DISPATCHED:
                        self.repo.transition_component_state(
                            version,
                            row["component_id"],
                            PENDING,
                            reason="recovery_requeue",
                        )
                        entry["actions"].append(f"{row['component_id']}:requeued_pending")
                entry["final_status"] = "REQUEUED"
            report["runs"].append(entry)
        return report

    def requeue_and_run(self, version: int) -> dict:
        """对恢复后重新排队的运行重新触发分发（仅 PENDING 组件会被执行）。"""

        run = self.repo.get_run(version)
        if run is None:
            raise KeyError(version)
        if run["status"] != "ABORTED":
            return run
        self.repo.update_run_status(version, "PENDING", error_code=None, error_message=None)
        return self.coordinator.dispatch(version, failure_policy=run["failure_policy"])
