"""结果验证工具：读取持久化状态，输出结构化一致性验证结论。"""

from __future__ import annotations

import json
from dataclasses import dataclass, field

from .models import compute_checksum
from .planner import detect_cycle
from .repository import Repository
from .state_machine import ALL_STATES, LEGAL_TRANSITIONS


@dataclass
class VerificationResult:
    ok: bool
    checks: list[dict] = field(default_factory=list)
    violations: list[dict] = field(default_factory=list)

    def add(self, name: str, ok: bool, detail: dict | None = None) -> None:
        self.checks.append({"name": name, "ok": ok, "detail": detail or {}})
        if not ok:
            self.ok = False
            self.violations.append({"check": name, "detail": detail or {}})

    def to_dict(self) -> dict:
        return {"ok": self.ok, "checks": self.checks, "violations": self.violations}


def verify(repository: Repository) -> VerificationResult:
    result = VerificationResult(ok=True)
    view = repository.read_only_snapshot_view()
    declarations = repository.load_declarations()

    # 1) 快照校验摘要与版本单调
    versions = [row["version"] for row in view["snapshots"]]
    result.add("snapshot_versions_monotonic", versions == sorted(versions) and len(versions) == len(set(versions)),
               {"versions": versions})
    checksum_ok = True
    checksum_detail: dict = {}
    for row in view["snapshots"]:
        snapshot = repository.get_snapshot(row["version"])
        actual = compute_checksum(snapshot.config_items)
        valid = actual == snapshot.checksum
        checksum_ok = checksum_ok and valid
        checksum_detail[f"v{snapshot.version}"] = valid
    result.add("snapshot_checksums_valid", checksum_ok, checksum_detail)

    # 2) 依赖无环
    cycle = detect_cycle(declarations)
    result.add("dependency_acyclic", not cycle, {"cycle": cycle})

    # 3) 组件状态记录与快照版本严格绑定，且状态值合法
    states = view["states"]
    version_set = set(versions)
    binding_ok = all(
        row["version"] in version_set and row["state"] in ALL_STATES for row in states
    )
    result.add("state_version_binding", binding_ok, {"records": len(states)})

    # 4) 状态迁移审计中的每一步都合法
    audit_ok = True
    illegal: list[dict] = []
    for row in view["audits"]:
        if row["from_state"] is None:
            continue
        if row["to_state"] not in LEGAL_TRANSITIONS.get(row["from_state"], frozenset()):
            audit_ok = False
            illegal.append(row)
    result.add("state_transitions_legal", audit_ok, {"illegal": illegal})

    # 5) 每个终结运行满足对应策略的一致结论（部分应用与回滚不得混用）
    runs_ok = True
    run_violations: list[dict] = []
    plan_closure_ok = True
    closure_violations: list[dict] = []
    for run in view["runs"]:
        version = run["version"]
        run_states = [row for row in states if row["version"] == version]
        state_names = {row["state"] for row in run_states}
        status = run["status"]
        if status == "COMPLETED":
            expected = repository.get_plan(version)
            applied = {row["component_id"] for row in run_states if row["state"] == "APPLIED"}
            planned = set(expected["ordered_components"]) if expected else set()
            if applied != planned or state_names - {"APPLIED"}:
                runs_ok = False
                run_violations.append({"version": version, "status": status, "states": sorted(state_names)})
        elif status == "PARTIAL":
            if "ROLLED_BACK" in state_names:
                runs_ok = False
                run_violations.append({"version": version, "reason": "partial_mixed_with_rollback"})
            if not state_names.intersection({"APPLIED", "FAILED", "PENDING"}):
                runs_ok = False
                run_violations.append({"version": version, "reason": "partial_without_fact"})
        elif status == "ROLLED_BACK":
            if "APPLIED" in state_names or "DISPATCHED" in state_names:
                runs_ok = False
                run_violations.append({"version": version, "reason": "rollback_not_converged",
                                       "states": sorted(state_names)})
        elif status in ("PENDING", "RUNNING", "ABORTED"):
            continue
        # 依赖闭合仅约束要求全量一致的运行（COMPLETED/ROLLED_BACK）。
        # PARTIAL 是 continue 策略显式接受的“部分应用”结论：依赖组件失败而
        # 下游成功应用属于被允许且已标记的事实，不构成闭合违例。
        applied_map = {row["component_id"]: row["state"] for row in run_states}
        if status in {"COMPLETED", "ROLLED_BACK"}:
            for component_id, state in applied_map.items():
                if state != "APPLIED":
                    continue
                for dependency in declarations[component_id].depends_on_components:
                    if dependency in applied_map and applied_map[dependency] != "APPLIED":
                        plan_closure_ok = False
                        closure_violations.append(
                            {"version": version, "component_id": component_id, "dependency": dependency}
                        )
    result.add("run_conclusion_consistent", runs_ok, {"violations": run_violations})
    result.add("applied_dependency_closure", plan_closure_ok, {"violations": closure_violations})
    return result


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="配置分发结果验证工具")
    parser.add_argument("--db", required=True, help="SQLite 数据库路径")
    parser.add_argument("--output", help="将结构化验证结论写入 JSON 文件")
    args = parser.parse_args()
    repository = Repository(args.db)
    try:
        result = verify(repository).to_dict()
    finally:
        repository.close()
    text = json.dumps(result, ensure_ascii=False, indent=2)
    if args.output:
        with open(args.output, "w", encoding="utf-8") as handle:
            handle.write(text)
    print(text)
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
