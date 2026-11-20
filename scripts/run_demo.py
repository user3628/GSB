"""端到端验收演示：配额准入 / 批组织取舍 / 抢占回收 / 强制终止恢复。

所有数据来自本地确定性样本，输出写入 data/demo_report.json。
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from gpworkbench.database import Database  # noqa: E402
from gpworkbench.errors import BusinessError, CrashSimulated  # noqa: E402
from gpworkbench.scheduler import SchedulerService  # noqa: E402
from gpworkbench.spec import load_workload_file  # noqa: E402
from gpworkbench.statemachine import ADMITTED  # noqa: E402

SAMPLE_DIR = ROOT / "data" / "samples"
REPORT_PATH = ROOT / "data" / "demo_report.json"


def _admit_all(service: SchedulerService) -> dict:
    admitted, rejected = [], []
    for request in service.repo.list_requests():
        try:
            service.admit_request(request["request_id"])
            admitted.append(request["request_id"])
        except BusinessError as exc:
            rejected.append(
                {
                    "request_id": request["request_id"],
                    "code": exc.code,
                    "remaining_memory": exc.details.get("usage", {}).get("remaining_memory"),
                    "remaining_compute": exc.details.get("usage", {}).get("remaining_compute"),
                }
            )
    return {"admitted": len(admitted), "rejected": rejected}


def run_scale(sample_name: str, db_dir: Path) -> dict:
    db_path = db_dir / f"{sample_name}.sqlite3"
    if db_path.exists():
        db_path.unlink()
    service = SchedulerService(Database(db_path))
    spec = load_workload_file(SAMPLE_DIR / f"workload_{sample_name}.json")
    load_result = service.load_spec(spec)
    admission = _admit_all(service)
    batch_report = service.run_batching()
    return {
        "sample": sample_name,
        "load": load_result,
        "admission": {
            "admitted": admission["admitted"],
            "rejected_count": len(admission["rejected"]),
            "first_rejections": admission["rejected"][:5],
        },
        "batching": {
            "strategies": [g["strategy"] for g in batch_report["groups"]],
            "batches_created": batch_report["batches_created"],
            "fragmented_count": len(batch_report["fragmented"]),
            "fragmented_sample": batch_report["fragmented"][:10],
            "oversized_count": len(batch_report["oversized"]),
            "group_reports": [
                {
                    "group_id": g["group_id"],
                    "strategy": g["strategy"],
                    "batches_created": g["batches_created"],
                    "fragmented": len(g["fragmented_request_ids"]),
                    "utilization": g["utilization"],
                    "reorganized": g.get("reorganized", False),
                }
                for g in batch_report["groups"]
            ],
        },
        "quota_after": service.quota_snapshot(),
        "state_counts": service.repo.counts_by_state(),
    }


TIGHT_PREEMPTION_WORKLOAD = {
    "quota_groups": [
        {"group_id": "g-tight", "memory_limit": "30Gi", "compute_limit": "300m"}
    ],
    "nodes": [
        {
            "node_id": "n-tight",
            "total_memory": "30Gi",
            "total_compute": "300m",
            "used_memory": "0Gi",
            "group_id": "g-tight",
            "health": "HEALTHY",
        }
    ],
    "models": [{"model_id": "m", "cost_per_sample": 2, "max_batch_samples": 16}],
    "requests": [
        {
            "request_id": "low-1",
            "service_name": "svc",
            "model_id": "m",
            "memory_need": "10Gi",
            "compute_need": "100m",
            "samples_per_batch": 2,
            "latency_limit_ms": 100,
            "priority": 1,
            "group_id": "g-tight",
        },
        {
            "request_id": "low-2",
            "service_name": "svc",
            "model_id": "m",
            "memory_need": "10Gi",
            "compute_need": "100m",
            "samples_per_batch": 2,
            "latency_limit_ms": 100,
            "priority": 2,
            "group_id": "g-tight",
        },
        {
            "request_id": "high-1",
            "service_name": "svc",
            "model_id": "m",
            "memory_need": "15Gi",
            "compute_need": "150m",
            "samples_per_batch": 2,
            "latency_limit_ms": 100,
            "priority": 9,
            "group_id": "g-tight",
        },
    ],
}


def run_preemption_demo(db_dir: Path) -> dict:
    from gpworkbench.spec import parse_workload

    db_path = db_dir / "preemption.sqlite3"
    if db_path.exists():
        db_path.unlink()
    service = SchedulerService(Database(db_path))
    service.load_spec(parse_workload(TIGHT_PREEMPTION_WORKLOAD))
    # 两个低优先级请求先占住 20Gi/200m，高优先级请求 15Gi/150m 无法直接准入
    service.admit_request("low-1")
    service.admit_request("low-2")
    before = service.quota_snapshot("g-tight")[0]
    preempt_result = service.preempt_for("high-1")
    after = service.quota_snapshot("g-tight")[0]
    # 被抢占者状态与预留量核验
    victim_states = {
        rid: service.repo.get_request(rid)
        for rid in (v["request_id"] for v in preempt_result["preempted"])
    }
    return {
        "target_request": "high-1",
        "preempted": preempt_result["preempted"],
        "victim_states_after": {
            rid: {"state": row["state"], "reserved_memory": row["reserved_memory"],
                  "reserved_compute": row["reserved_compute"]}
            for rid, row in victim_states.items()
        },
        "quota_before": before,
        "quota_after": after,
        "target_state": service.repo.get_request("high-1")["state"],
        "target_reserved": {
            "memory": service.repo.get_request("high-1")["reserved_memory"],
            "compute": service.repo.get_request("high-1")["reserved_compute"],
        },
    }


def run_crash_recovery_demo(db_dir: Path) -> dict:
    db_path = db_dir / "crash.sqlite3"
    if db_path.exists():
        db_path.unlink()
    service = SchedulerService(Database(db_path))
    service.load_spec(load_workload_file(SAMPLE_DIR / "workload_small.json"))
    for request in service.repo.list_requests()[:12]:
        service.admit_request(request["request_id"])

    # 模拟批调度中途 kill -9：BATCH 意图已 PENDING 落盘，应用事务未提交
    service.repo.crash_next_intent_type = "BATCH"
    crash = None
    try:
        service.run_batching("grp-alpha")
    except CrashSimulated as exc:
        crash = {"intent_id": exc.intent_id, "intent_type": exc.intent_type}
    pending = service.repo.list_pending_intents()
    pre_recover = {
        "pending_intents": len(pending),
        "batches_persisted": len(service.repo.list_batches()),
        "states": service.repo.counts_by_state(),
    }
    report = service.recover()
    post_recover = {
        "pending_intents": len(service.repo.list_pending_intents()),
        "batches_persisted": len(service.repo.list_batches()),
        "states": service.repo.counts_by_state(),
        "recovery_events": service.repo.list_recovery_events(),
    }
    return {
        "simulated_crash": crash,
        "before_recovery": pre_recover,
        "recovery_report": report,
        "after_recovery": post_recover,
    }


def run_invalid_spec_demo(db_dir: Path) -> dict:
    db_path = db_dir / "invalid.sqlite3"
    service = SchedulerService(Database(db_path))
    try:
        service.load_spec(load_workload_file(SAMPLE_DIR / "workload_invalid.json"))
        return {"accepted": True}
    except BusinessError as exc:
        return {
            "accepted": False,
            "code": exc.code,
            "message": str(exc),
            "details": exc.details,
            "rows_written": {
                "nodes": len(service.repo.list_nodes()),
                "groups": len(service.repo.list_groups()),
                "requests": len(service.repo.list_requests()),
            },
        }


def main() -> None:
    db_dir = ROOT / "data"
    report = {
        "scales": [run_scale(name, db_dir) for name in ("small", "medium", "large")],
        "invalid_spec": run_invalid_spec_demo(db_dir),
        "preemption": run_preemption_demo(db_dir),
        "crash_recovery": run_crash_recovery_demo(db_dir),
    }
    REPORT_PATH.write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(f"demo report written to {REPORT_PATH}")
    for scale in report["scales"]:
        b = scale["batching"]
        print(
            f"[{scale['sample']}] requests={scale['load']['result']['requests']} "
            f"admitted={scale['admission']['admitted']} "
            f"rejected={scale['admission']['rejected_count']} "
            f"batches={b['batches_created']} strategy={b['strategies']} "
            f"fragmented={b['fragmented_count']} oversized={b['oversized_count']}"
        )
    print("invalid_spec:", report["invalid_spec"]["code"])
    if "target_state" in report["preemption"]:
        print("preemption target state:", report["preemption"]["target_state"])
        print("preempted victims:", len(report["preemption"].get("preempted", [])))
    else:
        print("preemption failed:", report["preemption"]["preemption_failed"]["code"])
    print(
        "crash recovery actions:",
        [a["action"] for a in report["crash_recovery"]["recovery_report"]["actions"]],
    )


if __name__ == "__main__":
    main()
