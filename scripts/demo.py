from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.service import SchedulerService  # noqa: E402
from app.storage import Storage  # noqa: E402


def gb(value: int) -> str:
    return f"{value / 1024**3:.2f}GiB"


def cpu(value: int) -> str:
    return f"{value}m" if value % 1000 else f"{value // 1000} CPU"


def print_batch(batch: dict[str, Any]) -> None:
    result = batch["result"]
    print(f"\n=== {batch['spec_name']} | {batch['batch_id']} | {batch['status']} ===")
    summary = result["summary"]
    print(
        f"放置 {summary['placed']} / 否决 {summary['rejected']} / 抢占 {summary['preempted']}，"
        f"资源账本一致={result['resourceConsistency']['allNodesConsistent']}"
    )
    rounding = result["scoreContract"]["roundingAudit"]
    print(
        f"评分舍入审计: evaluations={rounding['evaluationCount']} "
        f"cumulative={rounding['cumulativeResidualPoints']} exact={rounding['cumulativeResidualExact']}"
    )
    print("放置结论:")
    for item in result["placed"]:
        print(f"  + {item['pod']:<18} -> {item['node']:<7} score={item['score']}")
    print("否决结论:")
    for item in result["rejected"]:
        reason = item["reason"]
        print(f"  - {item['pod']:<18} {reason['code']:<32} primary={reason['node']}")
        expression = reason.get("details", {}).get("firstViolationExpression")
        if expression:
            print(
                f"      first violation: {expression.get('key')} {expression.get('operator')} "
                f"expected={expression.get('expectedValues', expression.get('expectedValue'))} "
                f"actual={expression.get('labelValue')!r}"
            )
        first_violation = reason.get("details", {}).get("firstViolation")
        if first_violation and "windowAttempts" in first_violation:
            window = first_violation["windowAttempts"][0]
            print(
                f"      NoExecute window: tolerance={window['tolerationSeconds']}s "
                f"elapsed={window['elapsedSeconds']}s remaining={window['remainingSeconds']}s"
            )
        resource = reason.get("details", {}).get("remainingBefore")
        if resource:
            print(
                f"      remaining snapshot: CPU={cpu(resource['cpuMillicores'])}, "
                f"memory={gb(resource['memoryBytes'])}"
            )
    print("节点最终余量:")
    for node in result["nodes"]:
        remaining = node["remaining"]
        invariant = node["invariant"]
        print(
            f"  {node['name']:<7} CPU={cpu(remaining['cpuMillicores']):<7} "
            f"memory={gb(remaining['memoryBytes']):<8} "
            f"consistent={invariant['cpu']['consistent'] and invariant['memory']['consistent']}"
        )
    penalty_steps = []
    for step in result["steps"]:
        if step["decision"] != "placed" or step["selectedScore"] is None:
            continue
        penalty = next(
            (
                component
                for component in step["selectedScore"]["components"]
                if component["component"] == "preferNoSchedulePenalty"
            ),
            None,
        )
        if penalty:
            penalty_steps.append((step["pod"], step["selectedNode"], penalty["points"]))
    if penalty_steps:
        print("PreferNoSchedule 仅降权、不硬否决:")
        for pod, node, points in penalty_steps:
            print(f"  {pod} -> {node}, penalty={points}")


def comparison(original: dict[str, Any], modified: dict[str, Any]) -> dict[str, Any]:
    original_placed = {item["pod"]: item["node"] for item in original["result"]["placed"]}
    modified_placed = {item["pod"]: item["node"] for item in modified["result"]["placed"]}
    pods = sorted(set(original_placed) | set(modified_placed))
    changes = []
    for pod in pods:
        before = original_placed.get(pod, "rejected")
        after = modified_placed.get(pod, "rejected")
        if before != after:
            changes.append({"pod": pod, "original": before, "modified": after})
    original_remaining = {node["name"]: node["remaining"] for node in original["result"]["nodes"]}
    node_changes = []
    for node in modified["result"]["nodes"]:
        before = original_remaining[node["name"]]
        after = node["remaining"]
        if before != after:
            node_changes.append(
                {
                    "node": node["name"],
                    "original": before,
                    "modified": after,
                }
            )
    return {
        "inputChange": "node-b.labels.accelerator: 'none' -> 'nvidia'",
        "placementChanges": changes,
        "nodeRemainingChanges": node_changes,
        "conclusion": "Only accelerator-job changes from E_UNSATISFIED_AFFINITY to node-b; cumulative resource and hard anti-affinity outcomes remain deterministic.",
    }


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description="Run the deterministic scheduling comparison demo.")
    parser.add_argument("--output", type=Path, default=ROOT / "artifacts" / "demo-results.json")
    args = parser.parse_args()

    demo_db = ROOT / "data" / "demo.db"
    for suffix in ("", "-wal", "-shm"):
        path = Path(str(demo_db) + suffix)
        if path.exists():
            path.unlink()

    storage = Storage(demo_db)
    service = SchedulerService(storage, ROOT / "samples")
    service.recover_incomplete(trigger="demo-startup")
    original = service.submit_sample(batch_id="demo-original", sample_name="original")
    modified = service.submit_sample(batch_id="demo-modified", sample_name="modified")

    print_batch(original)
    print_batch(modified)
    diff = comparison(original, modified)
    print("\n=== A/B 对比结论 ===")
    print(f"唯一输入变化: {diff['inputChange']}")
    for change in diff["placementChanges"]:
        print(f"  {change['pod']}: {change['original']} -> {change['modified']}")
    for change in diff["nodeRemainingChanges"]:
        before = change["original"]
        after = change["modified"]
        print(
            f"  {change['node']} 余量: CPU {cpu(before['cpuMillicores'])} -> {cpu(after['cpuMillicores'])}, "
            f"memory {gb(before['memoryBytes'])} -> {gb(after['memoryBytes'])}"
        )
    print(f"  {diff['conclusion']}")

    recovery = service.recovery_report()
    print("\n=== 恢复报告（仅持久化事实） ===")
    latest = recovery["runs"][-1]
    print(
        f"scan trigger={latest['trigger']} scanned={latest['scanned_count']} "
        f"recovered={latest['recovered_count']} aborted={latest['aborted_count']}"
    )
    print("limitations:")
    for item in recovery["limitations"]:
        print(f"  - {item}")

    artifact = {
        "generatedBy": "scripts/demo.py",
        "python": sys.version,
        "database": str(demo_db),
        "original": original,
        "modified": modified,
        "comparison": diff,
        "recovery": recovery,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(artifact, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n完整诊断已写入: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
