#!/usr/bin/env python3
"""一键本地验收演示（确定性、无外部服务、无网络）。

产出：
* 多路改写候选（同义词 / 字形近似 / 跨语言桥接含 UNK 回退）；
* 预算约束编排（全局预算 / 通道配额 / 延迟预算）；
* 一次优雅停止 → 检查点恢复 → 与全新运行逐字节一致；
* 并行 vs 串行归约逐字节一致；
* 分层汇总与裁决轨迹；
* 多进程文件锁发布保护（E_STALE_CHECKPOINT）。
"""
import json
import os
import subprocess
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import yaml  # noqa: E402

from qrwb.config import load_config, write_default_config  # noqa: E402
from qrwb.pipeline import Pipeline  # noqa: E402
from qrwb.sampling import generate_workspace_inputs  # noqa: E402
from qrwb.util import canonical_pretty  # noqa: E402
from qrwb.workspace import load_workspace  # noqa: E402


def build(ws: str, budget: int | None = None,
          latency_ms: float | None = None) -> Pipeline:
    os.makedirs(ws, exist_ok=True)
    cfg_path = os.path.join(ws, "config.yaml")
    write_default_config(cfg_path)
    if budget is not None or latency_ms is not None:
        data = yaml.safe_load(open(cfg_path, encoding="utf-8"))
        if budget is not None:
            data["budget"]["global_evaluations"] = budget
            data["budget"]["per_channel_quota"] = budget
        if latency_ms is not None:
            data["budget"]["per_request_latency_ms"] = latency_ms
        with open(cfg_path, "w", encoding="utf-8", newline="\n") as handle:
            yaml.safe_dump(data, handle, sort_keys=True,
                           allow_unicode=True)
    cfg = load_config(cfg_path)
    generate_workspace_inputs(cfg, os.path.join(ws, "inputs"),
                              shard_count=2, rows_per_shard=10)
    resources, records, manifest = load_workspace(ws, cfg)
    return Pipeline(ws, cfg, resources, records, manifest)


def heading(title: str) -> None:
    print("\n" + "=" * 72)
    print(title)
    print("=" * 72)


def main() -> int:
    root = tempfile.mkdtemp(prefix="qrwb-demo-")
    print(f"演示工作区根目录: {root}")
    results: dict = {}

    # 1. 多路改写 + 完整并行运行
    heading("1. 多路改写候选与并行编排（默认预算）")
    pipe_par = build(os.path.join(root, "par"))
    report_par = pipe_par.run(parallel=True)
    print(f"状态={report_par['status']} 记录={report_par['total_records']} "
          f"编排={report_par['orchestrated']} 验证={report_par['validated']}")
    sample_q = sorted(pipe_par.records, key=lambda r: r["query_id"])[0]["query_id"]
    candidates = json.load(open(
        os.path.join(root, "par", "state", "candidates.json"),
        encoding="utf-8"))[sample_q]
    routes = sorted({c["route"] for c in candidates["candidates"]})
    print(f"样例查询 {sample_q} 命中路线: {routes}")
    print(f"各路候选数: {candidates['route_counts']}")
    unk = [c for c in candidates["candidates"]
           if c["basis"].get("type") == "unknown_marker_fallback"]
    print(f"显式 UNK 回退候选数: {len(unk)}")
    results["multi_route"] = {"sample": sample_q, "routes": routes,
                              "route_counts": candidates["route_counts"],
                              "unk_fallbacks": len(unk)}

    # 2. 并行 vs 串行逐字节一致
    heading("2. 并行 vs 串行 逐字节一致性")
    pipe_ser = build(os.path.join(root, "ser"))
    report_ser = pipe_ser.run(parallel=False)
    identical = report_par["run_fingerprint"] == report_ser["run_fingerprint"]
    byte_checks = {}
    for name in ("plans.json", "candidates.json", "orchestration.json",
                 "validation.json", "summary.json"):
        with open(os.path.join(root, "par", "state", name), "rb") as fa, \
             open(os.path.join(root, "ser", "state", name), "rb") as fb:
            byte_checks[name] = (fa.read() == fb.read())
    print(f"run_fingerprint 一致: {identical}")
    print(f"产物逐字节一致: {byte_checks}")
    results["parallel_vs_serial"] = {"fingerprint": identical,
                                     "byte_identical": byte_checks}

    # 3. 预算编排：小预算触发拒绝且不超全局预算
    heading("3. 预算约束编排（小预算）")
    pipe_bud = build(os.path.join(root, "budget"), budget=20)
    report_bud = pipe_bud.run(parallel=True)
    codes = sorted({u["reason"] for u in report_bud["unprocessed"]})
    print(f"未处理原因码: {codes}")
    print(f"全局评估 {report_bud['budget']['global_used']} / "
          f"{report_bud['budget']['global_limit']}")
    print(f"通道消耗: {report_bud['budget']['channel_used']}")
    results["budget"] = {"codes": codes,
                         "within_limit": report_bud["budget"]["global_used"]
                         <= report_bud["budget"]["global_limit"]}

    # 4. 延迟预算
    heading("4. 每请求延迟预算（预算=0ms 必触发）")
    pipe_lat = build(os.path.join(root, "latency"), latency_ms=0.0)
    report_lat = pipe_lat.run(parallel=True)
    lat_codes = sorted({u["reason"] for u in report_lat["unprocessed"]})
    print(f"未处理原因码: {lat_codes}")
    results["latency"] = {"codes": lat_codes}

    # 5. 优雅停止 → 恢复 → 与全新运行一致
    heading("5. 优雅停止 → 检查点恢复 → 与全新运行逐字节一致")
    pipe_stop = build(os.path.join(root, "stop"))
    stopped = pipe_stop.run(parallel=True, stop_after=4)
    print(f"停止时状态={stopped['status']} 已派发={stopped['dispatched']} "
          f"待处理={sum(1 for u in stopped['unprocessed'] if u['reason'] == 'STOP_REQUESTED')}")
    resumed_pipe = build(os.path.join(root, "stop"))  # 复用同一工作区
    resumed = resumed_pipe.run(parallel=True)
    pipe_fresh = build(os.path.join(root, "fresh"))
    fresh = pipe_fresh.run(parallel=True)
    resume_match = resumed["run_fingerprint"] == fresh["run_fingerprint"]
    print(f"恢复后状态={resumed['status']} 与全新运行指纹一致={resume_match}")
    seq_ok = True
    for name in os.listdir(os.path.join(root, "stop", "checkpoints")):
        if not name.endswith(".jsonl"):
            continue
        seqs = [json.loads(line)["seq"] for line in open(
            os.path.join(root, "stop", "checkpoints", name), encoding="utf-8")]
        if seqs != list(range(1, len(seqs) + 1)):
            seq_ok = False
    print(f"检查点序号连续推进: {seq_ok}")
    results["stop_resume"] = {"resume_match": resume_match,
                              "sequence_continuous": seq_ok}

    # 6. 分层汇总与裁决轨迹
    heading("6. 分层验证汇总与裁决轨迹")
    summary = json.load(open(os.path.join(root, "par", "state",
                                          "summary.json"), encoding="utf-8"))
    print(f"分层数: {len(summary['by_lang_channel'])}")
    print(f"总采纳率: {summary['totals']['adoption_rate']} "
          f"路命中: {summary['totals']['route_hits']}")
    trace = pipe_par.adjudication_trace(sample_q)
    print(f"样例 {sample_q} 采纳={len(trace['adoptions'])} "
          f"淘汰={len(trace['rejected'])}（淘汰依据已保留）")
    results["summary"] = {"strata": len(summary["by_lang_channel"]),
                          "route_hits": summary["totals"]["route_hits"]}

    # 7. 多进程文件锁发布保护
    heading("7. 多进程文件锁发布保护（E_STALE_CHECKPOINT）")
    proc = [sys.executable, os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "mp_lock_demo.py"),
        os.path.join(root, "lockdemo")]
    proc_out = subprocess.run(proc, capture_output=True, text=True,
                              timeout=60)
    print(proc_out.stdout.strip())
    results["locking"] = json.loads(proc_out.stdout.strip().splitlines()[-1])

    heading("验收结论")
    all_ok = (identical and all(byte_checks.values())
              and results["budget"]["within_limit"]
              and "E_LATENCY_BUDGET" in lat_codes and resume_match and seq_ok
              and results["locking"]["stale_rejected"]
              and results["locking"]["sequential_publish"])
    print(canonical_pretty({"all_checks_passed": all_ok, "results": results}))
    print(f"完整产物目录: {root}")
    return 0 if all_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
