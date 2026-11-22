"""命令行接口：导入、运行、状态查询、停止/恢复与一致性比对。"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys

from .config import write_default_config, load_config
from .pipeline import Pipeline
from .sampling import generate_workspace_inputs
from .util import canonical_pretty
from .workspace import load_workspace


def _load(workspace: str, config_path: str):
    cfg = load_config(config_path)
    resources, records, manifest = load_workspace(workspace, cfg)
    return cfg, Pipeline(workspace, cfg, resources, records, manifest)


def cmd_init(args: argparse.Namespace) -> int:
    os.makedirs(args.workspace, exist_ok=True)
    config_path = os.path.join(args.workspace, "config.yaml")
    if not os.path.exists(config_path):
        write_default_config(config_path)
    cfg = load_config(config_path)
    paths = generate_workspace_inputs(
        cfg, os.path.join(args.workspace, "inputs"),
        shard_count=args.shards, rows_per_shard=args.rows)
    print(json.dumps({"workspace": args.workspace,
                      "config": config_path, "inputs": paths},
                     ensure_ascii=False, indent=2, sort_keys=True))
    return 0


def cmd_import(args: argparse.Namespace) -> int:
    cfg = load_config(args.config)
    os.makedirs(args.workspace, exist_ok=True)
    shutil.copyfile(args.config, os.path.join(args.workspace, "config.yaml"))
    paths = generate_workspace_inputs(
        cfg, os.path.join(args.workspace, "inputs"),
        shard_count=args.shards, rows_per_shard=args.rows)
    print(canonical_pretty({"imported": paths}))
    return 0


def cmd_run(args: argparse.Namespace) -> int:
    cfg, pipeline = _load(args.workspace, args.config)
    if args.stop:
        pipeline.request_stop()
    report = pipeline.run(parallel=(not args.serial),
                          stop_after=args.stop_after)
    print(canonical_pretty(report))
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    cfg, pipeline = _load(args.workspace, args.config)
    print(canonical_pretty(pipeline.query_status(args.query_id)))
    return 0


def cmd_budget(args: argparse.Namespace) -> int:
    cfg, pipeline = _load(args.workspace, args.config)
    print(canonical_pretty(pipeline.budget_view()))
    return 0


def cmd_trace(args: argparse.Namespace) -> int:
    cfg, pipeline = _load(args.workspace, args.config)
    print(canonical_pretty(pipeline.adjudication_trace(args.query_id)))
    return 0


def cmd_stop(args: argparse.Namespace) -> int:
    cfg, pipeline = _load(args.workspace, args.config)
    pipeline.request_stop()
    print(canonical_pretty({"stop_requested": True,
                            "workspace": args.workspace}))
    return 0


def cmd_resume(args: argparse.Namespace) -> int:
    cfg, pipeline = _load(args.workspace, args.config)
    pipeline.clear_stop()
    report = pipeline.run(parallel=(not args.serial),
                          stop_after=args.stop_after)
    print(canonical_pretty(report))
    return 0


def cmd_compare(args: argparse.Namespace) -> int:
    """比对两次运行产物的逐字节一致性（并行 vs 串行 / 恢复 vs 全新）。"""
    report_a = json.load(open(args.report_a, "r", encoding="utf-8"))
    report_b = json.load(open(args.report_b, "r", encoding="utf-8"))
    matched = report_a.get("run_fingerprint") == report_b.get("run_fingerprint") \
        and report_a.get("summary_fingerprint") == \
        report_b.get("summary_fingerprint")
    result = {
        "report_a": args.report_a,
        "report_b": args.report_b,
        "byte_identical": matched,
        "run_fingerprint_a": report_a.get("run_fingerprint"),
        "run_fingerprint_b": report_b.get("run_fingerprint"),
    }
    print(canonical_pretty(result))
    return 0 if matched else 2


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="qrwb", description="跨语言查询改写并行编排工作台")
    sub = parser.add_subparsers(dest="command", required=True)

    p_init = sub.add_parser("init", help="初始化工作区并生成确定性样本")
    p_init.add_argument("workspace")
    p_init.add_argument("--shards", type=int, default=2)
    p_init.add_argument("--rows", type=int, default=12)
    p_init.set_defaults(func=cmd_init)

    p_import = sub.add_parser("import", help="导入配置并生成分片/资源")
    p_import.add_argument("workspace")
    p_import.add_argument("--config", required=True)
    p_import.add_argument("--shards", type=int, default=2)
    p_import.add_argument("--rows", type=int, default=12)
    p_import.set_defaults(func=cmd_import)

    def add_run_args(sp: argparse.ArgumentParser) -> None:
        sp.add_argument("workspace")
        sp.add_argument("--config", required=True)
        sp.add_argument("--serial", action="store_true",
                        help="单线程顺序执行（默认线程池并行）")
        sp.add_argument("--stop", action="store_true",
                        help="运行前先请求停止（演示优雅停止）")
        sp.add_argument("--stop-after", type=int, default=None,
                        help="处理 N 个查询单元后优雅停止（确定性演示）")

    p_run = sub.add_parser("run")
    add_run_args(p_run)
    p_run.set_defaults(func=cmd_run)

    p_status = sub.add_parser("status", help="查询单查询状态与版本指纹")
    p_status.add_argument("workspace")
    p_status.add_argument("--config", required=True)
    p_status.add_argument("--query-id", required=True)
    p_status.set_defaults(func=cmd_status)

    p_budget = sub.add_parser("budget", help="查看预算消耗与未处理原因")
    p_budget.add_argument("workspace")
    p_budget.add_argument("--config", required=True)
    p_budget.set_defaults(func=cmd_budget)

    p_trace = sub.add_parser("trace", help="查看裁决轨迹")
    p_trace.add_argument("workspace")
    p_trace.add_argument("--config", required=True)
    p_trace.add_argument("--query-id", required=True)
    p_trace.set_defaults(func=cmd_trace)

    p_stop = sub.add_parser("stop", help="请求优雅停止")
    p_stop.add_argument("workspace")
    p_stop.add_argument("--config", required=True)
    p_stop.set_defaults(func=cmd_stop)

    p_resume = sub.add_parser("resume", help="清除停止标记并从检查点恢复")
    add_run_args(p_resume)
    p_resume.set_defaults(func=cmd_resume)

    p_cmp = sub.add_parser("compare", help="比对两份运行报告的归约指纹")
    p_cmp.add_argument("report_a")
    p_cmp.add_argument("report_b")
    p_cmp.set_defaults(func=cmd_compare)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except FileNotFoundError as exc:
        print(json.dumps({"error": str(exc)}, ensure_ascii=False),
              file=sys.stderr)
        return 1
    except Exception as exc:  # noqa: BLE001 - CLI 顶层兜底
        print(json.dumps({"error": f"{type(exc).__name__}: {exc}"},
                         ensure_ascii=False), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
