"""Command-line interface for deterministic execution and diagnostics."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from .api import create_app
from .config import RopeConfig
from .demo_data import generate_demo_assets
from .errors import RopeError
from .session import DiagnosticSession


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="rope-diag",
        description="本地确定性 RoPE 位置编码算子与诊断应用",
    )
    parser.add_argument(
        "--workspace",
        default="artifacts/demo_workspace",
        help="持久化工作区（默认 artifacts/demo_workspace）",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    generate = subparsers.add_parser("generate-data", help="生成固定种子分片与形状清单")
    generate.add_argument("--base", default=".", help="仓库根目录")

    init = subparsers.add_parser("init", help="导入 manifest 与 YAML 配置")
    init.add_argument("--manifest", required=True)
    init.add_argument("--config", required=True)
    init.add_argument("--reset", action="store_true", help="清空该工作区运行时状态")

    run = subparsers.add_parser("run", help="执行分阶段流水线")
    run.add_argument("--stop-after-stage", choices=["frequency_generation", "block_rotation", "scoring_normalization"])

    subparsers.add_parser("resume", help="清除停止请求后继续执行")
    subparsers.add_parser("stop", help="请求优雅停止")

    status = subparsers.add_parser("status", help="查看会话、阶段、版本指纹与调度状态")
    status.add_argument("--shard-id")
    status.add_argument("--stage")
    status.add_argument("--compact", action="store_true")

    config = subparsers.add_parser("config", help="查看或修改编码配置")
    config_sub = config.add_subparsers(dest="config_command", required=True)
    config_sub.add_parser("show", help="显示当前配置")
    config_set = config_sub.add_parser("set", help="配置键=值，修改后默认增量重算")
    config_set.add_argument("assignments", nargs="+", metavar="KEY=VALUE")
    config_set.add_argument("--no-run", action="store_true")
    config_set.add_argument("--no-verify", action="store_true")

    query = subparsers.add_parser("query", help="查询旋转角度、打分摘要或权重统计")
    query.add_argument("query_type", choices=["angles", "score_summary", "weight_stats"])
    query.add_argument("--shard-id", required=True)
    query.add_argument("--position", type=int)
    query.add_argument("--channel", help="逗号分隔的通道索引")
    query.add_argument("--batch", type=int)
    query.add_argument("--head", type=int)
    query.add_argument("--query-position", type=int)
    query.add_argument("--top-k", type=int, default=8)
    query.add_argument("--latency-budget-ms", type=float)
    query.add_argument("--bypass-cache", action="store_true")

    subparsers.add_parser("verify", help="从零全量重算并比对当前增量产物")
    export = subparsers.add_parser("export", help="导出结果 ZIP")
    export.add_argument("--output")

    serve = subparsers.add_parser("serve", help="启动本地 FastAPI 服务")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8765)

    demo = subparsers.add_parser("demo", help="运行完整确定性演示")
    demo.add_argument("--base", default=".", help="仓库根目录")
    demo.add_argument("--keep-existing", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "generate-data":
            return _print(generate_demo_assets(args.base))

        if args.command == "demo":
            from .demo import run_demo

            return _print(run_demo(args.base, reset=not args.keep_existing))

        if args.command == "serve":
            import uvicorn

            app = create_app(args.workspace)
            uvicorn.run(app, host=args.host, port=args.port, log_level="info")
            return 0

        if args.command == "init":
            session = DiagnosticSession.create(
                args.workspace, args.manifest, args.config, reset=args.reset
            )
            return _print(session.status_payload())

        session = DiagnosticSession.load(args.workspace)
        if session is None:
            raise RopeError(
                "E_NOT_FOUND",
                "工作区没有会话，请先运行 init",
                {"workspace": str(Path(args.workspace).resolve())},
            )
        if args.command == "run":
            return _print(session.run(stop_after_stage=args.stop_after_stage))
        if args.command == "resume":
            return _print(session.resume())
        if args.command == "stop":
            return _print(session.request_stop(reason="cli_request"))
        if args.command == "status":
            return _print(
                session.status_payload(shard_id=args.shard_id, stage=args.stage),
                compact=args.compact,
            )
        if args.command == "config":
            if args.config_command == "show":
                return _print(
                    {"config": session.config.to_mapping(), "version": session.config.version}
                )
            patch = _parse_assignments(args.assignments)
            return _print(
                session.update_config(
                    patch,
                    run=not args.no_run,
                    verify=not args.no_verify,
                )
            )
        if args.command == "query":
            options: dict[str, Any] = {"top_k": args.top_k}
            if args.position is not None:
                options["position"] = args.position
            if args.channel is not None:
                options["channel"] = [int(value) for value in args.channel.split(",") if value]
            if args.batch is not None:
                options["batch"] = args.batch
            if args.head is not None:
                options["head"] = args.head
            if args.query_position is not None:
                options["query_position"] = args.query_position
            return _print(
                session.query(
                    shard_id=args.shard_id,
                    query_type=args.query_type,
                    options=options,
                    latency_budget_ms=args.latency_budget_ms,
                    bypass_cache=args.bypass_cache,
                )
            )
        if args.command == "verify":
            return _print(session.verify())
        if args.command == "export":
            return _print(session.export(args.output))
        parser.error("unknown command")
        return 2
    except RopeError as exc:
        print(json.dumps(exc.to_dict(), ensure_ascii=False, indent=2), file=sys.stderr)
        return 2


def _parse_assignments(assignments: list[str]) -> dict[str, Any]:
    values: dict[str, Any] = {}
    for assignment in assignments:
        if "=" not in assignment:
            raise RopeError(
                "E_INVALID_CONFIG",
                "配置项必须使用 KEY=VALUE 形式",
                {"assignment": assignment},
            )
        key, raw = assignment.split("=", 1)
        values[key.strip()] = _parse_scalar(key.strip(), raw.strip())
    return values


def _parse_scalar(key: str, value: str) -> Any:
    if key == "causal_mask":
        lowered = value.lower()
        if lowered in {"true", "1", "yes"}:
            return True
        if lowered in {"false", "0", "no"}:
            return False
        raise RopeError(
            "E_INVALID_CONFIG", "causal_mask 必须是布尔值", {"value": value}
        )
    if key in {"rotation_block_size", "memory_budget_bytes", "row_chunk_size"}:
        try:
            return int(value)
        except ValueError as exc:
            raise RopeError(
                "E_INVALID_CONFIG", f"{key} 必须是整数", {"value": value}
            ) from exc
    if key in {"base_frequency", "scaling_factor", "numeric_tolerance", "latency_budget_ms"}:
        try:
            return float(value)
        except ValueError as exc:
            raise RopeError(
                "E_INVALID_CONFIG", f"{key} 必须是数值", {"value": value}
            ) from exc
    return value


def _print(value: Any, *, compact: bool = False) -> int:
    if compact:
        print(json.dumps(value, ensure_ascii=False, separators=(",", ":")))
    else:
        print(json.dumps(value, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
