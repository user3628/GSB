from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from .canonical import GracefulStop, PipelineError, canonical_bytes
from .config import Config, apply_assignments
from .http_server import serve
from .pipeline import Pipeline
from .query import QueryService


def _emit(args: argparse.Namespace, payload: Any) -> None:
    if getattr(args, "pretty", False):
        text = json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
    else:
        text = canonical_bytes(payload).decode("utf-8")
    sys.stdout.write(text)
    sys.stdout.flush()


def _exit_for(payload: dict[str, Any]) -> int:
    code = payload.get("code")
    if code in (None, "OK"):
        return 0
    if code == "E_LATENCY_BUDGET":
        return 3
    if code == "E_STOPPED":
        return 4
    return 1


def _config_overrides(args: argparse.Namespace) -> dict[str, Any] | None:
    values: dict[str, Any] | None = None
    if getattr(args, "config_file", None):
        raw = json.loads(Path(args.config_file).read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raise PipelineError("E_INVALID_CONFIG", "配置文件必须是 JSON 对象。")
        values = raw
    if getattr(args, "set_values", None):
        base = values if values is not None else Config.defaults().to_dict()
        values = apply_assignments(base, args.set_values)
    return values


def _add_run_dir(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--run-dir", default=".pipeline-run", help="运行状态目录")


def _add_set(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--set",
        dest="set_values",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="覆盖配置，例如 sentence_support_threshold=0.81",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="halu-pipeline", description="本地 LLM 评测、幻觉检测与分层汇总管线"
    )
    parser.add_argument("--pretty", action="store_true", help="格式化输出 JSON")
    subparsers = parser.add_subparsers(dest="command", required=True)

    init = subparsers.add_parser("init", help="初始化并导入 JSONL 分片")
    _add_run_dir(init)
    init.add_argument("--shard", action="append", required=True, help="JSONL 分片，可重复")
    init.add_argument("--config-file")
    _add_set(init)

    import_cmd = subparsers.add_parser("import", help="导入/替换 JSONL 分片")
    _add_run_dir(import_cmd)
    import_cmd.add_argument("--shard", action="append", required=True)
    import_cmd.add_argument("--config-file")
    _add_set(import_cmd)

    config = subparsers.add_parser("config", help="查看或编辑配置")
    _add_run_dir(config)
    _add_set(config)

    run = subparsers.add_parser("run", help="执行四阶段管线")
    _add_run_dir(run)
    run.add_argument("--no-verify", action="store_true", help="跳过从零全量字节一致性校验")
    run.add_argument(
        "--until-stage",
        type=int,
        choices=(1, 2, 3, 4),
        default=4,
        help="只执行到指定阶段（用于分阶段触发与安全停止演示）",
    )

    status = subparsers.add_parser("status", help="查询阶段状态与版本指纹")
    _add_run_dir(status)

    sample = subparsers.add_parser("sample", help="查询样本判定依据")
    _add_run_dir(sample)
    sample.add_argument("--sample-id", required=True)
    sample.add_argument("--budget-ms", type=float)
    sample.add_argument("--query-id")
    sample.add_argument("--no-cache", action="store_true")

    summary = subparsers.add_parser("summary", help="查询分层汇总")
    _add_run_dir(summary)
    summary.add_argument("--source")
    summary.add_argument("--lang")
    summary.add_argument("--budget-ms", type=float)
    summary.add_argument("--query-id")
    summary.add_argument("--no-cache", action="store_true")

    validation = subparsers.add_parser("validation", help="查看人工标注一致性验证")
    _add_run_dir(validation)

    stop = subparsers.add_parser("stop", help="请求优雅停止")
    _add_run_dir(stop)

    resume = subparsers.add_parser("resume", help="清除停止请求并允许恢复")
    _add_run_dir(resume)

    export = subparsers.add_parser("export", help="导出完整汇总报告")
    _add_run_dir(export)
    export.add_argument("--output", default="artifacts/report.json")

    checkpoints = subparsers.add_parser("checkpoints", help="列出阶段检查点")
    _add_run_dir(checkpoints)

    cache = subparsers.add_parser("cache", help="查询或清空查询缓存")
    _add_run_dir(cache)
    cache.add_argument("--clear", action="store_true")

    server = subparsers.add_parser("serve", help="启动本地 HTTP 接口")
    _add_run_dir(server)
    server.add_argument("--host", default="127.0.0.1")
    server.add_argument("--port", type=int, default=8765)
    server.add_argument("--verbose", action="store_true")

    repl = subparsers.add_parser("repl", help="启动简单交互式查询 shell")
    _add_run_dir(repl)
    return parser


def _execute(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    if args.command in {"init", "import"}:
        pipeline = Pipeline(args.run_dir)
        overrides = _config_overrides(args)
        if args.command == "init":
            payload = pipeline.initialize(args.shard, overrides)
        else:
            payload = pipeline.import_shards(args.shard, overrides)
        return payload, 0

    if args.command == "config":
        pipeline = Pipeline(args.run_dir)
        if not args.set_values:
            config = pipeline.load_config()
            return {
                "code": "OK",
                "config": config.to_dict(),
                "config_version": config.version,
            }, 0
        return pipeline.update_config(assignments=args.set_values), 0

    if args.command == "run":
        return (
            Pipeline(args.run_dir).run(
                verify_incremental=not args.no_verify, until_stage=args.until_stage
            ),
            0,
        )

    if args.command == "status":
        return Pipeline(args.run_dir).status(), 0

    if args.command == "sample":
        payload = QueryService(args.run_dir).query_sample(
            args.sample_id,
            budget_ms=args.budget_ms,
            query_id=args.query_id,
            use_cache=not args.no_cache,
        )
        return payload, _exit_for(payload)

    if args.command == "summary":
        payload = QueryService(args.run_dir).query_summary(
            source=args.source,
            lang=args.lang,
            budget_ms=args.budget_ms,
            query_id=args.query_id,
            use_cache=not args.no_cache,
        )
        return payload, _exit_for(payload)

    if args.command == "validation":
        pipeline = Pipeline(args.run_dir)
        state = pipeline.load_state()
        if not pipeline._artifact_is_valid(4, state):
            raise PipelineError("E_STAGE_NOT_READY", "阶段 4 验证产物尚未完成。")
        return pipeline.load_stage_payload(4), 0

    if args.command == "stop":
        return Pipeline(args.run_dir).request_stop(), 0

    if args.command == "resume":
        return Pipeline(args.run_dir).resume(), 0

    if args.command == "export":
        pipeline = Pipeline(args.run_dir)
        digest = pipeline.write_report(args.output)
        return {"code": "OK", "output": args.output, "sha256": digest}, 0

    if args.command == "checkpoints":
        pipeline = Pipeline(args.run_dir)
        return {
            "code": "OK",
            "last_sequence": pipeline.checkpoints.last_sequence(),
            "checkpoints": pipeline.checkpoints.list(),
        }, 0

    if args.command == "cache":
        service = QueryService(args.run_dir)
        return (service.clear_cache() if args.clear else service.cache_stats()), 0

    if args.command == "serve":
        serve(args.run_dir, args.host, args.port, verbose=args.verbose)
        return {"code": "OK", "event": "server_stopped"}, 0

    if args.command == "repl":
        return _repl(args), 0
    raise PipelineError("E_INVALID_REQUEST", "未知命令。")


def _repl(args: argparse.Namespace) -> dict[str, Any]:
    pipeline = Pipeline(args.run_dir)
    service = QueryService(args.run_dir)
    print("halu-pipeline repl；输入 help 查看命令。", file=sys.stderr)
    while True:
        try:
            line = input("halu> ").strip()
        except EOFError:
            break
        if not line:
            continue
        if line in {"quit", "exit"}:
            break
        parts = line.split()
        command = parts[0]
        try:
            if command == "help":
                print("status | run | summary | sample ID | config KEY=VALUE | stop | resume | cache | quit", file=sys.stderr)
                continue
            if command == "status":
                payload = pipeline.status()
            elif command == "run":
                payload = pipeline.run()
            elif command == "summary":
                payload = service.query_summary()
            elif command == "sample" and len(parts) == 2:
                payload = service.query_sample(parts[1])
            elif command == "config" and len(parts) >= 2:
                payload = pipeline.update_config(assignments=parts[1:])
            elif command == "stop":
                payload = pipeline.request_stop()
            elif command == "resume":
                payload = pipeline.resume()
            elif command == "cache":
                payload = service.cache_stats()
            else:
                print("未知命令或参数不足。", file=sys.stderr)
                continue
            _emit(args, payload)
        except PipelineError as exc:
            _emit(args, exc.to_dict())
    return {"code": "OK", "event": "repl_exit"}


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        payload, exit_code = _execute(args)
    except GracefulStop as exc:
        payload, exit_code = exc.to_dict(), 4
    except PipelineError as exc:
        payload, exit_code = exc.to_dict(), _exit_for(exc.to_dict())
    except Exception as exc:
        payload = {
            "code": "E_INTERNAL",
            "message": "命令执行失败。",
            "details": {"error": f"{type(exc).__name__}: {exc}"},
        }
        exit_code = 1
    _emit(args, payload)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
