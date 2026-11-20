"""Command-line interface for the deterministic pipeline workbench."""

from __future__ import annotations

import argparse
import json
import signal
import sys
from pathlib import Path
from typing import Any

import yaml

from .demo import initialize_demo, run_demo
from .diagnostics import render_text, write_html_dashboard
from .errors import BusinessError, ErrorCode
from .pipeline import BuildHooks, BuildStopped, build_workspace, inspect_workspace, published_manifest, verify_against_full
from .workspace import update_shard


EXIT_CODES = {
    ErrorCode.E_INVALID_CONFIG: 2,
    ErrorCode.E_MISSING_REFERENCE: 3,
    ErrorCode.E_STALE_CHECKPOINT: 4,
    ErrorCode.E_INVALID_STAGE: 5,
    ErrorCode.E_INCONSISTENT_STATE: 6,
}


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except BusinessError as exc:
        if getattr(args, "json", False):
            print(json.dumps(exc.to_dict(), ensure_ascii=False, indent=2, sort_keys=True))
        else:
            location = ""
            if exc.shard:
                location += f" shard={exc.shard}"
            if exc.field:
                location += f" field={exc.field}"
            print(f"{exc.code.value}:{location} {exc.message}", file=sys.stderr)
            if exc.details:
                print(json.dumps(exc.details, ensure_ascii=False, indent=2, sort_keys=True), file=sys.stderr)
        return EXIT_CODES.get(exc.code, 1)
    except FileExistsError as exc:
        print(str(exc), file=sys.stderr)
        return 1


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="detdiff", description="本地确定性扩散管线工作台")
    subparsers = parser.add_subparsers(dest="command", required=True)

    init = subparsers.add_parser("init-demo", help="生成固定种子的演示图像与 YAML")
    init.add_argument("--workspace", type=Path, default=Path("demo_workspace"))
    init.add_argument("--reset", action="store_true")
    init.add_argument("--json", action="store_true")
    init.set_defaults(func=_cmd_init_demo)

    build = subparsers.add_parser("build", help="构建所有失效阶段并从最近一致检查点恢复")
    _add_build_hook_args(build)
    build.add_argument("--json", action="store_true")
    build.set_defaults(func=_cmd_build)

    status = subparsers.add_parser("status", help="只读查询阶段状态、指纹、失效链和 PNG 摘要")
    status.add_argument("--workspace", type=Path, default=Path("demo_workspace"))
    status.add_argument("--json", action="store_true")
    status.set_defaults(func=_cmd_status)

    diagnostics = subparsers.add_parser("diagnostics", help="输出命令行或静态 HTML 诊断快照")
    diagnostics.add_argument("--workspace", type=Path, default=Path("demo_workspace"))
    diagnostics.add_argument("--html", type=Path)
    diagnostics.add_argument("--json", action="store_true")
    diagnostics.set_defaults(func=_cmd_diagnostics)

    update = subparsers.add_parser("update", help="原子更新单个分片配置/参考图并仅重建失效链路")
    update.add_argument("--workspace", type=Path, default=Path("demo_workspace"))
    update.add_argument("--shard", required=True)
    update.add_argument("--patch", type=Path, help="包含单个分片字段的 YAML 映射")
    update.add_argument("--prompt")
    update.add_argument("--guidance-scale", type=float)
    update.add_argument("--steps", type=int)
    update.add_argument("--seed", type=int)
    update.add_argument("--blend-strength", type=float)
    update.add_argument("--reference", help="工作区内已有的引用路径，或 null 字符串移除")
    update.add_argument("--reference-file", type=Path, help="复制为内容寻址参考图后更新配置")
    update.add_argument("--remove-reference", action="store_true")
    update.add_argument("--no-build", action="store_true")
    update.add_argument("--json", action="store_true")
    update.set_defaults(func=_cmd_update)

    verify = subparsers.add_parser("verify", help="在临时目录从零全量构建并逐文件摘要比对")
    verify.add_argument("--workspace", type=Path, default=Path("demo_workspace"))
    verify.add_argument("--json", action="store_true")
    verify.set_defaults(func=_cmd_verify)

    demo = subparsers.add_parser("demo", help="执行停止/恢复、增量更新、全量比对与错误码探针")
    demo.add_argument("--workspace", type=Path, default=Path("demo_workspace"))
    demo.add_argument("--reset", action="store_true")
    demo.add_argument("--json", action="store_true")
    demo.set_defaults(func=_cmd_demo)
    return parser


def _add_build_hook_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--workspace", type=Path, default=Path("demo_workspace"))
    parser.add_argument("--stop-after-stage", choices=("encode", "schedule", "sample", "quantize"))
    parser.add_argument("--pause-file", type=Path, help="输入注入：该文件存在时在下一阶段边界停止")


def _cmd_init_demo(args: argparse.Namespace) -> int:
    root = initialize_demo(args.workspace, reset=args.reset)
    payload = {
        "ok": True,
        "workspace": str(root),
        "config": str(root / "config.yaml"),
        "shards": 4,
    }
    _emit(payload, args.json)
    return 0


def _cmd_build(args: argparse.Namespace) -> int:
    stop_state = {"requested": False, "signal": None}

    def handle_signal(signum: int, _frame: Any) -> None:
        stop_state["requested"] = True
        stop_state["signal"] = signal.Signals(signum).name

    previous_handlers: dict[int, Any] = {}
    for signum in (signal.SIGINT, signal.SIGTERM):
        previous_handlers[signum] = signal.getsignal(signum)
        signal.signal(signum, handle_signal)
    try:
        report = build_workspace(
            args.workspace,
            hooks=BuildHooks(
                stop_after_stage=args.stop_after_stage,
                pause_file=args.pause_file,
                should_stop=lambda: bool(stop_state["requested"]),
            ),
        )
    except BuildStopped as stopped:
        payload = stopped.report.to_dict()
        payload["ok"] = False
        payload["interrupted"] = True
        payload["resume_command"] = f"detdiff build --workspace {args.workspace}"
        _emit(payload, args.json)
        return 130
    finally:
        for signum, handler in previous_handlers.items():
            signal.signal(signum, handler)
    payload = report.to_dict()
    payload["ok"] = True
    _emit(payload, args.json)
    return 0


def _cmd_status(args: argparse.Namespace) -> int:
    from .config import load_config

    config = load_config(args.workspace.resolve() / "config.yaml")
    diagnostics = inspect_workspace(config)
    if args.json:
        payload = {
            "ok": True,
            "config_version": config.config_version,
            "shards": [item.to_dict() for item in diagnostics],
            "published_manifest": published_manifest(config),
        }
        print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    else:
        print(render_text(diagnostics))
    return 0


def _cmd_diagnostics(args: argparse.Namespace) -> int:
    from .config import load_config

    root = args.workspace.resolve()
    config = load_config(root / "config.yaml")
    diagnostics = inspect_workspace(config)
    if args.html:
        write_html_dashboard(diagnostics, args.html, config=config)
    if args.json:
        payload = {
            "ok": True,
            "html": str(args.html) if args.html else None,
            "shards": [item.to_dict() for item in diagnostics],
        }
        print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    else:
        print(render_text(diagnostics))
        if args.html:
            print(f"HTML diagnostics: {args.html}")
    return 0


def _cmd_update(args: argparse.Namespace) -> int:
    changes: dict[str, Any] = {}
    if args.patch:
        with args.patch.open("r", encoding="utf-8") as handle:
            patch = yaml.safe_load(handle)
        if not isinstance(patch, dict):
            raise BusinessError(
                ErrorCode.E_INVALID_CONFIG,
                "patch must be a YAML mapping",
                shard=args.shard,
                field="patch",
            )
        if "id" in patch:
            patch.pop("id")
        changes.update(patch)
    if args.prompt is not None:
        changes["prompt"] = args.prompt
    if args.guidance_scale is not None:
        changes["guidance_scale"] = args.guidance_scale
    if args.steps is not None:
        changes["steps"] = args.steps
    if args.seed is not None:
        changes["seed"] = args.seed
    if args.blend_strength is not None:
        changes["blend_strength"] = args.blend_strength
    if args.reference is not None:
        changes["reference"] = None if args.reference.lower() == "null" else args.reference
    if args.remove_reference:
        changes["reference"] = None

    result = update_shard(
        args.workspace,
        args.shard,
        changes,
        reference_file=args.reference_file,
        build=not args.no_build,
    )
    _emit(result, args.json)
    return 0


def _cmd_verify(args: argparse.Namespace) -> int:
    result = verify_against_full(args.workspace)
    _emit(result, args.json)
    return 0


def _cmd_demo(args: argparse.Namespace) -> int:
    result = run_demo(args.workspace, reset=args.reset)
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
        return 0
    print(f"workspace: {result['workspace']}")
    if "interrupted_build" in result:
        print(f"stopped after: {result['interrupted_build'].get('interrupted_after')}")
        print(f"next invalid stage: {result.get('resume_from')}")
    print(f"resume: rebuilt={result['resumed_build']['rebuilt']} verified={result['resumed_build']['consistency_verified']}")
    print(f"baseline full consistency: {result['baseline_full_consistency']}")
    print(f"incremental rebuilt: {result['incremental_update']['build']['rebuilt']}")
    print(f"post-update full consistency: {result['post_update_full_consistency']}")
    for key, value in result["error_code_probes"].items():
        print(f"probe {key}: {value['code']} shard={value.get('shard')} field={value.get('field')}")
    print(f"diagnostics: {result['diagnostics_html']}")
    print(f"report: {result['report_path']}")
    return 0


def _emit(payload: dict[str, Any], as_json: bool) -> None:
    if as_json:
        print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
        return
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    raise SystemExit(main())

