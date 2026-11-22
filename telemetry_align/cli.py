"""Command-line interface for the local telemetry alignment workbench."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from .demo import run_demo
from .errors import TelemetryError, error_payload
from .http_api import serve
from .workspace import AlignmentWorkspace


def _print_json(value: Any) -> None:
    print(json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2))


def _kv_pairs(values: list[str] | None, label: str) -> dict[str, str]:
    result: dict[str, str] = {}
    for value in values or []:
        if "=" not in value:
            raise TelemetryError(
                "E_CONFIG_INVALID",
                f"{label} 必须使用 name=path / expected name=path",
                {"value": value},
            )
        name, path = value.split("=", 1)
        result[name] = path
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="telemetry-align",
        description="多源时序遥测增量对齐工作台 / Multi-source telemetry alignment workbench",
    )
    parser.add_argument(
        "--root",
        default="demo/workspace",
        help="工作区目录，默认 demo/workspace / workspace root",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init-demo", help="生成确定性三源样本与 V1/V2 配置")

    align = sub.add_parser("align", help="运行或继续四阶段对齐")
    align.add_argument("--config", choices=["v1", "v2"], default=None)
    align.add_argument("--resume", action="store_true")
    align.add_argument(
        "--until-stage",
        choices=["normalize", "resample", "fusion", "summary"],
        default=None,
    )

    sub.add_parser("status", help="查看阶段状态、版本指纹与检查点")
    sub.add_parser("fingerprints", help="查询当前各阶段版本指纹")
    sub.add_parser("conflicts", help="查看多源冲突裁决明细")
    sub.add_parser("gaps", help="查看缺口明细")
    sub.add_parser("cleaning", help="查看归一化清洗报告")
    sub.add_parser("summary", help="输出对齐摘要")
    sub.add_parser("checkpoints", help="查看检查点账本")
    sub.add_parser("verify", help="校验检查点、锚点与产物摘要")

    export = sub.add_parser("export", help="导出对齐摘要")
    export.add_argument("--output", default="out/alignment_summary.json")

    stop = sub.add_parser("stop", help="请求优雅停止")
    stop.add_argument(
        "--after-stage",
        choices=["normalize", "resample", "fusion", "summary"],
        default=None,
    )

    resume = sub.add_parser("resume", help="恢复最近一次停止的运行")
    resume.add_argument("--config", choices=["v1", "v2"], default=None)

    mutate = sub.add_parser("mutate", help="修改一个源并执行增量重建")
    mutate.add_argument("--sensor", default="sensor_a")
    mutate.add_argument("--config", choices=["v1", "v2"], default=None)

    upgrade = sub.add_parser("upgrade-v1", help="显式升级 V1 配置为 V2")
    upgrade.add_argument("--config", default="v1")
    upgrade.add_argument("--output", default="out/upgraded_config_v2.yaml")

    imp = sub.add_parser("import", help="导入三源分片与 V1/V2 配置")
    imp.add_argument("--source", action="append", help="sensor_id=path，可重复")
    imp.add_argument("--config-file", action="append", help="v1=path 或 v2=path")
    imp.add_argument("--active", choices=["v1", "v2"], default=None)

    server = sub.add_parser("serve", help="启动本地 HTTP 控制台")
    server.add_argument("--host", default="127.0.0.1")
    server.add_argument("--port", type=int, default=8765)
    server.add_argument("--quiet", action="store_true")

    demo = sub.add_parser("demo", help="运行可重复完整演示")
    demo.add_argument("--output-root", default="demo")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "demo":
            report = run_demo(args.output_root)
            _print_json(
                {
                    "ok": True,
                    "report_path": report["report_path"],
                    "fixed_summary_digest": report["fixed_summary_digest"],
                    "initial": report["initial"],
                    "incremental": report["incremental"],
                    "stop_resume": report["stop_resume"],
                }
            )
            return 0

        workspace = AlignmentWorkspace(args.root)
        if args.command == "init-demo":
            _print_json(workspace.init_demo(reset=True))
        elif args.command == "align":
            _print_json(
                workspace.align(
                    config_name=args.config,
                    resume=args.resume,
                    until_stage=args.until_stage,
                )
            )
        elif args.command == "status":
            _print_json(workspace.status())
        elif args.command == "fingerprints":
            _print_json(workspace.fingerprints())
        elif args.command == "conflicts":
            _print_json(workspace.conflicts())
        elif args.command == "gaps":
            _print_json(workspace.gaps())
        elif args.command == "cleaning":
            _print_json(workspace.cleaning())
        elif args.command == "summary":
            _print_json(workspace.summary())
        elif args.command == "checkpoints":
            _print_json({"ok": True, "checkpoints": workspace.store.summary()})
        elif args.command == "verify":
            _print_json({"ok": True, "verification": workspace.store.verify()})
        elif args.command == "export":
            _print_json(workspace.export_summary(output_path=args.output))
        elif args.command == "stop":
            _print_json({"ok": True, "stop": workspace.request_stop(args.after_stage)})
        elif args.command == "resume":
            _print_json(workspace.align(config_name=args.config, resume=True))
        elif args.command == "mutate":
            _print_json(
                workspace.mutate_source_and_rebuild(
                    sensor_id=args.sensor, config_name=args.config
                )
            )
        elif args.command == "upgrade-v1":
            _print_json(
                workspace.upgrade_v1_config(
                    config_name=args.config, output_path=args.output
                )
            )
        elif args.command == "import":
            _print_json(
                workspace.import_inputs(
                    sources=_kv_pairs(args.source, "--source"),
                    configs=_kv_pairs(args.config_file, "--config-file"),
                    active_config=args.active,
                )
            )
        elif args.command == "serve":
            serve(
                args.root,
                host=args.host,
                port=args.port,
                quiet=args.quiet,
            )
        else:  # pragma: no cover
            parser.error("unknown command")
        return 0
    except TelemetryError as exc:
        _print_json(exc.to_dict())
        return 2
    except KeyboardInterrupt:
        return 130
    except Exception as exc:  # pragma: no cover - defensive CLI boundary
        _print_json(error_payload(exc))
        return 1

