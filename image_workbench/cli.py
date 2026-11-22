"""Command-line interface."""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any

from .demo import DEMO_SEED, generate_demo
from .errors import WorkbenchError
from .workbench import Workbench, import_assets


def _json(value: Any) -> None:
    print(json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2))


def _workbench(args: argparse.Namespace) -> Workbench:
    return Workbench(
        args.workspace,
        tasks_file=getattr(args, "tasks_file", None),
        config_file=getattr(args, "config_file", None),
        shards_dir=getattr(args, "shards_dir", None),
    )


def _run_summary_text(result: dict[str, Any]) -> str:
    status = result.get("status")
    if status == "stopped":
        return "stopped: completed checkpoints retained; use 'resume' to continue"
    if status == "partial":
        return f"partial: completed through stage {result.get('stage')}"
    counts = result.get("task_counts", {})
    summary = result.get("summary_sha256", "")
    return (
        "completed: "
        f"total={counts.get('total', 0)} accepted={counts.get('accepted', 0)} "
        f"verified={counts.get('verified', 0)} rejected_chain={counts.get('rejected_chain', 0)} "
        f"rejected_budget={counts.get('rejected_budget', 0)} "
        f"summary_sha256={summary}"
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="image-workbench",
        description="Deterministic local image-edit batch workbench",
    )
    parser.add_argument("--version", action="version", version="image-workbench 1.0.0")
    sub = parser.add_subparsers(dest="command", required=True)

    demo = sub.add_parser("generate-demo", help="generate the fixed local demo dataset")
    demo.add_argument("--out", default="demo")
    demo.add_argument("--seed", type=int, default=DEMO_SEED)

    imp = sub.add_parser("import", help="import local shards, tasks and config")
    imp.add_argument("--workspace", required=True)
    imp.add_argument("--shards", required=True)
    imp.add_argument("--tasks", required=True)
    imp.add_argument("--config", required=True)
    imp.add_argument("--json", action="store_true")

    plan = sub.add_parser("plan", help="show the deterministic budget plan")
    plan.add_argument("--workspace", default="demo")
    plan.add_argument("--tasks-file")
    plan.add_argument("--config-file")
    plan.add_argument("--shards-dir")

    budget = sub.add_parser("budget", help="alias of plan")
    budget.add_argument("--workspace", default="demo")
    budget.add_argument("--tasks-file")
    budget.add_argument("--config-file")
    budget.add_argument("--shards-dir")

    run = sub.add_parser("run", help="run or continue the staged pipeline")
    run.add_argument("--workspace", default="demo")
    run.add_argument("--tasks-file")
    run.add_argument("--config-file")
    run.add_argument("--shards-dir")
    run.add_argument(
        "--until-stage",
        default="exported",
        choices=["parsed", "executed", "size_checked", "exported"],
    )
    run.add_argument("--sequential", action="store_true", help="use one OS worker process")
    run.add_argument("--task-delay-ms", type=int, default=0)
    run.add_argument("--fresh", action="store_true", help="clear runtime checkpoints first")
    run.add_argument("--json", action="store_true")

    resume = sub.add_parser("resume", help="clear a stop request and continue")
    resume.add_argument("--workspace", default="demo")
    resume.add_argument("--tasks-file")
    resume.add_argument("--config-file")
    resume.add_argument("--shards-dir")
    resume.add_argument(
        "--until-stage",
        default="exported",
        choices=["parsed", "executed", "size_checked", "exported"],
    )
    resume.add_argument("--sequential", action="store_true")
    resume.add_argument("--task-delay-ms", type=int, default=0)
    resume.add_argument("--json", action="store_true")

    stop = sub.add_parser("stop", help="request graceful stop after current task")
    stop.add_argument("--workspace", default="demo")

    status = sub.add_parser("status", help="show task, checkpoint and worker status")
    status.add_argument("--workspace", default="demo")
    status.add_argument("--tasks-file")
    status.add_argument("--config-file")
    status.add_argument("--shards-dir")

    versions = sub.add_parser("versions", help="show fingerprints and invalidation history")
    versions.add_argument("--workspace", default="demo")
    versions.add_argument("--tasks-file")
    versions.add_argument("--config-file")
    versions.add_argument("--shards-dir")

    reports = sub.add_parser("reports", help="print final reports")
    reports.add_argument("--workspace", default="demo")

    serve = sub.add_parser("serve", help="start the local HTTP API")
    serve.add_argument("--workspace", default="demo")
    serve.add_argument("--tasks-file")
    serve.add_argument("--config-file")
    serve.add_argument("--shards-dir")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8765)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "generate-demo":
            _json(generate_demo(args.out, seed=args.seed))
            return 0
        if args.command == "import":
            result = import_assets(args.workspace, args.shards, args.tasks, args.config)
            if args.json:
                _json(result)
            else:
                print(f"imported {len(result['shards'])} shards into {result['workspace']}")
            return 0
        if args.command in {"plan", "budget"}:
            _json(_workbench(args).budget())
            return 0
        if args.command in {"run", "resume"}:
            workbench = _workbench(args)
            if args.command == "run" and args.fresh:
                workbench.reset()
            result = workbench.run(
                until_stage=args.until_stage,
                sequential=args.sequential,
                task_delay_ms=args.task_delay_ms,
                clear_stop=args.command == "resume",
            )
            if args.json:
                _json(result)
            else:
                print(_run_summary_text(result))
                if result.get("reports"):
                    for name, path in sorted(result["reports"].items()):
                        print(f"{name}: {path}")
            if result.get("status") == "stopped":
                return 2
            if result.get("status") == "completed" and not result.get("complete", True):
                return 3
            return 0
        if args.command == "stop":
            _json(Workbench(args.workspace).request_stop())
            return 0
        if args.command == "status":
            _json(_workbench(args).status())
            return 0
        if args.command == "versions":
            _json(_workbench(args).versions())
            return 0
        if args.command == "reports":
            _json(Workbench(args.workspace).reports())
            return 0
        if args.command == "serve":
            from .server import serve

            serve(
                workspace=args.workspace,
                host=args.host,
                port=args.port,
                tasks_file=args.tasks_file,
                config_file=args.config_file,
                shards_dir=args.shards_dir,
            )
            return 0
        parser.error("unknown command")
        return 1
    except WorkbenchError as exc:
        _json({"ok": False, "error": exc.to_dict()})
        return 1
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
