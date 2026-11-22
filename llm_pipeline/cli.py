"""Command-line interface for the local pipeline."""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
from pathlib import Path
from typing import Any

from .engine import PipelineEngine
from .errors import E_STOPPED, PipelineError
from .generator import generate_demo_assets
from .workspace import Workspace


def _json_value(text: str) -> Any:
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text


def _print(payload: Any) -> None:
    sys.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n")


def _workspace(args: argparse.Namespace) -> Workspace:
    return Workspace(args.workspace)


def _install_stop_handlers(workspace: Workspace) -> None:
    def handle(signum: int, frame: Any) -> None:  # noqa: ANN001
        workspace.request_stop(reason=f"signal-{signum}")

    try:
        signal.signal(signal.SIGTERM, handle)
        signal.signal(signal.SIGINT, handle)
    except ValueError:
        # Not in the main thread (for example when invoked from a test worker).
        pass


def _add_workspace(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--workspace",
        default=os.environ.get("LLM_PIPELINE_WORKSPACE", "runtime"),
        help="workspace directory (default: runtime or $LLM_PIPELINE_WORKSPACE)",
    )


def _add_stop_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--stop-after-samples",
        type=int,
        default=None,
        help="deterministic test hook: stop after N indivisible work units",
    )
    parser.add_argument(
        "--delay-ms",
        type=int,
        default=0,
        help="optional delay after each work unit (useful for stop tests)",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pipeline.py",
        description="Deterministic LLM pretraining data mixing and incremental dedup pipeline",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("init-demo", help="generate fixed-seed demo shards and initialize a workspace")
    _add_workspace(p)
    p.add_argument("--demo-dir", default="demo")
    p.add_argument("--force", action="store_true")

    p = sub.add_parser("generate-demo", help="regenerate deterministic demo assets only")
    p.add_argument("--output-dir", default="demo")

    p = sub.add_parser("demo", help="init demo, run all stages, and verify incremental/full equality")
    _add_workspace(p)
    p.add_argument("--demo-dir", default="demo")
    p.add_argument("--force", action="store_true")

    p = sub.add_parser("run", help="build or incrementally rebuild all affected stages")
    _add_workspace(p)
    p.add_argument("--force", action="store_true", help="rebuild every stage even if fingerprints match")
    _add_stop_options(p)

    p = sub.add_parser("run-stage", help="build one stage if its prerequisites are current")
    _add_workspace(p)
    p.add_argument("stage", help="normalize|allocate|assemble|validate")
    _add_stop_options(p)

    p = sub.add_parser("status", help="show stages, fingerprints, checkpoint sequence, and invalidation")
    _add_workspace(p)

    p = sub.add_parser("fingerprints", help="show compact stage/version fingerprints")
    _add_workspace(p)

    p = sub.add_parser("import-shard", help="atomically import a new JSONL shard")
    _add_workspace(p)
    p.add_argument("--name", required=True)
    p.add_argument("--path", required=True)
    p.add_argument("--run", action="store_true", help="run incremental update after import")

    p = sub.add_parser("replace-shard", help="replace one shard and incrementally rebuild")
    _add_workspace(p)
    p.add_argument("--name", required=True)
    p.add_argument("--path", required=True)
    p.add_argument("--no-run", action="store_true")

    p = sub.add_parser("config-change", help="apply one config patch and incrementally rebuild")
    _add_workspace(p)
    p.add_argument("--patch", default=None, help="JSON object deep-merged into config")
    p.add_argument("--patch-file", default=None, help="path to a JSON patch object")
    p.add_argument(
        "--set",
        action="append",
        default=[],
        metavar="KEY=JSON",
        help="set one dotted key, for example allocation.target_samples=30",
    )
    p.add_argument("--no-run", action="store_true")

    p = sub.add_parser("stop", help="write a cooperative stop request")
    _add_workspace(p)
    p.add_argument("--reason", default="cli")

    p = sub.add_parser("resume", help="clear a stop request and resume from the latest checkpoint")
    _add_workspace(p)
    _add_stop_options(p)

    p = sub.add_parser("export-mixed", help="export the assembled mixed set as canonical JSONL")
    _add_workspace(p)
    p.add_argument("--output", required=True)

    p = sub.add_parser("report", help="print the validation report")
    _add_workspace(p)
    p.add_argument("--output", default=None, help="optionally write canonical report JSON to a file")

    p = sub.add_parser("verify-incremental", help="byte-compare incremental products with a fresh full build")
    _add_workspace(p)
    p.add_argument("--no-run", action="store_true", help="do not rebuild stale stages first")

    p = sub.add_parser("serve", help="run the local HTTP API")
    _add_workspace(p)
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8765)

    return parser


def _parse_config_args(args: argparse.Namespace) -> tuple[dict[str, Any], dict[str, Any]]:
    patch: dict[str, Any] = {}
    if args.patch:
        parsed = json.loads(args.patch)
        if not isinstance(parsed, dict):
            raise ValueError("--patch must be a JSON object")
        patch = parsed
    if args.patch_file:
        parsed = json.loads(Path(args.patch_file).read_text(encoding="utf-8"))
        if not isinstance(parsed, dict):
            raise ValueError("--patch-file must contain a JSON object")
        patch = _deep_merge(patch, parsed)
    sets: dict[str, Any] = {}
    for item in args.set:
        if "=" not in item:
            raise ValueError(f"--set requires KEY=JSON, got {item!r}")
        key, raw = item.split("=", 1)
        sets[key] = _json_value(raw)
    return patch, sets


def _deep_merge(left: dict[str, Any], right: dict[str, Any]) -> dict[str, Any]:
    from .util import deep_merge

    return deep_merge(left, right)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "generate-demo":
            _print(generate_demo_assets(args.output_dir))
            return 0
        if args.command == "init-demo":
            workspace = _workspace(args)
            manifest = workspace.initialize_demo(args.demo_dir, force=args.force)
            _print(
                {
                    "ok": True,
                    "workspace": str(workspace.root),
                    "config": str(workspace.config_path),
                    "shards": manifest["shards"],
                    "seed": manifest["seed"],
                }
            )
            return 0
        if args.command == "demo":
            workspace = _workspace(args)
            workspace.initialize_demo(args.demo_dir, force=args.force)
            engine = PipelineEngine(workspace)
            run_result = engine.run(force=True)
            verify_result = engine.verify_incremental(run_if_needed=False)
            _print(
                {
                    "ok": True,
                    "workspace": str(workspace.root),
                    "run": run_result,
                    "verification": verify_result,
                }
            )
            return 0
        if args.command == "serve":
            from .server import serve

            workspace = _workspace(args)
            if not workspace.initialized():
                workspace.initialize()
            serve(workspace, host=args.host, port=args.port)
            return 0

        workspace = _workspace(args)
        engine = PipelineEngine(workspace)
        if args.command == "run":
            _install_stop_handlers(workspace)
            _print(
                engine.run(
                    force=args.force,
                    stop_after_samples=args.stop_after_samples,
                    delay_ms=args.delay_ms,
                )
            )
            return 0
        if args.command == "run-stage":
            _install_stop_handlers(workspace)
            _print(
                engine.run_stage(
                    args.stage,
                    stop_after_samples=args.stop_after_samples,
                    delay_ms=args.delay_ms,
                )
            )
            return 0
        if args.command == "status":
            _print(engine.status())
            return 0
        if args.command == "fingerprints":
            _print(engine.fingerprints())
            return 0
        if args.command == "import-shard":
            result: dict[str, Any] = {
                "ok": True,
                "import": engine.import_shard(args.name, args.path),
            }
            if args.run:
                result["run"] = engine.run()
            _print(result)
            return 0
        if args.command == "replace-shard":
            result = {
                "ok": True,
                "replace": engine.replace_shard(args.name, args.path),
            }
            if not args.no_run:
                result["run"] = engine.run()
            _print(result)
            return 0
        if args.command == "config-change":
            patch, sets = _parse_config_args(args)
            result = {"ok": True, "change": engine.change_config(patch=patch, set_values=sets)}
            if not args.no_run:
                result["run"] = engine.run()
            _print(result)
            return 0
        if args.command == "stop":
            _print({"ok": True, "stop": workspace.request_stop(reason=args.reason)})
            return 0
        if args.command == "resume":
            _install_stop_handlers(workspace)
            _print(
                engine.resume(
                    stop_after_samples=args.stop_after_samples,
                    delay_ms=args.delay_ms,
                )
            )
            return 0
        if args.command == "export-mixed":
            _print(engine.export_mixed(args.output))
            return 0
        if args.command == "report":
            if args.output:
                _print(engine.export_report(args.output))
            else:
                _print(engine.report())
            return 0
        if args.command == "verify-incremental":
            _print(engine.verify_incremental(run_if_needed=not args.no_run))
            return 0
        parser.error(f"unknown command: {args.command}")
        return 2
    except PipelineError as exc:
        _print(exc.to_dict())
        return 2 if exc.code == E_STOPPED else 1
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        _print({"ok": False, "code": "E_INVALID_INPUT", "message": str(exc)})
        return 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
