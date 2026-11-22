"""Command-line interface for the dependency engine."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from .config import AnalysisConfig
from .demo import run_demo
from .engine import Engine
from .errors import EngineError
from .generator import DEMO_SEED, generate_demo_data


def _print(value: Any) -> None:
    print(json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2))


def _engine(args: argparse.Namespace) -> Engine:
    return Engine(
        Path(args.root),
        state_dir=args.state_dir,
        views_dir=args.views_dir,
        config_path=args.config,
        manifest_path=args.manifest,
    )


def _add_workspace_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--root", default=".", help="workspace root (default: .)")
    parser.add_argument("--state-dir", default="runtime/state")
    parser.add_argument("--views-dir", default="runtime/views")
    parser.add_argument("--config", default="config/analysis.yaml")
    parser.add_argument("--manifest", default="data/manifest.json")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m dep_engine",
        description="Deterministic incremental dependency parsing engine",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    generate = subparsers.add_parser("generate", help="regenerate fixed demo inputs")
    generate.add_argument("--root", default=".")
    generate.add_argument("--seed", type=int, default=DEMO_SEED)

    build = subparsers.add_parser("build", help="build or resume all shards")
    _add_workspace_options(build)
    build.add_argument(
        "--stop-after-shards",
        type=int,
        default=None,
        help="stop gracefully after N complete shards",
    )

    resume = subparsers.add_parser("resume", help="alias for build")
    _add_workspace_options(resume)

    query = subparsers.add_parser("query", help="query persisted outputs")
    _add_workspace_options(query)
    query.add_argument(
        "kind",
        choices=[
            "trees",
            "diagnostics",
            "summary",
            "fingerprints",
            "invalidations",
            "checkpoints",
        ],
    )
    query.add_argument("--shard", type=int, default=None)

    replace = subparsers.add_parser("replace-shard", help="replace one shard and rebuild")
    _add_workspace_options(replace)
    replace.add_argument("--index", type=int, required=True)
    replace.add_argument("--file", required=True, help="UTF-8 replacement file")
    replace.add_argument("--no-build", action="store_true")

    update = subparsers.add_parser("update-config", help="replace config and rebuild")
    _add_workspace_options(update)
    update.add_argument("--file", required=True, help="UTF-8 YAML configuration file")
    update.add_argument("--no-build", action="store_true")

    run_stage = subparsers.add_parser(
        "run-stage", help="execute one stage after validating its prerequisite chain"
    )
    _add_workspace_options(run_stage)
    run_stage.add_argument("--shard", type=int, required=True)
    run_stage.add_argument("--stage", required=True)

    verify = subparsers.add_parser(
        "verify", help="compare incremental output with a from-scratch full build"
    )
    _add_workspace_options(verify)

    stop = subparsers.add_parser("stop", help="request graceful stop between shards")
    _add_workspace_options(stop)

    demo = subparsers.add_parser("demo", help="run the fixed stop/resume demonstration")
    demo.add_argument("--root", default=".")

    return parser


def dispatch(args: argparse.Namespace) -> dict[str, Any]:
    if args.command == "generate":
        manifest = generate_demo_data(Path(args.root), seed=args.seed)
        return {"ok": True, "manifest": manifest}
    if args.command == "demo":
        return run_demo(Path(args.root))
    if args.command == "build":
        return _engine(args).build(stop_after_shards=args.stop_after_shards)
    if args.command == "resume":
        return _engine(args).build()
    if args.command == "query":
        engine = _engine(args)
        if args.kind == "trees":
            return engine.get_trees(shard_index=args.shard)
        if args.kind == "diagnostics":
            return engine.get_diagnostics(shard_index=args.shard)
        if args.kind == "summary":
            return engine.get_summary()
        if args.kind == "fingerprints":
            return engine.get_fingerprints()
        if args.kind == "invalidations":
            return engine.get_invalidations()
        if args.kind == "checkpoints":
            return engine.checkpoint_summary()
        raise AssertionError(args.kind)
    if args.command == "replace-shard":
        engine = _engine(args)
        path = Path(args.file)
        if not path.is_absolute():
            path = Path(args.root).resolve() / path
        text = path.read_text(encoding="utf-8")
        return engine.replace_shard(
            args.index,
            text,
            build=not args.no_build,
        )
    if args.command == "update-config":
        engine = _engine(args)
        path = Path(args.file)
        if not path.is_absolute():
            path = Path(args.root).resolve() / path
        config = AnalysisConfig.from_yaml(path)
        return engine.update_config(config, build=not args.no_build)
    if args.command == "run-stage":
        return _engine(args).run_stage(args.shard, args.stage)
    if args.command == "verify":
        return _engine(args).verify_against_full()
    if args.command == "stop":
        engine = _engine(args)
        engine.request_stop()
        return {
            "ok": True,
            "stop_requested": True,
            "message": "the current shard will finish, then the build will stop",
        }
    raise AssertionError(args.command)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        result = dispatch(args)
        _print(result)
        return 0
    except EngineError as exc:
        _print(exc.to_dict())
        return 2
    except (OSError, ValueError) as exc:
        _print(
            {
                "ok": False,
                "error": {
                    "code": "E_IO",
                    "message": str(exc),
                    "details": {},
                },
            }
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
