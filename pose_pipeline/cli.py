"""Command-line interface for the deterministic pose pipeline."""

from __future__ import annotations

import argparse
import sys
from typing import Any, Sequence

from .engine import PipelineEngine
from .errors import PipelineError
from .generator import generate_dataset
from .http_api import serve
from .util import pretty_json_bytes


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        result = _dispatch(args)
    except PipelineError as exc:
        sys.stderr.buffer.write(pretty_json_bytes(exc.to_dict()))
        return 2
    except (ValueError, OSError, RuntimeError) as exc:
        error = {
            "ok": False,
            "code": "E_OPERATION_FAILED",
            "message": str(exc),
            "details": {"exception": type(exc).__name__},
        }
        sys.stderr.buffer.write(pretty_json_bytes(error))
        return 2
    if result is not None:
        sys.stdout.buffer.write(pretty_json_bytes(result))
    return 0


def _dispatch(args: argparse.Namespace) -> Any:
    if args.command == "generate":
        return generate_dataset(args.out, seed=args.seed, width=args.width, height=args.height)
    if args.command == "serve":
        serve(
            workspace=args.workspace,
            host=args.host,
            port=args.port,
            config_path=args.config,
        )
        return None
    if args.command == "demo":
        from scripts.run_demo import run_demo

        return run_demo(args.out, seed=args.seed, quiet=args.quiet)

    engine = _engine(args)
    if args.command == "import":
        return engine.import_samples(args.manifest)
    if args.command == "build":
        if args.manifest:
            engine.import_samples(args.manifest)
        if args.strict_shard or args.strict_stage:
            if not args.strict_shard or not args.strict_stage:
                raise PipelineError(
                    "E_INVALID_STAGE",
                    "--strict-shard and --strict-stage must be supplied together",
                    details={"current_stage": None},
                )
            return engine.build(
                strict_shard_id=args.strict_shard,
                strict_stage=args.strict_stage,
            )
        return engine.build(
            to_stage=args.to_stage,
            stop_after_units=args.stop_after_units,
        )
    if args.command == "status":
        return engine.status()
    if args.command == "fingerprints":
        return engine.fingerprints()
    if args.command == "invalidations":
        return engine.invalidations(refresh=args.refresh)
    if args.command == "errors":
        return engine.errors()
    if args.command == "stop":
        return engine.request_stop()
    if args.command == "resume":
        if args.manifest:
            engine.import_samples(args.manifest)
        return engine.resume(
            to_stage=args.to_stage,
            stop_after_units=args.stop_after_units,
        )
    if args.command == "export":
        return engine.export(args.out)
    raise ValueError(f"unknown command: {args.command}")


def _engine(args: argparse.Namespace) -> PipelineEngine:
    return PipelineEngine(args.workspace, config=args.config)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pose-pipeline",
        description="Deterministic local keypoint and pose validation pipeline",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    generate = subparsers.add_parser("generate", help="generate deterministic demo data")
    generate.add_argument("--out", required=True)
    generate.add_argument("--seed", type=int, default=20261122)
    generate.add_argument("--width", type=int, default=128)
    generate.add_argument("--height", type=int, default=128)

    import_parser = subparsers.add_parser("import", help="import manifest and image shards")
    _add_workspace(import_parser)
    import_parser.add_argument("--manifest", required=True)
    _add_config(import_parser)

    build = subparsers.add_parser("build", help="build or incrementally update all stages")
    _add_workspace(build)
    _add_config(build)
    build.add_argument("--manifest", help="import this manifest before building")
    build.add_argument("--to-stage", choices=["preprocess", "decode", "assembly", "validation"])
    build.add_argument("--stop-after-units", type=int, help="deterministic test hook")
    build.add_argument("--strict-shard", help="strict single-stage shard id")
    build.add_argument("--strict-stage", help="strict single stage name")

    status = subparsers.add_parser("status", help="show stage status and checkpoints")
    _add_workspace(status)
    _add_config(status)

    fingerprints = subparsers.add_parser("fingerprints", help="show current version fingerprints")
    _add_workspace(fingerprints)
    _add_config(fingerprints)

    invalidations = subparsers.add_parser("invalidations", help="show invalidation reasons")
    _add_workspace(invalidations)
    _add_config(invalidations)
    invalidations.add_argument(
        "--refresh", action="store_true", help="recompute pending invalidation instead of reading the last report"
    )

    errors = subparsers.add_parser("errors", help="show persisted business errors")
    _add_workspace(errors)
    _add_config(errors)

    stop = subparsers.add_parser("stop", help="request graceful stop after current unit")
    _add_workspace(stop)
    _add_config(stop)

    resume = subparsers.add_parser("resume", help="clear stop request and resume")
    _add_workspace(resume)
    _add_config(resume)
    resume.add_argument("--manifest")
    resume.add_argument("--to-stage", choices=["preprocess", "decode", "assembly", "validation"])
    resume.add_argument("--stop-after-units", type=int)

    export = subparsers.add_parser("export", help="export poses and verification report")
    _add_workspace(export)
    _add_config(export)
    export.add_argument("--out")

    server = subparsers.add_parser("serve", help="run the local HTTP API")
    _add_workspace(server)
    _add_config(server)
    server.add_argument("--host", default="127.0.0.1")
    server.add_argument("--port", type=int, default=8765)

    demo = subparsers.add_parser("demo", help="run the full deterministic acceptance demo")
    demo.add_argument("--out", default="demo_workspace")
    demo.add_argument("--seed", type=int, default=20261122)
    demo.add_argument("--quiet", action="store_true")
    return parser


def _add_workspace(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--workspace", required=True)


def _add_config(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--config", help="pipeline configuration JSON path")


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
