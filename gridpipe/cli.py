"""Command line entry point."""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path
from typing import Any, Optional, Sequence

from .canonical import read_json
from .errors import PipelineError
from .export import write_exports
from .generator import generate_samples
from .importer import import_inputs
from .pipeline import Pipeline


def _print(value: Any, *, compact: bool = False) -> None:
    if compact:
        print(json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
    else:
        print(json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2))


def _root_path(value: str) -> Path:
    return Path(value).expanduser().resolve()


def _pipeline_from_args(args: argparse.Namespace) -> Pipeline:
    root = _root_path(args.root)
    manifest = Path(args.manifest)
    if not manifest.is_absolute():
        manifest = root / manifest
    work = Path(args.work)
    if not work.is_absolute():
        work = root / work
    output = Path(args.output)
    if not output.is_absolute():
        output = root / output
    overrides = None
    if getattr(args, "config", None):
        overrides = read_json(Path(args.config))
    return Pipeline(root, manifest, work_dir=work, output_dir=output, config_overrides=overrides)


def _add_common(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--root", default=".", help="project root (default: current directory)")
    parser.add_argument("--manifest", default="data/manifest.json")
    parser.add_argument("--work", default="work")
    parser.add_argument("--output", default="output")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="gridpipe", description="Deterministic table/chart pipeline")
    subparsers = parser.add_subparsers(dest="command", required=True)

    init_parser = subparsers.add_parser("init", help="create data/work/output directories")
    init_parser.add_argument("--root", default=".")
    init_parser.add_argument("--clean-work", action="store_true")

    generate_parser = subparsers.add_parser("generate", help="generate deterministic samples")
    generate_parser.add_argument("--root", default=".")
    generate_parser.add_argument("--data", default="data")
    generate_parser.add_argument("--seed", type=int, default=20261122)
    generate_parser.set_defaults(handler=_handle_generate)

    import_parser = subparsers.add_parser("import", help="validate and register local inputs")
    _add_common(import_parser)
    import_parser.add_argument("--copy-to")
    import_parser.set_defaults(handler=_handle_import)

    run_parser = subparsers.add_parser("run", help="execute one stage or all stages")
    _add_common(run_parser)
    run_parser.add_argument("--stage", default="all", choices=["all", "detection", "table", "chart", "validation"])
    run_parser.add_argument("--resume", action="store_true")
    run_parser.add_argument("--fresh", action="store_true")
    run_parser.add_argument("--stop-after", type=int)
    run_parser.add_argument("--workers", type=int)
    run_parser.add_argument("--config")
    run_parser.add_argument("--json", action="store_true")
    run_parser.set_defaults(handler=_handle_run)

    resume_parser = subparsers.add_parser("resume", help="resume the current deterministic run")
    _add_common(resume_parser)
    resume_parser.add_argument("--stage", default="all", choices=["all", "detection", "table", "chart", "validation"])
    resume_parser.add_argument("--stop-after", type=int)
    resume_parser.add_argument("--workers", type=int)
    resume_parser.add_argument("--config")
    resume_parser.add_argument("--json", action="store_true")
    resume_parser.set_defaults(handler=_handle_resume)

    for name, handler in (
        ("status", _handle_status),
        ("fingerprints", _handle_fingerprints),
        ("budget", _handle_budget),
        ("stop", _handle_stop),
        ("export", _handle_export),
    ):
        command = subparsers.add_parser(name)
        _add_common(command)
        command.add_argument("--config")
        command.add_argument("--json", action="store_true")
        command.set_defaults(handler=handler)

    serve_parser = subparsers.add_parser("serve", help="start the local HTTP service")
    _add_common(serve_parser)
    serve_parser.add_argument("--host", default="127.0.0.1")
    serve_parser.add_argument("--port", type=int, default=8765)
    serve_parser.add_argument("--config")
    serve_parser.set_defaults(handler=_handle_serve)

    demo_parser = subparsers.add_parser("demo", help="run full, serial, stop/resume and error probes")
    demo_parser.add_argument("--root", default=".")
    demo_parser.add_argument("--seed", type=int, default=20261122)
    demo_parser.set_defaults(handler=_handle_demo)
    return parser


def _handle_generate(args: argparse.Namespace) -> int:
    root = _root_path(args.root)
    data = Path(args.data)
    if not data.is_absolute():
        data = root / data
    summary = generate_samples(data, seed=args.seed)
    _print(summary)
    return 0


def _handle_import(args: argparse.Namespace) -> int:
    root = _root_path(args.root)
    manifest = Path(args.manifest)
    if not manifest.is_absolute():
        manifest = root / manifest
    record = import_inputs(
        root,
        manifest,
        work_dir=root / args.work,
        output_dir=root / args.output,
        copy_to=args.copy_to,
    )
    _print(record)
    return 0


def _handle_run(args: argparse.Namespace) -> int:
    pipeline = _pipeline_from_args(args)
    result = pipeline.run(
        stage=args.stage,
        resume=bool(args.resume),
        fresh=bool(args.fresh),
        stop_after=args.stop_after,
        execution_workers=args.workers,
    )
    _print(result, compact=args.json)
    return 0


def _handle_resume(args: argparse.Namespace) -> int:
    pipeline = _pipeline_from_args(args)
    result = pipeline.run(
        stage=args.stage,
        resume=True,
        stop_after=args.stop_after,
        execution_workers=args.workers,
    )
    _print(result, compact=args.json)
    return 0


def _handle_status(args: argparse.Namespace) -> int:
    _print(_pipeline_from_args(args).status(), compact=args.json)
    return 0


def _handle_fingerprints(args: argparse.Namespace) -> int:
    _print(_pipeline_from_args(args).fingerprints(), compact=args.json)
    return 0


def _handle_budget(args: argparse.Namespace) -> int:
    _print(_pipeline_from_args(args).budget_report(), compact=args.json)
    return 0


def _handle_stop(args: argparse.Namespace) -> int:
    _print(_pipeline_from_args(args).request_stop(), compact=args.json)
    return 0


def _handle_export(args: argparse.Namespace) -> int:
    pipeline = _pipeline_from_args(args)
    _print(write_exports(pipeline), compact=args.json)
    return 0


def _handle_serve(args: argparse.Namespace) -> int:
    from .http_api import create_server

    root = _root_path(args.root)
    manifest = Path(args.manifest)
    if not manifest.is_absolute():
        manifest = root / manifest
    pipeline_args = argparse.Namespace(**vars(args))
    pipeline = _pipeline_from_args(pipeline_args)
    overrides = read_json(Path(args.config)) if args.config else None
    server = create_server(
        root,
        manifest,
        pipeline.work_dir,
        pipeline.output_dir,
        host=args.host,
        port=args.port,
        config_overrides=overrides,
    )
    host, port = server.server_address[:2]
    _print({"ok": True, "host": host, "port": int(port), "pid": None}, compact=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


def _handle_demo(args: argparse.Namespace) -> int:
    from .demo import run_demo

    result = run_demo(_root_path(args.root), seed=args.seed)
    _print(result)
    return 0 if result.get("passed") else 2


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "init":
        root = _root_path(args.root)
        for name in ("data", "work", "output"):
            (root / name).mkdir(parents=True, exist_ok=True)
        if args.clean_work:
            work_path = root / "work"
            if work_path.exists():
                shutil.rmtree(work_path)
            work_path.mkdir(parents=True, exist_ok=True)
        _print(
            {
                "ok": True,
                "root": str(root),
                "directories": ["data", "work", "output"],
                "work_cleaned": bool(args.clean_work),
            }
        )
        return 0
    try:
        return int(args.handler(args))
    except PipelineError as exc:
        _print({"ok": False, "error": exc.to_dict()})
        return 2
    except KeyboardInterrupt:
        return 130
    except Exception as exc:
        _print(
            {
                "ok": False,
                "error": {
                    "code": "E_UNHANDLED",
                    "message": str(exc),
                    "diagnostics": {"exception_type": type(exc).__name__},
                },
            }
        )
        return 1
