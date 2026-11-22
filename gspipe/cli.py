"""Command-line interface for the local deterministic editing pipeline."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Sequence

from .canonical import canonical_json_bytes
from .config import PipelineConfig, load_config
from .errors import E_INVALID_INPUT, E_INVALID_TASKS, PipelineError
from .fsutil import atomic_write_bytes, ensure_dir
from .models import load_tasks, tasks_to_manifest
from .pipeline import Pipeline
from .sample_data import generate_samples
from .workspace import Workspace


DEFAULT_WORKSPACE = os.environ.get("GSPIPE_WORKSPACE", "workspace")


def _workspace(args: argparse.Namespace) -> Workspace:
    workspace = Workspace(args.workspace)
    workspace.ensure_layout()
    return workspace


def _print_json(value: Any, *, pretty: bool = True) -> None:
    if pretty:
        text = json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True)
    else:
        text = canonical_json_bytes(value).decode("utf-8")
    print(text)


def _pipeline(args: argparse.Namespace, *, on_progress=None) -> Pipeline:
    workspace = _workspace(args)
    config: PipelineConfig | None = None
    config_path = getattr(args, "config", None)
    if config_path:
        config = load_config(config_path)
    return Pipeline(workspace.root, config=config, on_progress=on_progress)


def cmd_write_samples(args: argparse.Namespace) -> int:
    output = Path(args.output).resolve()
    shards_dir = output / "shards"
    ensure_dir(shards_dir)
    shards, tasks_manifest, config = generate_samples()
    for name, data in sorted(shards.items()):
        atomic_write_bytes(shards_dir / name, data)
    atomic_write_bytes(output / "tasks.json", canonical_json_bytes(tasks_manifest) + b"\n")
    atomic_write_bytes(output / "config.json", canonical_json_bytes(config) + b"\n")
    _print_json(
        {
            "ok": True,
            "output": str(output),
            "shard_count": len(shards),
            "task_count": len(tasks_manifest["tasks"]),
            "seed": 20261123,
        }
    )
    return 0


def cmd_init_demo(args: argparse.Namespace) -> int:
    workspace = _workspace(args)
    result = workspace.init_demo(force=args.force)
    _print_json(result)
    return 0


def cmd_import(args: argparse.Namespace) -> int:
    workspace = _workspace(args)
    shards_dir = Path(args.shards_dir).resolve()
    if not shards_dir.is_dir():
        raise PipelineError(E_INVALID_INPUT, f"Shard directory does not exist: {shards_dir}")
    shards: dict[str, bytes] = {}
    for path in sorted(shards_dir.glob("*.png")):
        if path.name not in shards:
            shards[path.name] = path.read_bytes()
    if not shards:
        raise PipelineError(E_INVALID_INPUT, "No PNG shards found for import")
    try:
        tasks_manifest = json.loads(Path(args.tasks).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PipelineError(E_INVALID_TASKS, f"Unable to read tasks for import: {exc}") from exc
    config_override: dict[str, Any] | None = None
    if args.config:
        try:
            loaded = json.loads(Path(args.config).read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise PipelineError(E_INVALID_INPUT, f"Unable to read config for import: {exc}") from exc
        if not isinstance(loaded, dict):
            raise PipelineError(E_INVALID_INPUT, "Config override must be a JSON object")
        config_override = loaded
    result = workspace.import_data(shards, tasks_manifest, config_override=config_override)
    _print_json(result)
    return 0


def cmd_run(args: argparse.Namespace) -> int:
    pipeline = _pipeline(args)
    result = pipeline.run(args.stages, resume=args.resume, parallel=not args.sequential)
    _print_json(result)
    return 0


def cmd_resume(args: argparse.Namespace) -> int:
    pipeline = _pipeline(args)
    result = pipeline.run(args.stages, resume=True, parallel=not args.sequential)
    _print_json(result)
    return 0


def cmd_stop(args: argparse.Namespace) -> int:
    workspace = _workspace(args)
    _print_json(workspace.request_stop(args.reason))
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    pipeline = _pipeline(args)
    _print_json(pipeline.status())
    return 0


def cmd_fingerprints(args: argparse.Namespace) -> int:
    pipeline = _pipeline(args)
    _print_json(pipeline.fingerprints())
    return 0


def cmd_validate(args: argparse.Namespace) -> int:
    pipeline = _pipeline(args)
    _print_json(pipeline.validate_task(args.task_id))
    return 0


def cmd_query(args: argparse.Namespace) -> int:
    pipeline = _pipeline(args)
    payload, cache_hit = pipeline.query_task(args.task_id, force_recompute=args.recompute)
    if args.json:
        sys.stdout.buffer.write(payload)
        if not payload.endswith(b"\n"):
            sys.stdout.buffer.write(b"\n")
    else:
        value = json.loads(payload.decode("utf-8"))
        value["_cache_hit"] = cache_hit
        _print_json(value)
    return 0


def cmd_export(args: argparse.Namespace) -> int:
    pipeline = _pipeline(args)
    result = pipeline.export(parallel=not args.sequential)
    _print_json(result)
    return 0


def cmd_config(args: argparse.Namespace) -> int:
    workspace = _workspace(args)
    if args.config_file:
        try:
            raw = json.loads(Path(args.config_file).read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise PipelineError(E_INVALID_INPUT, f"Unable to read config: {exc}") from exc
        if not isinstance(raw, dict):
            raise PipelineError(E_INVALID_INPUT, "Config root must be an object")
        validated = PipelineConfig(raw)
        with workspace.run_lock():
            workspace.save_config(validated)
    config = workspace.load_config()
    _print_json({"ok": True, "config": config.to_dict(), "config_version": config.config_version})
    return 0


def cmd_tasks(args: argparse.Namespace) -> int:
    workspace = _workspace(args)
    tasks = workspace.load_tasks()
    _print_json(tasks_to_manifest(tasks))
    return 0


def cmd_serve(args: argparse.Namespace) -> int:
    from .server import serve

    serve(args.workspace, host=args.host, port=args.port)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="gspipe",
        description="Local deterministic four-stage image editing pipeline",
    )
    parser.add_argument("--version", action="version", version="gspipe 1.0.0")
    subparsers = parser.add_subparsers(dest="command", required=True)

    write_samples = subparsers.add_parser("write-samples", help="Regenerate deterministic sample files")
    write_samples.add_argument("--output", default="samples")
    write_samples.set_defaults(func=cmd_write_samples)

    init_demo = subparsers.add_parser("init-demo", help="Initialize a workspace with 4 shards and 6 tasks")
    init_demo.add_argument("--workspace", default=DEFAULT_WORKSPACE)
    init_demo.add_argument("--force", action="store_true")
    init_demo.set_defaults(func=cmd_init_demo)

    import_cmd = subparsers.add_parser("import", help="Import local PNG shards and a task manifest")
    import_cmd.add_argument("--workspace", default=DEFAULT_WORKSPACE)
    import_cmd.add_argument("--shards-dir", required=True)
    import_cmd.add_argument("--tasks", required=True)
    import_cmd.add_argument("--config")
    import_cmd.set_defaults(func=cmd_import)

    run = subparsers.add_parser("run", help="Run one or more pipeline stages")
    run.add_argument("--workspace", default=DEFAULT_WORKSPACE)
    run.add_argument("--stages", default="parse,execute,verify,export")
    run.add_argument("--config", help="Optional config override for this run")
    run.add_argument("--resume", action="store_true", help="Clear a stop request before running")
    run.add_argument("--sequential", action="store_true", help="Execute accepted tasks sequentially for determinism checks")
    run.set_defaults(func=cmd_run)

    resume = subparsers.add_parser("resume", help="Clear stop request and continue execution")
    resume.add_argument("--workspace", default=DEFAULT_WORKSPACE)
    resume.add_argument("--stages", default="parse,execute,verify,export")
    resume.add_argument("--config")
    resume.add_argument("--sequential", action="store_true")
    resume.set_defaults(func=cmd_resume)

    stop = subparsers.add_parser("stop", help="Request graceful stop at the next task boundary")
    stop.add_argument("--workspace", default=DEFAULT_WORKSPACE)
    stop.add_argument("--reason", default="cli_stop")
    stop.set_defaults(func=cmd_stop)

    status = subparsers.add_parser("status", help="Show stage state, fingerprints, timings, and budgets")
    status.add_argument("--workspace", default=DEFAULT_WORKSPACE)
    status.add_argument("--config")
    status.set_defaults(func=cmd_status)

    fingerprints = subparsers.add_parser("fingerprints", help="Show per-stage version fingerprints")
    fingerprints.add_argument("--workspace", default=DEFAULT_WORKSPACE)
    fingerprints.add_argument("--config")
    fingerprints.set_defaults(func=cmd_fingerprints)

    validate = subparsers.add_parser("validate", help="Parse and validate one task chain")
    validate.add_argument("task_id")
    validate.add_argument("--workspace", default=DEFAULT_WORKSPACE)
    validate.add_argument("--config")
    validate.set_defaults(func=cmd_validate)

    query = subparsers.add_parser("query", help="Query one task; result is cached by content/config/state")
    query.add_argument("task_id")
    query.add_argument("--workspace", default=DEFAULT_WORKSPACE)
    query.add_argument("--config")
    query.add_argument("--recompute", action="store_true")
    query.add_argument("--json", action="store_true", help="Emit the exact cached payload bytes")
    query.set_defaults(func=cmd_query)

    export = subparsers.add_parser("export", help="Build the deterministic export bundle")
    export.add_argument("--workspace", default=DEFAULT_WORKSPACE)
    export.add_argument("--config")
    export.add_argument("--sequential", action="store_true")
    export.set_defaults(func=cmd_export)

    config = subparsers.add_parser("config", help="Show or replace workspace config")
    config.add_argument("--workspace", default=DEFAULT_WORKSPACE)
    config.add_argument("--file", dest="config_file")
    config.set_defaults(func=cmd_config)

    tasks = subparsers.add_parser("tasks", help="Show the canonical task manifest")
    tasks.add_argument("--workspace", default=DEFAULT_WORKSPACE)
    tasks.set_defaults(func=cmd_tasks)

    serve = subparsers.add_parser("serve", help="Run the local HTTP API")
    serve.add_argument("--workspace", default=DEFAULT_WORKSPACE)
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8765)
    serve.set_defaults(func=cmd_serve)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except PipelineError as exc:
        _print_json(exc.as_response())
        return 1
    except KeyboardInterrupt:
        return 130
    except Exception as exc:
        _print_json(
            {
                "ok": False,
                "error": {
                    "code": "E_INTERNAL",
                    "message": f"{type(exc).__name__}: {exc}",
                },
            }
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
