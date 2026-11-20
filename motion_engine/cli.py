"""Command line entry point for local deterministic operation."""

from __future__ import annotations

import argparse
import json
import signal
import sys
import threading
from pathlib import Path
from typing import Any

from .errors import EngineError
from .runner import StopController
from .sample_gen import generate_samples
from .server import serve
from .workspace import Workspace


def _json_print(value: Any) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True))


def _add_series_arguments(parser: argparse.ArgumentParser, *, include_from_step: bool = False) -> None:
    parser.add_argument("--workspace", default="runtime", help="workspace directory")
    parser.add_argument("--series", default="all", help="series id or 'all'")
    parser.add_argument("--max-steps", type=int, default=None)
    parser.add_argument(
        "--stop-after",
        type=int,
        default=None,
        help="request a cooperative stop after this many newly performed steps",
    )
    parser.add_argument("--fresh", action="store_true", help="reset selected checkpoints first")
    if include_from_step:
        parser.add_argument(
            "--from-step",
            type=int,
            default=None,
            help="require the latest valid checkpoint to be exactly this step",
        )


def _run_selected(args: argparse.Namespace, *, from_step_allowed: bool = False) -> dict[str, Any]:
    if args.max_steps is not None and args.max_steps < 0:
        raise EngineError(
            "E_INVALID_STATE", "max_steps must be non-negative", position="max_steps"
        )
    if args.stop_after is not None and args.stop_after < 0:
        raise EngineError(
            "E_INVALID_STATE",
            "stop_after must be non-negative",
            position="stop_after",
        )
    workspace = Workspace(args.workspace)
    series_ids = workspace.series_ids()
    if args.series != "all":
        if args.series not in series_ids:
            raise EngineError(
                "E_INVALID_STATE",
                "unknown series",
                series_id=args.series,
                position="series_id",
                available=series_ids,
            )
        selected = [args.series]
    else:
        selected = series_ids
    if getattr(args, "from_step", None) is not None and len(selected) != 1:
        raise EngineError(
            "E_INVALID_STATE",
            "--from-step requires exactly one --series",
            position="from_step",
        )

    results: list[dict[str, Any]] = []
    interrupted = threading.Event()
    current_controller: list[StopController | None] = [None]
    previous_handlers: dict[int, Any] = {}

    def request_stop(signum: int, frame: Any) -> None:  # noqa: ARG001
        interrupted.set()
        if current_controller[0] is not None:
            current_controller[0].request()

    for signal_name in ("SIGINT", "SIGTERM"):
        if hasattr(signal, signal_name):
            signum = getattr(signal, signal_name)
            previous_handlers[signum] = signal.getsignal(signum)
            signal.signal(signum, request_stop)
    try:
        for series_id in selected:
            if interrupted.is_set():
                break
            if args.fresh:
                workspace.reset_series(series_id)
            controller = StopController()
            current_controller[0] = controller
            if args.stop_after == 0:
                controller.request()
            performed = 0

            def on_step(record: dict[str, Any]) -> None:  # noqa: ARG001
                nonlocal performed
                performed += 1
                if args.stop_after is not None and performed >= args.stop_after:
                    controller.request()

            report = workspace.run(
                series_id,
                max_steps=args.max_steps,
                stop_controller=controller,
                from_step=getattr(args, "from_step", None) if from_step_allowed else None,
                on_step=on_step,
            )
            results.append(
                {
                    "series_id": series_id,
                    "complete": report["complete"],
                    "run": report["run"],
                    "report_fingerprint": report["report_fingerprint"],
                    "aggregate": report["aggregate"],
                }
            )
    finally:
        current_controller[0] = None
        for signum, handler in previous_handlers.items():
            signal.signal(signum, handler)
    return {"series": results, "interrupted": interrupted.is_set()}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="motion-engine",
        description="Deterministic local video prediction and motion-state engine",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    generate = subparsers.add_parser("generate", help="generate deterministic demo samples")
    generate.add_argument("--out", default="data")
    generate.add_argument("--seed", type=int, default=20261121)

    import_parser = subparsers.add_parser("import", help="import a frame manifest")
    import_parser.add_argument("--workspace", default="runtime")
    import_parser.add_argument("--manifest", required=True)
    import_parser.add_argument("--reset", action="store_true")

    run_parser = subparsers.add_parser("run", help="run or continue rolling prediction")
    _add_series_arguments(run_parser)

    resume_parser = subparsers.add_parser("resume", help="resume from persisted checkpoints")
    _add_series_arguments(resume_parser, include_from_step=True)

    step_parser = subparsers.add_parser("step", help="perform exactly one rolling step")
    _add_series_arguments(step_parser)

    status_parser = subparsers.add_parser("status", help="query checkpoints and versions")
    status_parser.add_argument("--workspace", default="runtime")
    status_parser.add_argument("--series", default="all")

    report_parser = subparsers.add_parser("report", help="emit the deterministic report")
    report_parser.add_argument("--workspace", default="runtime")
    report_parser.add_argument("--series", required=True)
    report_parser.add_argument("--output", default=None)

    reset_parser = subparsers.add_parser("reset", help="remove checkpoints for selected series")
    reset_parser.add_argument("--workspace", default="runtime")
    reset_parser.add_argument("--series", default="all")

    serve_parser = subparsers.add_parser("serve", help="serve the local JSON HTTP app")
    serve_parser.add_argument("--workspace", default="runtime")
    serve_parser.add_argument("--host", default="127.0.0.1")
    serve_parser.add_argument("--port", type=int, default=18765)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "generate":
            _json_print(generate_samples(args.out, seed=args.seed))
            return 0
        if args.command == "import":
            workspace = Workspace(args.workspace)
            _json_print(workspace.import_manifest(args.manifest, reset=args.reset))
            return 0
        if args.command == "run":
            _json_print(_run_selected(args))
            return 0
        if args.command == "resume":
            _json_print(_run_selected(args, from_step_allowed=True))
            return 0
        if args.command == "step":
            args.max_steps = 1
            args.stop_after = None
            _json_print(_run_selected(args))
            return 0
        if args.command == "status":
            workspace = Workspace(args.workspace)
            if args.series == "all":
                _json_print(workspace.all_status())
            else:
                _json_print(workspace.inspect_series(args.series))
            return 0
        if args.command == "report":
            workspace = Workspace(args.workspace)
            body = workspace.report(args.series, canonical=True)
            if args.output:
                Path(args.output).write_bytes(body)
                print(str(Path(args.output).resolve()))
            else:
                sys.stdout.buffer.write(body)
            return 0
        if args.command == "reset":
            workspace = Workspace(args.workspace)
            selected = workspace.series_ids() if args.series == "all" else [args.series]
            _json_print(
                {"reset": [workspace.reset_series(series_id) for series_id in selected]}
            )
            return 0
        if args.command == "serve":
            serve(args.workspace, args.host, args.port)
            return 0
        parser.error("unknown command")
    except EngineError as error:
        print(json.dumps(error.to_dict(), ensure_ascii=False, indent=2), file=sys.stderr)
        return 2
    return 1
