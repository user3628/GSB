"""Command-line interface for the deterministic local TTS engine."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .engine import StreamingTTSEngine
from .errors import TTSError
from .inputs import parse_samples, samples_to_jsonl
from .utils import atomic_write_text


def _engine(args: argparse.Namespace) -> StreamingTTSEngine:
    workspace = Path(args.workspace).resolve()
    config = Path(args.config)
    if not config.is_absolute():
        config = workspace / config
    return StreamingTTSEngine(
        workspace=workspace,
        config_path=config,
        run_id=args.run_id,
    )


def _print(value, *, compact: bool = False) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=None if compact else 2, sort_keys=True))


def cmd_run(args: argparse.Namespace) -> int:
    engine = _engine(args)
    outcome = engine.run(
        samples_path=args.samples,
        resume=args.resume,
        stop_after_chunks=args.stop_after_chunks,
    )
    _print(outcome.to_dict())
    return 0 if outcome.status in {"complete", "stopped"} else 1


def cmd_status(args: argparse.Namespace) -> int:
    _print(_engine(args).status())
    return 0


def cmd_stop(args: argparse.Namespace) -> int:
    _print(_engine(args).request_stop())
    return 0


def cmd_trace(args: argparse.Namespace) -> int:
    engine = _engine(args)
    path = engine.timing_path if args.timing else engine.trace_path
    if not path.exists():
        raise TTSError("E_INVALID_STAGE", f"trace file is not available: {path}")
    sys.stdout.write(path.read_text(encoding="utf-8"))
    return 0


def cmd_version(args: argparse.Namespace) -> int:
    _print(_engine(args).version_report(args.samples))
    return 0


def cmd_stages(args: argparse.Namespace) -> int:
    _print(_engine(args).stage_report())
    return 0


def cmd_import(args: argparse.Namespace) -> int:
    text = Path(args.file).read_text(encoding="utf-8") if args.file else sys.stdin.read()
    utterances = parse_samples(text, source=args.file or "<stdin>")
    target = Path(args.output)
    if not target.is_absolute():
        target = Path(args.workspace) / target
    if args.append and target.exists():
        text = target.read_text(encoding="utf-8") + samples_to_jsonl(utterances)
        parse_samples(text, source=str(target))
    atomic_write_text(target, text)
    _print({"ok": True, "path": str(target), "utterance_count": len(parse_samples(text))})
    return 0


def cmd_serve(args: argparse.Namespace) -> int:
    import uvicorn

    from .service import create_app

    app = create_app(workspace=args.workspace, config_path=args.config)
    uvicorn.run(app, host=args.host, port=args.port, log_level=args.log_level)
    return 0


def cmd_demo(args: argparse.Namespace) -> int:
    args.resume = False
    return cmd_run(args)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="stream-tts", description=__doc__)
    parser.add_argument("--workspace", default=".", help="workspace root (default: current directory)")
    parser.add_argument("--config", default="config/default.yaml", help="YAML configuration path")
    parser.add_argument("--run-id", default="main", help="deterministic run/artifact namespace")
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="run streaming synthesis")
    run.add_argument("--samples", default=None)
    run.add_argument("--resume", action="store_true")
    run.add_argument("--stop-after-chunks", type=int, default=None)
    run.set_defaults(func=cmd_run)

    demo = sub.add_parser("demo", help="run the complete three-utterance demo")
    demo.add_argument("--samples", default=None)
    demo.add_argument("--stop-after-chunks", type=int, default=None)
    demo.set_defaults(func=cmd_demo)

    status = sub.add_parser("status", help="show persisted run status")
    status.set_defaults(func=cmd_status)

    stop = sub.add_parser("stop", help="request graceful stop after the current block")
    stop.set_defaults(func=cmd_stop)

    trace = sub.add_parser("trace", help="print the deterministic latency trace")
    trace.add_argument("--timing", action="store_true", help="print actual measured timing audit instead")
    trace.set_defaults(func=cmd_trace)

    version = sub.add_parser("version", help="show configuration and per-utterance fingerprints")
    version.add_argument("--samples", default=None)
    version.set_defaults(func=cmd_version)

    stages = sub.add_parser("stages", help="show four-stage checkpoint status")
    stages.set_defaults(func=cmd_stages)

    imp = sub.add_parser("import", help="import a JSONL sample file")
    imp.add_argument("--file", default=None, help="source JSONL; stdin when omitted")
    imp.add_argument("--output", default="data/samples.jsonl")
    imp.add_argument("--append", action="store_true")
    imp.set_defaults(func=cmd_import)

    serve = sub.add_parser("serve", help="serve the local FastAPI interface")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=18765)
    serve.add_argument("--log-level", default="info")
    serve.set_defaults(func=cmd_serve)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except TTSError as exc:
        print(json.dumps(exc.to_dict(), ensure_ascii=False, indent=2, sort_keys=True), file=sys.stderr)
        return 2
    except FileNotFoundError as exc:
        print(json.dumps({"ok": False, "error": {"code": "E_NOT_FOUND", "message": str(exc)}}, ensure_ascii=False), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
