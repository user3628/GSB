"""Command-line interface for the POS migration and alignment engine."""

from __future__ import annotations

import argparse
import glob
import json
import sys
from pathlib import Path
from typing import Any

from .canonical import atomic_write_bytes
from .engine import Engine
from .errors import BusinessError, E_INTERNAL


def _json_print(value: Any) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True))


def _expand_shards(values: list[str]) -> list[str]:
    expanded: list[str] = []
    for value in values:
        if any(char in value for char in "*?["):
            matches = sorted(glob.glob(value))
            if not matches:
                raise BusinessError(
                    "E_INVALID_INPUT",
                    f"shard glob matched no files: {value}",
                    {"pattern": value},
                )
            expanded.extend(matches)
        else:
            expanded.append(value)
    return expanded


def _workspace(value: str | None) -> Path:
    return Path(value).expanduser().resolve() if value else (Path.cwd() / "runtime" / "workspace")


def _stop(workspace: Path) -> dict[str, Any]:
    target = workspace / "state" / "STOP"
    atomic_write_bytes(target, b"stop requested\n")
    return {"ok": True, "status": "stop_requested", "stop_file": str(target)}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="posalign",
        description="Deterministic POS tagset migration, alignment, adjudication, and versioning",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="run a migration batch")
    run.add_argument("--workspace", default=None)
    run.add_argument("--config", required=True)
    run.add_argument("--shards", nargs="+", required=True)
    run.add_argument("--reset", action="store_true", help="clear managed state/mapping/output")
    run.add_argument("--stop-after-sentences", type=int, default=None)
    run.add_argument("--clear-stop", action="store_true")

    status = sub.add_parser("status", help="query checkpoint, invalidation, mapping, and output state")
    status.add_argument("--workspace", default=None)

    state = sub.add_parser("state", help="alias for status")
    state.add_argument("--workspace", default=None)

    resolved = sub.add_parser("resolved", help="query adjudicated mappings")
    resolved.add_argument("--workspace", default=None)
    resolved.add_argument("--allow-stale", action="store_true")

    candidates = sub.add_parser("candidates", help="query retained candidate mappings")
    candidates.add_argument("--workspace", default=None)
    candidates.add_argument("--label", default=None)
    candidates.add_argument("--allow-stale", action="store_true")

    export_v1 = sub.add_parser("export-v1", help="reverse-export v1-origin records to strict v1 JSONL")
    export_v1.add_argument("--config", required=True)
    export_v1.add_argument("--shards", nargs="+", required=True)
    export_v1.add_argument("--output", required=True)

    stop = sub.add_parser("stop", help="request a graceful stop at the next sentence boundary")
    stop.add_argument("--workspace", default=None)

    clear_stop = sub.add_parser("clear-stop", help="remove a stop request")
    clear_stop.add_argument("--workspace", default=None)

    serve = sub.add_parser("serve", help="serve the local HTTP API")
    serve.add_argument("--workspace", default=None)
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8765)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "run":
            engine = Engine(_workspace(args.workspace))
            result = engine.run(
                _expand_shards(args.shards),
                config_path=args.config,
                reset=args.reset,
                stop_after_sentences=args.stop_after_sentences,
                clear_stop=args.clear_stop,
            )
            _json_print(result)
            return 0
        if args.command in {"status", "state"}:
            _json_print(Engine(_workspace(args.workspace)).status())
            return 0
        if args.command == "resolved":
            _json_print(
                Engine(_workspace(args.workspace)).mapping_resolved(
                    allow_stale=args.allow_stale
                )
            )
            return 0
        if args.command == "candidates":
            _json_print(
                Engine(_workspace(args.workspace)).mapping_candidates(
                    args.label, allow_stale=args.allow_stale
                )
            )
            return 0
        if args.command == "export-v1":
            _json_print(
                Engine(_workspace(None)).export_v1(
                    _expand_shards(args.shards),
                    config_path=args.config,
                    output_path=args.output,
                )
            )
            return 0
        if args.command == "stop":
            _json_print(_stop(_workspace(args.workspace)))
            return 0
        if args.command == "clear-stop":
            engine = Engine(_workspace(args.workspace))
            engine.clear_stop()
            _json_print({"ok": True, "status": "stop_cleared"})
            return 0
        if args.command == "serve":
            from .http_server import run_server

            run_server(_workspace(args.workspace), host=args.host, port=args.port)
            return 0
        parser.error("unknown command")
        return 2
    except BusinessError as error:
        print(json.dumps(error.to_dict(), ensure_ascii=False, indent=2, sort_keys=True), file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        return 130
    except Exception as error:
        payload = BusinessError(
            E_INTERNAL,
            "unexpected internal error",
            {"cause": str(error), "type": type(error).__name__},
        )
        print(json.dumps(payload.to_dict(), ensure_ascii=False, indent=2, sort_keys=True), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

