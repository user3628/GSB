from __future__ import annotations

import argparse
import json
import socket
import sys
from pathlib import Path

from .api import create_app
from .engine import Engine
from .errors import BusinessError, E_BAD_REQUEST, E_PORT_UNAVAILABLE
from .samples import read_snapshot_file
from .utils import pretty_json


def check_loopback_port(host: str, port: int) -> None:
    if host not in {"127.0.0.1", "localhost", "::1"}:
        raise BusinessError(
            E_BAD_REQUEST,
            "the validation/deployment server must bind to a loopback address",
            {"host": host, "allowed_hosts": ["127.0.0.1", "localhost", "::1"]},
        )
    if port < 1 or port > 65535:
        raise BusinessError(
            E_PORT_UNAVAILABLE,
            "port is outside the legal TCP port range",
            {"host": host, "port": port, "legal_interval": [1, 65535]},
        )
    family = socket.AF_INET6 if ":" in host else socket.AF_INET
    probe = socket.socket(family, socket.SOCK_STREAM)
    try:
        probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        probe.bind((host, port))
    except OSError as exc:
        raise BusinessError(
            E_PORT_UNAVAILABLE,
            "loopback port is not available",
            {"host": host, "port": port, "reason": str(exc)},
        ) from exc
    finally:
        probe.close()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="hotroute",
        description="Local deterministic model graph hot-switch router",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    serve = subparsers.add_parser("serve", help="start the local HTTP API")
    serve.add_argument("--db", required=True, help="SQLite database path")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8765)

    snapshot_load = subparsers.add_parser(
        "snapshot-load", help="load a full snapshot file and atomically replace state"
    )
    snapshot_load.add_argument("--db", required=True)
    snapshot_load.add_argument("--file", required=True)

    increment_load = subparsers.add_parser(
        "increment-load", help="load an increment file against an explicit base version"
    )
    increment_load.add_argument("--db", required=True)
    increment_load.add_argument("--file", required=True)

    recover = subparsers.add_parser("recover", help="scan unfinished intents and emit a report")
    recover.add_argument("--db", required=True)

    state = subparsers.add_parser("state", help="print the current committed state")
    state.add_argument("--db", required=True)

    verify = subparsers.add_parser(
        "verify", help="run the deterministic local HTTP acceptance workflow"
    )
    verify.add_argument("--output", default="artifacts/verification.json")
    verify.add_argument("--markdown", default="artifacts/VERIFICATION.md")
    verify.add_argument("--keep-run-dir", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "serve":
            check_loopback_port(args.host, args.port)
            engine = Engine(args.db, recover=True)
            app = create_app(engine)
            app.run(
                host=args.host,
                port=args.port,
                threaded=True,
                use_reloader=False,
                debug=False,
            )
            return 0

        if args.command == "snapshot-load":
            engine = Engine(args.db)
            payload = read_snapshot_file(args.file)
            result = engine.submit_snapshot(payload)
            print(pretty_json(result))
            return 0

        if args.command == "increment-load":
            engine = Engine(args.db)
            payload = read_snapshot_file(args.file)
            result = engine.submit_increment(payload)
            print(pretty_json(result))
            return 0

        if args.command == "recover":
            engine = Engine(args.db, recover=True)
            print(pretty_json(engine.last_recovery_report))
            return 0

        if args.command == "state":
            engine = Engine(args.db, recover=False)
            print(pretty_json(engine.get_state()))
            return 0

        if args.command == "verify":
            from .verify import run_verification

            result = run_verification(
                output_path=Path(args.output),
                markdown_path=Path(args.markdown),
                keep_run_dir=args.keep_run_dir,
            )
            print(pretty_json({k: result[k] for k in ("passed", "output", "markdown")}))
            return 0 if result["passed"] else 1
    except BusinessError as exc:
        print(json.dumps(exc.to_dict(), ensure_ascii=False, indent=2), file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        return 130
    return 1

