from __future__ import annotations

import argparse
import json
import sys

from .server import main as server_main
from .store import Store


def _print(value) -> None:
    print(json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2))


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    parser = argparse.ArgumentParser(prog="alert-sieve", description="Local alert reduction and root-cause component")
    sub = parser.add_subparsers(dest="command", required=True)

    serve = sub.add_parser("serve", help="run the local HTTP API")
    serve.add_argument("--db", required=True)
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8765)
    serve.add_argument("--enable-fault-injection", action="store_true")

    recover = sub.add_parser("recover", help="scan pending intents and print persisted recovery facts")
    recover.add_argument("--db", required=True)

    state = sub.add_parser("state", help="print committed version state")
    state.add_argument("--db", required=True)

    args = parser.parse_args(argv)
    if args.command == "serve":
        return server_main(
            [
                "--db",
                args.db,
                "--host",
                args.host,
                "--port",
                str(args.port),
                *(["--enable-fault-injection"] if args.enable_fault_injection else []),
            ]
        )
    if args.command == "recover":
        store = Store(args.db)
        _print(store.recover_pending())
        return 0
    if args.command == "state":
        store = Store(args.db)
        _print(store.query_state())
        return 0
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
