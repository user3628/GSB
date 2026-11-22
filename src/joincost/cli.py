from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .formats import decode_snapshot
from .http_server import create_server, serve_forever
from .service import Service
from .store import Store


def _serve(args: argparse.Namespace) -> int:
    db = Path(args.db).resolve()
    workspace = Path(args.workspace).resolve()
    service = Service(Store(db), workspace_root=workspace)
    server = create_server(service, host=args.host, port=args.port)
    address, port = server.server_address[:2]
    print(
        json.dumps(
            {
                "ok": True,
                "url": f"http://{address}:{port}/",
                "db": str(db),
                "workspace": str(workspace),
                "loopback_only": True,
                "startup_recovery": service.startup_recovery,
            },
            ensure_ascii=False,
        ),
        flush=True,
    )
    try:
        serve_forever(server)
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


def _inspect(args: argparse.Namespace) -> int:
    data = Path(args.source).read_bytes()
    try:
        decoded = decode_snapshot(data, format_hint=args.format)
    except Exception as exc:
        if hasattr(exc, "to_dict"):
            print(json.dumps(exc.to_dict(), ensure_ascii=False, indent=2))
            return 2
        raise
    print(
        json.dumps(
            {"ok": True, "report": decoded.report, "snapshot": decoded.snapshot.public_dict()},
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
    )
    return 0


def _recover(args: argparse.Namespace) -> int:
    store = Store(args.db)
    report = store.recover()
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="joincost")
    subparsers = parser.add_subparsers(dest="command", required=True)

    serve = subparsers.add_parser("serve", help="run the loopback diagnostic HTTP service")
    serve.add_argument("--db", default="artifacts/joincost.db")
    serve.add_argument("--workspace", default=".")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8765)
    serve.set_defaults(func=_serve)

    inspect = subparsers.add_parser("inspect", help="decode and print a statistics snapshot")
    inspect.add_argument("source")
    inspect.add_argument("--format", choices=["auto", "binary", "jsonl"], default="auto")
    inspect.set_defaults(func=_inspect)

    recover = subparsers.add_parser("recover", help="scan and resolve pending SQLite intents")
    recover.add_argument("--db", default="artifacts/joincost.db")
    recover.set_defaults(func=_recover)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    sys.exit(main())

