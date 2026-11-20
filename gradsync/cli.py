from __future__ import annotations

import argparse
import sys
from pathlib import Path

import uvicorn

from .api import create_app
from .scheduler import validate_loopback_url
from .worker_node import create_worker_app


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="gradsync")
    subparsers = parser.add_subparsers(dest="command", required=True)

    serve = subparsers.add_parser("serve", help="start the gradient scheduler")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8000)
    serve.add_argument("--db", default="artifacts/scheduler.db")
    serve.add_argument("--log-level", default="warning")

    worker = subparsers.add_parser("worker", help="start a local HTTP test worker")
    worker.add_argument("--host", default="127.0.0.1")
    worker.add_argument("--port", type=int, required=True)
    worker.add_argument("--name", default="worker")
    worker.add_argument("--log-level", default="warning")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    validate_loopback_url(f"http://{args.host}", field="host")
    if args.command == "serve":
        app = create_app(Path(args.db))
        uvicorn.run(app, host=args.host, port=args.port, log_level=args.log_level)
        return 0
    if args.command == "worker":
        app = create_worker_app(args.name)
        uvicorn.run(app, host=args.host, port=args.port, log_level=args.log_level)
        return 0
    return 2


if __name__ == "__main__":
    sys.exit(main())

