from __future__ import annotations

import argparse
from pathlib import Path

import uvicorn

from .api import create_app


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Local HTTP/2 multiplexing diagnostics workbench")
    parser.add_argument(
        "--db",
        default=str(Path("var") / "h2workbench.db"),
        help="SQLite database file used for windows, states, intents, and traces",
    )
    parser.add_argument("--host", default="127.0.0.1", help="loopback bind address")
    parser.add_argument("--port", default=8765, type=int, help="local TCP port")
    parser.add_argument("--reload", action="store_true", help="enable uvicorn reload")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.host not in {"127.0.0.1", "localhost", "::1"}:
        raise SystemExit("Refusing to bind outside the local loopback interface")
    app = create_app(args.db)
    uvicorn.run(app, host=args.host, port=args.port, reload=False, log_level="info")


if __name__ == "__main__":
    main()
