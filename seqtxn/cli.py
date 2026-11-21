"""Run the middleware component."""

from __future__ import annotations

import argparse
import os

import uvicorn

from .api import create_app


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default="runtime/middleware.db")
    parser.add_argument("--coordinator-url", default="http://127.0.0.1:18081")
    parser.add_argument("--unreachable-url", default="http://127.0.0.1:1")
    parser.add_argument("--query-timeout", type=float, default=0.5)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--no-auto-recover", action="store_true")
    parser.add_argument("--no-auto-renegotiate", action="store_true")
    args = parser.parse_args()

    app = create_app(
        db_path=args.db,
        coordinator_url=args.coordinator_url,
        unreachable_url=args.unreachable_url,
        query_timeout_seconds=args.query_timeout,
        auto_recover=not args.no_auto_recover,
        auto_renegotiate=not args.no_auto_renegotiate,
    )
    uvicorn.run(app, host=args.host, port=args.port, log_level="warning")


if __name__ == "__main__":
    main()

