"""Command-line entry point for the local workbench."""

from __future__ import annotations

import argparse
import asyncio
import json
import signal
import sys
from pathlib import Path
from typing import Any

from .models import is_loopback_host
from .runtime import Runtime
from .server import WorkbenchServer


def _load_json(path: str | None) -> dict[str, Any] | None:
    if path is None:
        return None
    with open(path, "r", encoding="utf-8") as handle:
        value = json.load(handle)
    if not isinstance(value, dict):
        raise ValueError("seed file must contain a JSON object")
    return value


def _emit(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True), flush=True)


async def _run_serve(args: argparse.Namespace) -> int:
    if not is_loopback_host(args.host):
        raise SystemExit("--host must be a loopback address (127.0.0.1, ::1, or localhost)")
    seed = _load_json(args.seed)
    runtime = await Runtime.create(args.db, seed=seed)
    server = WorkbenchServer(
        runtime=runtime,
        host=args.host,
        port=args.port,
        graceful_timeout=args.graceful_timeout,
    )
    await server.start()
    display_host = f"[{server.host}]" if ":" in server.host else server.host
    _emit(
        {
            "event": "ready",
            "host": server.host,
            "port": server.port,
            "version": runtime.current.version if runtime.current else None,
            "db": str(Path(args.db).resolve()),
            "recovery": runtime.recovery.public(),
            "dashboard": f"http://{display_host}:{server.port}/dashboard",
        }
    )

    loop = asyncio.get_running_loop()

    def request_shutdown() -> None:
        server.request_shutdown()

    installed: list[signal.Signals] = []
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, request_shutdown)
            installed.append(sig)
        except (NotImplementedError, RuntimeError, ValueError):
            # Windows' ProactorEventLoop does not implement add_signal_handler.
            try:
                signal.signal(sig, lambda _signum, _frame: loop.call_soon_threadsafe(request_shutdown))
            except (ValueError, OSError):
                pass
    try:
        shutdown = await server.wait_closed()
    finally:
        for sig in installed:
            try:
                loop.remove_signal_handler(sig)
            except (NotImplementedError, RuntimeError):
                pass
    _emit({"event": "stopped", "shutdown": shutdown})
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m proxy_workbench",
        description="Local-only forward/reverse proxy workbench with atomic config snapshots.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    serve = subparsers.add_parser("serve", help="run the proxy, admin API, and dashboard")
    serve.add_argument("--db", default="artifacts/workbench.db", help="SQLite database path")
    serve.add_argument(
        "--seed",
        default=None,
        help="optional full snapshot JSON used only when the database has no complete snapshot",
    )
    serve.add_argument("--host", default="127.0.0.1", help="loopback bind address")
    serve.add_argument("--port", type=int, default=18080, help="bind port; use 0 for an ephemeral port")
    serve.add_argument("--graceful-timeout", type=float, default=8.0, help="seconds to drain admitted requests")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "serve":
        try:
            return asyncio.run(_run_serve(args))
        except KeyboardInterrupt:
            return 130
    parser.error("unknown command")
    return 2

