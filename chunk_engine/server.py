from __future__ import annotations

import argparse
import os

import uvicorn

from .api import create_app
from .engine import Engine, EngineConfig


LOOPBACK_HOSTS = {"127.0.0.1", "localhost", "::1"}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Loopback-only chunk column query HTTP service")
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--db", default=None)
    parser.add_argument("--snapshot", default=None, help="optional initial snapshot directory")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--memory-budget", type=int, default=8 * 1024 * 1024)
    parser.add_argument("--single-task-budget", type=int, default=64 * 1024)
    parser.add_argument("--max-parallel-blocks", type=int, default=8)
    parser.add_argument("--max-keys", type=int, default=20_000)
    parser.add_argument("--max-page-size", type=int, default=500)
    parser.add_argument("--allow-debug-kill", action="store_true")
    parser.add_argument("--log-level", default="info")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.host not in LOOPBACK_HOSTS:
        raise SystemExit("refusing to bind outside loopback; use 127.0.0.1, localhost, or ::1")
    if args.port <= 0 or args.port > 65535:
        raise SystemExit("port must be between 1 and 65535")
    config = EngineConfig(
        total_memory_budget_bytes=args.memory_budget,
        single_task_budget_bytes=args.single_task_budget,
        max_parallel_blocks=args.max_parallel_blocks,
        max_keys=args.max_keys,
        max_page_size=args.max_page_size,
        default_page_size=min(100, args.max_page_size),
    )
    engine = Engine(args.data_dir, config=config, db_path=args.db)
    if args.snapshot:
        engine.load_snapshot(args.snapshot)
    allow_debug_kill = args.allow_debug_kill or os.environ.get("CHUNK_ENGINE_ALLOW_DEBUG_KILL") == "1"
    app = create_app(engine, allow_debug_kill=allow_debug_kill)
    uvicorn.run(app, host=args.host, port=args.port, log_level=args.log_level, workers=1)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
