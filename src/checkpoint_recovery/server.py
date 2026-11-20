from __future__ import annotations

import argparse
from pathlib import Path

import uvicorn

from .api import create_app
from .config import ManagerConfig
from .manager import CheckpointManager


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the local checkpoint recovery HTTP service")
    parser.add_argument("--data-dir", required=True, type=Path)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", required=True, type=int)
    parser.add_argument("--max-writing", type=int, default=2)
    parser.add_argument("--max-checkpoints", type=int, default=3)
    parser.add_argument("--min-round-interval", type=int, default=1)
    parser.add_argument("--writer-delay", type=float, default=0.0)
    parser.add_argument("--fault-crash-after-temp", action="store_true")
    parser.add_argument("--fault-crash-delay", type=float, default=0.8)
    parser.add_argument("--fault-corrupt-after-temp", action="store_true")
    parser.add_argument("--log-level", default="warning")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    if args.host not in {"127.0.0.1", "localhost", "::1"}:
        parser.error("this local component only binds to a loopback host")
    config = ManagerConfig(
        data_dir=args.data_dir.resolve(),
        max_writing=args.max_writing,
        max_checkpoints=args.max_checkpoints,
        min_retain_round_interval=args.min_round_interval,
        writer_delay_seconds=args.writer_delay,
        fault_crash_after_temp_files=args.fault_crash_after_temp,
        fault_crash_delay_seconds=args.fault_crash_delay,
        fault_corrupt_after_temp_write=args.fault_corrupt_after_temp,
    )
    manager = CheckpointManager(config)
    app = create_app(manager=manager, config=config)
    try:
        uvicorn.run(
            app,
            host=args.host,
            port=args.port,
            log_level=args.log_level,
            access_log=False,
            workers=1,
        )
    finally:
        manager.shutdown()


if __name__ == "__main__":
    main()

