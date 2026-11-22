"""Command-line entry point for the loopback HTTP coordinator."""

from __future__ import annotations

import argparse
import json
import signal
import sys
from pathlib import Path
from typing import Sequence

from .config import CoordinatorConfig
from .coordinator import Coordinator
from .http_api import create_server


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the local lease election coordinator over loopback HTTP."
    )
    parser.add_argument(
        "--db",
        default=".data/leasecoord.sqlite",
        help="SQLite database path (default: .data/leasecoord.sqlite)",
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", default=8765, type=int)
    parser.add_argument("--exact-limit", type=int, default=None)
    parser.add_argument("--heartbeat-timeout-ms", type=int, default=None)
    parser.add_argument("--sample-fraction", type=float, default=None)
    parser.add_argument(
        "--allow-time-override",
        action="store_true",
        help="enable X-Coordinator-Now-Ms / now_ms for deterministic local tests",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = CoordinatorConfig.from_env()
    overrides: dict[str, object] = {}
    if args.exact_limit is not None:
        overrides["exact_limit"] = args.exact_limit
    if args.heartbeat_timeout_ms is not None:
        overrides["heartbeat_timeout_ms"] = args.heartbeat_timeout_ms
    if args.sample_fraction is not None:
        overrides["sample_fraction"] = args.sample_fraction
    if args.allow_time_override:
        overrides["allow_time_override"] = True
    if overrides:
        from dataclasses import replace

        config = replace(config, **overrides)
        config.validate()

    coordinator = Coordinator(Path(args.db), config)
    server = create_server(
        coordinator,
        args.host,
        args.port,
        allow_time_override=config.allow_time_override,
    )

    host, port = server.server_address[:2]
    recovery = coordinator.get_recovery_report()
    print(
        json.dumps(
            {
                "event": "listening",
                "host": host,
                "port": port,
                "db": str(Path(args.db).resolve()),
                "instance_id": coordinator.instance_id,
                "config": coordinator.get_config(),
                "startup_recovery": {
                    "available": recovery.get("available", False),
                    "report_id": recovery.get("report_id"),
                    "pending_intents_aborted_count": recovery.get(
                        "pending_intents_aborted_count"
                    ),
                    "invalid_node_ids": recovery.get("heartbeat_recheck", {}).get(
                        "invalid_node_ids", []
                    ),
                },
            },
            ensure_ascii=False,
        ),
        flush=True,
    )

    def stop(_signum: int, _frame: object) -> None:
        raise KeyboardInterrupt

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    try:
        server.serve_forever(poll_interval=0.2)
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
