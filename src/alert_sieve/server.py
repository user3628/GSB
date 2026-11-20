from __future__ import annotations

import argparse
import json
import signal
import threading
from pathlib import Path

from .http_api import make_server
from .store import Store


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Alert Sieve local HTTP server")
    parser.add_argument("--db", required=True, help="SQLite database path")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765, help="0 selects an ephemeral port")
    parser.add_argument(
        "--enable-fault-injection",
        action="store_true",
        help="enable local-only X-Debug-* forced-crash headers for recovery demos",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    Path(args.db).parent.mkdir(parents=True, exist_ok=True)
    store = Store(args.db)
    server = make_server(
        store,
        args.host,
        args.port,
        enable_fault_injection=args.enable_fault_injection,
    )
    bound_port = int(server.server_address[1])
    print(
        json.dumps(
            {
                "ready": True,
                "host": args.host,
                "port": bound_port,
                "db": str(Path(args.db).resolve()),
                "fault_injection": args.enable_fault_injection,
            },
            sort_keys=True,
        ),
        flush=True,
    )

    stopping = threading.Event()

    def _stop(signum, frame):  # noqa: ARG001
        stopping.set()
        threading.Thread(target=server.shutdown, daemon=True).start()

    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _stop)
    if hasattr(signal, "SIGINT"):
        signal.signal(signal.SIGINT, _stop)
    try:
        server.serve_forever(poll_interval=0.1)
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
