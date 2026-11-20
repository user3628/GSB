#!/usr/bin/env python3
"""Start the loopback MVCC HTTP service."""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import threading
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from mvcc_engine.config import load_settings  # noqa: E402
from mvcc_engine.engine import MVCCEngine  # noqa: E402
from mvcc_engine.http_api import build_server  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the local MVCC visibility engine")
    parser.add_argument("--root", default=str(ROOT))
    parser.add_argument("--params", default=None)
    parser.add_argument("--data-dir", default=None)
    parser.add_argument("--db", default=None)
    parser.add_argument("--host", default=None)
    parser.add_argument("--port", type=int, default=None)
    parser.add_argument("--quiet", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    settings = load_settings(
        root=args.root,
        params_path=args.params,
        data_dir=args.data_dir,
        db_path=args.db,
        host=args.host,
        port=args.port,
    )
    engine = MVCCEngine(settings)
    server = build_server(engine, settings.host, settings.port, quiet=args.quiet)
    actual_host, actual_port = server.server_address[:2]
    runtime = {
        "host": actual_host,
        "port": int(actual_port),
        "pid": os.getpid(),
        "started_at": server.started_at,
        "data_dir": str(settings.data_dir),
        "db_path": str(settings.db_path),
        "recovery_status": engine.get_recovery_report().get("status"),
    }
    engine.storage.write_json_atomic(settings.runtime_path, runtime)

    stop_requested = threading.Event()

    def request_stop(signum: int, _frame: object) -> None:
        if stop_requested.is_set():
            return
        stop_requested.set()
        threading.Thread(target=server.shutdown, name="http-shutdown", daemon=True).start()

    for signal_name in ("SIGINT", "SIGTERM"):
        if hasattr(signal, signal_name):
            try:
                signal.signal(getattr(signal, signal_name), request_stop)
            except (ValueError, OSError):
                pass

    print(
        "SERVER_READY " + json.dumps(runtime, ensure_ascii=False, separators=(",", ":")),
        flush=True,
    )
    try:
        server.serve_forever(poll_interval=0.2)
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        report = engine.graceful_shutdown("service_stop")
        print(
            "SERVER_STOPPED "
            + json.dumps(
                {
                    "pid": os.getpid(),
                    "report_kind": report["report_kind"],
                    "active_transactions_rolled_back": report[
                        "active_transactions_rolled_back"
                    ],
                },
                ensure_ascii=False,
                separators=(",", ":"),
            ),
            flush=True,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

