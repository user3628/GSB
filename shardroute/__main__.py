from __future__ import annotations

import argparse
import json
import os
import signal
import threading

from .datastore import SQLiteDataStore
from .engine import ShardRouteService
from .server import create_http_server, write_port_file
from .util import read_json


def _serve(args: argparse.Namespace) -> int:
    service = ShardRouteService(
        metadata_db=args.metadata_db,
        data_dir=args.data_dir,
        snapshot_file=args.snapshot,
        auto_load_snapshot=args.auto_load_snapshot,
    )
    server = create_http_server(service, host=args.host, port=args.port)
    if args.port_file:
        write_port_file(args.port_file, server)
    stop_event = server.stop_event

    def stop_handler(signum: int, frame: object) -> None:
        stop_event.set()

    previous_sigint = signal.signal(signal.SIGINT, stop_handler)
    previous_sigterm = signal.signal(signal.SIGTERM, stop_handler)
    server.timeout = 0.25
    readiness = {
        "event": "ready",
        "host": server.server_address[0],
        "port": server.server_address[1],
        "pid": os.getpid(),
        "run_id": service.run_id,
        "active_version": service._current_state_optional().version
        if service._current_state_optional()
        else None,
        "recovery_report": service.latest_recovery_report(),
    }
    print(json.dumps(readiness, ensure_ascii=False), flush=True)
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    try:
        while not stop_event.wait(0.2):
            pass
    finally:
        service.close(graceful=True)
        server.shutdown()
        server_thread.join(timeout=5)
        server.server_close()
        signal.signal(signal.SIGINT, previous_sigint)
        signal.signal(signal.SIGTERM, previous_sigterm)
    return 0


def _init_data(args: argparse.Namespace) -> int:
    seed = read_json(args.seed)
    report = SQLiteDataStore(args.data_dir).initialize(seed, replace=args.replace)
    print(json.dumps({"ok": True, **report}, ensure_ascii=False, indent=2))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m shardroute",
        description="Deterministic local multi-database sharding router",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    serve = subparsers.add_parser("serve", help="start the local HTTP routing service")
    serve.add_argument("--metadata-db", required=True)
    serve.add_argument("--data-dir", required=True)
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=0)
    serve.add_argument("--port-file")
    serve.add_argument("--snapshot")
    serve.add_argument(
        "--auto-load-snapshot",
        action="store_true",
        help="load --snapshot only when it is newer than the active version",
    )
    serve.set_defaults(func=_serve)
    init_data = subparsers.add_parser("init-data", help="initialize deterministic SQLite files")
    init_data.add_argument("--data-dir", required=True)
    init_data.add_argument("--seed", required=True)
    init_data.add_argument("--no-replace", action="store_false", dest="replace")
    init_data.set_defaults(func=_init_data, replace=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())

