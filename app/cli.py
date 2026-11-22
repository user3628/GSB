from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import uvicorn

from .api import create_app
from .db import remove_database
from .service import TieringService


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Local hot/cold tiering migration service")
    subparsers = parser.add_subparsers(dest="command", required=True)

    init_parser = subparsers.add_parser("init", help="initialize and seed a local database")
    init_parser.add_argument("--db", default="data/tiering.db")
    init_parser.add_argument("--samples", default="samples")
    init_parser.add_argument("--reset", action="store_true")

    serve_parser = subparsers.add_parser("serve", help="start the local HTTP service")
    serve_parser.add_argument("--db", default="data/tiering.db")
    serve_parser.add_argument("--samples", default="samples")
    serve_parser.add_argument("--host", default="127.0.0.1")
    serve_parser.add_argument("--port", type=int, default=8765)
    serve_parser.add_argument("--log-level", default="info")

    inspect_parser = subparsers.add_parser("inspect", help="print current persisted summaries")
    inspect_parser.add_argument("--db", default="data/tiering.db")
    inspect_parser.add_argument("--samples", default="samples")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    if args.command == "init":
        if args.reset:
            remove_database(args.db)
        service = TieringService(args.db, samples_dir=args.samples, auto_seed=True)
        service.startup()
        try:
            print(json.dumps(service.get_occupancy(), ensure_ascii=False, indent=2))
        finally:
            service.safe_shutdown("cli_init_complete")
        return 0

    if args.command == "inspect":
        service = TieringService(args.db, samples_dir=args.samples, auto_seed=False)
        service.startup()
        try:
            payload = {
                "policy": service.get_current_policy(),
                "occupancy": service.get_occupancy(),
                "recovery": service.get_recovery_report(),
            }
            print(json.dumps(payload, ensure_ascii=False, indent=2))
        finally:
            service.safe_shutdown("cli_inspect_complete")
        return 0

    if args.command == "serve":
        app = create_app(db_path=args.db, samples_dir=args.samples, auto_seed=True)
        uvicorn.run(app, host=args.host, port=args.port, log_level=args.log_level, workers=1)
        return 0

    return 2


if __name__ == "__main__":
    sys.exit(main())
