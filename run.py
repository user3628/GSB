from __future__ import annotations

import argparse
import json
import sys

from slowquery.api import create_app
from slowquery.config import WorkbenchConfig


def _add_common(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--config", help="JSON configuration file")
    parser.add_argument("--database", dest="database_path", help="Workbench SQLite database path")
    parser.add_argument("--sample-db", dest="sample_database_path", help="Sample SQLite database path")
    parser.add_argument("--host", help="HTTP bind host")
    parser.add_argument("--port", type=int, help="HTTP port")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Local SQLite slow-query diagnosis and rewrite workbench"
    )
    sub = parser.add_subparsers(dest="command", required=True)
    serve = sub.add_parser("serve", help="Run the local Flask HTTP API")
    _add_common(serve)
    check = sub.add_parser("check", help="Initialize the DB, run recovery, and print a JSON health summary")
    _add_common(check)
    return parser


def _config(args: argparse.Namespace) -> WorkbenchConfig:
    overrides = {
        "database_path": args.database_path,
        "sample_database_path": args.sample_database_path,
        "host": args.host,
        "port": args.port,
    }
    return WorkbenchConfig.from_sources(args.config, overrides)


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = _config(args)
    if args.command == "check":
        app = create_app(config)
        service = app.extensions["slowquery"]
        print(
            json.dumps(
                {
                    "database": str(config.database_path),
                    "sample_database": str(config.sample_database_path),
                    "active_snapshot_version": service.repo.active_snapshot_version(),
                    "recovery_report_id": (
                        None
                        if service.last_recovery is None
                        else service.last_recovery.get("report_id")
                    ),
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0
    app = create_app(config)
    print(f"Slow-query workbench listening on http://{config.host}:{config.port}", flush=True)
    app.run(host=config.host, port=config.port, threaded=True, use_reloader=False)
    return 0


if __name__ == "__main__":
    sys.exit(main())
