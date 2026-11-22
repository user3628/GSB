"""Command line entry points for local operation and verification."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from .engine import CapacityEngine
from .errors import AppError
from .repository import Database
from .server import CapacityApplication, CapacityHttpServer


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m capacity_lab.cli")
    subparsers = parser.add_subparsers(dest="command", required=True)

    serve = subparsers.add_parser("serve", help="start the HTTP application")
    _db_argument(serve)
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=18080)

    submit = subparsers.add_parser("submit", help="submit a plan JSON file")
    _db_argument(submit)
    submit.add_argument("--plan-file", required=True)
    submit.add_argument("--plan-id")

    run = subparsers.add_parser("run", help="execute the latest plan version")
    _db_argument(run)
    run.add_argument("--plan-id", required=True)
    run.add_argument("--version", type=int)

    report = subparsers.add_parser("report", help="print a run or plan report")
    _db_argument(report)
    target = report.add_mutually_exclusive_group(required=True)
    target.add_argument("--run-id")
    target.add_argument("--plan-id")

    recover = subparsers.add_parser("recover", help="scan unresolved intents and print a report")
    _db_argument(recover)

    args = parser.parse_args(argv)
    try:
        if args.command == "serve":
            application = CapacityApplication(args.db)
            server = CapacityHttpServer((args.host, args.port), application)
            print(
                f"capacity lab listening on http://{args.host}:{server.server_port}",
                flush=True,
            )
            try:
                server.serve_forever(poll_interval=0.2)
            except KeyboardInterrupt:
                pass
            finally:
                server.server_close()
            return 0

        database = Database(args.db)
        database.initialise()
        if args.command == "recover":
            return _print({"ok": True, "data": database.recover()})
        if args.command == "submit":
            plan_file = Path(args.plan_file)
            payload = json.loads(plan_file.read_text(encoding="utf-8"))
            plan_id = args.plan_id or payload.get("plan_id") or payload.get("id")
            if not isinstance(plan_id, str) or not plan_id:
                raise AppError(
                    "E_PLAN_INVALID",
                    "plan id must be supplied with --plan-id or plan JSON plan_id",
                    http_status=422,
                )
            return _print({"ok": True, "data": database.submit_plan(plan_id, payload)})
        if args.command == "run":
            capacity = CapacityEngine(database)
            result = capacity.create_and_execute_run(args.plan_id, args.version)
            return _print({"ok": True, "data": result})
        if args.command == "report":
            if args.run_id:
                data = database.get_run(args.run_id)
            else:
                data = database.latest_run_for_plan(args.plan_id)
                if data is None:
                    raise AppError("E_NOT_FOUND", "no run found for plan", http_status=404)
            return _print({"ok": True, "data": data})
    except AppError as exc:
        print(json.dumps(exc.to_dict(), ensure_ascii=False, indent=2))
        return 2
    except Exception as exc:
        payload = {
            "ok": False,
            "error": {
                "code": "E_INTERNAL",
                "message": "unexpected command failure",
                "details": {"type": type(exc).__name__, "message": str(exc)},
            },
        }
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 1
    return 0


def _db_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--db", default="capacity_lab.db")


def _print(payload: dict[str, Any]) -> int:
    print(json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())

