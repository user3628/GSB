"""Command-line interface for local operations and crash-recovery probes."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Any

from .errors import SloError
from .models import dumps_json
from .service import SloService
from .storage import Storage


def print_json(value: Any, *, pretty: bool = True) -> None:
    sys.stdout.write(dumps_json(value, pretty=pretty) + "\n")


def add_db(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--db", required=True)


def add_format(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--format", default="auto", choices=["auto", "binary", "jsonl", "bin", "json"])


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="slo-budget")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("compatibility", help="print the explicit compatibility matrix")
    p.add_argument("--db")
    p.add_argument("--fixed-clock-ms", type=int)

    p = sub.add_parser("preflight", help="detect format/version and return a verdict")
    p.add_argument("path")
    add_format(p)
    p.add_argument("--db")
    p.add_argument("--fixed-clock-ms", type=int)

    p = sub.add_parser("import", help="atomically merge snapshot records")
    add_db(p)
    p.add_argument("path")
    add_format(p)
    p.add_argument("--truncation-rule", default="floor", choices=["floor", "ceil", "half_up"])
    p.add_argument("--batch-id")
    p.add_argument("--fixed-clock-ms", type=int)

    p = sub.add_parser("replace", help="atomically replace the active whole snapshot")
    add_db(p)
    p.add_argument("path")
    add_format(p)
    p.add_argument("--truncation-rule", default="floor", choices=["floor", "ceil", "half_up"])
    p.add_argument("--batch-id")
    p.add_argument("--fixed-clock-ms", type=int)

    p = sub.add_parser("export", help="export active records as binary or JSON Lines")
    add_db(p)
    p.add_argument("path")
    p.add_argument("--format", required=True, choices=["binary", "jsonl", "bin", "json"])
    p.add_argument("--endian", default="little", choices=["little", "big", "<", ">"])
    p.add_argument("--fixed-clock-ms", type=int)

    p = sub.add_parser("query", help="calculate multi-window SLI and burn rate")
    add_db(p)
    p.add_argument("--service", required=True)
    p.add_argument("--sli-type", required=True)
    p.add_argument("--as-of-ms", type=int, required=True)
    p.add_argument("--events-path")
    p.add_argument("--slo-target", default=None)
    p.add_argument("--min-samples", type=int, default=None)
    p.add_argument("--persist-alert", action="store_true")
    p.add_argument("--fixed-clock-ms", type=int)

    p = sub.add_parser("analyze-events", help="calculate directly from a request-event JSONL file")
    add_db(p)
    p.add_argument("--events-path", required=True)
    p.add_argument("--service", required=True)
    p.add_argument("--sli-type", required=True)
    p.add_argument("--as-of-ms", type=int, required=True)
    p.add_argument("--slo-target", default=None)
    p.add_argument("--min-samples", type=int, default=None)
    p.add_argument("--persist-alert", action="store_true")
    p.add_argument("--fixed-clock-ms", type=int)

    for name in ("state", "windows", "imports", "alerts", "recovery"):
        p = sub.add_parser(name)
        add_db(p)
        if name == "windows":
            p.add_argument("--service")
            p.add_argument("--sli-type")
        if name in {"imports", "alerts"}:
            p.add_argument("--limit", type=int, default=100)
        p.add_argument("--fixed-clock-ms", type=int)

    p = sub.add_parser("inject-crash", help="write a pending intent and exit without commit")
    add_db(p)
    p.add_argument("--kind", default="simulated_crash")
    p.add_argument("--batch-id", default="demo-crash-batch")
    p.add_argument("--fixed-clock-ms", type=int)
    return parser


def make_service(args: argparse.Namespace, *, db_required: bool = True) -> SloService:
    if args.fixed_clock_ms is not None:
        os.environ["SLO_FIXED_NOW_MS"] = str(args.fixed_clock_ms)
    db = getattr(args, "db", None)
    if db_required and not db:
        raise SloError("E_REQUEST_INVALID", "--db is required for this command")
    if not db:
        db = ":memory:"
    return SloService(db)


def run(args: argparse.Namespace) -> int:
    if args.command == "inject-crash":
        if args.fixed_clock_ms is not None:
            os.environ["SLO_FIXED_NOW_MS"] = str(args.fixed_clock_ms)
        storage = Storage(args.db)
        storage.begin_write_intent(
            args.kind,
            batch_id=args.batch_id,
            payload={"probe": "forced_process_exit", "pid": os.getpid()},
        )
        # Deliberately start a business transaction and never commit it. The OS
        # exit forces SQLite to roll it back while the committed intent survives.
        storage.connection.execute("BEGIN IMMEDIATE")
        storage.connection.execute("UPDATE snapshot_state SET record_count=999999 WHERE id=1")
        print_json(
            {
                "ok": False,
                "event": "forced_exit_with_pending_intent",
                "db": str(Path(args.db).resolve()),
                "batch_id": args.batch_id,
            }
        )
        sys.stdout.flush()
        os._exit(17)

    service = make_service(args, db_required=args.command not in {"compatibility", "preflight"})
    try:
        if args.command == "compatibility":
            print_json(service.compatibility())
        elif args.command == "preflight":
            print_json(service.preflight(args.path, expected_format=args.format))
        elif args.command == "import":
            print_json(
                service.import_file(
                    args.path,
                    expected_format=args.format,
                    truncation_rule=args.truncation_rule,
                    batch_id=args.batch_id,
                )
            )
        elif args.command == "replace":
            print_json(
                service.replace_snapshot(
                    args.path,
                    expected_format=args.format,
                    truncation_rule=args.truncation_rule,
                    batch_id=args.batch_id,
                )
            )
        elif args.command == "export":
            print_json(
                service.export_snapshot(
                    args.path,
                    file_format=args.format,
                    endian=args.endian,
                )
            )
        elif args.command == "query":
            print_json(
                service.query(
                    service=args.service,
                    sli_type=args.sli_type,
                    as_of_ms=args.as_of_ms,
                    events_path=args.events_path,
                    slo_target=args.slo_target,
                    min_samples=args.min_samples,
                    persist_alert=args.persist_alert,
                )
            )
        elif args.command == "analyze-events":
            print_json(
                service.analyze_events_file(
                    events_path=args.events_path,
                    service=args.service,
                    sli_type=args.sli_type,
                    as_of_ms=args.as_of_ms,
                    slo_target=args.slo_target,
                    min_samples=args.min_samples,
                    persist_alert=args.persist_alert,
                )
            )
        elif args.command == "state":
            print_json(service.state())
        elif args.command == "windows":
            print_json(service.windows(service=args.service, sli_type=args.sli_type))
        elif args.command == "imports":
            print_json(service.imports(limit=args.limit))
        elif args.command == "alerts":
            print_json(service.alerts(limit=args.limit))
        elif args.command == "recovery":
            print_json(service.recovery())
        else:
            raise SloError("E_REQUEST_INVALID", f"unknown command: {args.command}")
        return 0
    finally:
        service.close()


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return run(args)
    except SloError as error:
        print_json(error.to_dict())
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
