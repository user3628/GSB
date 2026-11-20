"""Command-line entry points for setup, serving, direct planning, and demos."""

from __future__ import annotations

import argparse
import platform
import signal
import sqlite3
import sys
import threading
from pathlib import Path
from typing import Any

from .business import EngineError
from .executor import execute_plan
from .expressions import canonical_json
from .parser import parse_sql
from .plan import format_plan_text, node_summary, serialize_plan
from .planner import build_logical_plan
from .rewriter import normalize_plan
from .sample import (
    DEFAULT_MANIFEST,
    DEFAULT_RUNTIME_DB,
    DEFAULT_SAMPLE_DB,
    Catalog,
    generate_sample_database,
    load_manifest,
)
from .server import create_server
from .service import PlanService
from .store import PlanStore


def _require_python() -> None:
    if sys.version_info < (3, 11):
        raise SystemExit("Python 3.11 or newer is required")


def _ensure_sample(path: Path) -> None:
    if not path.exists():
        generate_sample_database(path)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="sqlplan-engine")
    subparsers = parser.add_subparsers(dest="command", required=True)

    init = subparsers.add_parser("init", help="create the fixed sample DB and runtime store")
    init.add_argument("--sample-db", type=Path, default=DEFAULT_SAMPLE_DB)
    init.add_argument("--store-db", type=Path, default=DEFAULT_RUNTIME_DB)

    serve = subparsers.add_parser("serve", help="run the local HTTP service")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8765)
    serve.add_argument("--sample-db", type=Path, default=DEFAULT_SAMPLE_DB)
    serve.add_argument("--store-db", type=Path, default=DEFAULT_RUNTIME_DB)
    serve.add_argument("--max-nodes", type=int, default=64)
    serve.add_argument("--max-rewrite-steps", type=int, default=500)
    serve.add_argument("--exit-summary", type=Path, default=None)

    plan = subparsers.add_parser("plan", help="plan one SQL statement without HTTP")
    plan.add_argument("--sql", required=True)
    plan.add_argument("--sample-db", type=Path, default=DEFAULT_SAMPLE_DB)
    plan.add_argument("--abort-after", type=int, default=None)

    manifest = subparsers.add_parser("manifest", help="print the fixed statement manifest")
    manifest.add_argument("--path", type=Path, default=DEFAULT_MANIFEST)

    doctor = subparsers.add_parser("doctor", help="check local prerequisites")
    doctor.add_argument("--sample-db", type=Path, default=DEFAULT_SAMPLE_DB)

    demo = subparsers.add_parser("demo", help="run the deterministic end-to-end demonstration")
    demo.add_argument("--output", type=Path, default=None)

    return parser


def main(argv: list[str] | None = None) -> int:
    _require_python()
    args = build_parser().parse_args(argv)
    try:
        if args.command == "init":
            sample = generate_sample_database(args.sample_db)
            store = PlanStore(args.store_db)
            report = store.recover_pending_intents()
            print(
                canonical_json(
                    {
                        "ok": True,
                        "sample_db": str(sample),
                        "store_db": str(args.store_db),
                        "recovery_report": report,
                    }
                )
            )
            return 0
        if args.command == "serve":
            return _serve(args)
        if args.command == "plan":
            return _plan(args)
        if args.command == "manifest":
            print(canonical_json({"ok": True, "statements": load_manifest(args.path)}))
            return 0
        if args.command == "doctor":
            return _doctor(args)
        if args.command == "demo":
            from .demo import run_demo

            report = run_demo(output=args.output)
            print(canonical_json({"ok": True, "demo_report": str(report)}))
            return 0
    except EngineError as exc:
        print(canonical_json(exc.to_dict()), file=sys.stderr)
        return 2
    return 1


def _serve(args: argparse.Namespace) -> int:
    _ensure_sample(args.sample_db)
    service = PlanService(
        sample_db=args.sample_db,
        store_db=args.store_db,
        max_nodes=args.max_nodes,
        max_rewrite_steps=args.max_rewrite_steps,
    )
    server = create_server(service, host=args.host, port=args.port)
    host, port = server.server_address[:2]

    def request_shutdown(signum: int | None = None, frame: Any = None) -> None:
        del signum, frame
        threading.Thread(target=server.shutdown, daemon=True).start()

    for signal_name in ("SIGTERM", "SIGINT"):
        if hasattr(signal, signal_name):
            try:
                signal.signal(getattr(signal, signal_name), request_shutdown)
            except (ValueError, OSError):
                pass

    print(
        canonical_json(
            {
                "ok": True,
                "event": "server_started",
                "host": host,
                "port": port,
                "recovery_report": service.recovery_report,
            }
        ),
        flush=True,
    )
    try:
        server.serve_forever(poll_interval=0.1)
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        summary = service.safe_exit_summary()
        if args.exit_summary is not None:
            args.exit_summary.parent.mkdir(parents=True, exist_ok=True)
            args.exit_summary.write_text(canonical_json(summary) + "\n", encoding="utf-8")
        print(canonical_json({"event": "graceful_shutdown", **summary}), flush=True)
    return 0


def _plan(args: argparse.Namespace) -> int:
    _ensure_sample(args.sample_db)
    catalog = Catalog(args.sample_db)
    statement = parse_sql(args.sql)
    initial = build_logical_plan(statement, catalog)
    normalized = normalize_plan(initial, abort_after=args.abort_after)
    execution = execute_plan(normalized.plan, args.sample_db)
    print(
        canonical_json(
            {
                "ok": True,
                "business_code": "OK",
                "plan_text": format_plan_text(normalized.plan),
                "normalized_plan": serialize_plan(normalized.plan),
                "node_summary": node_summary(normalized.plan),
                "rewrite_log": normalized.log,
                "rejections": normalized.rejections,
                "node_mapping": normalized.node_mapping,
                "query_result": execution.as_dict(),
            }
        )
    )
    return 0


def _doctor(args: argparse.Namespace) -> int:
    _ensure_sample(args.sample_db)
    connection = sqlite3.connect(
        args.sample_db.resolve().as_uri() + "?mode=ro", uri=True
    )
    try:
        tables = [
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
            )
        ]
    finally:
        connection.close()
    print(
        canonical_json(
            {
                "ok": True,
                "python": platform.python_version(),
                "python_requirement": ">=3.11",
                "sqlite_version": sqlite3.sqlite_version,
                "sample_db": str(args.sample_db),
                "tables": tables,
                "stdlib_only_core": True,
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

