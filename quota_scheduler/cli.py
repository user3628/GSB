"""Command-line interface for local evaluation and deterministic demos."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Sequence

from .api import create_app
from .errors import SchedulerError
from .service import QuotaScheduler


def _print(value: Any) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=False, default=str))


def _load_json_file(path: str) -> Any:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _service(args: argparse.Namespace) -> QuotaScheduler:
    overrides: dict[str, Any] = {}
    if getattr(args, "config_json", None):
        loaded = json.loads(args.config_json)
        if not isinstance(loaded, dict):
            raise ValueError("--config-json must be a JSON object")
        overrides = loaded
    return QuotaScheduler(args.db, overrides)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="quota-scheduler",
        description="Local GPU quota admission, inference batching and round scheduler",
    )
    parser.add_argument("--db", default="quota_scheduler.db", help="SQLite database path")
    parser.add_argument("--config-json", default=None, help="configuration override JSON")
    sub = parser.add_subparsers(dest="command", required=True)

    init = sub.add_parser("init", help="create schema and load deterministic specifications")
    init.add_argument("--samples", default="sample_data")
    init.add_argument("--load-requests", action="store_true")
    init.add_argument("--auto-admit", action="store_true")

    submit = sub.add_parser("submit", help="submit one request or a JSON array of requests")
    submit.add_argument("--file", required=True)
    submit.add_argument("--auto-admit", action="store_true")

    admit = sub.add_parser("admit", help="admit pending requests")
    admit.add_argument("request_id", nargs="?")
    admit.add_argument("--all", action="store_true")
    admit.add_argument("--no-preemption", action="store_true")

    transition = sub.add_parser("transition", help="perform a state-machine transition")
    transition.add_argument("request_id")
    transition.add_argument("to_state")
    transition.add_argument("--reason", default="manual_cli_transition")

    preempt = sub.add_parser("preempt", help="preempt a request and recycle its quota")
    preempt.add_argument("request_id")
    preempt.add_argument("--reason", default="manual_cli_preemption")
    preempt.add_argument("--by", dest="by_request_id", default=None)

    organize = sub.add_parser("organize", help="organize ADMITTED requests into inference batches")
    organize.add_argument("--strategy", choices=["auto", "greedy", "submission", "memory", "memory_descending"], default=None)
    organize.add_argument("--strict", action="store_true")
    organize.add_argument("--request-id", action="append", dest="request_ids")

    schedule = sub.add_parser("schedule", help="dispatch one execution round")
    schedule.add_argument("--round", type=int, default=None)
    schedule.add_argument("--max-parallel", type=int, default=None)
    schedule.add_argument("--time-budget", default=None)
    schedule.add_argument("--queue-capacity", type=int, default=None)

    complete = sub.add_parser("complete", help="complete a running batch or all running batches in a round")
    target = complete.add_mutually_exclusive_group(required=True)
    target.add_argument("--batch")
    target.add_argument("--round", type=int)

    recovery = sub.add_parser("recover", help="scan durable intents and recover")
    recovery.add_argument("--abort-running", action="store_true")

    sub.add_parser("summary", help="show persisted state and invariants")
    sub.add_parser("quota", help="show quota and node usage")

    intents = sub.add_parser("intents", help="show intent journal")
    intents.add_argument("--status", choices=["PREPARED", "COMMITTED", "ABORTED"], default=None)
    intents.add_argument("--limit", type=int, default=200)

    serve = sub.add_parser("serve", help="run the Flask HTTP API")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8000)
    serve.add_argument("--samples", default="sample_data")
    serve.add_argument("--bootstrap", action="store_true")
    serve.add_argument("--load-requests", action="store_true")
    serve.add_argument("--auto-admit", action="store_true")
    serve.add_argument("--no-recover-on-start", action="store_true")

    demo = sub.add_parser("demo", help="run the complete deterministic local scenario")
    demo.add_argument("--samples", default="sample_data")
    demo.add_argument("--keep", action="store_true", help="do not remove the database first")

    return parser


def main(argv: Sequence[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "demo":
            _print(_run_demo(args))
            return
        if args.command == "serve":
            app = create_app(
                args.db,
                sample_dir=args.samples,
                bootstrap=args.bootstrap,
                load_requests=args.load_requests,
                auto_admit=args.auto_admit,
                recover_on_start=not args.no_recover_on_start,
            )
            app.run(host=args.host, port=args.port, debug=False)
            return
        service = _service(args)
        if args.command == "init":
            result = service.bootstrap(
                args.samples,
                load_requests=args.load_requests,
                auto_admit=args.auto_admit,
            )
        elif args.command == "submit":
            payload = _load_json_file(args.file)
            if isinstance(payload, list):
                results = [
                    service.submit_request(item, auto_admit=args.auto_admit)
                    for item in payload
                ]
                result = {"ok": True, "count": len(results), "requests": results}
            elif isinstance(payload, dict):
                result = service.submit_request(payload, auto_admit=args.auto_admit)
            else:
                raise SchedulerError("E_VALIDATION", "request file must contain an object or array")
        elif args.command == "admit":
            if args.all:
                pending = service.list_requests("PENDING")["requests"]
                results = [
                    service.admit_request(
                        item["request_id"], allow_preemption=not args.no_preemption
                    )
                    for item in pending
                ]
                result = {"ok": True, "count": len(results), "admissions": results}
            elif args.request_id:
                result = service.admit_request(
                    args.request_id, allow_preemption=not args.no_preemption
                )
            else:
                raise SchedulerError("E_VALIDATION", "provide request_id or --all")
        elif args.command == "transition":
            result = service.transition_request(args.request_id, args.to_state, args.reason)
        elif args.command == "preempt":
            result = service.preempt_request(
                args.request_id, args.reason, by_request_id=args.by_request_id
            )
        elif args.command == "organize":
            result = service.organize_batches(
                strategy=args.strategy,
                strict=True if args.strict else None,
                request_ids=args.request_ids,
            )
        elif args.command == "schedule":
            result = service.schedule_round(
                round_no=args.round,
                max_parallel_batches=args.max_parallel,
                time_budget_millis=args.time_budget,
                queue_capacity=args.queue_capacity,
            )
        elif args.command == "complete":
            if args.batch:
                result = service.complete_batch(args.batch)
            else:
                result = service.complete_round(args.round)
        elif args.command == "recover":
            result = service.recover(abort_running=args.abort_running)
        elif args.command == "summary":
            result = service.state_summary()
        elif args.command == "quota":
            result = service.quota_snapshot()
        elif args.command == "intents":
            result = service.list_intents(args.status, limit=args.limit)
        else:  # pragma: no cover
            parser.error("unknown command")
            return
        _print(result)
    except (SchedulerError, OSError, ValueError, json.JSONDecodeError) as exc:
        if isinstance(exc, SchedulerError):
            payload = {"ok": False, "error": exc.to_dict()}
        else:
            payload = {
                "ok": False,
                "error": {"code": "E_VALIDATION", "message": str(exc)},
            }
        print(json.dumps(payload, ensure_ascii=False, indent=2), file=sys.stderr)
        raise SystemExit(1)


def _run_demo(args: argparse.Namespace) -> dict[str, Any]:
    db_path = Path(args.db)
    if not args.keep:
        for suffix in ("", "-wal", "-shm", ".intents.jsonl"):
            try:
                Path(str(db_path) + suffix).unlink()
            except FileNotFoundError:
                pass
    service = QuotaScheduler(db_path)
    bootstrap = service.bootstrap(args.samples)
    bundle_requests = service.load_sample_requests(args.samples, auto_admit=False)
    admissions: list[dict[str, Any]] = []
    for item in bundle_requests["requests"]:
        if item.get("status") == "PENDING":
            admissions.append(service.admit_request(item["request_id"]))
    batching = service.organize_batches(strategy="auto")
    rounds: list[dict[str, Any]] = []
    for _ in range(12):
        summary = service.state_summary()
        pending_batches = sum(
            summary["batches"].get(status, 0)
            for status in ("PLANNED", "QUEUED", "RUNNING")
        )
        if pending_batches == 0:
            break
        scheduled = service.schedule_round()
        completed = service.complete_round(scheduled["round_no"])
        rounds.append({"scheduled": scheduled, "completed": completed})
    recovery = service.recover(abort_running=False)
    final = service.state_summary()
    requests = service.list_requests()["requests"]
    return {
        "ok": final["invariants"]["ok"] and batching["ok"],
        "database": service.database_path,
        "bootstrap": bootstrap,
        "submitted": bundle_requests["requests"],
        "admissions": [
            {
                "request_id": item["request_id"],
                "status": item["status"],
                "reservation": item["reservation"],
                "preemptions": item.get("preemptions", []),
            }
            for item in admissions
        ],
        "batching": batching,
        "rounds": rounds,
        "recovery": recovery,
        "final_requests": [
            {
                "request_id": item["request_id"],
                "status": item["status"],
                "reservation_active": item["reservation"]["active"]
                if item["reservation"]
                else False,
                "batches": [
                    {"batch_id": batch["batch_id"], "status": batch["status"]}
                    for batch in item["batches"]
                ],
            }
            for item in requests
        ],
        "summary": final,
    }


if __name__ == "__main__":  # pragma: no cover
    main()

