"""Command line entry points for the local middleware."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from .api import create_app, start_reaper
from .constants import DELIVERY_MODES
from .errors import MWError
from .samples import load_samples
from .scheduler import Scheduler
from .store import SQLiteStore
from .utils import new_id, validate_loopback_url


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True)


def _load_config(path: str | None, inline: str | None) -> dict[str, Any]:
    if path:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
    elif inline:
        value = json.loads(inline)
    else:
        value = {}
    if not isinstance(value, dict):
        raise MWError("E_INVALID_ARGUMENT", "round config must be a JSON object")
    return value


def cmd_init(args: argparse.Namespace) -> int:
    store = SQLiteStore(args.db)
    result = {"ok": True, "db": args.db, "revision": store.revision()}
    if args.recover:
        result["recovery"] = store.recover()
    print(_json(result))
    return 0


def cmd_recover(args: argparse.Namespace) -> int:
    store = SQLiteStore(args.db)
    report = store.recover(run_expired=not args.no_reap)
    if args.output:
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        Path(args.output).write_text(_json(report) + "\n", encoding="utf-8")
    print(_json(report))
    return 0


def cmd_load_samples(args: argparse.Namespace) -> int:
    store = SQLiteStore(args.db)
    result = load_samples(
        store,
        queues_path=args.queues,
        consumers_path=args.consumers,
        messages_path=args.messages,
    )
    print(_json(result))
    return 0


def cmd_run_round(args: argparse.Namespace) -> int:
    if args.mode not in DELIVERY_MODES:
        raise MWError("E_INVALID_ARGUMENT", f"mode must be one of {sorted(DELIVERY_MODES)}")
    store = SQLiteStore(args.db)
    scheduler = Scheduler(store)
    config = _load_config(args.config_file, args.config_json)
    request_id = args.request_id or new_id("cli_round_")
    validate_loopback_url(args.ack_base_url, "ack_base_url")
    result, replayed = store.execute_request(
        request_id,
        "round",
        lambda: scheduler.run_round(
            args.mode,
            config,
            request_id=request_id,
            ack_base_url=args.ack_base_url,
            consumer_id=args.consumer_id,
        ),
    )
    result["replayed"] = replayed
    result["traces"] = store.get_round(result["round_id"], include_traces=True)["traces"]
    if args.output:
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        Path(args.output).write_text(_json(result) + "\n", encoding="utf-8")
    print(_json(result))
    return 0


def cmd_snapshot(args: argparse.Namespace) -> int:
    store = SQLiteStore(args.db)
    snap = store.snapshot(args.queue)
    if args.output:
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        Path(args.output).write_text(_json(snap) + "\n", encoding="utf-8")
    print(_json(snap))
    return 0


def cmd_queue(args: argparse.Namespace) -> int:
    store = SQLiteStore(args.db)
    print(_json(store.get_queue_summary(args.name)))
    return 0


def cmd_message(args: argparse.Namespace) -> int:
    store = SQLiteStore(args.db)
    print(_json(store.get_message(args.queue, args.message_id)))
    return 0


def cmd_serve(args: argparse.Namespace) -> int:
    if args.host not in {"127.0.0.1", "::1", "localhost"}:
        raise MWError("E_INVALID_ARGUMENT", "serve only binds to a loopback host")
    validate_loopback_url(args.ack_base_url, "ack_base_url")
    app = create_app(
        args.db,
        ack_base_url=args.ack_base_url,
        recover_on_start=not args.no_recover,
    )
    if args.reaper_interval_ms > 0:
        start_reaper(app, args.reaper_interval_ms)
    if not args.no_recover:
        report = app.extensions["mqmw"].get("last_recovery")
        if report:
            print(_json({"startup_recovery": report}), file=sys.stderr)
    print(
        _json(
            {
                "ok": True,
                "url": f"http://{args.host}:{args.port}",
                "db": args.db,
                "reaper_interval_ms": args.reaper_interval_ms,
            }
        ),
        file=sys.stderr,
    )
    app.run(host=args.host, port=args.port, threaded=True, use_reloader=False)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m mqmw.cli")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("init", help="initialize a SQLite database")
    p.add_argument("--db", required=True)
    p.add_argument("--recover", action="store_true")
    p.set_defaults(func=cmd_init)

    p = sub.add_parser("recover", help="scan and roll back unfinished intents")
    p.add_argument("--db", required=True)
    p.add_argument("--output")
    p.add_argument("--no-reap", action="store_true")
    p.set_defaults(func=cmd_recover)

    p = sub.add_parser("load-samples", help="load fixed local JSON fixtures")
    p.add_argument("--db", required=True)
    p.add_argument("--queues", default="samples/queues.json")
    p.add_argument("--consumers", default="samples/consumers.json")
    p.add_argument("--messages", default="samples/messages.json")
    p.set_defaults(func=cmd_load_samples)

    p = sub.add_parser("run-round", help="execute one budgeted dispatch round")
    p.add_argument("--db", required=True)
    p.add_argument("--mode", required=True)
    p.add_argument("--config-json")
    p.add_argument("--config-file")
    p.add_argument("--consumer-id")
    p.add_argument("--request-id")
    p.add_argument("--ack-base-url", default="http://127.0.0.1:8080")
    p.add_argument("--output")
    p.set_defaults(func=cmd_run_round)

    p = sub.add_parser("snapshot", help="read one consistent DB snapshot")
    p.add_argument("--db", required=True)
    p.add_argument("--queue")
    p.add_argument("--output")
    p.set_defaults(func=cmd_snapshot)

    p = sub.add_parser("queue", help="show queue summary")
    p.add_argument("--db", required=True)
    p.add_argument("--name", required=True)
    p.set_defaults(func=cmd_queue)

    p = sub.add_parser("message", help="show message history")
    p.add_argument("--db", required=True)
    p.add_argument("--queue", required=True)
    p.add_argument("--message-id", required=True)
    p.set_defaults(func=cmd_message)

    p = sub.add_parser("serve", help="run the middleware HTTP API")
    p.add_argument("--db", required=True)
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8080)
    p.add_argument("--ack-base-url", default=None)
    p.add_argument("--reaper-interval-ms", type=int, default=500)
    p.add_argument("--no-recover", action="store_true")
    p.set_defaults(func=cmd_serve)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if getattr(args, "ack_base_url", None) is None:
        args.ack_base_url = f"http://127.0.0.1:{args.port}" if hasattr(args, "port") else "http://127.0.0.1:8080"
    try:
        return int(args.func(args))
    except MWError as exc:
        print(_json(exc.to_dict()), file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
