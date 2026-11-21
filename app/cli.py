from __future__ import annotations

import argparse
import json
import socket
import sys
from pathlib import Path
from typing import Any

import uvicorn

from .db import Database
from .main import PROJECT_ROOT, create_app
from .service import ScalingControlService


DEFAULT_DB = PROJECT_ROOT / "data" / "scaler.db"


def _remove_sqlite_files(path: Path) -> None:
    for candidate in (path, Path(str(path) + "-wal"), Path(str(path) + "-shm")):
        try:
            candidate.unlink()
        except FileNotFoundError:
            pass


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _cmd_init(args: argparse.Namespace) -> int:
    db_path = Path(args.db)
    if args.reset:
        _remove_sqlite_files(db_path)
    db = Database(db_path)
    service = ScalingControlService(db)
    policy_path = Path(args.policy)
    raw = _read_json(policy_path)
    result = service.initialize_policy(raw)
    output = {
        "db": str(db_path),
        "policy_version": result["policy_version"],
        "revision": result["revision"],
        "services": [
            {
                "id": item["id"],
                "state": item["state"],
                "replicas": item["current_replicas"],
            }
            for item in result["state"]["services"]
        ],
    }
    print(json.dumps(output, ensure_ascii=False, indent=2))
    return 0


def _cmd_serve(args: argparse.Namespace) -> int:
    app = create_app(args.db)
    uvicorn.run(
        app,
        host=args.host,
        port=args.port,
        log_level=args.log_level,
        access_log=True,
    )
    return 0


def _cmd_recover(args: argparse.Namespace) -> int:
    db = Database(args.db)
    events = db.recover_incomplete()
    print(json.dumps({"new_events": events, "report": db.recovery_report()}, ensure_ascii=False, indent=2))
    return 0


def _cmd_status(args: argparse.Namespace) -> int:
    db = Database(args.db)
    state = db.latest_state()
    print(
        json.dumps(
            {
                "db": args.db,
                "policy_version": db.current_policy_version(),
                "latest_revision": db.latest_revision(),
                "pending_intents": db.pending_intent_count(),
                "state": None
                if state is None
                else {
                    "revision": state.revision,
                    "services": [item.as_dict() for item in state.services],
                    "nodes": [item.as_dict() for item in state.nodes],
                },
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


def _cmd_check_port(args: argparse.Namespace) -> int:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((args.host, args.port))
        print(f"PORT_AVAILABLE {args.host}:{args.port}")
        return 0
    except OSError as exc:
        print(f"PORT_UNAVAILABLE {args.host}:{args.port}: {exc}", file=sys.stderr)
        return 2
    finally:
        sock.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m app.cli",
        description="Local deterministic scaling arbiter control CLI",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    init = sub.add_parser("init", help="initialize or update a policy snapshot")
    init.add_argument("--db", default=str(DEFAULT_DB))
    init.add_argument("--policy", default=str(PROJECT_ROOT / "samples" / "policy_v1.json"))
    init.add_argument("--reset", action="store_true")
    init.set_defaults(func=_cmd_init)

    serve = sub.add_parser("serve", help="start the local HTTP service")
    serve.add_argument("--db", default=str(DEFAULT_DB))
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", default=8000, type=int)
    serve.add_argument("--log-level", default="info")
    serve.set_defaults(func=_cmd_serve)

    recover = sub.add_parser("recover", help="scan and abort unfinished intents")
    recover.add_argument("--db", default=str(DEFAULT_DB))
    recover.set_defaults(func=_cmd_recover)

    status = sub.add_parser("status", help="print the latest committed state")
    status.add_argument("--db", default=str(DEFAULT_DB))
    status.set_defaults(func=_cmd_status)

    check = sub.add_parser("check-port", help="verify a local TCP port can be bound")
    check.add_argument("--host", default="127.0.0.1")
    check.add_argument("--port", default=8000, type=int)
    check.set_defaults(func=_cmd_check_port)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
