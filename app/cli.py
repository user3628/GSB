from __future__ import annotations

import argparse
import json
import socket
import sys
from pathlib import Path

from app.config import Settings
from app.models import BatchIn
from app.seed import seed_if_empty
from app.service import GatewayService
from app.store import Store


def _service(database: str | Path, samples: str | Path | None = None, allow_chaos: bool = False) -> GatewayService:
    settings = Settings(
        database_path=Path(database),
        samples_dir=Path(samples) if samples else Path(__file__).resolve().parent.parent / "samples",
        allow_chaos=allow_chaos,
    )
    return GatewayService(Store(settings.database_path), settings)


def port_is_free(host: str, port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind((host, port))
        except OSError:
            return False
    return True


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Local object gateway quota control plane CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    seed = sub.add_parser("seed")
    seed.add_argument("--database", default="data/gateway.db")
    seed.add_argument("--samples", default="samples")

    crash = sub.add_parser("crash-batch", help="Persist intent then terminate with exit code 91")
    crash.add_argument("--database", required=True)
    crash.add_argument("--samples", default="samples")
    crash.add_argument("--batch-file", default="samples/crash_batch.json")

    crash_rebuild = sub.add_parser(
        "crash-rebuild",
        help="Persist a rebuild intent, start the rebuild transaction, then exit with code 92",
    )
    crash_rebuild.add_argument("--database", required=True)
    crash_rebuild.add_argument("--samples", default="samples")
    crash_rebuild.add_argument("--patch-file", default="samples/incremental_patch.json")

    recover = sub.add_parser("recover")
    recover.add_argument("--database", required=True)
    recover.add_argument("--samples", default="samples")

    status = sub.add_parser("status")
    status.add_argument("--database", default="data/gateway.db")
    status.add_argument("--samples", default="samples")

    serve = sub.add_parser("serve")
    serve.add_argument("--database", default="data/gateway.db")
    serve.add_argument("--samples", default="samples")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=18081)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "seed":
        service = _service(args.database, args.samples)
        report = seed_if_empty(service, Path(args.samples))
        print(json.dumps(report, indent=2, sort_keys=True))
        service.store.close()
        return 0
    if args.command == "crash-batch":
        service = _service(args.database, args.samples, allow_chaos=True)
        service.recover_pending("crash_worker_startup")
        seed_if_empty(service, Path(args.samples))
        payload = BatchIn.model_validate(json.loads(Path(args.batch_file).read_text(encoding="utf-8")))
        # With ALLOW_CHAOS enabled, process_batch calls os._exit(91) after the
        # durable intent commit and before the state-changing transaction.
        service.process_batch(payload)
        print("crash point was not reached", file=sys.stderr)
        return 2
    if args.command == "crash-rebuild":
        from app.models import PolicyPatchIn

        service = _service(args.database, args.samples, allow_chaos=True)
        service.recover_pending("crash_worker_startup")
        seed_if_empty(service, Path(args.samples))
        payload = PolicyPatchIn.model_validate(
            json.loads(Path(args.patch_file).read_text(encoding="utf-8"))
        )
        payload.faults.crash_during_rebuild = True
        service.patch_policy(payload)
        print("rebuild crash point was not reached", file=sys.stderr)
        return 2
    if args.command == "recover":
        service = _service(args.database, args.samples)
        report = service.recover_pending("cli_restart")
        print(json.dumps(report, indent=2, sort_keys=True))
        service.store.close()
        return 0
    if args.command == "status":
        service = _service(args.database, args.samples)
        print(
            json.dumps(
                {
                    "database": str(service.store.database_path),
                    "policy_version": service.store.current_version(),
                    "last_event_ms": service.store.last_event_ms(),
                    "pending_intents": service.store.scalar(
                        "SELECT COUNT(*) FROM intents WHERE status='PENDING'"
                    ),
                    "recovery_report": service.latest_recovery_report(),
                },
                indent=2,
                sort_keys=True,
            )
        )
        service.store.close()
        return 0
    if args.command == "serve":
        if not port_is_free(args.host, args.port):
            print(f"port {args.host}:{args.port} is already in use", file=sys.stderr)
            return 3
        settings = Settings(database_path=Path(args.database), samples_dir=Path(args.samples))
        from app.main import create_app
        import uvicorn

        uvicorn.run(create_app(settings), host=args.host, port=args.port, log_level="info")
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
