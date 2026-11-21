from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path
from typing import Sequence


ROOT = Path(__file__).resolve().parents[1]


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="vgw",
        description="Loopback tunnel gateway: server, demo, recovery, and crash worker",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    serve = sub.add_parser("serve", help="run the FastAPI control plane")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=18765)
    serve.add_argument("--db", default=str(ROOT / "artifacts" / "vgw.db"))
    serve.add_argument("--topology", default=str(ROOT / "samples" / "topology.json"))
    serve.add_argument("--samples", default=str(ROOT / "samples"))
    serve.add_argument("--no-auto-sample", action="store_true")

    demo = sub.add_parser("demo", help="run the deterministic end-to-end demonstration")
    demo.add_argument("--output", default=str(ROOT / "artifacts" / "demo" / "current"))
    demo.add_argument("--keep-db", action="store_true", help="retain demo SQLite databases")

    recover = sub.add_parser("recover", help="scan incomplete intents and print a recovery report")
    recover.add_argument("--db", required=True)

    worker = sub.add_parser("crash-worker", help=argparse.SUPPRESS)
    worker.add_argument("--db", required=True)
    worker.add_argument("--snapshot", required=True)
    worker.add_argument("--marker", required=True)
    worker.add_argument("--frame-id", default="crash-probe")
    worker.add_argument("--session", default="alpha")
    worker.add_argument("--sequence", type=int, default=1)
    worker.add_argument("--delay-ms", type=int, default=5000)
    worker.add_argument("--timeout-ms", type=int, default=10000)

    args = parser.parse_args(argv)
    if args.command == "serve":
        return _serve(args)
    if args.command == "demo":
        from .demo import run_demo

        return run_demo(Path(args.output), keep_db=args.keep_db)
    if args.command == "recover":
        return asyncio.run(_recover(Path(args.db)))
    if args.command == "crash-worker":
        return asyncio.run(_crash_worker(args))
    return 2


def _serve(args: argparse.Namespace) -> int:
    import uvicorn

    from .app import AppSettings, create_app

    if args.host not in {"127.0.0.1", "localhost", "::1"}:
        print("refusing non-loopback HTTP bind; use localhost, 127.0.0.1 or ::1", file=sys.stderr)
        return 2
    app = create_app(
        AppSettings(
            db_path=Path(args.db),
            topology_path=Path(args.topology),
            samples_dir=Path(args.samples),
            auto_load_sample=not args.no_auto_sample,
        )
    )
    uvicorn.run(app, host=args.host, port=args.port, log_level="info")
    return 0


async def _recover(db_path: Path) -> int:
    from .gateway import Gateway
    from .store import Store

    store = Store(db_path)
    gateway = Gateway(store)
    try:
        await gateway.start()
        report = gateway.latest_recovery()
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0
    finally:
        await gateway.stop()
        store.close()


async def _crash_worker(args: argparse.Namespace) -> int:
    from .gateway import Gateway
    from .models import FrameSubmission
    from .store import Store

    db_path = Path(args.db)
    marker_path = Path(args.marker)
    store = Store(db_path)
    gateway = Gateway(store)
    await gateway.start()
    if gateway.public_state().get("snapshot") is None:
        snapshot = json.loads(Path(args.snapshot).read_text(encoding="utf-8"))
        await gateway.load_snapshot(snapshot)

    frame = FrameSubmission(
        frame_id=args.frame_id,
        session_id=args.session,
        sequence=args.sequence,
        payload={
            "op": "echo",
            "body": "crash-probe",
            "options": {"delay_ms": max(1, args.delay_ms)},
        },
        timeout_ms=max(10, args.timeout_ms),
    )
    task = asyncio.create_task(gateway.submit_frame(frame))
    deadline = asyncio.get_running_loop().time() + 10
    while asyncio.get_running_loop().time() < deadline:
        row = store.get_frame(frame.frame_id)
        leases = store.load_leases()
        lease = leases.get(frame.session_id)
        if (
            row is not None
            and row["status"] == "dispatched"
            and lease is not None
            and lease.get("sent_unacked_seq") == frame.sequence
        ):
            marker_path.parent.mkdir(parents=True, exist_ok=True)
            marker_path.write_text(
                json.dumps(
                    {
                        "ready": True,
                        "frame_id": frame.frame_id,
                        "session_id": frame.session_id,
                        "sequence": frame.sequence,
                        "frame_status": row["status"],
                        "lease": lease,
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            break
        if task.done():
            print("crash worker frame finished before marker", file=sys.stderr)
            break
        await asyncio.sleep(0.02)
    await asyncio.sleep(3600)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

