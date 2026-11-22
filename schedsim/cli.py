"""Local command line workflows for demonstration and crash recovery tests."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import uvicorn

from .api import create_app
from .engine import SchedulingEngine
from .errors import SchedError
from .samples import SampleRepository


def _print(payload: Any) -> None:
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))


def _engine(args: argparse.Namespace) -> SchedulingEngine:
    return SchedulingEngine(
        args.db,
        crash_after_intent=getattr(args, "crash_after_intent", False),
        crash_after_placements=getattr(args, "crash_after_placements", None),
    )


def command_serve(args: argparse.Namespace) -> int:
    app = create_app(db_path=args.db, sample_dir=args.samples)
    uvicorn.run(app, host=args.host, port=args.port, log_level=args.log_level)
    return 0


def command_recover(args: argparse.Namespace) -> int:
    engine = _engine(args)
    _print({"recovered": engine.recover_pending()})
    return 0


def command_run_sample(args: argparse.Namespace) -> int:
    engine = _engine(args)
    recovered = engine.recover_pending()
    if recovered:
        _print({"startup_recovery": recovered})
    repository = SampleRepository(args.samples)
    payload = repository.load(args.sample, batch_id=args.batch_id)
    try:
        submitted = engine.submit_batch(payload)
        rounds: list[dict[str, Any]] = []
        for _ in range(args.max_rounds):
            status = engine.get_batch_summary(submitted["batch_id"])["status"]
            if status in {"COMPLETED", "ABORTED"}:
                break
            rounds.append(engine.run_round(submitted["batch_id"]))
        results = engine.get_results(submitted["batch_id"])
        _print(
            {
                "submitted": submitted,
                "rounds": rounds,
                "results": results,
            }
        )
        return 0
    except SchedError as exc:
        _print(exc.to_dict())
        return 2


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m schedsim.cli",
        description="Local Kubernetes scheduling budget and affinity simulator",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    serve = subparsers.add_parser("serve", help="run the local FastAPI service")
    serve.add_argument("--db", default=str(Path.cwd() / "data" / "scheduler.db"))
    serve.add_argument("--samples", default=str(Path.cwd() / "samples"))
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8000)
    serve.add_argument("--log-level", default="info")
    serve.set_defaults(func=command_serve)

    recover = subparsers.add_parser("recover", help="scan and recover pending intents")
    recover.add_argument("--db", default=str(Path.cwd() / "data" / "scheduler.db"))
    recover.add_argument("--crash-after-intent", action="store_true")
    recover.add_argument("--crash-after-placements", type=int, default=None)
    recover.set_defaults(func=command_recover)

    run_sample = subparsers.add_parser(
        "run-sample", help="submit a local sample and run rounds until completion"
    )
    run_sample.add_argument("--db", default=str(Path.cwd() / "data" / "scheduler.db"))
    run_sample.add_argument("--samples", default=str(Path.cwd() / "samples"))
    run_sample.add_argument("--sample", default="default")
    run_sample.add_argument("--batch-id", default=None)
    run_sample.add_argument("--max-rounds", type=int, default=50)
    run_sample.add_argument("--crash-after-intent", action="store_true")
    run_sample.add_argument("--crash-after-placements", type=int, default=None)
    run_sample.set_defaults(func=command_run_sample)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    sys.exit(main())

