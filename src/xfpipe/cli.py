"""Command line interface for the xfpipe workbench."""
from __future__ import annotations

import argparse
import json
import os
import sys

from .errors import PipelineError
from .sample_gen import DEFAULT_ROWS, DEFAULT_SEED, generate_sample_csv, write_config
from .workbench import Workbench


def _emit(obj) -> None:
    json.dump(obj, sys.stdout, indent=2, sort_keys=True, ensure_ascii=False)
    sys.stdout.write("\n")


def _workbench(args) -> Workbench:
    return Workbench(os.path.abspath(args.workdir))


def cmd_init(args) -> None:
    workdir = os.path.abspath(args.workdir)
    sample = generate_sample_csv(
        os.path.join(workdir, "data", "sample.csv"), rows=args.rows, seed=args.seed
    )
    config = write_config(os.path.join(workdir, "data", "config.json"))
    _emit({"sample": sample, "config": config, "rows": args.rows, "seed": args.seed})


def cmd_import_sample(args) -> None:
    wb = _workbench(args)
    path = wb.import_sample(os.path.abspath(args.path))
    _emit({"sample_path": path, "sample_fingerprint": wb.pipeline.sample_fingerprint})


def cmd_run(args) -> None:
    _emit(_workbench(args).pipeline.run(stop_after_stage=args.stop_after_stage))


def cmd_recover(args) -> None:
    _emit(_workbench(args).pipeline.recover())


def cmd_status(args) -> None:
    _emit(_workbench(args).pipeline.status())


def cmd_query(args) -> None:
    params = json.loads(args.params) if args.params else {}
    _emit(_workbench(args).query_engine.query(args.kind, **params))


def cmd_verify(args) -> None:
    _emit(_workbench(args).pipeline.verify_against_full_rebuild())


def cmd_stop(args) -> None:
    wb = _workbench(args)
    wb.pipeline.request_stop()
    _emit({"stop_requested": True})


def cmd_stop_clear(args) -> None:
    wb = _workbench(args)
    wb.pipeline.clear_stop()
    _emit({"stop_requested": False})


def cmd_cache_stats(args) -> None:
    _emit(_workbench(args).cache_stats())


def cmd_checkpoints(args) -> None:
    store = _workbench(args).pipeline.checkpoints
    _emit({"snapshot": store.classify_snapshot(), "records": store.records()})


def cmd_serve(args) -> None:
    import uvicorn

    from .server import create_app

    uvicorn.run(create_app(os.path.abspath(args.workdir)), host="127.0.0.1", port=args.port)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="xfpipe")
    parser.add_argument("--workdir", default=".")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("init", help="generate the deterministic sample CSV and config")
    p.add_argument("--rows", type=int, default=DEFAULT_ROWS)
    p.add_argument("--seed", type=int, default=DEFAULT_SEED)
    p.set_defaults(func=cmd_init)

    p = sub.add_parser("import-sample", help="import a sample CSV into the workbench")
    p.add_argument("path")
    p.set_defaults(func=cmd_import_sample)

    p = sub.add_parser("run", help="run the four-stage pipeline")
    p.add_argument("--stop-after-stage", default=None,
                   choices=["infer", "cross", "reduce", "summary"])
    p.set_defaults(func=cmd_run)

    sub.add_parser("recover", help="resume from persisted checkpoints").set_defaults(func=cmd_recover)
    sub.add_parser("status", help="stage status and version fingerprints").set_defaults(func=cmd_status)

    p = sub.add_parser("query", help="run an interactive query")
    p.add_argument("kind", choices=["feature_summary", "explained_variance", "combinations"])
    p.add_argument("--params", default=None, help="JSON object of query parameters")
    p.set_defaults(func=cmd_query)

    sub.add_parser("verify", help="compare artefacts against a from-scratch rebuild").set_defaults(func=cmd_verify)
    sub.add_parser("stop", help="request a graceful stop").set_defaults(func=cmd_stop)
    sub.add_parser("stop-clear", help="clear the stop flag").set_defaults(func=cmd_stop_clear)
    sub.add_parser("cache-stats", help="cache hit/eviction counters").set_defaults(func=cmd_cache_stats)
    sub.add_parser("checkpoints", help="inspect the checkpoint journal").set_defaults(func=cmd_checkpoints)

    p = sub.add_parser("serve", help="start the local HTTP interface")
    p.add_argument("--port", type=int, default=8077)
    p.set_defaults(func=cmd_serve)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        args.func(args)
    except PipelineError as exc:
        _emit(exc.to_dict())
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
