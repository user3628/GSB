"""Command line interface for the rerank workbench."""

from __future__ import annotations

import argparse
import json
import os
import sys

from . import errors
from .errors import PipelineError
from .generator import DEFAULT_SEED, generate
from .pipeline import Workspace
from .runner import export_summary, rerank_trace, run_pipeline, status
from .server import DEFAULT_PORT, serve

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _print(obj):
    print(json.dumps(obj, ensure_ascii=False, sort_keys=True, indent=2))


def _workspace(args) -> Workspace:
    return Workspace(args.workspace)


def cmd_generate(args):
    manifest = generate(args.data_dir, seed=args.seed,
                        num_shards=args.num_shards)
    _print({"generated": manifest})


def cmd_import(args):
    ws = _workspace(args)
    _print({"imported": ws.import_data(args.data_dir)})


def cmd_run(args):
    ws = _workspace(args)
    report = run_pipeline(ws, serial=args.serial, stage_limit=args.stage,
                          repo_root=REPO_ROOT, stop_after=args.stop_after)
    _print(report)


def cmd_stop(args):
    ws = _workspace(args)
    ws.request_stop()
    _print({"stop_requested": True})


def cmd_resume(args):
    ws = _workspace(args)
    ws.clear_stop()
    report = run_pipeline(ws, serial=args.serial, repo_root=REPO_ROOT)
    _print(report)


def cmd_status(args):
    _print(status(_workspace(args)))


def cmd_budget(args):
    from .server import Api

    _print(Api(args.workspace, repo_root=REPO_ROOT)._budget())


def cmd_trace(args):
    _print(rerank_trace(_workspace(args), args.shard_id))


def cmd_fingerprints(args):
    st = status(_workspace(args))
    for shard in st["shards"]:
        if shard["shard_id"] == args.shard_id:
            _print(shard)
            return
    raise PipelineError(errors.E_INVALID_STAGE,
                        f"unknown shard {args.shard_id}",
                        shard_id=args.shard_id)


def cmd_export(args):
    summary = export_summary(_workspace(args))
    if args.out:
        with open(args.out, "w", encoding="utf-8", newline="\n") as fh:
            json.dump(summary, fh, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"))
            fh.write("\n")
        _print({"exported": args.out})
    else:
        _print(summary)


def cmd_serve(args):
    serve(args.workspace, port=args.port, repo_root=REPO_ROOT)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="rerank_bench",
        description="Local precision-rank / rerank parallel scheduling bench")
    parser.add_argument("--workspace", default="workspace",
                        help="workspace directory (default: ./workspace)")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("generate", help="generate deterministic sample data")
    p.add_argument("--data-dir", default="data")
    p.add_argument("--seed", type=int, default=DEFAULT_SEED)
    p.add_argument("--num-shards", type=int, default=4)
    p.set_defaults(func=cmd_generate)

    p = sub.add_parser("import", help="import a generated data dir")
    p.add_argument("--data-dir", default="data")
    p.set_defaults(func=cmd_import)

    p = sub.add_parser("run", help="run the pipeline")
    p.add_argument("--serial", action="store_true")
    p.add_argument("--stage", default=None,
                   choices=["assemble", "score", "rerank", "aggregate"])
    p.add_argument("--stop-after", type=int, default=None,
                   help="request stop after N shards complete (demo/testing)")
    p.set_defaults(func=cmd_run)

    p = sub.add_parser("stop", help="request graceful stop")
    p.set_defaults(func=cmd_stop)

    p = sub.add_parser("resume", help="clear stop flag and resume")
    p.add_argument("--serial", action="store_true")
    p.set_defaults(func=cmd_resume)

    p = sub.add_parser("status", help="show workspace status")
    p.set_defaults(func=cmd_status)

    p = sub.add_parser("budget", help="show budget consumption")
    p.set_defaults(func=cmd_budget)

    p = sub.add_parser("trace", help="show rerank decision trace for a shard")
    p.add_argument("shard_id")
    p.set_defaults(func=cmd_trace)

    p = sub.add_parser("fingerprints", help="show version fingerprints")
    p.add_argument("shard_id")
    p.set_defaults(func=cmd_fingerprints)

    p = sub.add_parser("export", help="export ranked summary")
    p.add_argument("--out", default=None)
    p.set_defaults(func=cmd_export)

    p = sub.add_parser("serve", help="start the local HTTP API")
    p.add_argument("--port", type=int, default=DEFAULT_PORT)
    p.set_defaults(func=cmd_serve)
    return parser


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    try:
        args.func(args)
        return 0
    except PipelineError as exc:
        _print({"ok": False, "error": exc.to_dict()})
        return 2


if __name__ == "__main__":
    sys.exit(main())
