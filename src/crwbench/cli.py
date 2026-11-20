"""Command-line interface for the contract risk workbench."""
from __future__ import annotations

import argparse
import glob
import json
import os
import sys

from .checkpoints import CheckpointStore
from .config import load_config
from .errors import WorkbenchError
from .generator import generate_dataset
from .pipeline import Pipeline, load_shards, verify_workdir_outputs
from .server import serve
from .util import canonical_dumps


def _emit(obj, code: int = 0):
    sys.stdout.buffer.write(canonical_dumps(obj))
    return code


def _shards_from_args(args) -> list[str]:
    if args.shards:
        return sorted(args.shards)
    return sorted(glob.glob(os.path.join(args.data_dir, "shard_*.jsonl")))


def cmd_generate(args) -> int:
    sizes = json.loads(args.sizes)
    result = generate_dataset(args.seed, sizes, args.data_dir, shard_count=args.shard_count)
    return _emit(result)


def cmd_run(args) -> int:
    config = load_config(args.config)
    pipeline = Pipeline(config, args.workdir)
    report = pipeline.run(_shards_from_args(args), force=args.force)
    return _emit(report)


def cmd_status(args) -> int:
    store = CheckpointStore(args.workdir)
    records = store.load_records()
    return _emit({"workdir": args.workdir, "records": records})


def cmd_fingerprints(args) -> int:
    config = load_config(args.config)
    pipeline = Pipeline(config, args.workdir)
    clauses, input_digest, shard_info = load_shards(_shards_from_args(args))
    return _emit(
        {
            "input_digest": input_digest,
            "shards": shard_info,
            "config_fingerprint": config.fingerprint,
            "stage_digests": pipeline.expected_digests(clauses),
        }
    )


def cmd_strategy(args) -> int:
    store = CheckpointStore(args.workdir)
    final = store.load_artifact("summary")
    return _emit(
        {
            "strategy_decision": final["strategy_decision"],
            "candidate_details": final["candidate_details"],
            "plan_membership": final["plan_membership"],
        }
    )


def cmd_evidence(args) -> int:
    store = CheckpointStore(args.workdir)
    final = store.load_artifact("summary")
    scored = store.load_artifact("scoring")
    score_map = {s["clause_id"]: s for s in scored}
    rows = final["level_list"]
    if args.clause_id:
        rows = [r for r in rows if r["clause_id"] == args.clause_id]
    return _emit(
        [
            {"level_record": row, "score_detail": score_map.get(row["clause_id"])}
            for row in rows
        ]
    )


def cmd_stop(args) -> int:
    Pipeline(load_config(args.config), args.workdir).request_stop()
    return _emit({"stop_requested": True})


def cmd_resume(args) -> int:
    config = load_config(args.config)
    report = Pipeline(config, args.workdir).resume(_shards_from_args(args))
    return _emit(report)


def cmd_export(args) -> int:
    store = CheckpointStore(args.workdir)
    final = store.load_artifact("summary")
    if args.output:
        from .util import write_json

        write_json(args.output, final["level_list"])
        return _emit({"exported": args.output, "count": len(final["level_list"])})
    return _emit(final["level_list"])


def cmd_verify_incremental(args) -> int:
    verify_workdir_outputs(args.reference, args.candidate)
    return _emit({"status": "identical", "reference": args.reference, "candidate": args.candidate})


def cmd_serve(args) -> int:
    httpd = serve(args.config, args.workdir, host=args.host, port=args.port)
    print(f"crwbench HTTP listening on http://{args.host}:{args.port}", file=sys.stderr)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        httpd.shutdown()
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="crwbench", description="contract risk workbench")
    parser.add_argument("--config", default="config/default_config.yaml")
    parser.add_argument("--workdir", default="work/default")
    parser.add_argument("--data-dir", default="data/demo")
    sub = parser.add_subparsers(dest="command", required=True)

    gen = sub.add_parser("generate", help="regenerate deterministic JSONL shards")
    gen.add_argument("--seed", type=int, default=20261121)
    gen.add_argument("--sizes", default='{"small": 24, "medium": 120, "large": 360}')
    gen.add_argument("--shard-count", type=int, default=3)
    gen.set_defaults(func=cmd_generate)

    run = sub.add_parser("run", help="run the four-stage pipeline")
    run.add_argument("shards", nargs="*")
    run.add_argument("--force", action="store_true")
    run.set_defaults(func=cmd_run)

    status = sub.add_parser("status", help="show checkpoint records")
    status.set_defaults(func=cmd_status)

    fp = sub.add_parser("fingerprints", help="show version fingerprints")
    fp.add_argument("shards", nargs="*")
    fp.set_defaults(func=cmd_fingerprints)

    st = sub.add_parser("strategy", help="show strategy selection rationale")
    st.set_defaults(func=cmd_strategy)

    ev = sub.add_parser("evidence", help="show level evidence")
    ev.add_argument("--clause-id")
    ev.add_argument("shards", nargs="*")
    ev.set_defaults(func=cmd_evidence)

    stop = sub.add_parser("stop", help="request graceful stop")
    stop.set_defaults(func=cmd_stop)

    resume = sub.add_parser("resume", help="resume after stop")
    resume.add_argument("shards", nargs="*")
    resume.set_defaults(func=cmd_resume)

    export = sub.add_parser("export", help="export level list")
    export.add_argument("--output")
    export.set_defaults(func=cmd_export)

    verify = sub.add_parser("verify-incremental", help="byte-compare two workdirs")
    verify.add_argument("--reference", required=True)
    verify.add_argument("--candidate", required=True)
    verify.set_defaults(func=cmd_verify_incremental)

    srv = sub.add_parser("serve", help="start local HTTP API")
    srv.add_argument("--host", default="127.0.0.1")
    srv.add_argument("--port", type=int, default=8765)
    srv.set_defaults(func=cmd_serve)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except WorkbenchError as exc:
        sys.stdout.buffer.write(canonical_dumps(exc.to_dict()))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
