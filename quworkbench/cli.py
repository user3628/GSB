"""Command line workbench interface."""

import argparse
import json
import sys
from pathlib import Path

from .errors import WorkbenchError
from .generator import SEED, generate
from .pipeline import Pipeline


def _print(obj):
    print(json.dumps(obj, ensure_ascii=False, sort_keys=True, indent=2))


def _read_jsonl(path):
    rows = []
    for line in Path(path).read_bytes().decode("utf-8").splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows


def main(argv=None):
    parser = argparse.ArgumentParser(prog="quworkbench")
    parser.add_argument("--workspace", default="workspace")
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("generate")
    sub.add_parser("run").add_argument("--full", action="store_true")
    sub.add_parser("status")
    p = sub.add_parser("replace-shard")
    p.add_argument("--shard", required=True)
    p.add_argument("--file", required=True)
    p = sub.add_parser("change-config")
    p.add_argument("--key", required=True)
    p.add_argument("--value", required=True, help="JSON-encoded value")
    p = sub.add_parser("diagnostics")
    p.add_argument("--query", required=True)
    sub.add_parser("invalidations")
    sub.add_parser("verify")
    sub.add_parser("recover")
    p = sub.add_parser("serve")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8737)
    sub.add_parser("demo")

    args = parser.parse_args(argv)
    ws = Path(args.workspace)
    try:
        if args.cmd == "generate":
            _print(generate(ws, seed=SEED))
        elif args.cmd == "run":
            pipe = Pipeline(ws)
            _print(pipe.run_full() if args.full else pipe.run())
        elif args.cmd == "status":
            _print(Pipeline(ws).status())
        elif args.cmd == "replace-shard":
            _print(Pipeline(ws).replace_shard(args.shard, _read_jsonl(args.file)))
        elif args.cmd == "change-config":
            _print(Pipeline(ws).change_config(args.key, json.loads(args.value)))
        elif args.cmd == "diagnostics":
            _print(Pipeline(ws).slot_diagnostics(args.query))
        elif args.cmd == "invalidations":
            _print({"events": Pipeline(ws).invalidations()})
        elif args.cmd == "verify":
            _print(Pipeline(ws).verify_against_full())
        elif args.cmd == "recover":
            _print({"report": Pipeline(ws).recover()})
        elif args.cmd == "serve":
            from .server import serve

            serve(ws, host=args.host, port=args.port)
        elif args.cmd == "demo":
            from .demo import run_demo

            _print(run_demo(ws))
    except WorkbenchError as exc:
        _print({"error": exc.to_dict()})
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
