"""Command-line interface for the voxel reconstruction engine.

Run from the repository root, e.g.:
    python -m apps.cli generate --out data/sample
    python -m apps.cli import --workspace ws --manifest data/sample/manifest.json
    python -m apps.cli build --workspace ws --mode full
    python -m apps.cli status --workspace ws
"""

from __future__ import annotations

import argparse
import json
import sys

from voxel_engine.engine import Engine
from voxel_engine.errors import EngineError
from voxel_engine.generator import DEFAULT_SEED, generate_dataset


def _print(obj) -> None:
    print(json.dumps(obj, indent=2, sort_keys=True, ensure_ascii=False))


def _parse_config_value(raw: str):
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return raw


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="voxel-cli", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("generate", help="generate the deterministic sample dataset")
    p.add_argument("--out", required=True)
    p.add_argument("--seed", type=int, default=DEFAULT_SEED)
    p.add_argument("--views", type=int, default=5)
    p.add_argument("--width", type=int, default=64)
    p.add_argument("--height", type=int, default=48)

    p = sub.add_parser("import", help="import manifest + shards into a workspace")
    p.add_argument("--workspace", required=True)
    p.add_argument("--manifest", required=True)

    p = sub.add_parser("build", help="run the staged build")
    p.add_argument("--workspace", required=True)
    p.add_argument("--mode", choices=["incremental", "full", "resume"], default="incremental")
    p.add_argument("--stop-after", default=None, help="request stop after this instance")
    p.add_argument("--lock-timeout-ms", type=int, default=5000)

    p = sub.add_parser("status", help="show stage states, fingerprints, sparsity stats")
    p.add_argument("--workspace", required=True)

    p = sub.add_parser("recover", help="recover from the checkpoint log")
    p.add_argument("--workspace", required=True)

    p = sub.add_parser("stop", help="request a cooperative stop between stages")
    p.add_argument("--workspace", required=True)

    p = sub.add_parser("replace-view", help="replace one view shard wholesale")
    p.add_argument("--workspace", required=True)
    p.add_argument("--view", required=True)
    p.add_argument("--image", required=True)

    p = sub.add_parser("set-config", help="update global config values, e.g. --set voxel_resolution=0.4")
    p.add_argument("--workspace", required=True)
    p.add_argument("--set", dest="pairs", action="append", required=True, metavar="KEY=VALUE")

    p = sub.add_parser("verify", help="byte-compare current state vs a fresh full build")
    p.add_argument("--workspace", required=True)

    p = sub.add_parser("export", help="export the global voxel summary")
    p.add_argument("--workspace", required=True)

    p = sub.add_parser("serve", help="start the local HTTP interface")
    p.add_argument("--workspace", required=True)
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8765)

    p = sub.add_parser("demo", help="run the deterministic end-to-end demo")
    p.add_argument("--dir", required=True, help="demo output directory")
    p.add_argument("--seed", type=int, default=DEFAULT_SEED)
    return parser


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "generate":
            manifest = generate_dataset(
                args.out, seed=args.seed, num_views=args.views,
                width=args.width, height=args.height,
            )
            _print({"generated": args.out, "views": len(manifest["views"]),
                    "seed": args.seed})
            return 0

        if args.command == "serve":
            from apps.http_server import serve
            serve(args.workspace, host=args.host, port=args.port)
            return 0

        if args.command == "demo":
            from scripts.run_demo import run_demo
            summary = run_demo(args.dir, seed=args.seed)
            _print(summary)
            return 0

        engine = Engine(args.workspace)
        if args.command == "import":
            _print(engine.import_dataset(args.manifest))
        elif args.command == "build":
            _print(engine.build(mode=args.mode, stop_after=args.stop_after,
                                lock_timeout_ms=args.lock_timeout_ms))
        elif args.command == "status":
            _print(engine.status())
        elif args.command == "recover":
            _print(engine.recover())
        elif args.command == "stop":
            _print(engine.stop())
        elif args.command == "replace-view":
            _print(engine.replace_view(args.view, args.image))
        elif args.command == "set-config":
            updates = {}
            for pair in args.pairs:
                key, sep, value = pair.partition("=")
                if not sep:
                    raise SystemExit(f"invalid --set pair: {pair!r} (expected KEY=VALUE)")
                updates[key] = _parse_config_value(value)
            _print(engine.update_config(updates))
        elif args.command == "verify":
            _print(engine.verify_consistency())
        elif args.command == "export":
            _print(engine.export_summary())
        return 0
    except EngineError as exc:
        _print(exc.to_dict())
        return 2


if __name__ == "__main__":
    sys.exit(main())
