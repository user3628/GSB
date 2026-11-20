"""Command line interface for the incseg engine.

All commands operate on a local workspace directory and work fully offline.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path

from .config import TileConfig, WorkspaceConfig
from .engine import Engine
from .errors import EngineError
from .generator import generate_workspace
from .util import pretty_json


def _print_json(obj) -> None:
    sys.stdout.write(pretty_json(obj))


def _engine(args) -> Engine:
    return Engine(args.workspace)


# --------------------------------------------------------------------- commands
def cmd_init(args) -> int:
    config = generate_workspace(args.workspace, tiles=args.tiles, seed=args.seed, size=args.size)
    _print_json({"status": "ok", "workspace": str(args.workspace),
                 "tiles": [t.tile_id for t in config.tiles], "seed": args.seed})
    return 0


def cmd_run(args) -> int:
    report = _engine(args).run(tile_ids=args.tile, stop_after_units=args.stop_after_units)
    _print_json(report)
    return 0


def cmd_stop(args) -> int:
    _engine(args).request_stop()
    _print_json({"status": "ok", "stop_requested": True})
    return 0


def cmd_resume(args) -> int:
    report = _engine(args).run(tile_ids=args.tile)
    _print_json(report)
    return 0


def cmd_rebuild(args) -> int:
    report = _engine(args).run(tile_ids=[args.tile])
    _print_json(report)
    return 0


def cmd_status(args) -> int:
    engine = _engine(args)
    tile_ids = [args.tile] if args.tile else None
    _print_json(engine.inspect(tile_ids))
    return 0


def cmd_verify(args) -> int:
    engine = _engine(args)
    tile_ids = [args.tile] if args.tile else None
    _print_json(engine.verify(tile_ids))
    return 0


def cmd_export(args) -> int:
    engine = _engine(args)
    config = engine.load_config()
    tile_ids = [args.tile] if args.tile else [t.tile_id for t in config.tiles]
    exported = {tid: engine.export_tile(tid, args.out) for tid in tile_ids}
    _print_json({"status": "ok", "exported": exported})
    return 0


def cmd_set_config(args) -> int:
    engine = _engine(args)
    config = engine.load_config()
    tile = config.tile(args.tile)
    data = tile.to_dict()
    for assignment in args.set:
        if "=" not in assignment:
            raise EngineError(f"--set expects key=value, got {assignment!r}")
        key, raw = assignment.split("=", 1)
        if key not in data:
            raise EngineError(f"unknown tile config key {key!r}")
        try:
            data[key] = json.loads(raw)
        except ValueError:
            data[key] = raw
    config.replace_tile(TileConfig.from_dict(data))
    config.save(engine.config_path)
    _print_json({"status": "ok", "tile": config.tile(args.tile).to_dict()})
    return 0


def cmd_import(args) -> int:
    engine = _engine(args)
    config = engine.load_config()
    tiles_dir = engine.workspace / "tiles"
    tiles_dir.mkdir(parents=True, exist_ok=True)
    image_dest = tiles_dir / f"{args.tile}.png"
    shutil.copyfile(args.image, image_dest)

    if args.like:
        data = config.tile(args.like).to_dict()
    else:
        data = config.tiles[0].to_dict()
    data["tile_id"] = args.tile
    data["image"] = f"tiles/{args.tile}.png"
    for assignment in args.set or ():
        key, raw = assignment.split("=", 1)
        try:
            data[key] = json.loads(raw)
        except ValueError:
            data[key] = raw
    config.replace_tile(TileConfig.from_dict(data))
    config.save(engine.config_path)
    _print_json({"status": "ok", "imported": args.tile})
    return 0


def cmd_serve(args) -> int:
    from .server import serve

    serve(args.workspace, host=args.host, port=args.port)
    return 0


def cmd_demo(args) -> int:
    from .demo import run_demo

    return run_demo(args.workspace, with_http=args.with_http)


# --------------------------------------------------------------------- parser
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="incseg", description=__doc__)
    parser.add_argument("--workspace", default="workspace", help="workspace directory")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("init", help="generate deterministic tiles + config")
    p.add_argument("--tiles", type=int, default=4)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--size", type=int, default=96)
    p.set_defaults(func=cmd_init)

    p = sub.add_parser("run", help="incrementally build all (or selected) tiles")
    p.add_argument("--tile", action="append", help="restrict to tile id (repeatable)")
    p.add_argument("--stop-after-units", type=int, default=None,
                   help="simulate a stop request after N completed stage units")
    p.set_defaults(func=cmd_run)

    p = sub.add_parser("stop", help="request a graceful stop")
    p.set_defaults(func=cmd_stop)

    p = sub.add_parser("resume", help="resume after a stop")
    p.add_argument("--tile", action="append")
    p.set_defaults(func=cmd_resume)

    p = sub.add_parser("rebuild", help="incrementally rebuild one tile")
    p.add_argument("--tile", required=True)
    p.set_defaults(func=cmd_rebuild)

    p = sub.add_parser("status", help="show stage status, fingerprints, invalidation reasons")
    p.add_argument("--tile", default=None)
    p.set_defaults(func=cmd_status)

    p = sub.add_parser("verify", help="byte-compare incremental outputs vs clean full build")
    p.add_argument("--tile", default=None)
    p.set_defaults(func=cmd_verify)

    p = sub.add_parser("export", help="export masks + summary to a directory")
    p.add_argument("--tile", default=None)
    p.add_argument("--out", default="output")
    p.set_defaults(func=cmd_export)

    p = sub.add_parser("set-config", help="update one tile's config values")
    p.add_argument("--tile", required=True)
    p.add_argument("--set", action="append", required=True, metavar="KEY=VALUE")
    p.set_defaults(func=cmd_set_config)

    p = sub.add_parser("import", help="import a tile image into the workspace")
    p.add_argument("--tile", required=True)
    p.add_argument("--image", required=True)
    p.add_argument("--like", default=None, help="copy config from this tile id")
    p.add_argument("--set", action="append", default=None, metavar="KEY=VALUE")
    p.set_defaults(func=cmd_import)

    p = sub.add_parser("serve", help="run the local HTTP API (loopback only)")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8765)
    p.set_defaults(func=cmd_serve)

    p = sub.add_parser("demo", help="run the full offline demonstration")
    p.add_argument("--with-http", action="store_true", help="also exercise the HTTP API")
    p.set_defaults(func=cmd_demo)
    return parser


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return args.func(args)
    except EngineError as exc:
        sys.stderr.write(pretty_json({"error": exc.to_dict()}))
        return 2


if __name__ == "__main__":
    sys.exit(main())
