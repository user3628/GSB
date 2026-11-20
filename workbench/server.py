from __future__ import annotations

import argparse
import dataclasses
import shutil
from pathlib import Path

import uvicorn

from .app import create_app
from .config import WorkbenchConfig


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the local CDN edge cache consistency workbench")
    parser.add_argument("--config", default="config/default.json", help="Path to the JSON configuration file")
    parser.add_argument("--host", help="Override the configured loopback host")
    parser.add_argument("--port", type=int, help="Override the configured port")
    parser.add_argument("--sqlite", help="Override the SQLite database path")
    parser.add_argument("--reset", action="store_true", help="Delete the configured SQLite DB, WAL, and SHM files before starting")
    return parser


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    config = WorkbenchConfig.load(args.config)
    updates: dict[str, object] = {}
    if args.host:
        updates["host"] = args.host
    if args.port is not None:
        updates["port"] = args.port
        if config.origin_base_url is None:
            updates["origin_base_url"] = None
    if args.sqlite:
        sqlite_path = Path(args.sqlite).expanduser()
        if not sqlite_path.is_absolute():
            sqlite_path = (Path.cwd() / sqlite_path).resolve()
        updates["sqlite_path"] = sqlite_path
    if updates:
        config = dataclasses.replace(config, **updates)
    config.ensure_paths()
    if args.reset:
        for suffix in ("", "-wal", "-shm"):
            candidate = Path(str(config.sqlite_path) + suffix)
            if candidate.exists():
                candidate.unlink()
    app = create_app(config)
    uvicorn.run(
        app,
        host=config.host,
        port=config.port,
        log_level="info",
        workers=1,
        reload=False,
    )


if __name__ == "__main__":
    main()

