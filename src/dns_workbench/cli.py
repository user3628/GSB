from __future__ import annotations

import argparse
import asyncio
import json
import logging
from logging.handlers import RotatingFileHandler
import sys
from pathlib import Path

import uvicorn

from . import __version__
from .api import create_app
from .authority import AuthorityServer, ZoneData
from .config import AppConfig
from .runtime import Runtime


def _configure_logging(config: AppConfig | None = None) -> None:
    level = logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    if config is not None:
        config.log_path.mkdir(parents=True, exist_ok=True)
        log_file = config.log_path / "workbench.log"
        root = logging.getLogger()
        if not any(
            isinstance(handler, logging.FileHandler)
            and getattr(handler, "baseFilename", None) == str(log_file.resolve())
            for handler in root.handlers
        ):
            handler = RotatingFileHandler(
                log_file,
                maxBytes=5 * 1024 * 1024,
                backupCount=3,
                encoding="utf-8",
            )
            handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
            root.addHandler(handler)


async def _serve(config: AppConfig) -> None:
    runtime = Runtime(config)
    await runtime.start()
    app = create_app(runtime)
    uvicorn_config = uvicorn.Config(
        app,
        host=config.http_host,
        port=config.http_port,
        log_level="info",
        access_log=True,
    )
    server = uvicorn.Server(uvicorn_config)
    try:
        await server.serve()
    finally:
        await runtime.stop(clean=True)


async def _run_authorities(config: AppConfig, selected_ids: set[str] | None = None) -> None:
    servers: list[AuthorityServer] = []
    try:
        for item in config.authorities:
            if selected_ids and item.id not in selected_ids:
                continue
            if not item.zone_file:
                raise ValueError(f"authority {item.id} has no zone_file")
            server = AuthorityServer(item, ZoneData.load(item.zone_file))
            await server.start()
            servers.append(server)
            print(
                json.dumps(
                    {
                        "event": "authority_started",
                        "id": item.id,
                        "endpoint": f"{item.host}:{item.port}",
                        "zones": item.zones,
                    },
                    ensure_ascii=False,
                ),
                flush=True,
            )
        if not servers:
            raise ValueError("no authority matched the requested selection")
        await asyncio.Event().wait()
    finally:
        for server in reversed(servers):
            await server.stop()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="dns-workbench", description=__doc__)
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    subparsers = parser.add_subparsers(dest="command", required=True)

    serve = subparsers.add_parser("serve", help="start HTTP diagnostics, UDP listener, and TCP listener")
    serve.add_argument("--config", required=True, help="path to JSON configuration")

    authorities = subparsers.add_parser("authorities", help="start deterministic loopback authority fixture servers")
    authorities.add_argument("--config", required=True, help="path to JSON configuration")
    authorities.add_argument("--id", action="append", default=[], help="authority id to start; repeat as needed")

    demo = subparsers.add_parser("demo", help="run the repeatable end-to-end demonstration")
    demo.add_argument("--workdir", default="artifacts/demo", help="artifact directory for DB, logs, and report")
    demo.add_argument("--keep-running", action="store_true", help="do not stop the restarted service at the end")

    validate = subparsers.add_parser("validate-config", help="validate a configuration file")
    validate.add_argument("--config", required=True)
    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "serve":
            config = AppConfig.load(args.config)
            _configure_logging(config)
            asyncio.run(_serve(config))
        elif args.command == "authorities":
            config = AppConfig.load(args.config)
            _configure_logging(config)
            asyncio.run(_run_authorities(config, set(args.id) or None))
        elif args.command == "demo":
            from .demo import run_demo

            _configure_logging()
            exit_code = asyncio.run(
                run_demo(Path(args.workdir), keep_running=bool(args.keep_running))
            )
            raise SystemExit(exit_code)
        elif args.command == "validate-config":
            config = AppConfig.load(args.config)
            print(json.dumps(config.redacted(), ensure_ascii=False, indent=2))
        else:
            parser.error(f"unknown command: {args.command}")
    except KeyboardInterrupt:
        raise SystemExit(130) from None
    except Exception as exc:
        print(f"error: {type(exc).__name__}: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()

