from __future__ import annotations

import argparse
import json
import signal
import sys
from typing import Any

from .engine import Lab
from .errors import BusinessError
from .generate import generate_dataset


def _emit(value: Any, *, pretty: bool = True) -> None:
    text = json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2 if pretty else None)
    sys.stdout.write(text + "\n")


def _scales(value: str | None) -> list[str] | None:
    if value is None or value.strip().lower() in ("", "all"):
        return None
    result = [part.strip() for part in value.split(",") if part.strip()]
    return result or None


def _add_scale_option(parser: argparse.ArgumentParser, *, required: bool = False) -> None:
    parser.add_argument("--scale", required=required, help="scale name")


def _add_run_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--scales", default="all", help="comma-separated scales or all")
    parser.add_argument("--until", choices=("recall", "fusion", "summary"), default="summary")
    parser.add_argument("--stop-after", default=None, metavar="SCALE:STAGE")
    parser.add_argument("--no-export", action="store_true", help="do not write the aggregate summary")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m recall_lab",
        description="Deterministic local multi-route recall/fusion lab",
    )
    parser.add_argument("--workspace", default=".lab", help="initialized workspace directory")
    parser.add_argument("--config", default=None, help="override fusion YAML path")
    subparsers = parser.add_subparsers(dest="command", required=True)

    generate = subparsers.add_parser("generate", help="generate deterministic JSONL samples")
    generate.add_argument("--out", default="data")
    generate.add_argument("--seed", type=int, default=20261120)

    init = subparsers.add_parser("init", help="import manifest/config into a workspace")
    init.add_argument("--manifest", default="data/manifest.jsonl")
    init.add_argument("--config", dest="init_config", default="configs/fusion.yaml")
    init.add_argument("--force", action="store_true")

    run = subparsers.add_parser("run", help="run recall/fusion/summary stages")
    _add_run_options(run)

    resume = subparsers.add_parser("resume", help="clear the stop flag and resume")
    _add_run_options(resume)

    stage = subparsers.add_parser("stage", help="execute one stage")
    _add_scale_option(stage, required=True)
    stage.add_argument("--stage", choices=("recall", "fusion", "summary"), required=True)

    status = subparsers.add_parser("status", help="show scale status, decisions and versions")
    status.add_argument("--scale", default=None)

    decision = subparsers.add_parser("decision", help="show adaptive budget decision")
    _add_scale_option(decision, required=True)

    version = subparsers.add_parser("version", help="show version fingerprints for one scale")
    _add_scale_option(version, required=True)

    detail = subparsers.add_parser("fusion-detail", help="show fused candidates and provenance")
    _add_scale_option(detail, required=True)
    detail.add_argument("--query-id", default=None)

    validate = subparsers.add_parser("validate", help="validate all checkpoint chains and artifacts")

    stop = subparsers.add_parser("stop", help="request graceful stop after the current stage")
    stop.add_argument("--reason", default="cli_stop")

    export = subparsers.add_parser("export", help="write the aggregate experiment summary")
    export.add_argument("--scales", default="all")

    serve = subparsers.add_parser("serve", help="start the local HTTP API")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8765)
    serve.add_argument("--manifest", default="data/manifest.jsonl")
    serve.add_argument("--config-file", default="configs/fusion.yaml")
    return parser


def _open_lab(args: argparse.Namespace) -> Lab:
    return Lab(args.workspace, config_path=args.config)


def _install_graceful_stop(lab: Lab, reason: str):
    previous: dict[int, Any] = {}

    def handler(signum, frame):  # noqa: ARG001
        lab.request_stop(reason=f"{reason}:{signal.Signals(signum).name}")

    for signum in (signal.SIGINT, signal.SIGTERM):
        previous[signum] = signal.getsignal(signum)
        signal.signal(signum, handler)
    return previous


def _restore_signals(previous: dict[int, Any]) -> None:
    for signum, handler in previous.items():
        signal.signal(signum, handler)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "generate":
            path = generate_dataset(args.out, seed=args.seed)
            _emit({"status": "generated", "manifest": str(path), "seed": args.seed})
            return 0
        if args.command == "init":
            lab = Lab.initialize(args.workspace, args.manifest, args.init_config, force=args.force)
            _emit(
                {
                    "status": "initialized",
                    "workspace": str(lab.root),
                    "workspace_id": lab.workspace_id,
                    "config_version": lab.config.fingerprint,
                    "scales": [entry.scale for entry in lab.manifest.entries],
                }
            )
            return 0
        if args.command == "serve":
            from .http_api import serve

            serve(
                root=args.workspace,
                config_path=args.config or args.config_file,
                manifest_path=args.manifest,
                host=args.host,
                port=args.port,
            )
            return 0

        lab = _open_lab(args)
        if args.command == "status":
            _emit(lab.status(scales=_scales(args.scale)))
            return 0
        if args.command == "decision":
            _emit(lab.decision(args.scale))
            return 0
        if args.command == "version":
            _emit(lab.version_info(args.scale))
            return 0
        if args.command == "fusion-detail":
            _emit(lab.fusion_detail(args.scale, args.query_id))
            return 0
        if args.command == "validate":
            status = lab.status()
            _emit(
                {
                    "status": "valid",
                    "workspace_id": status["workspace_id"],
                    "config_version": status["config_version"],
                    "scales": {
                        item["scale"]: {
                            "sequence": item["sequence"],
                            "all_stages_valid": item["all_stages_valid"],
                            "earliest_invalid_stage": item["earliest_invalid_stage"],
                        }
                        for item in status["scales"]
                    },
                }
            )
            return 0
        if args.command == "stop":
            _emit({"status": "stop_requested", **lab.request_stop(reason=args.reason)})
            return 0
        if args.command == "export":
            result = lab.export_summary(scales=_scales(args.scales))
            _emit({"status": "exported", **{key: result[key] for key in ("path", "digest", "bytes")}})
            return 0
        if args.command == "stage":
            _emit(lab.run_stage(args.scale, args.stage))
            return 0
        if args.command in ("run", "resume"):
            previous = _install_graceful_stop(lab, f"cli_{args.command}")
            try:
                if args.command == "resume":
                    result = lab.resume(
                        scales=_scales(args.scales),
                        until=args.until,
                        export=not args.no_export,
                    )
                else:
                    result = lab.run(
                        scales=_scales(args.scales),
                        until=args.until,
                        stop_after=args.stop_after,
                        export=not args.no_export,
                    )
            finally:
                _restore_signals(previous)
            _emit(result)
            return 0
        parser.error("unknown command")
    except BusinessError as exc:
        sys.stderr.write(json.dumps(exc.as_dict(), ensure_ascii=False, sort_keys=True, indent=2) + "\n")
        return 2
    except KeyboardInterrupt:
        sys.stderr.write(json.dumps({"code": "E_INTERRUPTED", "message": "interrupted between stages"}) + "\n")
        return 130
    return 0
