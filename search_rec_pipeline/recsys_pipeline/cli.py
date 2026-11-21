"""Command-line interface shared with the HTTP service."""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path
from typing import Any

from .errors import E_INVALID_REQUEST, PipelineError
from .pipeline import PipelineManager


def _json_dump(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2)


def _manager(args: argparse.Namespace) -> PipelineManager:
    return PipelineManager(
        Path(args.project),
        config_path=Path(args.config) if args.config else None,
        workdir_override=Path(args.workdir) if args.workdir else None,
    )


def _scale_or_all(value: str) -> str:
    if value not in ("small", "medium", "large", "all"):
        raise PipelineError(
            E_INVALID_REQUEST,
            "scale must be small, medium, large, or all",
            scale=value,
        )
    return value


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", default=str(Path.cwd()), help="project root")
    parser.add_argument("--config", default=None, help="YAML config path")
    parser.add_argument("--workdir", default=None, help="override configured workdir")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("import", help="validate and import samples/config declarations")

    for name in ("run", "resume"):
        run_parser = subparsers.add_parser(name, help=f"{name} the pipeline")
        run_parser.add_argument("--scale", required=True)
        run_parser.add_argument("--stop-after-stage", default=None)

    status_parser = subparsers.add_parser("status", help="query stage/version status")
    status_parser.add_argument("--scale", required=True)

    fingerprints_parser = subparsers.add_parser(
        "fingerprints", help="query stage version fingerprints and invalidation"
    )
    fingerprints_parser.add_argument("--scale", required=True)

    for name in ("decision", "encoding", "budget"):
        query_parser = subparsers.add_parser(name, help=f"query {name}")
        query_parser.add_argument("--scale", required=True)

    stop_parser = subparsers.add_parser("stop", help="request graceful stop at a stage boundary")
    stop_parser.add_argument("--scale", default="all")

    export_parser = subparsers.add_parser("export", help="export an assembled artifact")
    export_parser.add_argument("--scale", required=True)
    export_parser.add_argument(
        "--kind",
        default="all",
        help="samples, discarded, stats, normalized, encoded, decision, budget, or all",
    )
    export_parser.add_argument("--out", default=None, help="destination path for one artifact")
    return parser


def dispatch(manager: PipelineManager, args: argparse.Namespace) -> Any:
    command = args.command
    if command == "import":
        return manager.import_samples()
    if command in {"run", "resume"}:
        scale = _scale_or_all(args.scale)
        if scale == "all":
            return manager.run_all(stop_after_stage=args.stop_after_stage)
        return manager.run(scale, stop_after_stage=args.stop_after_stage)
    if command == "status":
        scale = _scale_or_all(args.scale)
        if scale == "all":
            return {name: manager.status(name) for name in ("small", "medium", "large")}
        return manager.status(scale)
    if command == "fingerprints":
        scale = _scale_or_all(args.scale)
        if scale == "all":
            return {name: manager.get_fingerprints(name) for name in ("small", "medium", "large")}
        return manager.get_fingerprints(scale)
    if command == "decision":
        return manager.get_decision(_scale_or_all(args.scale))
    if command == "encoding":
        return manager.get_encoding_summary(_scale_or_all(args.scale))
    if command == "budget":
        return manager.get_budget(_scale_or_all(args.scale))
    if command == "stop":
        return manager.request_stop(args.scale)
    if command == "export":
        scale = _scale_or_all(args.scale)
        info = manager.export(scale, args.kind)
        if args.out and args.kind != "all":
            source = Path(info["path"])
            destination = Path(args.out)
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, destination)
            info = dict(info)
            info["exported_to"] = str(destination.resolve())
        return info
    raise PipelineError(E_INVALID_REQUEST, "unknown command", command=command)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        manager = _manager(args)
        result = dispatch(manager, args)
        print(_json_dump({"ok": True, "data": result}))
        return 0
    except PipelineError as exc:
        print(_json_dump({"ok": False, "error": exc.to_dict()}), file=sys.stderr)
        return 2
    except Exception as exc:  # unexpected local failure
        payload = {
            "ok": False,
            "error": {
                "code": "E_INTERNAL",
                "message": str(exc),
                "details": {"exception_type": type(exc).__name__},
            },
        }
        print(_json_dump(payload), file=sys.stderr)
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
