"""Command line interface for the dependency workbench."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from .errors import WorkbenchError
from .workspace import WorkbenchService, Workspace


def _print(payload: Any) -> None:
    sys.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2,
                                sort_keys=False) + "\n")


def _service(args: argparse.Namespace) -> WorkbenchService:
    workspace = Workspace.create(
        args.workspace,
        checkpoint_root=args.checkpoint_dir if args.checkpoint_dir else None,
    )
    return WorkbenchService(workspace)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="depworkbench",
        description="Deterministic dependency parsing scheduling workbench",
    )
    parser.add_argument("--workspace", default="workspaces/default",
                        help="workspace directory")
    parser.add_argument("--checkpoint-dir", default=None,
                        help="override checkpoint directory")
    sub = parser.add_subparsers(dest="command", required=True)

    p_import = sub.add_parser("import", help="import shards, manifest and config")
    p_import.add_argument("--shards-dir", required=True)
    p_import.add_argument("--manifest", required=True)
    p_import.add_argument("--config", required=True)

    sub.add_parser("plan", help="compute the admission plan")

    p_run = sub.add_parser("run", help="run (or resume) analysis")
    p_run.add_argument("--sequential", action="store_true",
                       help="force single-threaded execution")

    p_resume = sub.add_parser("resume", help="resume after stop/failure")
    p_resume.add_argument("--sequential", action="store_true")

    sub.add_parser("stop", help="request graceful stop")
    sub.add_parser("status", help="show shard states and version fingerprints")

    p_stats = sub.add_parser("stats", help="aggregate dependency statistics")
    p_stats.add_argument("--up-to-shard", type=int, default=None)

    p_export = sub.add_parser("export", help="export full parse results")
    p_export.add_argument("--output", required=True)

    sub.add_parser("reset", help="clear checkpoints and run state")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        service = _service(args)
        if args.command == "import":
            _print(service.import_bundle(args.shards_dir, args.manifest,
                                         args.config))
        elif args.command == "plan":
            _print(service.plan())
        elif args.command == "run":
            _print(service.run(sequential=args.sequential).to_dict())
        elif args.command == "resume":
            _print(service.resume(sequential=args.sequential).to_dict())
        elif args.command == "stop":
            _print(service.stop())
        elif args.command == "status":
            _print(service.status())
        elif args.command == "stats":
            _print(service.stats(args.up_to_shard))
        elif args.command == "export":
            path = service.export(args.output)
            _print({"exported": str(path)})
        elif args.command == "reset":
            service.reset()
            _print({"reset": True})
        return 0
    except WorkbenchError as exc:
        _print({"error": exc.to_dict()})
        return 2
    except FileNotFoundError as exc:
        _print({"error": {"code": "E_NOT_FOUND", "message": str(exc),
                          "details": {}}})
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
