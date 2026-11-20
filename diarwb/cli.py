"""Command-line interface for the diarization workbench."""
from __future__ import annotations

import argparse
import json
import sys
from typing import Any, Dict

from .config import default_config
from .datagen import build_demo_dataset
from .errors import WorkbenchError
from .server import serve
from .workbench import Workbench


def _print(payload: Any) -> None:
    print(json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False))


def _load(root: str) -> Workbench:
    return Workbench(root, default_config())


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="diarwb",
                                     description="speaker diarization workbench")
    parser.add_argument("--root", required=True, help="workspace root directory")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("generate", help="create deterministic demo dataset")
    sub.add_parser("import", help="import and validate the manifest")
    sub.add_parser("run-all", help="run separation and recognition")

    stage = sub.add_parser("run-stage")
    stage.add_argument("--shard", required=True)
    stage.add_argument("--stage", required=True,
                       choices=["separation", "recognition"])

    sub.add_parser("status", help="show stage versions and fingerprints")

    timeline = sub.add_parser("timeline")
    timeline.add_argument("--shard", required=True)

    query = sub.add_parser("query")
    query.add_argument("--shard", required=True)
    query.add_argument("--kind", required=True,
                       choices=["segments", "similarity", "overlaps"])
    query.add_argument("--budget-ms", type=int, default=None)

    edit = sub.add_parser("edit", help="publish a single edit atomically")
    edit.add_argument("--shard", required=True)
    edit.add_argument("--op", required=True)
    edit.add_argument("--expected-version", type=int, required=True)
    edit.add_argument("--params", required=True,
                      help="JSON object with edit parameters")

    draft = sub.add_parser("draft")
    draft.add_argument("--shard", required=True)
    draft.add_argument("--op", required=True)
    draft.add_argument("--params", required=True)

    publish_draft = sub.add_parser("publish-draft")
    publish_draft.add_argument("--shard", required=True)
    publish_draft.add_argument("--expected-version", type=int, required=True)

    discard = sub.add_parser("discard-draft")
    discard.add_argument("--shard", required=True)

    unify = sub.add_parser("unify")
    unify.add_argument("--mappings", required=True,
                       help='JSON object, e.g. {"S1":"ALICE"}')

    cfg = sub.add_parser("config")
    cfg.add_argument("--set", dest="changes", default="{}",
                     help="JSON object with config changes")

    sub.add_parser("stop")
    sub.add_parser("recover")
    sub.add_parser("consistency")
    sub.add_parser("export")

    http = sub.add_parser("serve")
    http.add_argument("--host", default="127.0.0.1")
    http.add_argument("--port", type=int, default=8765)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "generate":
            _print(build_demo_dataset(args.root))
            return 0
        wb = _load(args.root)
        if args.command == "import":
            _print(wb.import_workspace())
        elif args.command == "run-all":
            _print(wb.run_all())
        elif args.command == "run-stage":
            _print(wb.run_stage(args.shard, args.stage))
        elif args.command == "status":
            _print({"config_version": wb.cfg.fingerprint(),
                    "stages": wb.stage_status()})
        elif args.command == "timeline":
            _print(wb.get_timeline(args.shard))
        elif args.command == "query":
            _print(wb.query(args.shard, args.kind, args.budget_ms))
        elif args.command == "edit":
            params = json.loads(args.params)
            _print(wb.publish_edit(args.shard, args.op, params,
                                   args.expected_version, origin="cli"))
        elif args.command == "draft":
            _print(wb.save_draft(args.shard, args.op,
                                 json.loads(args.params), origin="cli"))
        elif args.command == "publish-draft":
            _print(wb.publish_draft(args.shard, args.expected_version))
        elif args.command == "discard-draft":
            _print(wb.discard_draft(args.shard))
        elif args.command == "unify":
            _print(wb.unify_identity(json.loads(args.mappings), origin="cli"))
        elif args.command == "config":
            changes = json.loads(args.changes)
            _print(wb.update_config(**changes) if changes else
                   {"config": wb.cfg.canonical(),
                    "config_version": wb.cfg.fingerprint()})
        elif args.command == "stop":
            _print(wb.request_stop())
        elif args.command == "recover":
            _print(wb.recover())
        elif args.command == "consistency":
            report = wb.verify_consistency()
            _print({"status": report["status"], "shards": report["shards"]})
        elif args.command == "export":
            _print({"path": wb.export_results()})
        elif args.command == "serve":
            serve(args.root, args.host, args.port)
        return 0
    except WorkbenchError as exc:
        _print(exc.to_dict())
        return 1


if __name__ == "__main__":
    sys.exit(main())
