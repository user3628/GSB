from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from .checkpoints import CheckpointStore
from .errors import BusinessError
from .pipeline import (
    export_scores,
    run_pipeline,
    verify_against_full,
    workspace_status,
)
from .util import atomic_write
from .workspace import (
    WorkspacePaths,
    clear_stop,
    import_bridge,
    import_shard,
    initialize_workspace,
    read_json,
    request_stop,
    update_bridge_entry,
    update_config,
    edit_pair,
)


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_WORKSPACE = ROOT / "artifacts" / "workspace"


def _print_json(value: Any) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True))


def _load_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="xrel-workbench",
        description="Cross-lingual relevance scoring and numeric error propagation workbench",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    init_parser = subparsers.add_parser("init-workspace", help="copy deterministic inputs into a workspace")
    init_parser.add_argument("--data", type=Path, default=ROOT / "data")
    init_parser.add_argument("--workspace", type=Path, default=DEFAULT_WORKSPACE)
    init_parser.add_argument("--force", action="store_true")

    run_parser = subparsers.add_parser("run", help="run full or incremental build")
    run_parser.add_argument("--workspace", type=Path, default=DEFAULT_WORKSPACE)
    run_parser.add_argument("--full", action="store_true")
    run_parser.add_argument("--no-verify", action="store_true")
    run_parser.add_argument("--stop-after-units", type=int)

    status_parser = subparsers.add_parser("status", help="show stages, fingerprints, and checkpoints")
    status_parser.add_argument("--workspace", type=Path, default=DEFAULT_WORKSPACE)

    fp_parser = subparsers.add_parser("fingerprints", help="show version fingerprints")
    fp_parser.add_argument("--workspace", type=Path, default=DEFAULT_WORKSPACE)

    report_parser = subparsers.add_parser("report", help="print a generated report")
    report_parser.add_argument("--workspace", type=Path, default=DEFAULT_WORKSPACE)
    report_parser.add_argument(
        "--kind",
        choices=["summary", "validation", "error_report", "grades"],
        default="summary",
    )
    report_parser.add_argument("--shard", default="part-001")

    edit_bridge = subparsers.add_parser("edit-bridge", help="edit one bridge entry and optionally rebuild")
    edit_bridge.add_argument("--workspace", type=Path, default=DEFAULT_WORKSPACE)
    edit_bridge.add_argument("--entry-id", required=True)
    edit_bridge.add_argument("--weight", type=float)
    edit_bridge.add_argument("--zh")
    edit_bridge.add_argument("--en", nargs="+")
    edit_bridge.add_argument("--no-recalculate", action="store_true")
    edit_bridge.add_argument("--no-verify", action="store_true")

    edit_config = subparsers.add_parser("edit-config", help="edit a dotted config path and optionally rebuild")
    edit_config.add_argument("--workspace", type=Path, default=DEFAULT_WORKSPACE)
    edit_config.add_argument("--path", required=True, dest="config_path")
    edit_config.add_argument("--value", required=True, help="JSON value")
    edit_config.add_argument("--no-recalculate", action="store_true")
    edit_config.add_argument("--no-verify", action="store_true")

    edit_pair_parser = subparsers.add_parser("edit-pair", help="edit one pair in a workspace shard")
    edit_pair_parser.add_argument("--workspace", type=Path, default=DEFAULT_WORKSPACE)
    edit_pair_parser.add_argument("--shard", required=True)
    edit_pair_parser.add_argument("--pair-id", required=True)
    edit_pair_parser.add_argument("--query")
    edit_pair_parser.add_argument("--doc")
    edit_pair_parser.add_argument("--human-relevance", type=int)
    edit_pair_parser.add_argument("--clear-human-relevance", action="store_true")
    edit_pair_parser.add_argument("--no-recalculate", action="store_true")
    edit_pair_parser.add_argument("--no-verify", action="store_true")

    import_shard_parser = subparsers.add_parser("import-shard", help="import JSONL into a workspace shard")
    import_shard_parser.add_argument("--workspace", type=Path, default=DEFAULT_WORKSPACE)
    import_shard_parser.add_argument("--shard", required=True)
    import_shard_parser.add_argument("--file", type=Path, required=True)
    import_shard_parser.add_argument("--no-recalculate", action="store_true")
    import_shard_parser.add_argument("--no-verify", action="store_true")

    import_bridge_parser = subparsers.add_parser("import-bridge", help="import a bridge JSON file")
    import_bridge_parser.add_argument("--workspace", type=Path, default=DEFAULT_WORKSPACE)
    import_bridge_parser.add_argument("--file", type=Path, required=True)
    import_bridge_parser.add_argument("--no-recalculate", action="store_true")
    import_bridge_parser.add_argument("--no-verify", action="store_true")

    stop_parser = subparsers.add_parser("stop", help="request graceful stop at the next shard boundary")
    stop_parser.add_argument("--workspace", type=Path, default=DEFAULT_WORKSPACE)

    resume_parser = subparsers.add_parser("resume", help="clear stop request and resume")
    resume_parser.add_argument("--workspace", type=Path, default=DEFAULT_WORKSPACE)
    resume_parser.add_argument("--no-verify", action="store_true")

    verify_parser = subparsers.add_parser("verify", help="byte-compare current outputs with a full build")
    verify_parser.add_argument("--workspace", type=Path, default=DEFAULT_WORKSPACE)

    export_parser = subparsers.add_parser("export", help="export scores JSONL")
    export_parser.add_argument("--workspace", type=Path, default=DEFAULT_WORKSPACE)
    export_parser.add_argument("--out", type=Path)

    checkpoint_parser = subparsers.add_parser("checkpoint", help="manual checkpoint publication")
    checkpoint_parser.add_argument("--workspace", type=Path, default=DEFAULT_WORKSPACE)
    checkpoint_parser.add_argument("--stage", required=True)
    checkpoint_parser.add_argument("--shard", required=True)
    checkpoint_parser.add_argument("--input-digest", required=True)
    checkpoint_parser.add_argument("--config-version", required=True)
    checkpoint_parser.add_argument("--artifact-path", required=True)
    checkpoint_parser.add_argument("--fingerprint", required=True)
    checkpoint_parser.add_argument("--expected-seq", type=int)

    serve_parser = subparsers.add_parser("serve", help="start local HTTP workbench")
    serve_parser.add_argument("--workspace", type=Path, default=DEFAULT_WORKSPACE)
    serve_parser.add_argument("--host", default="127.0.0.1")
    serve_parser.add_argument("--port", type=int, default=8765)

    demo_parser = subparsers.add_parser("demo", help="run deterministic full, stop/resume, and incremental demo")
    demo_parser.add_argument("--seed", type=int, default=20261121)
    demo_parser.add_argument("--data", type=Path, default=ROOT / "data")
    demo_parser.add_argument("--artifacts", type=Path, default=ROOT / "artifacts" / "demo")

    return parser


def _maybe_run(
    workspace: WorkspacePaths,
    *,
    recalculate: bool,
    verify: bool,
) -> dict[str, Any] | None:
    if not recalculate:
        return None
    return run_pipeline(workspace, verify_incremental=verify)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    workspace = WorkspacePaths(Path(getattr(args, "workspace", DEFAULT_WORKSPACE)).resolve())
    try:
        if args.command == "init-workspace":
            paths = initialize_workspace(args.data, workspace.root, force=args.force)
            _print_json({"ok": True, "workspace": str(paths.root)})
        elif args.command == "run":
            result = run_pipeline(
                workspace,
                full=args.full,
                verify_incremental=not args.no_verify,
                stop_after_units=args.stop_after_units,
            )
            _print_json(result)
        elif args.command == "status":
            _print_json(workspace_status(workspace))
        elif args.command == "fingerprints":
            manifest = read_json(workspace.manifest)
            _print_json(
                {
                    "config_version": manifest.get("config_version"),
                    "input_digests": manifest.get("input_digests"),
                    "fingerprints": manifest.get("fingerprints"),
                }
            )
        elif args.command == "report":
            if args.kind == "grades":
                path = workspace.artifacts / "stage4" / f"{args.shard}.jsonl"
                if not path.exists():
                    raise BusinessError("E_INPUT", f"grade artifact not found: {path}")
                print(path.read_text(encoding="utf-8"), end="")
            else:
                name = {
                    "summary": "summary.json",
                    "validation": "validation.json",
                    "error_report": "error_report.json",
                }[args.kind]
                print((workspace.reports / name).read_text(encoding="utf-8"), end="")
        elif args.command == "edit-bridge":
            changed = update_bridge_entry(
                workspace,
                args.entry_id,
                weight=args.weight,
                en=args.en,
                zh=args.zh,
            )
            build = _maybe_run(
                workspace,
                recalculate=not args.no_recalculate,
                verify=not args.no_verify,
            )
            _print_json({"ok": True, "edit": changed, "build": build})
        elif args.command == "edit-config":
            value = json.loads(args.value)
            changed = update_config(workspace, args.config_path, value)
            build = _maybe_run(
                workspace,
                recalculate=not args.no_recalculate,
                verify=not args.no_verify,
            )
            _print_json({"ok": True, "edit": changed, "build": build})
        elif args.command == "edit-pair":
            changed = edit_pair(
                workspace,
                args.shard,
                args.pair_id,
                query=args.query,
                doc=args.doc,
                human_relevance=args.human_relevance,
                clear_human_relevance=args.clear_human_relevance,
            )
            build = _maybe_run(
                workspace,
                recalculate=not args.no_recalculate,
                verify=not args.no_verify,
            )
            _print_json({"ok": True, "edit": changed, "build": build})
        elif args.command == "import-shard":
            changed = import_shard(workspace, args.shard, _load_text(args.file))
            build = _maybe_run(
                workspace,
                recalculate=not args.no_recalculate,
                verify=not args.no_verify,
            )
            _print_json({"ok": True, "import": changed, "build": build})
        elif args.command == "import-bridge":
            changed = import_bridge(workspace, _load_text(args.file))
            build = _maybe_run(
                workspace,
                recalculate=not args.no_recalculate,
                verify=not args.no_verify,
            )
            _print_json({"ok": True, "import": changed, "build": build})
        elif args.command == "stop":
            request_stop(workspace)
            _print_json({"ok": True, "status": "stop_requested"})
        elif args.command == "resume":
            clear_stop(workspace)
            result = run_pipeline(workspace, verify_incremental=not args.no_verify)
            _print_json(result)
        elif args.command == "verify":
            _print_json(verify_against_full(workspace))
        elif args.command == "export":
            data = export_scores(workspace)
            if args.out is None:
                sys.stdout.buffer.write(data)
            else:
                atomic_write(args.out, data)
                _print_json({"ok": True, "path": str(args.out), "bytes": len(data)})
        elif args.command == "checkpoint":
            store = CheckpointStore(workspace.root)
            record = store.publish(
                stage=args.stage,
                shard=args.shard,
                input_digest=args.input_digest,
                config_version=args.config_version,
                artifact_path=args.artifact_path,
                fingerprint=args.fingerprint,
                expected_seq=args.expected_seq,
            )
            _print_json({"ok": True, "checkpoint": record})
        elif args.command == "serve":
            from .server import create_app

            app = create_app(workspace.root)
            print(f"xrel workbench listening on http://{args.host}:{args.port}")
            app.run(host=args.host, port=args.port, debug=False, use_reloader=False)
        elif args.command == "demo":
            from scripts.run_demo import run_demo

            result = run_demo(args.data, args.artifacts, seed=args.seed)
            _print_json(result)
        else:
            parser.error("unknown command")
        return 0
    except BusinessError as exc:
        print(json.dumps(exc.to_dict(), ensure_ascii=False, indent=2, sort_keys=True))
        return 2
    except Exception as exc:  # pragma: no cover - defensive CLI boundary
        print(json.dumps({"ok": False, "error": {"code": "E_UNEXPECTED", "message": str(exc)}}, ensure_ascii=False, indent=2))
        return 1
