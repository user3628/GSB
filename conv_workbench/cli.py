"""Command-line interface for the deterministic convolution workbench."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from .canonical import canonical_json
from .engine import Workbench, request_stop
from .errors import WorkbenchError
from .ops import ExecOptions
from .samples import generate_demo_workspace
from .storage import atomic_write_bytes


def _print(value: Any) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True))


def _workbench(args: argparse.Namespace) -> Workbench:
    options = ExecOptions(
        block_h=args.block_h,
        block_w=args.block_w,
        out_channel_block=args.out_channel_block,
    )
    return Workbench(args.workspace, args.run_dir, exec_options=options)


def _add_execution_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--block-h", type=int, default=2, help="convolution output-row block size")
    parser.add_argument("--block-w", type=int, default=3, help="convolution output-column block size")
    parser.add_argument("--out-channel-block", type=int, default=2, help="convolution output-channel block size")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="conv-workbench",
        description="Deterministic local convolution/interpolation stage workbench",
    )
    parser.add_argument("--workspace", default="demo_workspace", help="workspace directory")
    parser.add_argument("--run-dir", default="runs/default", help="checkpoint/output run directory")
    subparsers = parser.add_subparsers(dest="command", required=True)

    init_parser = subparsers.add_parser("init", help="generate the deterministic demo workspace")
    init_parser.add_argument("--root", default=None, help="destination (defaults to --workspace)")
    init_parser.add_argument("--force", action="store_true")

    subparsers.add_parser("self-check", help="validate every manifest chain without executing it")
    subparsers.add_parser("list-shards", help="list imported shards")

    status = subparsers.add_parser("status", help="show stage checkpoint state")
    status.add_argument("--shard", default=None)

    fingerprints = subparsers.add_parser("fingerprints", help="show planned and active version fingerprints")
    fingerprints.add_argument("--shard", required=True)

    run = subparsers.add_parser("run", help="execute or continue the selected shard chains")
    run.add_argument("--shard", action="append", dest="shards", default=None)
    run.add_argument("--stop-after", type=int, default=None, help="gracefully stop after N newly executed stages")
    run.add_argument("--delay-between-stages", type=float, default=0.0)
    _add_execution_options(run)

    resume = subparsers.add_parser("resume", help="recover from the latest consistent checkpoint and finish")
    resume.add_argument("--shard", action="append", dest="shards", default=None)
    resume.add_argument("--stop-after", type=int, default=None)
    resume.add_argument("--delay-between-stages", type=float, default=0.0)
    _add_execution_options(resume)

    stage = subparsers.add_parser("stage", help="execute or request one operator stage")
    stage.add_argument("--shard", required=True)
    stage.add_argument("--index", required=True, type=int)
    _add_execution_options(stage)

    stop = subparsers.add_parser("stop", help="request a graceful stop after the current stage")
    stop.add_argument("--reason", default="CLI stop request")

    export = subparsers.add_parser("export", help="export completed final tensors and shape trace")
    export.add_argument("--shard", default=None)
    export.add_argument("--output", default=None, help="destination .npy when exporting one shard")
    _add_execution_options(export)

    import_parser = subparsers.add_parser("import-shard", help="copy a .npy shard and append its operator chain")
    import_parser.add_argument("--id", required=True)
    import_parser.add_argument("--source", required=True, help="local source .npy path")
    import_parser.add_argument("--chain", required=True, help="JSON file containing an operators array")
    import_parser.add_argument("--expectations", default=None, help="optional JSON file with a stage expectations object")

    serve = subparsers.add_parser("serve", help="start the local HTTP application")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8766)
    _add_execution_options(serve)
    return parser


def _run_command(args: argparse.Namespace) -> dict[str, Any]:
    workbench = _workbench(args)
    return workbench.run(
        shard_ids=args.shards,
        stop_after=args.stop_after,
        delay_between_stages=args.delay_between_stages,
    )


def dispatch(args: argparse.Namespace) -> dict[str, Any]:
    if args.command == "init":
        root = args.root or args.workspace
        return {"ok": True, **generate_demo_workspace(root, force=args.force)}
    if args.command == "serve":
        from .server import serve

        serve(
            workspace=args.workspace,
            run_dir=args.run_dir,
            host=args.host,
            port=args.port,
            exec_options=ExecOptions(args.block_h, args.block_w, args.out_channel_block),
        )
        return {"ok": True, "status": "server_stopped"}

    workbench = _workbench(args) if hasattr(args, "block_h") else Workbench(args.workspace, args.run_dir)
    if args.command == "self-check":
        return workbench.self_check()
    if args.command == "list-shards":
        return {"ok": True, "workspace": str(workbench.workspace.root), "shards": workbench.workspace.list_shards()}
    if args.command == "status":
        return workbench.status(args.shard)
    if args.command == "fingerprints":
        return workbench.fingerprints(args.shard)
    if args.command in {"run", "resume"}:
        return _run_command(args)
    if args.command == "stage":
        return workbench.execute_stage(args.shard, args.index)
    if args.command == "stop":
        payload = request_stop(workbench.run_dir, args.reason)
        return {
            "ok": True,
            "status": "stop_requested",
            "run_dir": str(workbench.run_dir),
            "request": payload,
            "note": "the worker exits after completing the current indivisible stage",
        }
    if args.command == "export":
        shard_ids = [args.shard] if args.shard else None
        result = workbench.export(shard_ids=shard_ids, write_trace=True)
        if args.output:
            if not args.shard:
                raise WorkbenchError("E_INVALID_REQUEST", "--output requires --shard")
            source = workbench.run_dir / result["outputs"][args.shard]["path"]
            destination = Path(args.output).expanduser().resolve()
            atomic_write_bytes(destination, source.read_bytes())
            result["custom_output"] = str(destination)
        return result
    if args.command == "import-shard":
        chain_document = json.loads(Path(args.chain).read_text(encoding="utf-8"))
        operators = chain_document.get("operators") if isinstance(chain_document, dict) else chain_document
        expectations = None
        if args.expectations:
            expectations = json.loads(Path(args.expectations).read_text(encoding="utf-8"))
            if isinstance(expectations, dict) and "shards" in expectations:
                expectations = expectations.get("shards", {}).get(args.id)
        return {
            "ok": True,
            **workbench.workspace.import_shard(
                {
                    "id": args.id,
                    "source_path": args.source,
                    "operators": operators,
                    "expectations": expectations,
                }
            ),
        }
    raise WorkbenchError("E_INVALID_REQUEST", f"unknown command {args.command!r}")


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        result = dispatch(args)
    except WorkbenchError as exc:
        print(json.dumps(exc.to_dict(), ensure_ascii=False, indent=2, sort_keys=True), file=sys.stderr)
        return 2
    except OSError as exc:
        error = WorkbenchError("E_IO", str(exc), path=getattr(exc, "filename", None))
        print(json.dumps(error.to_dict(), ensure_ascii=False, indent=2, sort_keys=True), file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print(canonical_json({"ok": False, "error": {"code": "E_INTERRUPTED", "message": "interrupted"}}), file=sys.stderr)
        return 130
    _print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

