from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Sequence

from .config import WorkbenchConfig
from .demo import generate_demo_data, run_demo
from .errors import WorkbenchError
from .workspace import Workspace


def _print_json(value: Any) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True))


def _read_json_argument(path_or_inline: str) -> Any:
    candidate = Path(path_or_inline)
    if candidate.exists():
        return json.loads(candidate.read_text(encoding="utf-8"))
    return json.loads(path_or_inline)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="transcript-workbench",
        description="Local deterministic transcript shard editing workbench",
    )
    parser.add_argument(
        "--root",
        default=".",
        help="Project root for demo/data commands (default: current directory)",
    )
    parser.add_argument(
        "--workspace",
        default="artifacts/workspace",
        help="Workspace directory relative to --root (default: artifacts/workspace)",
    )
    parser.add_argument(
        "--max-gap-ms",
        type=int,
        default=1000,
        help="Maximum tolerated idle gap inside a chunk",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    generate = subparsers.add_parser("generate-data", help="Generate deterministic demo inputs")
    generate.add_argument("--seed", type=int, default=20_261_120)

    demo = subparsers.add_parser("demo", help="Run the complete deterministic acceptance demo")
    demo.add_argument("--seed", type=int, default=20_261_120)

    init = subparsers.add_parser("init", help="Import a task manifest and create version 1")
    init.add_argument("--manifest", required=True)

    subparsers.add_parser("versions", help="List monotonically increasing versions")

    show = subparsers.add_parser("show", help="Show a version with full word timeline")
    show.add_argument("--version", type=int)

    diff = subparsers.add_parser("diff", help="Diff two versions")
    diff.add_argument("--left", type=int, required=True)
    diff.add_argument("--right", type=int, required=True)

    export = subparsers.add_parser("export", help="Export JSON, SRT or WebVTT")
    export.add_argument("--format", required=True, choices=["json", "srt", "vtt", "webvtt"])
    export.add_argument("--version", type=int)
    export.add_argument("--chunk", dest="chunk_id")
    export.add_argument("--out")

    import_text = subparsers.add_parser("import", help="Import a transcript into a new version")
    import_text.add_argument("--format", required=True, choices=["json", "srt", "vtt", "webvtt"])
    import_text.add_argument("--file", required=True)
    import_text.add_argument("--base-version", type=int, required=True)
    import_text.add_argument("--chunk", dest="chunk_id")

    edit = subparsers.add_parser("edit", help="Apply atomic word-level edit operations")
    edit.add_argument("--base-version", type=int, required=True)
    edit.add_argument("--ops", required=True, help="JSON file path or inline JSON array")

    fork = subparsers.add_parser("fork", help="Publish a version branching from an older version")
    fork.add_argument("--base-version", type=int, required=True)
    fork.add_argument("--parent-version", type=int, required=True)
    fork.add_argument("--ops", required=True)

    merge = subparsers.add_parser("merge", help="Merge two versions with a configured strategy")
    merge.add_argument("--base-version", type=int, required=True)
    merge.add_argument("--left", type=int, required=True)
    merge.add_argument("--right", type=int, required=True)
    merge.add_argument("--strategy", choices=["last-wins", "mark-conflict"], default="last-wins")

    roundtrip = subparsers.add_parser("roundtrip", help="Run JSON/SRT/WebVTT round-trip checks")
    roundtrip.add_argument("--version", type=int)
    roundtrip.add_argument("--chunk", dest="chunk_id")

    subparsers.add_parser("stop", help="Drain edits and write a stop marker")
    subparsers.add_parser("recover", help="Recover the latest consistent snapshot")
    subparsers.add_parser("snapshot", help="Inspect snapshot metadata")

    serve = subparsers.add_parser("serve", help="Start the local HTTP API")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8765)
    serve.add_argument("--exit-on-stop", action="store_true")
    return parser


def _workspace(args: argparse.Namespace) -> Workspace:
    root = Path(args.root).resolve()
    workspace_path = Path(args.workspace)
    if not workspace_path.is_absolute():
        workspace_path = root / workspace_path
    return Workspace(
        workspace_path,
        config=WorkbenchConfig(max_gap_ms=args.max_gap_ms),
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "generate-data":
            result = generate_demo_data(args.root, seed=args.seed, clean=True)
            _print_json(result)
            return 0
        if args.command == "demo":
            _print_json(run_demo(args.root, seed=args.seed))
            return 0
        if args.command == "serve":
            import uvicorn

            from .http_api import create_app

            if args.exit_on_stop:
                os.environ["WORKBENCH_EXIT_ON_STOP"] = "1"
            app = create_app(
                _workspace(args).root,
                config=WorkbenchConfig(max_gap_ms=args.max_gap_ms),
                exit_on_stop=args.exit_on_stop,
            )
            uvicorn.run(app, host=args.host, port=args.port)
            return 0
        workspace = _workspace(args)
        if args.command == "init":
            _print_json(workspace.bootstrap(args.manifest))
        elif args.command == "versions":
            history = workspace.version_history()
            _print_json({"ok": True, "versions": history, "latest_version": history[-1]["version"]})
        elif args.command == "show":
            state = workspace.load_state() if args.version is None else workspace.load_version(args.version)
            _print_json(
                {
                    **workspace._state_response(state),
                    "chunks": [chunk.to_dict() for chunk in state.chunks.values()],
                    "words": [word.to_dict() for word in state.sorted_words()],
                    "changes": state.metadata.get("changes", {}),
                }
            )
        elif args.command == "diff":
            _print_json(workspace.diff_versions(args.left, args.right))
        elif args.command == "export":
            result = workspace.export(
                fmt=args.format, version=args.version, chunk_id=args.chunk_id
            )
            if args.out:
                output_path = Path(args.out)
                if not output_path.is_absolute():
                    output_path = Path(args.root).resolve() / output_path
                output_path.parent.mkdir(parents=True, exist_ok=True)
                output_path.write_text(result["content"], encoding="utf-8")
                result = {**result, "content": None, "output_path": str(output_path)}
            _print_json(result)
        elif args.command == "import":
            content = Path(args.file).read_text(encoding="utf-8")
            _print_json(
                workspace.import_text(
                    content=content,
                    fmt=args.format,
                    expected_version=args.base_version,
                    chunk_id=args.chunk_id,
                )
            )
        elif args.command == "edit":
            operations = _read_json_argument(args.ops)
            if not isinstance(operations, list):
                raise WorkbenchError(
                    "E_INVALID_REQUEST", "Edit operations must be a JSON array"
                )
            _print_json(
                workspace.edit(operations, expected_version=args.base_version)
            )
        elif args.command == "fork":
            operations = _read_json_argument(args.ops)
            if not isinstance(operations, list):
                raise WorkbenchError(
                    "E_INVALID_REQUEST", "Fork operations must be a JSON array"
                )
            _print_json(
                workspace.fork_edit(
                    operations,
                    parent_version=args.parent_version,
                    expected_version=args.base_version,
                )
            )
        elif args.command == "merge":
            _print_json(
                workspace.merge_versions(
                    left_version=args.left,
                    right_version=args.right,
                    strategy=args.strategy,
                    expected_version=args.base_version,
                )
            )
        elif args.command == "roundtrip":
            _print_json(
                workspace.roundtrip_check(
                    version=args.version, chunk_id=args.chunk_id
                )
            )
        elif args.command == "stop":
            _print_json(workspace.request_stop())
        elif args.command == "recover":
            _print_json(workspace.recover())
        elif args.command == "snapshot":
            _print_json(workspace.snapshot_info())
        else:
            parser.error("Unknown command")
        return 0
    except WorkbenchError as exc:
        print(json.dumps(exc.to_dict(), ensure_ascii=False, indent=2, sort_keys=True), file=sys.stderr)
        return 2 if exc.code.startswith("E_") else 1
    except (OSError, json.JSONDecodeError) as exc:
        error = WorkbenchError("E_INVALID_REQUEST", str(exc))
        print(json.dumps(error.to_dict(), ensure_ascii=False, indent=2, sort_keys=True), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
