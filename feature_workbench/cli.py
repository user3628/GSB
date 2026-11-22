from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from .config import parse_scalar
from .engine import Workbench
from .errors import WorkbenchError
from .generator import generate_demo


def _resolve(base: Path, value: str | None) -> Path | None:
    if value is None:
        return None
    path = Path(value)
    return path if path.is_absolute() else base / path


def _workbench(args: argparse.Namespace) -> Workbench:
    base = Path(args.root).resolve()
    return Workbench(
        root=base,
        data_dir=_resolve(base, args.data_dir),
        config_path=_resolve(base, args.config),
        manifest_path=_resolve(base, args.manifest),
        state_dir=_resolve(base, args.state_dir),
        export_dir=_resolve(base, args.export_dir),
    )


def _print(value: Any) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=False))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="feature-workbench",
        description="Deterministic table feature encoding and selection workbench",
    )
    parser.add_argument("--root", default=".", help="Workspace root (default: current directory)")
    parser.add_argument("--data-dir", default=None, help="Data directory (default: ROOT/data)")
    parser.add_argument("--config", default=None, help="YAML config path (default: DATA/config.yaml)")
    parser.add_argument("--manifest", default=None, help="JSON manifest path (default: DATA/manifest.json)")
    parser.add_argument("--state-dir", default=None, help="Checkpoint directory (default: ROOT/.workbench)")
    parser.add_argument("--export-dir", default=None, help="Export directory (default: ROOT/exports)")
    subparsers = parser.add_subparsers(dest="command", required=True)

    generate = subparsers.add_parser("generate-samples", help="Generate deterministic CSV shards")
    generate.add_argument("--output", default=None, help="Output directory (default: DATA)")
    generate.add_argument("--seed", type=int, default=20260317)
    generate.add_argument("--small-rows", type=int, default=600)
    generate.add_argument("--medium-rows", type=int, default=6000)
    generate.add_argument("--small-shards", type=int, default=2)
    generate.add_argument("--medium-shards", type=int, default=3)

    import_samples = subparsers.add_parser("import-samples", help="Import a sample directory")
    import_samples.add_argument("source", help="Source directory containing manifest.json and shards")

    configure = subparsers.add_parser("configure", help="Atomically update configuration values")
    configure.add_argument(
        "--set",
        dest="changes",
        action="append",
        required=True,
        metavar="KEY=VALUE",
        help="Dotted key with a YAML scalar, e.g. encoding.strategy=target",
    )

    run = subparsers.add_parser("run", help="Run pipeline stages")
    run.add_argument(
        "--mode",
        choices=["process", "sequential"],
        default="process",
        help="Worker execution mode (default: process)",
    )
    run.add_argument(
        "--until-stage",
        default="aggregate",
        choices=["type_inference", "encoding", "selection", "aggregate"],
    )
    run.add_argument("--stop-after-shards", type=int, default=None)

    resume = subparsers.add_parser("resume", help="Clear stop request and resume pipeline")
    resume.add_argument("--mode", choices=["process", "sequential"], default="process")
    resume.add_argument(
        "--until-stage",
        default="aggregate",
        choices=["type_inference", "encoding", "selection", "aggregate"],
    )

    subparsers.add_parser("status", help="Show stage status, versions, budget, and stop state")
    subparsers.add_parser("version", help="Show stage version fingerprints and invalidation stage")
    subparsers.add_parser("budget", help="Show admitted shards, lanes, and unprocessed reasons")

    stop = subparsers.add_parser("stop", help="Request graceful stop after the current unit")
    stop.add_argument("--reason", default="user_requested")

    export = subparsers.add_parser("export", help="Materialize and show the selection result")
    export.add_argument("--full", action="store_true", help="Print the full deterministic summary")

    serve = subparsers.add_parser("serve", help="Run the local HTTP API")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8765)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "generate-samples":
            base = Path(args.root).resolve()
            output = _resolve(base, args.output) or (
                (_resolve(base, args.data_dir) or base / "data")
            )
            manifest = generate_demo(
                output,
                seed=args.seed,
                small_rows=args.small_rows,
                medium_rows=args.medium_rows,
                small_shards=args.small_shards,
                medium_shards=args.medium_shards,
            )
            _print(
                {
                    "ok": True,
                    "output": str(output),
                    "files": [entry["file"] for entry in manifest["files"]],
                    "tiers": {
                        tier: {
                            "rows": info["rows"],
                            "columns": info["columns"],
                            "shards": info["shards"],
                        }
                        for tier, info in manifest["tiers"].items()
                    },
                }
            )
            return 0

        workbench = _workbench(args)
        if args.command == "import-samples":
            _print(workbench.import_samples(args.source))
            return 0
        if args.command == "configure":
            changes: dict[str, Any] = {}
            for item in args.changes:
                if "=" not in item:
                    raise WorkbenchError(
                        "E_CONFIG_INVALID",
                        "--set values must use KEY=VALUE syntax",
                        {"value": item},
                    )
                key, raw_value = item.split("=", 1)
                changes[key] = parse_scalar(raw_value)
            _print(workbench.update_config(changes))
            return 0
        if args.command == "run":
            _print(
                workbench.run(
                    until_stage=args.until_stage,
                    worker_mode=args.mode,
                    stop_after_shards=args.stop_after_shards,
                )
            )
            return 0
        if args.command == "resume":
            _print(
                workbench.run(
                    until_stage=args.until_stage,
                    worker_mode=args.mode,
                    resume=True,
                )
            )
            return 0
        if args.command == "status":
            _print(workbench.status())
            return 0
        if args.command == "version":
            _print(workbench.version_report())
            return 0
        if args.command == "budget":
            _print(workbench.budget_report())
            return 0
        if args.command == "stop":
            _print(workbench.request_stop(args.reason))
            return 0
        if args.command == "export":
            exported = workbench.export_result()
            if args.full:
                _print(exported)
            else:
                _print(
                    {
                        "ok": exported["ok"],
                        "status": exported["status"],
                        "result_digest": exported["result_digest"],
                        "exports": exported["exports"],
                        "tiers": {
                            tier: {
                                "rows": info["rows"],
                                "selected_feature_count": info["selected_feature_count"],
                                "selected": info["selected"],
                                "selected_matrix_digest": info["selected_matrix_digest"],
                            }
                            for tier, info in exported["summary"]["tiers"].items()
                        },
                    }
                )
            return 0
        if args.command == "serve":
            from .http_server import serve

            serve(workbench, host=args.host, port=args.port)
            return 0
        parser.error(f"Unknown command: {args.command}")
        return 2
    except WorkbenchError as exc:
        print(json.dumps(exc.to_dict(), ensure_ascii=False, indent=2), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

