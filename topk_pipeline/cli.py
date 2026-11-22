"""Command line entry points for generation, execution and inspection."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from .checkpoints import CheckpointStore
from .demo_data import generate_demo_data
from .manifest import load_manifest
from .runner import export_summary, run_manifest


def _print_json(value: Any) -> None:
    print(json.dumps(value, sort_keys=True, indent=2, ensure_ascii=False))


def _cmd_generate(args: argparse.Namespace) -> int:
    _print_json(generate_demo_data(args.out))
    return 0


def _cmd_run(args: argparse.Namespace) -> int:
    workspace = Path(args.workspace).resolve()
    summary_path = Path(args.summary).resolve() if args.summary else workspace / "summary.json"
    result = run_manifest(
        args.manifest,
        workspace,
        stop_file=args.stop_file,
        stop_after_completed=args.stop_after_shards,
        reset_stop_file=args.reset_stop,
        workers=args.workers,
    )
    if result["stopped"]:
        status_path = workspace / "stop_status.json"
        export_summary(
            status_path,
            {
                "status": "stopped",
                "stop_file": result["stop_file"],
                "stats": result["summary"]["stats"],
                "shards": result["summary"]["shards"],
            },
        )
        print(
            json.dumps(
                {
                    "status": "stopped",
                    "stop_status": str(status_path),
                    "completed_shards": result["summary"]["stats"]["successful_shards"],
                },
                sort_keys=True,
            )
        )
        return 130

    export_summary(summary_path, result["summary"])
    stats = result["summary"]["stats"]
    print(
        json.dumps(
            {
                "status": "complete",
                "summary": str(summary_path),
                "output_sha256": result["summary"]["output_sha256"],
                "successful_shards": stats["successful_shards"],
                "error_shards": stats["error_shards"],
                "unprocessed_shards": stats["unprocessed_shards"],
                "elements_evaluated": stats["elements_evaluated"],
            },
            sort_keys=True,
        )
    )
    return 0


def _cmd_inspect(args: argparse.Namespace) -> int:
    manifest = load_manifest(args.manifest)
    store = CheckpointStore(args.workspace)
    shards = []
    for spec in manifest["shards"]:
        try:
            state = store.load_state(spec["shard_id"])
            shards.append(
                {
                    "shard_id": spec["shard_id"],
                    "last_sequence": state["last_sequence"],
                    "records": [
                        {
                            "sequence": record["sequence"],
                            "stage": record["stage"],
                            "input_fingerprint": record["input_fingerprint"],
                            "artifact_digest": record["artifact_digest"],
                        }
                        for record in state["records"]
                    ],
                }
            )
        except Exception as exc:  # inspection should continue across shards
            shards.append({"shard_id": spec["shard_id"], "error": f"{type(exc).__name__}: {exc}"})
    _print_json({"workspace": str(Path(args.workspace).resolve()), "shards": shards})
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m topk_pipeline")
    subparsers = parser.add_subparsers(dest="command", required=True)

    generate = subparsers.add_parser("generate", help="generate deterministic local samples")
    generate.add_argument("--out", default="data")
    generate.set_defaults(handler=_cmd_generate)

    run = subparsers.add_parser("run", help="run or resume a manifest")
    run.add_argument("--manifest", required=True)
    run.add_argument("--workspace", required=True)
    run.add_argument("--summary")
    run.add_argument("--workers", type=int)
    run.add_argument("--stop-file")
    run.add_argument("--stop-after-shards", type=int)
    run.add_argument("--reset-stop", action="store_true")
    run.set_defaults(handler=_cmd_run)

    inspect = subparsers.add_parser("inspect", help="show checkpoint sequence and stages")
    inspect.add_argument("--manifest", required=True)
    inspect.add_argument("--workspace", required=True)
    inspect.set_defaults(handler=_cmd_inspect)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.handler(args))


if __name__ == "__main__":
    raise SystemExit(main())
