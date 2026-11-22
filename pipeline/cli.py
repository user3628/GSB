from __future__ import annotations

import argparse
import json
import signal
import sys
import threading
import time
from pathlib import Path
from typing import Any

from .batch import build_cache_plan
from .checkpoints import STAGE_INDEX, CheckpointManager
from .config import load as load_config
from .data import load_dataset
from .errors import PipelineError
from .environment import environment_report
from .generate import generate_demo, verify_demo
from .incremental import compare_workspaces, replace_batch
from .runner import run_pipeline
from .util import atomic_write_json, sha256_file


def _print(value: Any) -> None:
    print(json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2))


def _run(args: argparse.Namespace) -> dict[str, Any]:
    stop_event = threading.Event()
    watcher_stop = threading.Event()

    def handler(_signum: int, _frame: Any) -> None:
        stop_event.set()

    previous_int = signal.signal(signal.SIGINT, handler)
    previous_term = signal.signal(signal.SIGTERM, handler)
    watcher: threading.Thread | None = None
    if args.stop_file:
        stop_path = Path(args.stop_file)

        def watch() -> None:
            while not watcher_stop.is_set():
                if stop_path.exists():
                    stop_event.set()
                    return
                time.sleep(0.05)

        watcher = threading.Thread(target=watch, name="stop-file-watcher", daemon=True)
        watcher.start()
    try:
        return run_pipeline(
            workspace=args.workspace,
            config_path=args.config,
            dataset_index_path=args.data,
            stop_event=stop_event,
            stop_after_batches=args.stop_after_batches,
            execution_workers=args.execution_workers,
        )
    finally:
        watcher_stop.set()
        if watcher is not None:
            watcher.join(timeout=1.0)
        signal.signal(signal.SIGINT, previous_int)
        signal.signal(signal.SIGTERM, previous_term)


def _checkpoint_publish(args: argparse.Namespace) -> dict[str, Any]:
    workspace = Path(args.workspace).resolve()
    artifacts: list[dict[str, Any]] = []
    for relative in args.artifact:
        path = workspace / relative
        if not path.is_file():
            raise PipelineError("E_MISSING_ARTIFACT", "artifact does not exist", {"path": relative})
        artifacts.append(
            {
                "path": relative,
                "digest": sha256_file(path),
                "summary": {"size_bytes": path.stat().st_size},
            }
        )
    manager = CheckpointManager(workspace)
    return manager.publish(
        batch_id=args.batch_id,
        stage=args.stage,
        input_fingerprint=args.input_fingerprint,
        config_version=args.config_version,
        artifacts=artifacts,
        expected_sequence=args.expected_sequence,
    )


def _checkpoint_invalidate(args: argparse.Namespace) -> dict[str, Any]:
    manager = CheckpointManager(args.workspace)
    return manager.invalidate(
        batch_id=args.batch_id,
        earliest_stage=args.stage,
        reason=args.reason,
        old_fingerprint=args.old_fingerprint,
        new_fingerprint=args.new_fingerprint,
        config_version=args.config_version or "",
    )


def _detect_changes(args: argparse.Namespace) -> dict[str, Any]:
    workspace = Path(args.workspace).resolve()
    config = load_config(args.config)
    dataset = load_dataset(args.data)
    manager = CheckpointManager(workspace)
    state = manager.state()
    batches: list[dict[str, Any]] = []
    for batch in dataset.batches:
        plan = build_cache_plan(batch, config, workspace, state)
        batches.append(
            {
                "batch_id": batch.batch_id,
                "current_next_stage_index": plan.current_next_index,
                "earliest_affected_stage_index": plan.earliest_index,
                "earliest_affected_stage": (
                    ["features", "train_score", "calibration", "summarize", None][plan.earliest_index]
                ),
                "requires_invalidation": plan.requires_invalidation,
                "fully_cached": plan.fully_cached,
            }
        )
    return {
        "status": "ok",
        "config_version": config.version,
        "config_digest": config.digest,
        "batches": batches,
    }


def _replace_batch(args: argparse.Namespace) -> dict[str, Any]:
    return replace_batch(
        workspace=args.workspace,
        config_path=args.config,
        dataset_index_path=args.data,
        batch_id=args.batch_id,
        source_path=args.source,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="batch-text-pipeline")
    subparsers = parser.add_subparsers(dest="command", required=True)

    generate = subparsers.add_parser("generate-data")
    generate.add_argument("--out", default="data")
    generate.add_argument("--seed", type=int, default=20261123)

    verify = subparsers.add_parser("verify-data")
    verify.add_argument("--index", default="data/index.json")
    verify.add_argument("--output")

    run = subparsers.add_parser("run")
    run.add_argument("--workspace", required=True)
    run.add_argument("--config", default="data/config.yaml")
    run.add_argument("--data", default="data/index.json")
    run.add_argument("--stop-after-batches", type=int)
    run.add_argument("--stop-file")
    run.add_argument("--execution-workers", type=int)

    status = subparsers.add_parser("checkpoint-status")
    status.add_argument("--workspace", required=True)

    publish = subparsers.add_parser("checkpoint-publish")
    publish.add_argument("--workspace", required=True)
    publish.add_argument("--batch-id", required=True)
    publish.add_argument("--stage", required=True, choices=list(STAGE_INDEX))
    publish.add_argument("--artifact", action="append", required=True)
    publish.add_argument("--input-fingerprint", required=True)
    publish.add_argument("--config-version", required=True)
    publish.add_argument("--expected-sequence", type=int)

    invalidate = subparsers.add_parser("checkpoint-invalidate")
    invalidate.add_argument("--workspace", required=True)
    invalidate.add_argument("--batch-id", required=True)
    invalidate.add_argument("--stage", required=True, choices=list(STAGE_INDEX))
    invalidate.add_argument("--reason", required=True)
    invalidate.add_argument("--old-fingerprint")
    invalidate.add_argument("--new-fingerprint")
    invalidate.add_argument("--config-version")

    compare = subparsers.add_parser("compare")
    compare.add_argument("--left", required=True)
    compare.add_argument("--right", required=True)
    compare.add_argument("--fallback-batch-id")

    detect = subparsers.add_parser("detect-changes")
    detect.add_argument("--workspace", required=True)
    detect.add_argument("--config", default="data/config.yaml")
    detect.add_argument("--data", default="data/index.json")

    replace = subparsers.add_parser("replace-batch")
    replace.add_argument("--workspace", required=True)
    replace.add_argument("--config", default="data/config.yaml")
    replace.add_argument("--data", default="data/index.json")
    replace.add_argument("--batch-id", required=True)
    replace.add_argument("--source", required=True)

    verify_run = subparsers.add_parser("verify-run")
    verify_run.add_argument("--workspace", required=True)

    doctor = subparsers.add_parser("doctor")
    doctor.add_argument("--workspace")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "generate-data":
            _print(generate_demo(args.out, args.seed))
        elif args.command == "verify-data":
            result = verify_demo(args.index)
            if args.output:
                atomic_write_json(args.output, result)
            _print(result)
            return 0 if result["passed"] else 1
        elif args.command == "run":
            _print(_run(args))
        elif args.command == "checkpoint-status":
            manager = CheckpointManager(args.workspace)
            _print({"state": manager.state(), "integrity": manager.verify_integrity()})
        elif args.command == "checkpoint-publish":
            _print(_checkpoint_publish(args))
        elif args.command == "checkpoint-invalidate":
            _print(_checkpoint_invalidate(args))
        elif args.command == "compare":
            result = compare_workspaces(args.left, args.right, args.fallback_batch_id)
            _print(result)
            return 0 if result["status"] == "ok" else 1
        elif args.command == "detect-changes":
            _print(_detect_changes(args))
        elif args.command == "replace-batch":
            _print(_replace_batch(args))
        elif args.command == "verify-run":
            manager = CheckpointManager(args.workspace)
            workspace = Path(args.workspace)
            report_path = workspace / "report.json"
            report = json.loads(report_path.read_text(encoding="utf-8")) if report_path.is_file() else None
            _print(
                {
                    "status": "ok" if report else "missing_report",
                    "checkpoint_integrity": manager.verify_integrity(),
                    "report": report,
                }
            )
            return 0 if report else 1
        elif args.command == "doctor":
            result = environment_report(args.workspace)
            _print(result)
            return 0 if result["status"] == "ok" else 1
        return 0
    except PipelineError as exc:
        _print(exc.as_dict())
        return 2
    except Exception as exc:
        _print({"status": "error", "error_code": "E_UNHANDLED", "message": str(exc)})
        return 3


if __name__ == "__main__":
    sys.exit(main())
