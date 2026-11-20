"""Command-line interface and deterministic end-to-end demonstration."""

from __future__ import annotations

import argparse
import hashlib
import json
import multiprocessing
import shutil
import sys
from pathlib import Path
from typing import Any

from .checkpoints import CheckpointStore
from .config import EngineConfig
from .engine import DialogueEngine
from .errors import E_INPUT_LENGTH, E_INVALID_TURN, EngineError
from .generator import generate_demo_dataset
from .manifest import load_dataset
from .serialization import atomic_write_json, digest_json, read_json


def _config_from_path(path: str | None) -> EngineConfig:
    if not path:
        return EngineConfig()
    value = read_json(path)
    if not isinstance(value, dict):
        raise ValueError("config JSON root must be an object")
    return EngineConfig.from_dict(value)


def _print(value: Any) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True))


def _digest_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _publish_probe_worker(
    workspace: str, input_fingerprint: str, artifact_digest: str, queue: Any
) -> None:
    try:
        store = CheckpointStore(workspace)
        record = store.publish_checkpoint(
            shard_id="probe_shard",
            turn_index=0,
            stage="features",
            work_order=0,
            shard_order=0,
            input_fingerprint=input_fingerprint,
            config_version="probe-config",
            artifact_digest=artifact_digest,
            artifact_summary={"feature_dim": 2},
            shard_complete=False,
            expected_sequence=1,
        )
        queue.put({"accepted": True, "sequence": record["sequence"]})
    except EngineError as exc:
        queue.put(
            {
                "accepted": False,
                "code": exc.code,
                "kind": exc.details.get("kind"),
            }
        )
    except Exception as exc:  # pragma: no cover - reported through demo output
        queue.put({"accepted": False, "error": repr(exc)})


def _concurrency_probe(root: Path) -> dict[str, Any]:
    workspace = root / "concurrency_probe"
    store = CheckpointStore(workspace)
    fingerprint = digest_json({"probe": "concurrent-publication"})
    artifact = {"probe": "concurrent-publication", "vector": [1.0, 2.0]}
    artifact_digest, _ = store.write_artifact(
        "probe_shard", 0, "features", artifact, input_fingerprint=fingerprint
    )
    context = multiprocessing.get_context("spawn")
    queue = context.Queue()
    workers = [
        context.Process(
            target=_publish_probe_worker,
            args=(str(workspace), fingerprint, artifact_digest, queue),
        )
        for _ in range(2)
    ]
    for worker in workers:
        worker.start()
    results = [queue.get(timeout=20) for _ in workers]
    for worker in workers:
        worker.join(timeout=20)
    accepted = [item for item in results if item.get("accepted")]
    rejected = [item for item in results if not item.get("accepted")]
    return {
        "results": results,
        "accepted_count": len(accepted),
        "rejected_count": len(rejected),
        "rejected_code": rejected[0].get("code") if rejected else None,
        "passed": len(accepted) == 1
        and len(rejected) == 1
        and rejected[0].get("code") == "E_STALE_CHECKPOINT",
    }


def _clone_dataset(source: Path, destination: Path) -> Path:
    shutil.copytree(source, destination)
    return destination / "manifest.json"


def _error_probe(
    root: Path,
    name: str,
    source_data: Path,
    mutate: Any,
) -> dict[str, Any]:
    data_dir = root / f"error_{name}"
    manifest = _clone_dataset(source_data, data_dir)
    value = read_json(manifest)
    mutate(value)
    atomic_write_json(manifest, value)
    engine = DialogueEngine(root / f"error_{name}_workspace")
    try:
        engine.run(data_dir)
        return {"expected_error": True, "code": None, "passed": False}
    except EngineError as exc:
        return {
            "connection_ok": True,
            "code": exc.code,
            "message": exc.message,
            "shard_id": exc.shard_id,
            "turn_index": exc.turn_index,
            "stage": exc.stage,
            "passed": True,
        }


def _run_demo(workspace: Path, *, force: bool) -> dict[str, Any]:
    if workspace.exists() and any(workspace.iterdir()):
        if not force:
            raise FileExistsError(
                f"demo workspace is not empty: {workspace}; pass --force to replace"
            )
        shutil.rmtree(workspace)
    workspace.mkdir(parents=True, exist_ok=True)
    data_dir = workspace / "demo_data"
    generated = generate_demo_dataset(data_dir, force=True)

    fresh_ws = workspace / "fresh"
    full_engine = DialogueEngine(fresh_ws)
    full = full_engine.run(data_dir)
    full_summary_path = fresh_ws / "exports" / "summary.json"

    resume_ws = workspace / "resume"
    resume_engine = DialogueEngine(resume_ws)
    stopped = resume_engine.run(data_dir, stop_after_turns=2)
    interrupted_error = None
    try:
        resume_engine.query_turn("shard_01", 3)
    except EngineError as exc:
        interrupted_error = {"code": exc.code, "stage": exc.stage}
    resumed = resume_engine.resume(data_dir)
    resumed_summary_path = resume_ws / "exports" / "summary.json"

    fresh_artifacts = {
        str(path.relative_to(fresh_ws / "checkpoints" / "artifacts")): _digest_file(path)
        for path in (fresh_ws / "checkpoints" / "artifacts").rglob("*.json")
    }
    resumed_artifacts = {
        str(path.relative_to(resume_ws / "checkpoints" / "artifacts")): _digest_file(path)
        for path in (resume_ws / "checkpoints" / "artifacts").rglob("*.json")
    }
    fresh_journal = {
        path.name: _digest_file(path)
        for path in (fresh_ws / "checkpoints" / "journal").glob("*.json")
    }
    resumed_journal = {
        path.name: _digest_file(path)
        for path in (resume_ws / "checkpoints" / "journal").glob("*.json")
    }

    def bad_length(manifest: dict[str, Any]) -> None:
        manifest["shards"][0]["sample_count"] += 1

    def bad_turn(manifest: dict[str, Any]) -> None:
        first = manifest["shards"][0]["turns"][0]
        second = manifest["shards"][0]["turns"][1]
        second["start_ms"] = first["end_ms"] - 1

    error_probes = {
        "declared_length": _error_probe(
            workspace, "declared_length", data_dir, bad_length
        ),
        "invalid_turn_boundary": _error_probe(
            workspace, "invalid_turn_boundary", data_dir, bad_turn
        ),
    }
    concurrency = _concurrency_probe(workspace)

    report = {
        "demo": "dialogue-voice-emotion-understanding-v1",
        "workspace": str(workspace),
        "generated_data": generated,
        "full_run": {
            "status": full["status"],
            "sequence": full["sequence"],
            "summary_digest": full["summary_digest"],
            "emotion_accuracy": full["metrics"]["emotion_accuracy"],
            "behavior_accuracy": full["metrics"]["behavior_accuracy"],
            "interruption_recall": full["metrics"]["interruption_recall"],
            "state_trace_deviation": full["metrics"]["state_trace_deviation"],
        },
        "stop_resume": {
            "stopped": {
                "status": stopped["status"],
                "reason": stopped["reason"],
                "sequence": stopped["sequence"],
                "completed_state_count": stopped["completed_state_count"],
            },
            "missing_stage_probe": interrupted_error,
            "resumed": {
                "status": resumed["status"],
                "sequence": resumed["sequence"],
                "reused_stages": resumed["reused_stages"],
                "computed_stages": resumed["computed_stages"],
                "summary_digest": resumed["summary_digest"],
            },
            "summary_bytes_identical": full_summary_path.read_bytes()
            == resumed_summary_path.read_bytes(),
            "artifact_bytes_identical": fresh_artifacts == resumed_artifacts,
            "journal_bytes_identical": fresh_journal == resumed_journal,
        },
        "error_probes": error_probes,
        "concurrency_probe": concurrency,
    }
    report["all_checks_passed"] = bool(
        full["status"] == "completed"
        and resumed["status"] == "completed"
        and report["stop_resume"]["summary_bytes_identical"]
        and report["stop_resume"]["artifact_bytes_identical"]
        and report["stop_resume"]["journal_bytes_identical"]
        and error_probes["declared_length"]["code"] == E_INPUT_LENGTH
        and error_probes["invalid_turn_boundary"]["code"] == E_INVALID_TURN
        and concurrency["passed"]
    )
    atomic_write_json(workspace / "demo_report.json", report)
    return report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m dialogue_engine",
        description="Deterministic local dialogue voice emotion and understanding engine",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    generate = subparsers.add_parser("generate", help="generate deterministic demo data")
    generate.add_argument("--out", required=True)
    generate.add_argument("--seed", type=int, default=20_261_121)
    generate.add_argument("--force", action="store_true")

    run = subparsers.add_parser("run", help="import data and process every turn")
    run.add_argument("--workspace", required=True)
    run.add_argument("--data", required=True)
    run.add_argument("--config")
    run.add_argument("--stop-after-turns", type=int)

    resume = subparsers.add_parser("resume", help="clear a stop request and resume")
    resume.add_argument("--workspace", required=True)
    resume.add_argument("--data", required=True)
    resume.add_argument("--config")

    status = subparsers.add_parser("status", help="query checkpoint and version status")
    status.add_argument("--workspace", required=True)
    status.add_argument("--data")
    status.add_argument("--config")

    query_turn = subparsers.add_parser("query-turn", help="query all four stage results")
    query_turn.add_argument("--workspace", required=True)
    query_turn.add_argument("--data", required=True)
    query_turn.add_argument("--shard", required=True)
    query_turn.add_argument("--turn", type=int, required=True)
    query_turn.add_argument("--config")

    query_stage = subparsers.add_parser("query-stage", help="query one stage artifact")
    query_stage.add_argument("--workspace", required=True)
    query_stage.add_argument("--data", required=True)
    query_stage.add_argument("--shard", required=True)
    query_stage.add_argument("--turn", type=int, required=True)
    query_stage.add_argument("--stage", required=True)
    query_stage.add_argument("--config")

    fingerprints = subparsers.add_parser(
        "fingerprints", help="query per-stage expected and published fingerprints"
    )
    fingerprints.add_argument("--workspace", required=True)
    fingerprints.add_argument("--data", required=True)
    fingerprints.add_argument("--shard", required=True)
    fingerprints.add_argument("--turn", type=int, required=True)
    fingerprints.add_argument("--config")

    export = subparsers.add_parser("export", help="export the deterministic summary")
    export.add_argument("--workspace", required=True)
    export.add_argument("--data", required=True)
    export.add_argument("--config")
    export.add_argument("--output")

    stop = subparsers.add_parser("request-stop", help="write a graceful stop request")
    stop.add_argument("--workspace", required=True)
    stop.add_argument("--reason", default="cli stop request")

    validate = subparsers.add_parser("validate-data", help="validate WAV/manifest consistency")
    validate.add_argument("--data", required=True)

    demo = subparsers.add_parser(
        "demo", help="run full, stop/resume, error-code, and concurrency demonstration"
    )
    demo.add_argument("--workspace", default="demo_runtime")
    demo.add_argument("--force", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "generate":
            _print(
                generate_demo_dataset(
                    args.out, seed=args.seed, force=args.force
                )
            )
            return 0
        if args.command == "validate-data":
            dataset = load_dataset(args.data)
            _print(
                {
                    "valid": True,
                    "manifest": str(dataset.manifest_path),
                    "shard_count": len(dataset.shards),
                    "turn_count": sum(shard.turn_count for shard in dataset.shards),
                    "shards": [
                        {
                            "shard_id": shard.shard_id,
                            "sample_count": shard.sample_count,
                            "turn_count": shard.turn_count,
                            "samples_digest": shard.samples_digest,
                        }
                        for shard in dataset.shards
                    ],
                }
            )
            return 0
        if args.command == "request-stop":
            engine = DialogueEngine(args.workspace)
            engine.request_stop(args.reason)
            _print({"status": "stop_requested", "workspace": args.workspace})
            return 0
        if args.command == "demo":
            report = _run_demo(Path(args.workspace).expanduser().resolve(), force=args.force)
            _print(report)
            return 0 if report["all_checks_passed"] else 1

        config = _config_from_path(getattr(args, "config", None))
        engine = DialogueEngine(args.workspace, config=config)
        if args.command == "run":
            _print(
                engine.run(args.data, stop_after_turns=args.stop_after_turns)
            )
            return 0
        if args.command == "resume":
            _print(engine.resume(args.data))
            return 0
        if args.command == "status":
            if args.data:
                engine.import_dataset(args.data)
            _print(engine.status())
            return 0
        if args.command == "query-turn":
            engine.import_dataset(args.data)
            _print(engine.query_turn(args.shard, args.turn))
            return 0
        if args.command == "query-stage":
            engine.import_dataset(args.data)
            _print(engine.query_stage(args.shard, args.turn, args.stage))
            return 0
        if args.command == "fingerprints":
            engine.import_dataset(args.data)
            _print(engine.version_fingerprints(args.shard, args.turn))
            return 0
        if args.command == "export":
            engine.import_dataset(args.data)
            _print(engine.export_dialogue_summary(args.output))
            return 0
        parser.error(f"unsupported command {args.command}")
        return 2
    except EngineError as exc:
        print(json.dumps(exc.to_dict(), ensure_ascii=False, sort_keys=True), file=sys.stderr)
        return 2
    except (OSError, ValueError, FileExistsError) as exc:
        print(
            json.dumps(
                {"code": "CLI_ERROR", "message": str(exc)},
                ensure_ascii=False,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

