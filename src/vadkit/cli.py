"""Command-line interface for vadkit."""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path
from typing import Any, Sequence

from .audio import load_dataset, read_wav
from .checkpoint import CheckpointStore
from .config import VADConfig
from .errors import ERROR_EXIT_CODES, VadError
from .injection import execute_injection
from .pipeline import evaluate_dataset, workspace_status
from .samplegen import DEFAULT_SEED, generate_dataset
from .stream import StreamingVAD, detect_samples
from .util import pretty_json


def _print_json(value: Any) -> None:
    sys.stdout.write(pretty_json(value))


def _add_config_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--frame-ms", type=float, default=None)
    parser.add_argument("--hop-ms", type=float, default=None)
    parser.add_argument("--energy-weight", type=float, default=None)
    parser.add_argument("--flatness-weight", type=float, default=None)
    parser.add_argument("--energy-floor-db", type=float, default=None)
    parser.add_argument("--energy-ceiling-db", type=float, default=None)
    parser.add_argument("--high-threshold", type=float, default=None)
    parser.add_argument("--low-threshold", type=float, default=None)
    parser.add_argument("--min-speech-ms", type=float, default=None)
    parser.add_argument("--min-silence-ms", type=float, default=None)


def _config_from_args(args: argparse.Namespace) -> VADConfig:
    fields = {
        "frame_ms": args.frame_ms,
        "hop_ms": args.hop_ms,
        "energy_weight": args.energy_weight,
        "flatness_weight": args.flatness_weight,
        "energy_floor_db": args.energy_floor_db,
        "energy_ceiling_db": args.energy_ceiling_db,
        "high_threshold": args.high_threshold,
        "low_threshold": args.low_threshold,
        "min_speech_ms": args.min_speech_ms,
        "min_silence_ms": args.min_silence_ms,
    }
    values = {key: value for key, value in fields.items() if value is not None}
    return VADConfig(**values).validate()


def _add_evaluate_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--workspace", required=True)
    parser.add_argument("--tolerance-ms", type=float, default=80.0)
    parser.add_argument("--stop-after-chunks", type=int, default=None)
    parser.add_argument("--crash-after-chunks", type=int, default=None)
    _add_config_arguments(parser)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="vadkit", description="Deterministic streaming voice activity detection"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    generate = subparsers.add_parser("generate", help="generate deterministic WAV fixtures")
    generate.add_argument("--out", default="data")
    generate.add_argument("--seed", type=int, default=DEFAULT_SEED)

    detect = subparsers.add_parser("detect", help="batch endpoint detection for one WAV")
    detect.add_argument("--wav", required=True)
    detect.add_argument("--chunk-id", default=None)
    detect.add_argument("--expected-sample-count", type=int, default=None)
    detect.add_argument("--inject-feature-dimensions", type=int, default=None)
    detect.add_argument("--no-reject-clipping", action="store_true")
    detect.add_argument("--include-frames", action="store_true")
    _add_config_arguments(detect)

    stream = subparsers.add_parser("stream", help="feed one WAV through a streaming session")
    stream.add_argument("--wav", required=True)
    stream.add_argument("--chunk-id", default=None)
    stream.add_argument("--chunk-size", type=int, default=333)
    stream.add_argument("--expected-sample-count", type=int, default=None)
    _add_config_arguments(stream)

    evaluate = subparsers.add_parser("evaluate", help="checkpointed dataset evaluation")
    _add_evaluate_arguments(evaluate)
    evaluate.add_argument("--resume", action="store_true")

    resume = subparsers.add_parser("resume", help="clear a pending stop request and resume")
    _add_evaluate_arguments(resume)

    status = subparsers.add_parser("status", help="show checkpoint and version state")
    status.add_argument("--manifest", required=True)
    status.add_argument("--workspace", required=True)
    _add_config_arguments(status)

    versions = subparsers.add_parser("versions", help="show per-chunk version fingerprints")
    versions.add_argument("--manifest", required=True)
    versions.add_argument("--workspace", required=True)
    _add_config_arguments(versions)

    stop = subparsers.add_parser("stop", help="write a graceful stop request")
    stop.add_argument("--workspace", required=True)

    inject = subparsers.add_parser("inject", help="execute one deterministic anomaly injection")
    inject.add_argument("--data", default="data")
    inject.add_argument(
        "--kind",
        required=True,
        choices=[
            "silent_input",
            "clipped_peak",
            "length_mismatch",
            "invalid_threshold",
            "feature_dimension",
        ],
    )
    _add_config_arguments(inject)

    demo = subparsers.add_parser("demo", help="run the complete deterministic demonstration")
    demo.add_argument("--data", default="data")
    demo.add_argument("--out", default="artifacts/demo")
    demo.add_argument("--tolerance-ms", type=float, default=80.0)
    _add_config_arguments(demo)

    config = subparsers.add_parser("config", help="print the active configuration fingerprint")
    _add_config_arguments(config)

    return parser


def _run_detect(args: argparse.Namespace) -> int:
    config = _config_from_args(args)
    chunk_id = args.chunk_id or Path(args.wav).stem
    samples, _ = read_wav(
        args.wav,
        chunk_id=chunk_id,
        expected_sample_count=args.expected_sample_count,
    )
    result = detect_samples(
        samples,
        config,
        chunk_id=chunk_id,
        expected_sample_count=args.expected_sample_count,
        inject_feature_dimensions=args.inject_feature_dimensions,
        reject_clipping=not args.no_reject_clipping,
    )
    _print_json(result.as_dict(include_frames=args.include_frames))
    return 0


def _run_stream(args: argparse.Namespace) -> int:
    config = _config_from_args(args)
    chunk_id = args.chunk_id or Path(args.wav).stem
    if args.chunk_size <= 0:
        raise VadError(
            "E_INVALID_CONFIG",
            "chunk_size must be positive",
            details={"chunk_size": args.chunk_size},
        )
    samples, _ = read_wav(args.wav, chunk_id=chunk_id)
    session = StreamingVAD(
        config,
        chunk_id=chunk_id,
        expected_sample_count=args.expected_sample_count,
    )
    publish_events: list[dict[str, Any]] = []
    offset = 0
    while offset < len(samples):
        before = len(session.published_endpoints)
        session.feed_samples(samples[offset : offset + args.chunk_size])
        offset += args.chunk_size
        newly_published = session.published_endpoints[before:]
        if newly_published:
            publish_events.append(
                {
                    "after_sample": min(offset, len(samples)),
                    "endpoints": [endpoint.as_dict() for endpoint in newly_published],
                }
            )
    tentative = list(session.tentative_endpoints)
    result = session.finalize()
    batch = detect_samples(samples, config, chunk_id=chunk_id)
    endpoints = [endpoint.as_dict() for endpoint in result.endpoints]
    batch_endpoints = [endpoint.as_dict() for endpoint in batch.endpoints]
    _print_json(
        {
            "chunk_id": chunk_id,
            "sample_count": result.sample_count,
            "chunk_size": args.chunk_size,
            "frame_count": result.frame_count,
            "truncated_frame_count": result.truncated_frame_count,
            "tentative_before_finalize": tentative,
            "publish_events": publish_events,
            "endpoints": endpoints,
            "batch_endpoints": batch_endpoints,
            "consistent_with_batch": endpoints == batch_endpoints,
        }
    )
    return 0


def _run_evaluate(args: argparse.Namespace) -> int:
    outcome = evaluate_dataset(
        args.manifest,
        args.workspace,
        config=_config_from_args(args),
        tolerance_ms=args.tolerance_ms,
        stop_after_chunks=args.stop_after_chunks,
        crash_after_chunks=args.crash_after_chunks,
        clear_stop=bool(getattr(args, "resume", False) or args.command == "resume"),
    )
    payload = {
        "status": outcome.status,
        "generation": outcome.generation,
        "result_path": str(outcome.result_path) if outcome.result_path else None,
        "result_sha256": outcome.result_sha256,
        "computed_chunks": list(outcome.computed_chunks),
        "cache_reused_chunks": list(outcome.cache_reused_chunks),
        "checkpoint_reused_chunks": list(outcome.checkpoint_reused_chunks),
    }
    if outcome.result is not None:
        payload["aggregate"] = outcome.result["aggregate"]
        payload["batch_stream_consistent"] = outcome.result["batch_stream_consistent"]
    _print_json(payload)
    return 0


def _run_status(args: argparse.Namespace) -> int:
    _print_json(
        workspace_status(
            args.manifest,
            args.workspace,
            config=_config_from_args(args),
        )
    )
    return 0


def _run_inject(args: argparse.Namespace) -> int:
    config = _config_from_args(args)
    try:
        execute_injection(args.kind, args.data, config=config)
    except VadError as exc:
        _print_json({"injection": args.kind, "raised": exc.as_dict(), "handled": True})
        return ERROR_EXIT_CODES[exc.code]
    _print_json({"injection": args.kind, "handled": False, "error": "fault was not raised"})
    return 1


def _resolved_under(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except ValueError:
        return False


def _reset_demo_workspace(path: Path, root: Path) -> None:
    if path.exists():
        if not _resolved_under(path, root):
            raise VadError(
                "E_INVALID_CONFIG",
                "refusing to reset a demo workspace outside the requested output directory",
                details={"workspace": str(path), "root": str(root)},
            )
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def _capture_injection(kind: str, data: str, config: VADConfig) -> dict[str, Any]:
    try:
        execute_injection(kind, data, config=config)
    except VadError as exc:
        return exc.as_dict()
    return {"code": None, "message": "fault not raised"}


def _run_demo(args: argparse.Namespace) -> int:
    config = _config_from_args(args)
    data = Path(args.data)
    output = Path(args.out)
    generated = generate_dataset(data)
    output.mkdir(parents=True, exist_ok=True)

    full_workspace = output / "workspace-full"
    resume_workspace = output / "workspace-stop-resume"
    _reset_demo_workspace(full_workspace, output)
    _reset_demo_workspace(resume_workspace, output)

    full = evaluate_dataset(
        data / "manifest.json",
        full_workspace,
        config=config,
        tolerance_ms=args.tolerance_ms,
    )
    if full.status != "complete" or full.result is None:
        raise VadError("E_INVALID_STAGE", "full demo evaluation did not complete")

    stopped = evaluate_dataset(
        data / "manifest.json",
        resume_workspace,
        config=config,
        tolerance_ms=args.tolerance_ms,
        stop_after_chunks=2,
    )
    status_after_stop = workspace_status(data / "manifest.json", resume_workspace, config=config)
    resumed = evaluate_dataset(
        data / "manifest.json",
        resume_workspace,
        config=config,
        tolerance_ms=args.tolerance_ms,
        clear_stop=True,
    )
    full_bytes = full.result_path.read_bytes() if full.result_path else b""
    resumed_bytes = resumed.result_path.read_bytes() if resumed.result_path else b""

    anomaly_results = {
        kind: _capture_injection(kind, str(data), config)
        for kind in [
            "silent_input",
            "clipped_peak",
            "length_mismatch",
            "invalid_threshold",
            "feature_dimension",
        ]
    }
    demo_summary = {
        "schema": "vadkit.demo-summary.v1",
        "generator": generated,
        "config": config.as_dict(),
        "tolerance_ms": args.tolerance_ms,
        "full_run": {
            "status": full.status,
            "result_path": str(full.result_path.relative_to(output)) if full.result_path else None,
            "result_sha256": full.result_sha256,
            "batch_and_stream_consistent": full.result["batch_stream_consistent"],
            "aggregate": full.result["aggregate"],
        },
        "graceful_stop_resume": {
            "stopped_status": stopped.status,
            "checkpoint_count_after_stop": status_after_stop["checkpoint_count"],
            "stop_requested_after_stop": status_after_stop["stop_requested"],
            "resumed_status": resumed.status,
            "resumed_result_sha256": resumed.result_sha256,
            "byte_identical_to_full_run": full_bytes == resumed_bytes,
            "checkpoint_reused_on_resume": list(resumed.checkpoint_reused_chunks),
        },
        "anomaly_injections": anomaly_results,
        "all_expected_error_codes_observed": {
            "E_SILENT_INPUT": anomaly_results["silent_input"].get("code") == "E_SILENT_INPUT",
            "E_FEATURE_MISMATCH_CLIPPED": anomaly_results["clipped_peak"].get("code")
            == "E_FEATURE_MISMATCH",
            "E_INPUT_LENGTH": anomaly_results["length_mismatch"].get("code")
            == "E_INPUT_LENGTH",
            "E_INVALID_CONFIG": anomaly_results["invalid_threshold"].get("code")
            == "E_INVALID_CONFIG",
            "E_FEATURE_MISMATCH_DIMENSION": anomaly_results["feature_dimension"].get("code")
            == "E_FEATURE_MISMATCH",
        },
    }
    demo_path = output / "demo_summary.json"
    from .util import atomic_write_json

    atomic_write_json(demo_path, demo_summary, pretty=True)
    _print_json(
        {
            "demo_summary": str(demo_path),
            "full_result_sha256": full.result_sha256,
            "resume_byte_identical": demo_summary["graceful_stop_resume"][
                "byte_identical_to_full_run"
            ],
            "all_expected_error_codes_observed": all(
                demo_summary["all_expected_error_codes_observed"].values()
            ),
            "batch_and_stream_consistent": full.result["batch_stream_consistent"],
        }
    )
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "generate":
            _print_json(generate_dataset(args.out, seed=args.seed))
            return 0
        if args.command == "detect":
            return _run_detect(args)
        if args.command == "stream":
            return _run_stream(args)
        if args.command in {"evaluate", "resume"}:
            return _run_evaluate(args)
        if args.command in {"status", "versions"}:
            return _run_status(args)
        if args.command == "stop":
            store = CheckpointStore(args.workspace)
            store.request_stop()
            _print_json({"workspace": args.workspace, "stop_requested": True})
            return 0
        if args.command == "inject":
            return _run_inject(args)
        if args.command == "demo":
            return _run_demo(args)
        if args.command == "config":
            _print_json(_config_from_args(args).as_dict())
            return 0
        parser.error(f"unknown command: {args.command}")
    except VadError as exc:
        sys.stderr.write(pretty_json(exc.as_dict()))
        return ERROR_EXIT_CODES[exc.code]
    return 1


if __name__ == "__main__":
    raise SystemExit(main())

