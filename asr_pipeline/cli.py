"""Command line interface for the deterministic ASR pipeline."""

import argparse
import json
import os
import sys

from . import __version__
from .checkpoint import CheckpointStore
from .config import (
    AlignConfig,
    BudgetConfig,
    DecoderConfig,
    FrameConfig,
    PipelineConfig,
)
from .errors import InvalidFrameError, PipelineError
from .hashing import canonical_json
from .manifest import load_manifest
from .pipeline import Pipeline, clear_stop, request_stop
from .samplegen import generate_healthy, generate_injections


def _print(obj):
    print(canonical_json(obj))


def _frame_config(manifest, args):
    frame_ms = args.frame_ms if args.frame_ms is not None else manifest["frame_ms"]
    hop_ms = args.hop_ms if args.hop_ms is not None else manifest["hop_ms"]
    bands = args.bands if args.bands is not None else manifest["bands"]
    sample_rate = args.sample_rate if args.sample_rate is not None else manifest["sample_rate"]
    frame = FrameConfig(frame_ms=frame_ms, hop_ms=hop_ms, sample_rate=sample_rate, bands=bands)
    frame.validate()
    return frame


def _build_config(manifest, args):
    frame = _frame_config(manifest, args)
    decoder = DecoderConfig(
        min_silence_frames=args.min_silence_frames,
        min_word_frames=args.min_word_frames,
    )
    align = AlignConfig(tolerance_ms=args.tolerance_ms)
    budget = BudgetConfig(
        global_frame_budget=args.global_budget,
        worker_frame_quota=args.worker_quota,
        shard_frame_latency=args.shard_latency,
        workers=args.workers,
    )
    return PipelineConfig(frame=frame, decoder=decoder, align=align, budget=budget)


def _resolve_dirs(args):
    data_dir = os.path.abspath(args.data_dir)
    workspace = os.path.abspath(args.workspace)
    return data_dir, workspace


def cmd_generate(args):
    frame = FrameConfig()
    frame.validate()
    healthy_path, healthy_manifest = generate_healthy(args.data_dir, frame)
    injection_path, injection_manifest = generate_injections(
        os.path.join(args.data_dir, "injections"), frame
    )
    _print(
        {
            "manifest": healthy_path,
            "injection_manifest": injection_path,
            "shard_count": len(healthy_manifest["shards"]),
            "seed": 20261123,
        }
    )


def _load_and_build(args):
    data_dir, workspace = _resolve_dirs(args)
    manifest = load_manifest(os.path.join(data_dir, "manifest.json"))
    config = _build_config(manifest, args)
    return data_dir, workspace, manifest, config


def cmd_run(args):
    data_dir, workspace, manifest, config = _load_and_build(args)
    clear_stop(workspace)
    pipeline = Pipeline(config, manifest, data_dir, workspace)
    summary = pipeline.run()
    _print(summary)


def cmd_stage(args):
    data_dir, workspace, manifest, config = _load_and_build(args)
    pipeline = Pipeline(config, manifest, data_dir, workspace)
    plan, _ = pipeline.plan_admission()
    stages = args.stages.split(",") if args.stages else ["frontend", "decode", "align", "summary"]
    output = {"requested_stages": stages, "admission": plan}
    if "summary" in stages:
        summary = pipeline.run()
        output["summary"] = summary
    else:
        from .manifest import ordered_shards

        enabled = [stage for stage in stages if stage != "summary"]
        partial = []
        for item in plan:
            if not item["admitted"]:
                continue
            if pipeline.stop_requested():
                output["stopped_before"] = item["shard_id"]
                break
            entry = next(
                shard for shard in ordered_shards(manifest)
                if shard["shard_id"] == item["shard_id"]
            )
            result = pipeline.process_shard_stages(
                entry, os.path.join(data_dir, entry["wav_file"]), enabled
            )
            partial.append({"shard_id": result["shard_id"], "stages": result["stages"]})
        output["completed"] = partial
    _print(output)


def cmd_status(args):
    _, workspace = _resolve_dirs(args)
    store = CheckpointStore(workspace)
    state = store.stage_status()
    state["summary"] = store.read_summary()
    state["stop_requested"] = os.path.exists(
        os.path.join(workspace, "STOP_REQUESTED")
    )
    state["version"] = __version__
    _print(state)


def cmd_stop(args):
    _, workspace = _resolve_dirs(args)
    path = request_stop(workspace)
    _print({"stop_file": path, "status": "stop_requested"})


def cmd_resume(args):
    data_dir, workspace, manifest, config = _load_and_build(args)
    pipeline = Pipeline(config, manifest, data_dir, workspace)
    recovery = pipeline.store.recover()
    summary = pipeline.run()
    _print({"recovery": recovery, "summary": summary})


def cmd_fingerprint(args):
    data_dir, workspace, manifest, config = _load_and_build(args)
    store = CheckpointStore(workspace)
    state = store.stage_status()
    _print(
        {
            "config_fingerprint": config.fingerprint(),
            "manifest_fingerprint": __import__("asr_pipeline.hashing", fromlist=["digest_json"]).digest_json(manifest),
            "stages": state["shards"],
            "ignored": state["ignored"],
        }
    )


def cmd_inject(args):
    data_dir, workspace, manifest, config = _load_and_build(args)
    if args.kind == "bad_frame":
        raise InvalidFrameError(
            "injected illegal frame parameters",
            {"frame_ms": config.frame.frame_ms, "hop_ms": config.frame.hop_ms},
        )
    from .samplegen import generate_single_injection

    single_path = generate_single_injection(data_dir, args.kind, config.frame)
    manifest = load_manifest(single_path)
    config = _build_config(manifest, args)
    pipeline = Pipeline(config, manifest, data_dir, workspace)
    summary = pipeline.run()
    if summary["unprocessed"]:
        error = summary["unprocessed"][0]["error"]
        from .errors import PipelineError

        raise PipelineError(error["message"], error)
    _print(summary)


def cmd_serve(args):
    from .server import serve

    serve(host=args.host, port=args.port)


def build_parser():
    parser = argparse.ArgumentParser(prog="asr-pipeline")
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--workspace", default="workspace")
    parser.add_argument("--frame-ms", type=float)
    parser.add_argument("--hop-ms", type=float)
    parser.add_argument("--bands", type=int)
    parser.add_argument("--sample-rate", type=int)
    parser.add_argument("--tolerance-ms", type=float, default=40.0)
    parser.add_argument("--min-silence-frames", type=int, default=5)
    parser.add_argument("--min-word-frames", type=int, default=3)
    parser.add_argument("--global-budget", type=int, default=100000)
    parser.add_argument("--worker-quota", type=int, default=60000)
    parser.add_argument("--shard-latency", type=int, default=20000)
    parser.add_argument("--workers", type=int, default=2)
    sub = parser.add_subparsers(dest="command", required=True)

    gen = sub.add_parser("generate")
    gen.set_defaults(func=cmd_generate)

    run = sub.add_parser("run")
    run.set_defaults(func=cmd_run)

    stage = sub.add_parser("stage")
    stage.add_argument("--stages")
    stage.set_defaults(func=cmd_stage)

    status = sub.add_parser("status")
    status.set_defaults(func=cmd_status)

    stop = sub.add_parser("stop")
    stop.set_defaults(func=cmd_stop)

    resume = sub.add_parser("resume")
    resume.set_defaults(func=cmd_resume)

    fingerprint = sub.add_parser("fingerprint")
    fingerprint.set_defaults(func=cmd_fingerprint)

    inject = sub.add_parser("inject")
    inject.add_argument(
        "--kind",
        choices=(
            "length_mismatch",
            "rate_conflict",
            "silent",
            "clipped",
            "bad_frame",
        ),
        required=True,
    )
    inject.set_defaults(func=cmd_inject)

    serve_parser = sub.add_parser("serve")
    serve_parser.add_argument("--host", default="127.0.0.1")
    serve_parser.add_argument("--port", type=int, default=8080)
    serve_parser.set_defaults(func=cmd_serve)
    return parser


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        args.func(args)
        return 0
    except PipelineError as exc:
        payload = exc.to_dict()
        context_code = payload.get("context", {}).get("code")
        if context_code:
            payload["code"] = context_code
        print(canonical_json(payload), file=sys.stderr)
        return 2
    except FileNotFoundError as exc:
        print(
            canonical_json({"code": "E_MANIFEST", "message": str(exc)}),
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
