from __future__ import annotations

import argparse
import json
import shutil
import signal
import sys
import traceback
from pathlib import Path
from typing import Any

from .checkpoints import request_stop
from .config import list_versions, state_root
from .errors import (
    Interrupted,
    InvalidParam,
    QPipeError,
    ToleranceExceeded,
    VerifyFailed,
)
from .pipeline import Pipeline
from .samples import DEFAULT_SEED, generate_samples
from .verifier import verify_run


def _print_json(value: Any) -> None:
    print(
        json.dumps(
            value,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
            allow_nan=False,
        )
    )


def _pipeline(args: argparse.Namespace) -> Pipeline:
    return Pipeline(Path(args.workspace), state_dir=args.state_dir)


def _add_common(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--workspace", default=".", help="workspace root")
    parser.add_argument("--state-dir", default=".qpipe", help="checkpoint/cache root")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="qpipe",
        description="Deterministic local low-precision quantization pipeline",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    generate = subparsers.add_parser("generate", help="generate deterministic samples")
    _add_common(generate)
    generate.add_argument("--samples-dir", default="samples")
    generate.add_argument("--seed", type=int, default=DEFAULT_SEED)
    generate.add_argument("--clean", action="store_true")

    import_parser = subparsers.add_parser("import", help="import a versioned config")
    _add_common(import_parser)
    import_parser.add_argument("--manifest", required=True)
    import_parser.add_argument("--shapes", required=True)
    import_parser.add_argument("--version", default="v1")

    run = subparsers.add_parser("run", help="run or continue a pipeline")
    _add_common(run)
    run.add_argument("--run-id", default="default")
    run.add_argument("--version")
    run.add_argument("--resume", action="store_true")
    run.add_argument("--stop-after-units", type=int)

    resume = subparsers.add_parser("resume", help="clear stop request and resume")
    _add_common(resume)
    resume.add_argument("--run-id", default="default")
    resume.add_argument("--version")
    resume.add_argument("--stop-after-units", type=int)

    run_stage = subparsers.add_parser(
        "run-stage", help="execute exactly one stage and publish its artifact"
    )
    _add_common(run_stage)
    run_stage.add_argument(
        "--stage",
        required=True,
        choices=("calibration", "forward", "attribution", "validation"),
    )
    run_stage.add_argument("--run-id", default="default")
    run_stage.add_argument("--version")
    run_stage.add_argument("--resume", action="store_true")
    run_stage.add_argument("--stop-after-units", type=int)

    status = subparsers.add_parser("status", help="show stage status and fingerprints")
    _add_common(status)
    status.add_argument("--run-id")
    status.add_argument("--version")

    versions = subparsers.add_parser("versions", help="list imported config versions")
    _add_common(versions)

    layers = subparsers.add_parser("layers", help="show layer errors and attribution")
    _add_common(layers)
    layers.add_argument("--run-id")

    errors = subparsers.add_parser("errors", help="show the run error report")
    _add_common(errors)
    errors.add_argument("--run-id")

    stop = subparsers.add_parser("stop", help="request a graceful stop")
    _add_common(stop)

    verify = subparsers.add_parser("verify", help="verify run integrity and determinism")
    _add_common(verify)
    verify.add_argument("--run-id", default="default")
    verify.add_argument("--compare-run-id")

    publish = subparsers.add_parser(
        "publish-layer", help="low-level locked layer publication probe"
    )
    _add_common(publish)
    publish.add_argument("--run-id", required=True)
    publish.add_argument("--version")
    publish.add_argument("--stage", required=True)
    publish.add_argument("--layer-seq", required=True, type=int)
    publish.add_argument("--layer-id", required=True)
    publish.add_argument("--input-fingerprint", required=True)
    publish.add_argument("--artifact-digest", required=True)

    demo = subparsers.add_parser("demo", help="run the complete deterministic demo")
    _add_common(demo)
    demo.add_argument("--samples-dir", default="samples")
    demo.add_argument("--seed", type=int, default=DEFAULT_SEED)
    demo.add_argument("--clean-samples", action="store_true")
    demo.add_argument("--keep-runs", action="store_true")

    serve = subparsers.add_parser("serve", help="start the local HTTP interface")
    _add_common(serve)
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8787)

    return parser


def _install_stop_signals(root: Path) -> None:
    def handler(signum: int, frame: object) -> None:
        del frame
        request_stop(root, reason=f"signal_{signum}")

    for name in ("SIGINT", "SIGTERM"):
        signum = getattr(signal, name, None)
        if signum is not None:
            try:
                signal.signal(signum, handler)
            except (OSError, ValueError):
                pass


def _run_command(pipeline: Pipeline, args: argparse.Namespace, *, resume: bool) -> int:
    _install_stop_signals(pipeline.root)
    outcome = pipeline.run(
        run_id=args.run_id,
        version=args.version,
        resume=resume or bool(args.resume),
        stop_after_units=args.stop_after_units,
    )
    _print_json(
        {
            "ok": True,
            "run_id": outcome.run_id,
            "status": outcome.status,
            "code": outcome.code,
            "exit_code": outcome.exit_code,
            "output_dir": outcome.output_dir,
            "final_digest": outcome.final_digest,
            "invalidation": outcome.invalidation,
            "end_to_end": outcome.result["end_to_end"],
            "review_required_layers": outcome.result["review_required_layers"],
        }
    )
    return outcome.exit_code


def _demo(pipeline: Pipeline, args: argparse.Namespace) -> int:
    samples_dir = Path(args.samples_dir)
    if not samples_dir.is_absolute():
        samples_dir = Path(args.workspace) / samples_dir
    generation = generate_samples(
        samples_dir, seed=args.seed, clean=args.clean_samples
    )
    manifest = samples_dir / "operator_manifest.json"
    shapes = samples_dir / "shape_declarations.json"
    imported = pipeline.import_config(
        manifest_path=manifest, shapes_path=shapes, version="v1"
    )
    run_prefix = "demo"
    full_run = f"{run_prefix}-full"
    resume_run = f"{run_prefix}-resume"
    if not args.keep_runs:
        for run_id in (full_run, resume_run, "demo-nan", "demo-zero", "demo-bits"):
            path = pipeline.root / "runs" / run_id
            if path.exists():
                shutil.rmtree(path)
    full = pipeline.run(run_id=full_run, version="v1")
    interrupted_units: int | None = None
    try:
        pipeline.run(
            run_id=resume_run,
            version="v1",
            stop_after_units=5,
        )
    except Interrupted as exc:
        interrupted_units = int(exc.details.get("completed_layer_units", 0))
    resumed = pipeline.run(run_id=resume_run, version="v1", resume=True)
    verification = verify_run(
        root=pipeline.root,
        run_id=resume_run,
        compare_run_id=full_run,
    )

    error_cases: dict[str, Any] = {}
    nan_import = pipeline.import_config(
        manifest_path=samples_dir / "operator_manifest_nan.json",
        shapes_path=shapes,
        version="invalid-nan",
    )
    try:
        _install_stop_signals(pipeline.root)
        pipeline.run(run_id="demo-nan", version="invalid-nan")
    except QPipeError as exc:
        error_cases["nan_activation"] = {
            "code": exc.code,
            "layer": exc.layer,
            "parameter": exc.parameter,
            "config_id": nan_import["config_id"],
        }

    zero_import = pipeline.import_config(
        manifest_path=samples_dir / "operator_manifest_zero_channel.json",
        shapes_path=shapes,
        version="invalid-zero",
    )
    try:
        pipeline.run(run_id="demo-zero", version="invalid-zero")
    except QPipeError as exc:
        error_cases["zero_channel"] = {
            "code": exc.code,
            "layer": exc.layer,
            "parameter": exc.parameter,
            "config_id": zero_import["config_id"],
        }

    bit_import = pipeline.import_config(
        manifest_path=samples_dir / "operator_manifest_unsupported_bits.json",
        shapes_path=shapes,
        version="invalid-bits",
    )
    try:
        pipeline.run(run_id="demo-bits", version="invalid-bits")
    except QPipeError as exc:
        error_cases["unsupported_bitwidth"] = {
            "code": exc.code,
            "layer": exc.layer,
            "parameter": exc.parameter,
            "config_id": bit_import["config_id"],
        }

    pipeline.import_config(manifest_path=manifest, shapes_path=shapes, version="v1")
    summary = {
        "ok": bool(
            full.status == "pass"
            and resumed.status == "pass"
            and verification["ok"]
            and verification["byte_identical"]
            and error_cases.get("nan_activation", {}).get("code")
            == "E_NUMERIC_INPUT"
            and error_cases.get("zero_channel", {}).get("code") == "E_INVALID_PARAM"
            and error_cases.get("unsupported_bitwidth", {}).get("code")
            == "E_INVALID_PARAM"
        ),
        "samples": generation,
        "config_id": imported["config_id"],
        "full_run": {
            "run_id": full.run_id,
            "status": full.status,
            "final_digest": full.final_digest,
            "end_to_end": full.result["end_to_end"],
            "review_required_layers": full.result["review_required_layers"],
        },
        "resume_run": {
            "run_id": resumed.run_id,
            "status": resumed.status,
            "final_digest": resumed.final_digest,
            "interrupted_after_layer_units": interrupted_units,
            "byte_identical": verification["byte_identical"],
        },
        "error_cases": error_cases,
        "verification": {
            "ok": verification["ok"],
            "checkpoint_count": verification["checkpoint_count"],
            "checks": len(verification["checks"]),
        },
    }
    _print_json(summary)
    return 0 if summary["ok"] else 7


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "generate":
            samples_dir = Path(args.samples_dir)
            if not samples_dir.is_absolute():
                samples_dir = Path(args.workspace) / samples_dir
            _print_json(
                generate_samples(samples_dir, seed=args.seed, clean=args.clean)
            )
            return 0
        if args.command == "serve":
            from .http_api import serve

            serve(
                Pipeline(Path(args.workspace), state_dir=args.state_dir),
                host=args.host,
                port=args.port,
            )
            return 0

        pipeline = _pipeline(args)
        if args.command == "import":
            manifest = Path(args.manifest)
            shapes = Path(args.shapes)
            if not manifest.is_absolute():
                manifest = Path(args.workspace) / manifest
            if not shapes.is_absolute():
                shapes = Path(args.workspace) / shapes
            result = pipeline.import_config(
                manifest_path=manifest,
                shapes_path=shapes,
                version=args.version,
            )
            _print_json(
                {
                    "ok": True,
                    "version": args.version,
                    "config_id": result["config_id"],
                    "pipeline_version": result["pipeline_version"],
                    "input_hashes": result["input_hashes"],
                    "config_dir": result["config_dir"],
                }
            )
            return 0
        if args.command == "run":
            return _run_command(pipeline, args, resume=False)
        if args.command == "resume":
            return _run_command(pipeline, args, resume=True)
        if args.command == "run-stage":
            _install_stop_signals(pipeline.root)
            _print_json(
                pipeline.run_stage(
                    stage=args.stage,
                    run_id=args.run_id,
                    version=args.version,
                    resume=args.resume,
                    stop_after_units=args.stop_after_units,
                )
            )
            return 0
        if args.command == "status":
            _print_json(pipeline.status(run_id=args.run_id, version=args.version))
            return 0
        if args.command == "versions":
            _print_json(list_versions(pipeline.root))
            return 0
        if args.command == "layers":
            _print_json(pipeline.layers(run_id=args.run_id))
            return 0
        if args.command == "errors":
            _print_json(pipeline.errors(run_id=args.run_id))
            return 0
        if args.command == "stop":
            request_stop(pipeline.root, reason="cli_request")
            _print_json({"ok": True, "code": "E_INTERRUPTED", "stop_requested": True})
            return 0
        if args.command == "verify":
            summary = verify_run(
                root=pipeline.root,
                run_id=args.run_id,
                compare_run_id=args.compare_run_id,
            )
            _print_json(summary)
            return 0
        if args.command == "publish-layer":
            record = pipeline.publish_layer_probe(
                run_id=args.run_id,
                stage=args.stage,
                layer_seq=args.layer_seq,
                layer_id=args.layer_id,
                input_fingerprint=args.input_fingerprint,
                artifact_digest=args.artifact_digest,
                version=args.version,
            )
            _print_json({"ok": True, "record": record})
            return 0
        if args.command == "demo":
            return _demo(pipeline, args)
        parser.error("unknown command")
        return 64
    except VerifyFailed as exc:
        _print_json(exc.to_dict())
        return exc.exit_code
    except ToleranceExceeded as exc:
        _print_json(exc.to_dict())
        return exc.exit_code
    except QPipeError as exc:
        _print_json(exc.to_dict())
        return exc.exit_code
    except Exception as exc:  # defensive CLI boundary, still an integer exit
        traceback.print_exc(file=sys.stderr)
        _print_json(
            {
                "code": "E_INTERNAL",
                "message": f"{type(exc).__name__}: {exc}",
            }
        )
        return 1
