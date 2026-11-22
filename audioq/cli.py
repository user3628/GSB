"""Command line interface for the local workbench."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from .canonical import atomic_write_json, canonical_json_bytes, read_json
from .demo import generate_demo_assets, run_demo
from .errors import AudioQError
from .models import decode_config, encode_config, load_config_file
from .pipeline import EvaluationWorkbench


def _json_print(value: Any) -> None:
    sys.stdout.write(json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False) + "\n")


def _short_run(result: dict[str, Any], include_report: bool = False) -> dict[str, Any]:
    output = {key: value for key, value in result.items() if key not in {"report", "status_snapshot"}}
    if result.get("report") is not None:
        output["report_summary"] = result["report"]["summary"]
        output["report_fingerprint"] = result["report"]["report_fingerprint"]
        if include_report:
            output["report"] = result["report"]
    return output


def _workbench(args: argparse.Namespace) -> EvaluationWorkbench:
    return EvaluationWorkbench(
        args.root,
        args.manifest,
        args.config,
        state_dir=args.state_dir,
        normalize_policy=getattr(args, "normalize_policy", "normalize"),
        snr_threshold_rule=getattr(args, "snr_threshold_rule", "fixed"),
        default_snr_threshold_db=getattr(args, "default_snr_threshold_db", 8.0),
    )


def _add_workbench_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--root", default=".", help="workspace root (default: current directory)")
    parser.add_argument("--manifest", required=True, help="shard manifest path, relative to root")
    parser.add_argument("--config", required=True, help="v1 or v2 evaluation config path, relative to root")
    parser.add_argument("--state-dir", default="work/state", help="checkpoint/report directory, relative to root")
    parser.add_argument("--normalize-policy", choices=("normalize", "strict_100", "divide_100"), default="normalize")
    parser.add_argument("--snr-threshold-rule", choices=("fixed", "weight_scaled"), default="fixed")
    parser.add_argument("--default-snr-threshold-db", type=float, default=8.0)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="audioq", description="Local deterministic audio quality workbench")
    sub = parser.add_subparsers(dest="command", required=True)

    generate = sub.add_parser("generate", help="generate deterministic demo WAVs, manifest and configs")
    generate.add_argument("--base", default="demo", help="asset directory, relative to root")
    generate.add_argument("--root", default=".", help="workspace root")

    demo = sub.add_parser("demo", help="run full acceptance demo, stop/resume and error injections")
    demo.add_argument("--root", default=".", help="workspace root")
    demo.add_argument("--keep", action="store_true", help="keep previous demo checkpoint directories")

    run = sub.add_parser("run", help="run or continue all four stages")
    _add_workbench_args(run)
    run.add_argument("--stop-after", type=int, default=None, help="deterministically stop after N completed shards")
    run.add_argument("--clear-stop", action="store_true", help="clear a pending stop request before running")
    run.add_argument("--report", default=None, help="optional report output path relative to root")
    run.add_argument("--include-report", action="store_true", help="include the full report in CLI output")

    resume = sub.add_parser("resume", help="clear stop request and resume from valid checkpoints")
    _add_workbench_args(resume)
    resume.add_argument("--report", default=None, help="optional report output path relative to root")
    resume.add_argument("--include-report", action="store_true")

    status = sub.add_parser("status", help="query stage states, version fingerprints and invalidations")
    _add_workbench_args(status)

    stop = sub.add_parser("stop", help="write a graceful stop request")
    stop.add_argument("--root", default=".", help="workspace root")
    stop.add_argument("--state-dir", default="work/state")
    stop.add_argument("--reason", default="user_request")

    convert = sub.add_parser("convert", help="import v1/v2 config and export the other structure version")
    convert.add_argument("--input", required=True, help="input config path relative to root")
    convert.add_argument("--output", required=True, help="output config path relative to root")
    convert.add_argument("--target-version", type=int, choices=(1, 2), required=True)
    convert.add_argument("--root", default=".")
    convert.add_argument("--normalize-policy", choices=("normalize", "strict_100", "divide_100"), default="normalize")
    convert.add_argument("--snr-threshold-rule", choices=("fixed", "weight_scaled"), default="fixed")
    convert.add_argument("--default-snr-threshold-db", type=float, default=8.0)

    export = sub.add_parser("export-report", help="verify checkpoints are current and export deterministic report")
    _add_workbench_args(export)
    export.add_argument("--output", default=None, help="report output path relative to root")

    checkpoint = sub.add_parser("checkpoint-publish", help="publish one checkpoint with strict stage/sequence checks")
    checkpoint.add_argument("--state-dir", default="work/state")
    checkpoint.add_argument("--shard-id", required=True)
    checkpoint.add_argument("--stage", choices=("frame", "metrics", "fusion", "diagnostics"), required=True)
    checkpoint.add_argument("--input-fingerprint", required=True)
    checkpoint.add_argument("--config-fingerprint", required=True)
    checkpoint.add_argument("--artifact", required=True, help="artifact JSON path")
    checkpoint.add_argument("--expected-seq", type=int, default=None)

    checkpoint_status = sub.add_parser("checkpoint-status", help="inspect valid, invalid and duplicate checkpoints")
    checkpoint_status.add_argument("--state-dir", default="work/state")
    checkpoint_status.add_argument("--shard-id", action="append", default=None)

    serve = sub.add_parser("serve", help="start the local HTTP diagnostic workbench")
    _add_workbench_args(serve)
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=5000)
    serve.add_argument("--debug", action="store_true")
    return parser


def _convert(args: argparse.Namespace) -> dict[str, Any]:
    root = Path(args.root).resolve()
    input_path = Path(args.input)
    if not input_path.is_absolute():
        input_path = root / input_path
    output_path = Path(args.output)
    if not output_path.is_absolute():
        output_path = root / output_path
    config = load_config_file(
        input_path,
        normalize_policy=args.normalize_policy,
        snr_threshold_rule=args.snr_threshold_rule,
        default_snr_threshold_db=args.default_snr_threshold_db,
    )
    exported = encode_config(config, args.target_version)
    atomic_write_json(output_path, exported, pretty=True)
    return {
        "ok": True,
        "source_schema_version": config.schema_version,
        "target_schema_version": args.target_version,
        "input": str(input_path.relative_to(root)) if input_path.is_relative_to(root) else str(input_path),
        "output": str(output_path.relative_to(root)) if output_path.is_relative_to(root) else str(output_path),
        "version_fingerprint": config.version_fingerprint,
        "derived_snr_threshold_db": config.metrics.snr_threshold_db,
        "downgrade_items": exported.get("downgrade_items", []),
        "downgrade_restore": exported.get("downgrade_restore", {}),
        "exported": exported,
    }


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "generate":
            assets = generate_demo_assets(args.base, root=args.root)
            root = Path(args.root).resolve()
            _json_print(
                {
                    "ok": True,
                    "manifest": assets["manifest"].relative_to(root).as_posix(),
                    "config_v2": assets["config_v2"].relative_to(root).as_posix(),
                    "config_v1": assets["config_v1"].relative_to(root).as_posix(),
                    "samples": [path.relative_to(root).as_posix() for path in assets["samples"]],
                }
            )
            return 0
        if args.command == "demo":
            summary = run_demo(args.root, force=not args.keep)
            _json_print(summary)
            return 0 if summary.get("acceptance_ok") else 1
        if args.command == "convert":
            _json_print(_convert(args))
            return 0
        if args.command == "stop":
            from .checkpoints import CheckpointManager

            manager = CheckpointManager(Path(args.root) / args.state_dir)
            payload = manager.request_stop(reason=args.reason)
            _json_print({"ok": True, **payload})
            return 0
        if args.command == "checkpoint-publish":
            from .checkpoints import CheckpointManager

            artifact = read_json(Path(args.artifact))
            manager = CheckpointManager(args.state_dir)
            record = manager.publish_checkpoint(
                shard_id=args.shard_id,
                stage=args.stage,
                input_fingerprint=args.input_fingerprint,
                config_fingerprint=args.config_fingerprint,
                artifact=artifact,
                expected_seq=args.expected_seq,
            )
            _json_print({"ok": True, "record": record})
            return 0
        if args.command == "checkpoint-status":
            from .checkpoints import CheckpointManager

            manager = CheckpointManager(args.state_dir)
            _json_print({"ok": True, **manager.summary(args.shard_id)})
            return 0

        workbench = _workbench(args)
        if args.command == "status":
            _json_print(workbench.status())
            return 0
        if args.command == "run":
            result = workbench.run(stop_after_shards=args.stop_after, clear_stop=args.clear_stop)
            if args.report and result.get("report") is not None:
                workbench.write_report(args.report)
            _json_print(_short_run(result, include_report=args.include_report))
            return 0
        if args.command == "resume":
            result = workbench.resume()
            if args.report and result.get("report") is not None:
                workbench.write_report(args.report)
            _json_print(_short_run(result, include_report=args.include_report))
            return 0
        if args.command == "export-report":
            report, data, path = workbench.write_report(args.output)
            _json_print(
                {
                    "ok": True,
                    "path": path.relative_to(workbench.root).as_posix()
                    if path.is_relative_to(workbench.root)
                    else str(path),
                    "report_sha256": __import__("hashlib").sha256(data).hexdigest(),
                    "report_fingerprint": report["report_fingerprint"],
                    "summary": report["summary"],
                }
            )
            return 0
        if args.command == "serve":
            from .web import create_app

            app = create_app(
                args.root,
                args.manifest,
                args.config,
                args.state_dir,
                normalize_policy=args.normalize_policy,
                snr_threshold_rule=args.snr_threshold_rule,
                default_snr_threshold_db=args.default_snr_threshold_db,
            )
            app.run(host=args.host, port=args.port, debug=args.debug, threaded=True)
            return 0
    except AudioQError as exc:
        sys.stdout.write(json.dumps(exc.to_dict(), ensure_ascii=False, sort_keys=True, indent=2) + "\n")
        return 2
    except FileNotFoundError as exc:
        error = AudioQError("E_IO", str(exc), observed={"path": getattr(exc, "filename", None)})
        sys.stdout.write(json.dumps(error.to_dict(), ensure_ascii=False, sort_keys=True, indent=2) + "\n")
        return 2
    except OSError as exc:
        error = AudioQError(
            "E_IO",
            str(exc) or exc.__class__.__name__,
            observed={"path": getattr(exc, "filename", None), "reason": exc.__class__.__name__},
        )
        sys.stdout.write(json.dumps(error.to_dict(), ensure_ascii=False, sort_keys=True, indent=2) + "\n")
        return 2
    parser.error("unknown command")
    return 2
