"""Command-line interface for the local panoptic pipeline."""

from __future__ import annotations

import argparse
import json
import shutil
import signal
import sys
from pathlib import Path
from typing import Any

from .demo import generate_demo
from .errors import PipelineError
from .fsutil import atomic_write_json, atomic_write_text
from .manifest import ParseOptions, load_manifest
from .pipeline import Pipeline
from .workspace import Workspace


def _json_print(value: Any) -> None:
    print(json.dumps(value, sort_keys=True, indent=2, ensure_ascii=False))


def _config_value(value: str) -> Any:
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


def _parse_kv(items: list[str]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for item in items:
        if "=" not in item:
            raise PipelineError("E_INVALID_CONFIG", "Configuration overrides use key=value syntax")
        key, value = item.split("=", 1)
        result[key] = _config_value(value)
    return result


def _parse_options(args: argparse.Namespace) -> ParseOptions:
    return ParseOptions(
        default_instance_start_id=args.default_instance_start_id,
        min_instance_area=args.min_instance_area,
        confidence_threshold=args.confidence_threshold,
        mask_format=args.mask_format,
        v1_drop_alpha=not args.keep_argb_alpha,
    )


def _load_config_file(path: Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PipelineError("E_INVALID_CONFIG", "Unable to read config file", {"path": str(path)}) from exc
    if not isinstance(value, dict):
        raise PipelineError("E_INVALID_CONFIG", "Config file must contain an object")
    return value


def _add_import_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--default-instance-start-id", type=int, default=100)
    parser.add_argument("--min-instance-area", type=int, default=4)
    parser.add_argument("--confidence-threshold", type=float, default=0.5)
    parser.add_argument("--mask-format", default="png_palette")
    parser.add_argument("--keep-argb-alpha", action="store_true")


def cmd_generate_demo(args: argparse.Namespace) -> int:
    result = generate_demo(args.out, seed=args.seed)
    _json_print(result)
    return 0


def cmd_import(args: argparse.Namespace) -> int:
    workspace = Workspace(args.workspace)
    overrides = _load_config_file(args.config_file)
    overrides.update(_parse_kv(args.set_config))
    manifest = workspace.import_manifest(
        args.manifest,
        parse_options=_parse_options(args),
        config=overrides or None,
    )
    _json_print(
        {
            "ok": True,
            "workspace": str(workspace.root),
            "project": manifest.project,
            "source_schema_version": manifest.schema_version,
            "shards": [shard.shard_id for shard in manifest.shards],
        }
    )
    return 0


def cmd_upgrade(args: argparse.Namespace) -> int:
    manifest = load_manifest(args.manifest, options=_parse_options(args))
    text = manifest.to_json(version=args.to_version)
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        atomic_write_text(args.out, text)
        _json_print({"ok": True, "output": str(args.out), "schema_version": args.to_version})
    else:
        sys.stdout.write(text)
    return 0


def cmd_build(args: argparse.Namespace) -> int:
    workspace = Workspace(args.workspace)
    if args.stop_after_shards is not None and args.stop_after_shards < 0:
        raise PipelineError("E_INVALID_CONFIG", "stop_after_shards must be non-negative")

    old_handlers: dict[int, Any] = {}
    if args.handle_signals:
        def handle_signal(signum: int, _frame: Any) -> None:
            workspace.request_stop(reason=f"signal:{signum}")

        for signum in (signal.SIGINT, signal.SIGTERM):
            old_handlers[signum] = signal.getsignal(signum)
            signal.signal(signum, handle_signal)
    try:
        result = Pipeline(workspace).build(
            shard_ids=args.shard or None,
            stages=args.stage or None,
            resume=args.resume,
            stop_after_shards=args.stop_after_shards,
        )
    finally:
        for signum, handler in old_handlers.items():
            signal.signal(signum, handler)
    _json_print(result)
    if result.get("stopped"):
        return 0
    return 0 if result.get("ok") else 3


def cmd_status(args: argparse.Namespace) -> int:
    _json_print(Pipeline(Workspace(args.workspace)).status(shard_ids=args.shard or None))
    return 0


def cmd_fingerprints(args: argparse.Namespace) -> int:
    _json_print(Pipeline(Workspace(args.workspace)).fingerprints())
    return 0


def cmd_checkpoints(args: argparse.Namespace) -> int:
    workspace = Workspace(args.workspace)
    records = workspace.checkpoints.list_records()
    _json_print(
        {
            "ok": True,
            "last_sequence": records[-1]["sequence"] if records else 0,
            "records": records,
        }
    )
    return 0


def cmd_stop(args: argparse.Namespace) -> int:
    workspace = Workspace(args.workspace)
    workspace.request_stop(reason=args.reason)
    _json_print({"ok": True, "stop_requested": True, "reason": args.reason})
    return 0


def cmd_resume(args: argparse.Namespace) -> int:
    workspace = Workspace(args.workspace)
    workspace.clear_stop()
    if args.build:
        result = Pipeline(workspace).build(
            shard_ids=args.shard or None,
            resume=True,
            stop_after_shards=args.stop_after_shards,
        )
        _json_print(result)
        return 0 if result.get("ok") or result.get("stopped") else 3
    _json_print({"ok": True, "stop_requested": False})
    return 0


def cmd_export(args: argparse.Namespace) -> int:
    pipeline = Pipeline(Workspace(args.workspace))
    content, content_type = pipeline.export(args.shard, args.format)
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_bytes(content)
        _json_print(
            {
                "ok": True,
                "shard_id": args.shard,
                "format": args.format,
                "output": str(args.out),
                "content_type": content_type,
                "bytes": len(content),
            }
        )
    else:
        if args.format in {"rle", "rle_string", "coco", "coco_json", "json"}:
            sys.stdout.write(content.decode("utf-8"))
        else:
            raise PipelineError(
                "E_INVALID_CONFIG",
                "Binary PNG export requires --out",
                {"format": args.format},
            )
    return 0


def cmd_validate(args: argparse.Namespace) -> int:
    result = Pipeline(Workspace(args.workspace)).validate(shard_ids=args.shard or None)
    _json_print(result)
    return 0 if result.get("ok") else 3


def cmd_serve(args: argparse.Namespace) -> int:
    from .httpapi import run_server

    run_server(
        workspace=Workspace(args.workspace),
        host=args.host,
        port=args.port,
    )
    return 0


def cmd_demo(args: argparse.Namespace) -> int:
    demo_dir = args.demo_dir.resolve()
    workspace_root = args.workspace.resolve()
    if args.fresh:
        shutil.rmtree(workspace_root, ignore_errors=True)
    generated = generate_demo(demo_dir, seed=args.seed)
    workspace = Workspace.create(workspace_root)
    source_manifest = demo_dir / ("manifest_v1.json" if args.schema_version == 1 else "manifest_v2.json")
    workspace.import_manifest(
        source_manifest,
        parse_options=ParseOptions(
            default_instance_start_id=args.default_instance_start_id,
            min_instance_area=args.min_instance_area,
            confidence_threshold=args.confidence_threshold,
            mask_format=args.mask_format,
        ),
    )
    result = Pipeline(workspace).build()
    expected = {
        "format": "PANO_DEMO_EXPECTED_V1",
        "seed": args.seed,
        "source_schema_version": args.schema_version,
        "summary_digest": result.get("summary_digest"),
        "ok": result.get("ok"),
        "checkpoint_sequence": result.get("checkpoint_sequence"),
        "shards": {
            shard_id: {
                "validation_ok": value.get("validation", {}).get("ok"),
                "model_digest": value.get("validation", {}).get("model_digest"),
                "chains": {
                    chain["chain"]: chain["ok"] for chain in value.get("validation", {}).get("chains", [])
                },
            }
            for shard_id, value in result.get("shards", {}).items()
        },
    }
    atomic_write_json(demo_dir / "expected_summary.json", expected, pretty=True)
    _json_print(
        {
            "ok": result.get("ok"),
            "workspace": str(workspace_root),
            "demo_dir": str(demo_dir),
            "generated": generated,
            "summary_digest": result.get("summary_digest"),
            "checkpoint_sequence": result.get("checkpoint_sequence"),
            "expected_summary": str(demo_dir / "expected_summary.json"),
        }
    )
    return 0 if result.get("ok") else 3


def cmd_verify_errors(args: argparse.Namespace) -> int:
    demo_dir = args.demo_dir.resolve()
    root = args.workspace.resolve()
    shutil.rmtree(root, ignore_errors=True)
    results: dict[str, Any] = {}

    try:
        load_manifest(demo_dir / "errors" / "unknown_schema_manifest.json")
        results["unknown_schema"] = {"ok": False, "code": None}
    except PipelineError as exc:
        results["unknown_schema"] = {"ok": exc.code == "E_UNSUPPORTED_SCHEMA", "code": exc.code}

    unknown_ws = Workspace.create(root / "unknown_color")
    unknown_ws.import_manifest(demo_dir / "errors" / "unknown_color_manifest.json")
    try:
        Pipeline(unknown_ws).build()
        results["unknown_color"] = {"ok": False, "code": None}
    except PipelineError as exc:
        results["unknown_color"] = {
            "ok": exc.code == "E_UNKNOWN_COLOR" and unknown_ws.checkpoints.last_sequence() == 0,
            "code": exc.code,
            "details": exc.details,
        }

    overflow_ws = Workspace.create(root / "segment_overflow")
    overflow_ws.import_manifest(demo_dir / "errors" / "segment_overflow_manifest.json")
    overflow_result = Pipeline(overflow_ws).build()
    validation = next(iter(overflow_result["shards"].values()))["validation"]
    results["segment_overflow"] = {
        "ok": "E_SEGMENT_ID_OVERFLOW" in validation.get("error_codes", []),
        "code": "E_SEGMENT_ID_OVERFLOW" if validation.get("error_codes") else None,
        "degradations": validation.get("degradations", []),
    }
    output = {"ok": all(item.get("ok") for item in results.values()), "results": results}
    _json_print(output)
    return 0 if output["ok"] else 3


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="panopipe", description="Local panoptic mask pipeline")
    subparsers = parser.add_subparsers(dest="command", required=True)

    generate = subparsers.add_parser("generate-demo", help="Generate deterministic local demo data")
    generate.add_argument("--out", type=Path, default=Path("demo"))
    generate.add_argument("--seed", type=int, default=20261120)
    generate.set_defaults(func=cmd_generate_demo)

    demo = subparsers.add_parser("demo", help="Generate demo data, import it, build and verify")
    demo.add_argument("--demo-dir", type=Path, default=Path("demo"))
    demo.add_argument("--workspace", type=Path, default=Path("demo-workspace"))
    demo.add_argument("--seed", type=int, default=20261120)
    demo.add_argument("--schema-version", type=int, choices=(1, 2), default=1)
    demo.add_argument("--fresh", action="store_true", default=True)
    demo.add_argument("--default-instance-start-id", type=int, default=100)
    demo.add_argument("--min-instance-area", type=int, default=4)
    demo.add_argument("--confidence-threshold", type=float, default=0.6)
    demo.add_argument("--mask-format", default="png_palette")
    demo.set_defaults(func=cmd_demo)

    imp = subparsers.add_parser("import", help="Import and normalize a manifest into a workspace")
    imp.add_argument("--manifest", type=Path, required=True)
    imp.add_argument("--workspace", type=Path, required=True)
    imp.add_argument("--config-file", type=Path)
    imp.add_argument("--set-config", action="append", default=[], metavar="KEY=VALUE")
    _add_import_options(imp)
    imp.set_defaults(func=cmd_import)

    upgrade = subparsers.add_parser("upgrade", help="Convert a manifest between schema versions")
    upgrade.add_argument("--manifest", type=Path, required=True)
    upgrade.add_argument("--out", type=Path)
    upgrade.add_argument("--to-version", type=int, choices=(1, 2), default=2)
    _add_import_options(upgrade)
    upgrade.set_defaults(func=cmd_upgrade)

    build = subparsers.add_parser("build", help="Run or resume the four-stage build")
    build.add_argument("--workspace", type=Path, required=True)
    build.add_argument("--shard", action="append", default=[])
    build.add_argument("--stage", action="append", choices=("decode", "merge", "encode", "validate"), default=[])
    build.add_argument("--resume", action="store_true")
    build.add_argument("--stop-after-shards", type=int)
    build.add_argument("--no-handle-signals", dest="handle_signals", action="store_false", default=True)
    build.set_defaults(func=cmd_build)

    status = subparsers.add_parser("status", help="Show stage status, fingerprints and invalidations")
    status.add_argument("--workspace", type=Path, required=True)
    status.add_argument("--shard", action="append", default=[])
    status.set_defaults(func=cmd_status)

    fingerprints = subparsers.add_parser("fingerprints", help="Show all current version fingerprints")
    fingerprints.add_argument("--workspace", type=Path, required=True)
    fingerprints.set_defaults(func=cmd_fingerprints)

    checkpoints = subparsers.add_parser("checkpoints", help="List immutable checkpoint records")
    checkpoints.add_argument("--workspace", type=Path, required=True)
    checkpoints.set_defaults(func=cmd_checkpoints)

    stop = subparsers.add_parser("stop", help="Request a graceful stop after the current shard")
    stop.add_argument("--workspace", type=Path, required=True)
    stop.add_argument("--reason", default="cli")
    stop.set_defaults(func=cmd_stop)

    resume = subparsers.add_parser("resume", help="Clear a stop request and optionally continue the build")
    resume.add_argument("--workspace", type=Path, required=True)
    resume.add_argument("--build", action="store_true")
    resume.add_argument("--shard", action="append", default=[])
    resume.add_argument("--stop-after-shards", type=int)
    resume.set_defaults(func=cmd_resume)

    export = subparsers.add_parser("export", help="Export a current encoded mask format")
    export.add_argument("--workspace", type=Path, required=True)
    export.add_argument("--shard", required=True)
    export.add_argument("--format", required=True, choices=("png", "png_palette", "rle", "coco", "coco_json", "json"))
    export.add_argument("--out", type=Path)
    export.set_defaults(func=cmd_export)

    validate = subparsers.add_parser("validate", help="Re-run or read the current validation stage")
    validate.add_argument("--workspace", type=Path, required=True)
    validate.add_argument("--shard", action="append", default=[])
    validate.set_defaults(func=cmd_validate)

    serve = subparsers.add_parser("serve", help="Run the local HTTP API on the loopback interface")
    serve.add_argument("--workspace", type=Path, required=True)
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8765)
    serve.set_defaults(func=cmd_serve)

    verify_errors = subparsers.add_parser("verify-errors", help="Verify injected business error samples")
    verify_errors.add_argument("--demo-dir", type=Path, default=Path("demo"))
    verify_errors.add_argument("--workspace", type=Path, default=Path(".error-workspaces"))
    verify_errors.set_defaults(func=cmd_verify_errors)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except PipelineError as exc:
        _json_print(exc.to_dict())
        return 2
    except KeyboardInterrupt:
        _json_print({"ok": False, "error": {"code": "E_INTERRUPTED", "message": "Interrupted"}})
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
