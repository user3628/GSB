"""Command-line interface for the deterministic separation workspace."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from .demo import run_demo
from .errors import AppError
from .generate import generate_dataset
from .pipeline import SeparationPipeline
from .server import DEFAULT_HOST, DEFAULT_PORT, serve
from .workspace import Workspace


def _json_print(value: Any) -> None:
    print(json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2))


def _parse_bands(text: str) -> tuple[int, int]:
    try:
        left, right = text.split(":", 1)
        return int(left), int(right)
    except (ValueError, AttributeError) as exc:
        raise argparse.ArgumentTypeError("bands must use START:END syntax") from exc


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="sonic-sep", description="Deterministic local block source separation")
    parser.add_argument("--root", default=".", help="project root containing samples/, configs/, state/")
    sub = parser.add_subparsers(dest="command", required=True)

    generate = sub.add_parser("generate", help="generate fixed-seed WAV fragments and manifest")
    generate.add_argument("--samples-dir", default=None)
    generate.add_argument("--seed", type=int, default=20261123)

    import_cmd = sub.add_parser("import", help="validate and import a manifest/configuration pair")
    import_cmd.add_argument("--manifest", default=None)
    import_cmd.add_argument("--config", default=None)
    import_cmd.add_argument("--preserve-overrides", action="store_true")

    build = sub.add_parser("build", help="run the five-stage build")
    build.add_argument("--slice", dest="slice_id", default="all")
    build.add_argument("--stop-after-units", type=int, default=None, help="deterministic graceful-stop simulation")

    incremental = sub.add_parser("incremental", help="update one source over a band interval")
    incremental.add_argument("--slice", dest="slice_id", required=True)
    incremental.add_argument("--source-id", type=int, required=True)
    incremental.add_argument("--bands", type=_parse_bands, required=True, help="START:END inclusive")
    incremental.add_argument("--f0-min", type=float, default=None)
    incremental.add_argument("--f0-max", type=float, default=None)

    status = sub.add_parser("status", help="show stage, version, and checkpoint status")
    status.add_argument("--slice", dest="slice_id", default=None)

    version = sub.add_parser("version", help="show stage input fingerprints")
    version.add_argument("--slice", dest="slice_id", default=None)

    invalidations = sub.add_parser("invalidations", help="show version invalidation history")
    invalidations.add_argument("--slice", dest="slice_id", default=None)

    changes = sub.add_parser("mask-changes", help="show per-round mask change magnitudes")
    changes.add_argument("--slice", dest="slice_id", default=None)

    verify = sub.add_parser("verify", help="compare persisted output against a fresh full computation")
    verify.add_argument("--slice", dest="slice_id", required=True)

    sub.add_parser("stop", help="request graceful stop at the next checkpoint boundary")
    resume = sub.add_parser("resume", help="clear stop request and continue from caches")
    resume.add_argument("--slice", dest="slice_id", default="all")

    checkpoints = sub.add_parser("checkpoints", help="list checkpoint journal state")
    checkpoints.add_argument("--slice", dest="slice_id", default=None)

    publish = sub.add_parser("publish-checkpoint", help="publish one checkpoint record under the file lock")
    publish.add_argument("--record", required=True, help="JSON file containing the checkpoint record")
    publish.add_argument("--sequence", type=int, default=None)
    publish.add_argument("--block-sequence", type=int, default=None)

    server = sub.add_parser("serve", help="start the local HTTP API and dashboard")
    server.add_argument("--host", default=DEFAULT_HOST)
    server.add_argument("--port", type=int, default=DEFAULT_PORT)

    demo = sub.add_parser("demo", help="run the full repeatable acceptance demonstration")
    demo.add_argument("--output", default=None)
    demo.add_argument("--no-reset", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    workspace = Workspace(args.root)
    try:
        if args.command == "generate":
            samples_dir = Path(args.samples_dir).resolve() if args.samples_dir else workspace.samples_dir
            path = generate_dataset(samples_dir, seed=args.seed)
            _json_print({"ok": True, "manifest_path": str(path), "samples_dir": str(samples_dir), "seed": args.seed})
            return 0
        if args.command == "import":
            _json_print(workspace.import_dataset(args.manifest, args.config, preserve_overrides=args.preserve_overrides))
            return 0
        if args.command == "serve":
            serve(workspace, args.host, args.port)
            return 0
        if args.command == "demo":
            report = run_demo(workspace, output_path=args.output, reset=not args.no_reset)
            _json_print(report)
            return 0
        if args.command == "stop":
            workspace.request_stop("cli_stop_request")
            _json_print({"ok": True, "status": "stop_requested"})
            return 0
        pipeline = SeparationPipeline(workspace)
        if args.command == "build":
            selected = None if args.slice_id == "all" else [args.slice_id]
            _json_print(pipeline.build_all(slice_ids=selected, stop_after_units=args.stop_after_units))
            return 0
        if args.command == "resume":
            selected = None if args.slice_id == "all" else [args.slice_id]
            _json_print(pipeline.resume(slice_ids=selected))
            return 0
        if args.command == "incremental":
            band_start, band_end = args.bands
            _json_print(
                pipeline.incremental_band_update(
                    args.slice_id,
                    source_id=args.source_id,
                    band_start=band_start,
                    band_end=band_end,
                    f0_min_hz=args.f0_min,
                    f0_max_hz=args.f0_max,
                )
            )
            return 0
        if args.command == "status":
            _json_print(pipeline.status(slice_id=args.slice_id))
            return 0
        if args.command == "version":
            _json_print(pipeline.version(slice_id=args.slice_id))
            return 0
        if args.command == "invalidations":
            _json_print(pipeline.invalidations(slice_id=args.slice_id))
            return 0
        if args.command == "mask-changes":
            _json_print(pipeline.mask_changes(slice_id=args.slice_id))
            return 0
        if args.command == "verify":
            _json_print(pipeline.verify_slice(args.slice_id))
            return 0
        if args.command == "checkpoints":
            from .checkpoints import checkpoint_status

            _json_print({"ok": True, **checkpoint_status(pipeline.checkpoints, slice_id=args.slice_id)})
            return 0
        if args.command == "publish-checkpoint":
            with open(args.record, "r", encoding="utf-8") as handle:
                record = json.load(handle)
            published = pipeline.checkpoints.publish(
                record,
                sequence=args.sequence,
                block_sequence=args.block_sequence,
            )
            _json_print({"ok": True, "record": published})
            return 0
        parser.error("unknown command")
        return 2
    except AppError as exc:
        print(json.dumps(exc.to_dict(), ensure_ascii=False, sort_keys=True, indent=2), file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        workspace.request_stop("keyboard_interrupt")
        print(json.dumps({"ok": False, "error": {"code": "E_INTERRUPTED", "message": "stop requested"}}), file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())

