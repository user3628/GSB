from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path
from typing import Any

from .errors import REEError
from .formats import (
    canonical_format_name,
    decode_format,
    encode_format,
)
from .models import canonical_bytes, canonical_json
from .pipeline import Pipeline
from .storage import pretty_json, resolve_workspace_path


def _emit(payload: Any, *, pretty: bool = True) -> None:
    if pretty:
        print(pretty_json(payload))
    else:
        print(canonical_json(payload))


def _pipeline(args: argparse.Namespace) -> Pipeline:
    return Pipeline(
        args.root,
        config_path=args.config,
        runtime_dir=args.runtime,
        catalog_path=args.catalog,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m ree",
        description="Deterministic local REE interoperability pipeline",
    )
    parser.add_argument("--root", default=".", help="workspace root, relative to cwd")
    parser.add_argument(
        "--config", default="config/extraction.yaml", help="workspace-relative config path"
    )
    parser.add_argument("--runtime", default=".ree", help="workspace-relative runtime directory")
    parser.add_argument(
        "--catalog", default="data/manifest.json", help="workspace-relative input manifest"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    generate = subparsers.add_parser("generate", help="generate deterministic sample shards")
    generate.add_argument("--compact", action="store_true")

    subparsers.add_parser("status", help="show stage state and fingerprints")
    subparsers.add_parser("fingerprints", help="show stage fingerprints only")
    subparsers.add_parser("reset", help="remove runtime checkpoints and artifacts")

    run = subparsers.add_parser("run", help="run all stale stages")
    run.add_argument("--stop-after-stage", choices=["parse", "estimate", "extract", "validate"])
    run.add_argument("--compact", action="store_true")

    resume = subparsers.add_parser("resume", help="clear stop request and resume")
    resume.add_argument("--stop-after-stage", choices=["parse", "estimate", "extract", "validate"])
    resume.add_argument("--compact", action="store_true")

    subparsers.add_parser("stop", help="request graceful stop after the current document")

    run_stage = subparsers.add_parser("run-stage", help="run exactly one stage")
    run_stage.add_argument("stage", choices=["parse", "estimate", "extract", "validate"])

    import_parser = subparsers.add_parser("import", help="import JSONL shards into the catalog")
    import_parser.add_argument("kind", choices=["documents", "seeds", "legacy"])
    import_parser.add_argument("paths", nargs="+")

    results = subparsers.add_parser("results", help="query extracted unified IR")
    results.add_argument("--doc-id")
    results.add_argument("--compact", action="store_true")

    subparsers.add_parser("diagnostics", help="show alignment and roundtrip diagnostics")

    export_parser = subparsers.add_parser("export", help="export unified intermediate representation")
    export_parser.add_argument("--output", help="workspace-relative JSON output path")
    export_parser.add_argument("--compact", action="store_true")

    convert = subparsers.add_parser("convert", help="convert one extracted document between formats")
    convert.add_argument("--doc-id", required=True)
    convert.add_argument("--from-format", required=True)
    convert.add_argument("--to-format", required=True)
    convert.add_argument("--compact", action="store_true")

    compat = subparsers.add_parser("compat-check", help="run a schema-version compatibility check")
    compat.add_argument("--schema-version", type=int, required=True)
    compat.add_argument("--doc-id", default="compat_probe")

    demo = subparsers.add_parser("demo", help="run deterministic demonstration")
    demo.add_argument("--compact", action="store_true")
    demo.add_argument("--no-regenerate", action="store_true")
    return parser


def command_diagnostics(pipeline: Pipeline) -> dict[str, Any]:
    parse_artifact = pipeline.get_stage_artifact("parse")
    validation_artifact = pipeline.get_stage_artifact("validate")
    return {
        "parse": parse_artifact["diagnostics"],
        "parse_summary": parse_artifact["summary"],
        "validation_summary": validation_artifact["summary"],
        "validation_failures": [
            check for check in validation_artifact["checks"] if not check["ok"]
        ],
        "source_upgrades": {
            check["doc_id"]: {
                "source_annotation_version": check["source_annotation_version"],
                "source_upgrades": check["source_upgrades"],
                "roundtrip_upgrades": sorted(
                    {
                        upgrade
                        for chain in check["chains"]
                        for upgrade in chain.get("upgrades", [])
                    }
                ),
            }
            for check in validation_artifact["checks"]
            if check["source_annotation_version"] == 1
        },
        "invalidations": pipeline.status()["invalidations"],
    }


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "generate":
        from .sample_data import generate_samples

        _emit(generate_samples(Path(args.root).resolve()), pretty=not args.compact)
        return 0
    if args.command == "demo":
        from .demo import run_demo

        _emit(
            run_demo(
                Path(args.root).resolve(),
                regenerate=not args.no_regenerate,
            ),
            pretty=not args.compact,
        )
        return 0
    try:
        pipeline = _pipeline(args)
        if args.command == "status":
            _emit(pipeline.status())
        elif args.command == "reset":
            runtime = pipeline.workspace.runtime
            removed = runtime.exists()
            if removed:
                shutil.rmtree(runtime)
            _emit(
                {
                    "status": "reset",
                    "removed": removed,
                    "runtime": runtime.relative_to(pipeline.root).as_posix(),
                }
            )
        elif args.command == "fingerprints":
            state = pipeline.status()
            _emit(
                {
                    "config_version": state["config_version"],
                    "last_checkpoint_seq": state["last_checkpoint_seq"],
                    "stages": {
                        stage: {
                            "state": item["state"],
                            "fingerprint": item["fingerprint"],
                            "expected_fingerprint": item["expected_fingerprint"],
                            "checkpoint_seq": item["checkpoint_seq"],
                        }
                        for stage, item in state["stages"].items()
                    },
                    "invalidations": state["invalidations"],
                }
            )
        elif args.command == "run":
            _emit(
                pipeline.run(stop_after_stage=args.stop_after_stage),
                pretty=not args.compact,
            )
        elif args.command == "resume":
            _emit(
                pipeline.resume(stop_after_stage=args.stop_after_stage),
                pretty=not args.compact,
            )
        elif args.command == "stop":
            _emit({"status": "stop_requested", **pipeline.request_stop()})
        elif args.command == "run-stage":
            _emit(pipeline.run_stage(args.stage))
        elif args.command == "import":
            _emit(pipeline.catalog.import_files(args.kind, args.paths))
        elif args.command == "results":
            if args.doc_id:
                _emit(
                    {
                        "schema_version": 2,
                        "document": pipeline.get_document(args.doc_id).model_dump(
                            mode="json"
                        ),
                    },
                    pretty=not args.compact,
                )
            else:
                _emit(
                    pipeline.get_stage_artifact("extract"),
                    pretty=not args.compact,
                )
        elif args.command == "diagnostics":
            _emit(command_diagnostics(pipeline))
        elif args.command == "export":
            artifact = pipeline.get_stage_artifact("extract")
            payload = {
                "schema_version": 2,
                "format": "UNIFIED_IR",
                "documents": artifact["documents"],
                "summary": artifact["summary"],
            }
            if args.output:
                output = resolve_workspace_path(pipeline.root, args.output)
                pipeline.workspace.atomic_write_bytes(output, canonical_bytes(payload))
                _emit(
                    {
                        "status": "exported",
                        "path": output.relative_to(pipeline.root).as_posix(),
                        "sha256": __import__("hashlib")
                        .sha256(canonical_bytes(payload))
                        .hexdigest(),
                    }
                )
            else:
                _emit(payload, pretty=not args.compact)
        elif args.command == "convert":
            source_format = canonical_format_name(args.from_format)
            target_format = canonical_format_name(args.to_format)
            document = pipeline.get_document(args.doc_id)
            source_payload = encode_format(source_format, document)
            decoded = decode_format(source_format, source_payload, pipeline.load_config())
            converted = encode_format(target_format, decoded.document)
            _emit(
                {
                    "doc_id": args.doc_id,
                    "from_format": source_format,
                    "to_format": target_format,
                    "diagnostics": decoded.diagnostics,
                    "upgrades": decoded.upgrades,
                    "payload": converted,
                },
                pretty=not args.compact,
            )
        elif args.command == "compat-check":
            probe = {
                "format": "JSON_INTERVALS",
                "schema_version": args.schema_version,
                "doc_id": args.doc_id,
                "tokens": ["probe"],
                "text": "probe",
                "source": "compat_check",
                "entities": [{"id": "e1", "type": "ORG", "start": 0, "end": 1}],
                "relations": [],
                "events": [],
            }
            decoded = decode_format("JSON_INTERVALS", probe, pipeline.load_config())
            _emit(
                {
                    "ok": True,
                    "source_version": decoded.source_version,
                    "upgrades": decoded.upgrades,
                    "diagnostics": decoded.diagnostics,
                }
            )
        else:
            parser.error(f"unknown command: {args.command}")
    except REEError as exc:
        print(canonical_json({"error": exc.as_dict()}), file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
