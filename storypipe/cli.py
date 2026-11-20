from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from .canonical import atomic_write_json
from .checkpoints import CheckpointStore
from .errors import PipelineError
from .generator import generate_samples
from .http_api import serve
from .pipeline import PipelineRunner
from .verify import compare_scales, validate_workspace
from .workspace import import_sample


def _print(value: Any) -> None:
    print(json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2))


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="storypipe")
    subparsers = parser.add_subparsers(dest="command", required=True)

    generate = subparsers.add_parser("generate", help="generate deterministic sample scales")
    generate.add_argument("--out", default="artifacts/samples")

    import_parser = subparsers.add_parser("import", help="import a sample into a workspace")
    import_parser.add_argument("--sample", required=True)
    import_parser.add_argument("--workspace", required=True)

    run = subparsers.add_parser("run", help="run or resume the four-stage pipeline")
    run.add_argument("--workspace", required=True)
    run.add_argument("--sample", help="import this sample before running")
    run.add_argument("--fresh", action="store_true")
    run.add_argument("--stop-after-units", type=int)

    for command in ("status", "fingerprints", "export-manifest", "verify"):
        command_parser = subparsers.add_parser(command)
        command_parser.add_argument("--workspace", required=True)

    for command in ("stop", "resume"):
        command_parser = subparsers.add_parser(command)
        command_parser.add_argument("--workspace", required=True)
        if command == "stop":
            command_parser.add_argument("--reason", default="operator request")

    export = subparsers.add_parser("export")
    export.add_argument("--workspace", required=True)
    export.add_argument("--out", required=True)

    compare = subparsers.add_parser("compare", help="verify and compare two sample scales")
    compare.add_argument("--small", required=True)
    compare.add_argument("--large", required=True)
    compare.add_argument("--out")

    serve_parser = subparsers.add_parser("serve")
    serve_parser.add_argument("--host", default="127.0.0.1")
    serve_parser.add_argument("--port", type=int, default=8765)

    checkpoint = subparsers.add_parser("checkpoint")
    checkpoint_sub = checkpoint.add_subparsers(dest="checkpoint_command", required=True)
    publish = checkpoint_sub.add_parser("publish")
    publish.add_argument("--workspace", required=True)
    publish.add_argument("--stage", required=True)
    publish.add_argument("--input-fingerprint", required=True)
    publish.add_argument("--artifact-digest", required=True)
    publish.add_argument("--artifact-path", default="")
    publish.add_argument("--config-version", default="1")
    publish.add_argument("--sequence", type=int)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "generate":
            _print(generate_samples(Path(args.out)))
        elif args.command == "import":
            _print(import_sample(Path(args.sample), Path(args.workspace)))
        elif args.command == "run":
            if args.sample:
                import_sample(Path(args.sample), Path(args.workspace))
            result = PipelineRunner(args.workspace).run(
                fresh=args.fresh, stop_after_units=args.stop_after_units
            )
            _print(result)
            return 0
        elif args.command == "status":
            _print(PipelineRunner(args.workspace).inspect())
        elif args.command == "fingerprints":
            inspection = PipelineRunner(args.workspace).inspect()
            _print(
                {
                    "ok": inspection.get("ok"),
                    "overall_fingerprint": inspection.get("overall_fingerprint"),
                    "stage_fingerprints": inspection.get("stage_fingerprints"),
                    "stages": inspection.get("stages"),
                    "earliest_invalid_stage": inspection.get("earliest_invalid_stage"),
                    "invalidated_stages": inspection.get("invalidated_stages"),
                    "error": inspection.get("error"),
                }
            )
        elif args.command == "export-manifest":
            _print(PipelineRunner(args.workspace).export_manifest())
        elif args.command == "export":
            manifest = PipelineRunner(args.workspace).export_manifest()
            atomic_write_json(Path(args.out), manifest)
            _print({"ok": True, "out": str(Path(args.out).resolve())})
        elif args.command == "verify":
            _print(validate_workspace(args.workspace))
        elif args.command == "compare":
            report = compare_scales(args.small, args.large, args.out)
            _print(report["comparison"] | {"ok": report["ok"]})
            return 0 if report["ok"] else 1
        elif args.command == "stop":
            _print(PipelineRunner(args.workspace).request_stop(args.reason))
        elif args.command == "resume":
            _print(PipelineRunner(args.workspace).resume())
        elif args.command == "serve":
            serve(args.host, args.port)
        elif args.command == "checkpoint":
            record = CheckpointStore(Path(args.workspace)).publish(
                stage=args.stage,
                input_fingerprint=args.input_fingerprint,
                config_version=args.config_version,
                artifact_digest=args.artifact_digest,
                artifact_path=args.artifact_path,
                components={},
                sequence=args.sequence,
            )
            _print(record)
        return 0
    except PipelineError as exc:
        print(json.dumps({"ok": False, "error": exc.to_dict()}, ensure_ascii=False, sort_keys=True))
        return 2
    except Exception as exc:
        print(
            json.dumps(
                {"ok": False, "error": {"code": "E_INTERNAL", "message": str(exc)}},
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 3


if __name__ == "__main__":
    sys.exit(main())
