"""Command-line interface for importing, running, querying, and stopping audits."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from .api import get_report, import_dataset, load_stage_artifact, query_status
from .errors import FingerprintError
from .pipeline import request_stop, resume_pipeline, run_pipeline
from .samples import generate_samples


def _print(payload: Any, canonical: bool = False) -> None:
    if canonical:
        text = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    else:
        text = json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2)
    print(text)


def _add_pipeline_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--workspace", required=True, help="workspace directory")
    parser.add_argument("--manifest", help="manifest path; defaults to workspace/manifest.json")
    parser.add_argument("--stop-after-stage", choices=("peaks", "pairs", "quantize", "audit"))
    parser.add_argument("--stop-after-shards", type=int)
    parser.add_argument("--json", action="store_true", help="emit compact canonical JSON")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="audiofp", description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    sample_parser = subparsers.add_parser("generate-samples", help="generate deterministic WAV corpus")
    sample_parser.add_argument("--samples-dir", required=True)
    sample_parser.add_argument("--seed", type=int, default=20261123)
    sample_parser.add_argument("--json", action="store_true")

    import_parser = subparsers.add_parser("import", help="import WAV shards and configuration")
    import_parser.add_argument("--workspace", required=True)
    import_parser.add_argument("--manifest", required=True)
    import_parser.add_argument("--json", action="store_true")

    run_parser = subparsers.add_parser("run", help="run or continue the audit pipeline")
    _add_pipeline_options(run_parser)
    run_parser.add_argument("--tolerate-business-errors", action="store_true")

    resume_parser = subparsers.add_parser("resume", help="clear stop request and resume")
    _add_pipeline_options(resume_parser)
    resume_parser.add_argument("--tolerate-business-errors", action="store_true")

    status_parser = subparsers.add_parser("status", help="query stage and version status")
    status_parser.add_argument("--workspace", required=True)
    status_parser.add_argument("--manifest")
    status_parser.add_argument("--shard")
    status_parser.add_argument("--json", action="store_true")

    report_parser = subparsers.add_parser("report", help="read the complete audit report")
    report_parser.add_argument("--workspace", required=True)
    report_parser.add_argument("--shard", help="load one shard's detailed audit stage artifact")
    report_parser.add_argument("--json", action="store_true")

    stop_parser = subparsers.add_parser("request-stop", help="request a stage-boundary stop")
    stop_parser.add_argument("--workspace", required=True)
    stop_parser.add_argument("--json", action="store_true")
    return parser


def _run_exit_code(workspace: str, tolerate: bool) -> int:
    if tolerate:
        return 0
    try:
        report = get_report(workspace)
    except FingerprintError:
        return 0
    summary = report["summary"]
    if summary.get("pipeline_errors"):
        return 2
    if any(not shard.get("status") in ("passed", "error") for shard in summary.get("shards", [])):
        return 3
    if any(shard.get("status") == "failed" for shard in summary.get("shards", [])):
        return 3
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "generate-samples":
            payload = generate_samples(args.samples_dir, args.seed)
            _print(payload, args.json)
            return 0
        if args.command == "import":
            payload = import_dataset(args.workspace, args.manifest)
            _print(payload, args.json)
            return 0
        if args.command in ("run", "resume"):
            runner = run_pipeline if args.command == "run" else resume_pipeline
            result = runner(
                args.workspace,
                args.manifest,
                stop_after_stage=args.stop_after_stage,
                stop_after_shards=args.stop_after_shards,
            )
            payload = result.as_dict()
            if result.summary_digest is not None and not args.json:
                try:
                    payload["report"] = get_report(args.workspace)["summary"]
                except FingerprintError:
                    pass
            _print(payload, args.json)
            if result.stopped:
                return 0
            return _run_exit_code(args.workspace, args.tolerate_business_errors)
        if args.command == "status":
            payload = query_status(args.workspace, args.manifest, args.shard)
            _print(payload, args.json)
            return 0
        if args.command == "report":
            if args.shard:
                payload = load_stage_artifact(args.workspace, args.shard, "audit")
            else:
                payload = get_report(args.workspace)
            _print(payload, args.json)
            return 0
        if args.command == "request-stop":
            request_stop(args.workspace)
            _print(
                {
                    "schema": "audiofp.stop-request-result.v1",
                    "workspace": str(Path(args.workspace).resolve()),
                    "stop_requested": True,
                },
                args.json,
            )
            return 0
    except FingerprintError as error:
        print(json.dumps(error.to_dict(), ensure_ascii=False, sort_keys=True), file=sys.stderr)
        return 1
    return 1


if __name__ == "__main__":
    raise SystemExit(main())

