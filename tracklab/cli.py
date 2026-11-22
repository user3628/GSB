"""Command-line entry point for the local tracking workbench."""

from __future__ import annotations

import argparse
import json
import signal
import sys
import threading
from pathlib import Path
from typing import Any

from .config import load_settings
from .demo import run_demo
from .errors import WorkbenchError, raise_error, E_NOT_FOUND
from .generator import generate_demo
from .http_api import serve as serve_http
from .manifest import validate_manifest
from .workbench import Workbench


def _print(value: Any) -> None:
    print(json.dumps(value, sort_keys=True, ensure_ascii=False, indent=2))


def _sequence_ids(workbench: Workbench, requested: list[str] | None) -> list[str]:
    if requested:
        return list(dict.fromkeys(requested))
    return [sequence["sequence_id"] for sequence in workbench.manifest.sequences]


def _install_signal_handler() -> tuple[threading.Event, Any]:
    event = threading.Event()
    previous: dict[int, Any] = {}

    def handler(signum: int, frame: Any) -> None:  # noqa: ARG001
        if event.is_set():
            raise KeyboardInterrupt
        event.set()
        print("graceful stop requested; finishing the current frame unit", file=sys.stderr)

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            previous[int(sig)] = signal.getsignal(sig)
            signal.signal(sig, handler)
        except (ValueError, OSError):
            pass
    return event, previous


def _restore_signal_handler(previous: dict[int, Any]) -> None:
    for sig, handler in previous.items():
        try:
            signal.signal(sig, handler)
        except (ValueError, OSError):
            pass


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="tracklab", description="Deterministic local video tracking workbench")
    parser.add_argument("--root", default=".", help="workspace root (default: current directory)")
    parser.add_argument("--config", default="config/workbench.yaml", help="YAML configuration path")
    sub = parser.add_subparsers(dest="command", required=True)

    generate = sub.add_parser("generate", help="generate deterministic demo sequences and manifest")
    generate.add_argument("--force", action="store_true", help="replace existing generated data")

    sub.add_parser("validate", help="validate the tracking manifest and referenced frames")

    imp = sub.add_parser("import", help="import a tracking manifest and its frame files")
    imp.add_argument("--manifest", required=True)
    imp.add_argument("--source-data-dir")

    run = sub.add_parser("run", help="run one or more sequences")
    run.add_argument("--sequence", action="append", dest="sequences")
    run.add_argument("--stage", choices=["all", "appearance", "candidates", "trajectory", "tracking", "report"], default="all")
    run.add_argument("--stop-after", type=int, help="gracefully stop after this many total frames per sequence")

    resume = sub.add_parser("resume", help="clear stop requests and resume one or more sequences")
    resume.add_argument("--sequence", action="append", dest="sequences")
    resume.add_argument("--stop-after", type=int)

    stop = sub.add_parser("stop", help="write a graceful stop request")
    stop.add_argument("--sequence", action="append", dest="sequences")
    stop.add_argument("--reason", default="CLI stop request")

    status = sub.add_parser("status", help="show sequence, stage and version status")
    status.add_argument("--sequence")

    diag = sub.add_parser("diagnose", help="show one frame's stage diagnostics")
    diag.add_argument("--sequence", required=True)
    diag.add_argument("--frame", type=int, required=True)

    invalidations = sub.add_parser("invalidations", help="show incremental invalidation events and errors")
    invalidations.add_argument("--sequence")

    report = sub.add_parser("report", help="build the deterministic run report")
    report.add_argument("--sequence", action="append", dest="sequences")

    serve = sub.add_parser("serve", help="start the local HTTP dashboard")
    serve.add_argument("--host")
    serve.add_argument("--port", type=int)

    demo = sub.add_parser("demo", help="generate data and verify full vs stopped/resumed runs")
    demo.add_argument("--json", action="store_true", help="print the full demo summary")

    sub.add_parser("verify", help="validate manifest, checkpoint chains, and report generation")

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        settings = load_settings(args.root, args.config)
        if args.command == "generate":
            result = generate_demo(settings, force=bool(args.force))
            _print(result)
            return 0
        if args.command == "validate":
            manifest = validate_manifest(settings, verify_frame_files=True)
            total_frames = sum(len(seq["frames"]) for seq in manifest.sequences)
            for seq in manifest.sequences:
                if len(seq["frames"]) != seq["visible_frames"]:
                    raise_error("E_MANIFEST", "visible_frames mismatch", context={"sequence_id": seq["sequence_id"]})
                if len(seq["frames"]) > seq["sample_count_upper_bound"]:
                    raise_error("E_MANIFEST", "sample count exceeds declared upper bound", context={"sequence_id": seq["sequence_id"]})
            _print(
                {
                    "ok": True,
                    "manifest_path": str(settings.manifest_path),
                    "manifest_fingerprint": manifest.fingerprint,
                    "sequence_count": len(manifest.sequences),
                    "total_visible_frames": total_frames,
                    "sequences": [
                        {
                            "sequence_id": seq["sequence_id"],
                            "visible_frames": seq["visible_frames"],
                            "sample_count_upper_bound": seq["sample_count_upper_bound"],
                            "occlusion_intervals": seq["occlusion_intervals"],
                        }
                        for seq in manifest.sequences
                    ],
                }
            )
            return 0
        workbench = Workbench(settings)
        if args.command == "import":
            _print(workbench.import_manifest(args.manifest, source_data_dir=args.source_data_dir))
            return 0
        if args.command == "run":
            stop_event, previous = _install_signal_handler()
            try:
                results = []
                for sequence_id in _sequence_ids(workbench, args.sequences):
                    if stop_event.is_set():
                        break
                    results.append(
                        workbench.run_stage(
                            sequence_id,
                            args.stage,
                            stop_after=args.stop_after,
                            stop_flag=stop_event.is_set,
                        )
                    )
                _print({"ok": all(item.get("ok", False) for item in results), "results": results})
                return 0
            finally:
                _restore_signal_handler(previous)
        if args.command == "resume":
            stop_event, previous = _install_signal_handler()
            try:
                results = []
                for sequence_id in _sequence_ids(workbench, args.sequences):
                    if stop_event.is_set():
                        break
                    results.append(
                        workbench.resume_sequence(
                            sequence_id,
                            stop_after=args.stop_after,
                            stop_flag=stop_event.is_set,
                        )
                    )
                _print({"ok": all(item.get("ok", False) for item in results), "results": results})
                return 0
            finally:
                _restore_signal_handler(previous)
        if args.command == "stop":
            results = []
            for sequence_id in _sequence_ids(workbench, args.sequences):
                results.append(workbench.request_stop(sequence_id, reason=args.reason))
            _print({"ok": True, "stop_requests": results})
            return 0
        if args.command == "status":
            if args.sequence:
                _print(workbench.sequence_status(args.sequence))
            else:
                _print(workbench.list_sequences())
            return 0
        if args.command == "diagnose":
            _print(workbench.diagnose_frame(args.sequence, args.frame))
            return 0
        if args.command == "invalidations":
            _print(workbench.invalidations(args.sequence))
            return 0
        if args.command == "report":
            _print(workbench.build_report(args.sequences))
            return 0
        if args.command == "serve":
            serve_http(settings, host=args.host, port=args.port)
            return 0
        if args.command == "demo":
            result = run_demo(settings)
            _print(
                result
                if args.json
                else {
                    k: result[k]
                    for k in (
                        "verification_passed",
                        "byte_identical",
                        "checkpoint_trees_identical",
                        "replayed_frames",
                        "baseline_summary_sha256",
                        "demo_summary_sha256",
                    )
                }
            )
            return 0 if result["verification_passed"] else 1
        if args.command == "verify":
            manifest = validate_manifest(settings, verify_frame_files=True)
            workbench.reload_manifest()
            statuses = [workbench.sequence_status(sequence["sequence_id"]) for sequence in manifest.sequences]
            complete = all(status["complete"] for status in statuses)
            report_result = workbench.build_report() if complete else None
            _print(
                {
                    "ok": complete,
                    "manifest_fingerprint": manifest.fingerprint,
                    "sequences": [
                        {
                            "sequence_id": status["sequence_id"],
                            "valid_frames": status["valid_frames"],
                            "complete": status["complete"],
                            "checkpoint_issues": status["checkpoint_issues"],
                            "chain_digest": status["versions"]["checkpoint"]["chain_digest"],
                        }
                        for status in statuses
                    ],
                    "report": report_result,
                }
            )
            return 0 if complete else 1
        raise_error(E_NOT_FOUND, "unknown command", context={"command": args.command})
    except WorkbenchError as exc:
        print(json.dumps(exc.to_dict(), ensure_ascii=False, indent=2), file=sys.stderr)
        return exc.exit_code
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
