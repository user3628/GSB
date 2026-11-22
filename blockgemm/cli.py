"""Command-line interface for samples, planning, execution, and recovery."""

from __future__ import annotations

import argparse
import json
import signal
from pathlib import Path
from typing import Any, Sequence

from .checkpoint import CheckpointStore
from .engine import (
    describe_execution,
    execute_problem,
    query_plan,
)
from .errors import BlockGemmError, E_INVALID_SHAPE, E_NUMERIC_MISMATCH, E_STALE_CHECKPOINT
from .manifest import case_entry, generate_samples, load_case, load_manifest
from .tiling import DEFAULT_L1_CACHE_KIB, DEFAULT_L2_CACHE_KIB


EXIT_CODES = {
    E_INVALID_SHAPE: 3,
    E_NUMERIC_MISMATCH: 4,
    E_STALE_CHECKPOINT: 5,
}


def _print_json(value: Any) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True, allow_nan=False))


def _sample_source(loaded: Any) -> dict[str, Any]:
    return {
        "manifest_case": loaded.entry.get("id"),
        "a_file": str(loaded.a_path),
        "b_file": str(loaded.b_path),
    }


def _execution_payload(result: Any, command: str) -> dict[str, Any]:
    summary = result.summary or {}
    return {
        "ok": True,
        "command": command,
        "workspace": str(result.workspace),
        "completed": result.completed,
        "stopped": result.stopped,
        "computed_blocks": result.computed_blocks,
        "reused_blocks": result.reused_blocks,
        "next_sequence": result.next_sequence,
        "resume_decision": result.resume_decision.to_dict(),
        "output_path": str(result.output_path) if result.output_path else None,
        "output_digest": result.output_digest,
        "summary_path": str(result.summary_path) if result.summary_path else None,
        "summary": {
            "status": summary.get("status"),
            "block_count": summary.get("block_count"),
            "output": summary.get("output"),
            "plan": summary.get("plan"),
            "plan_fingerprint": summary.get("plan_fingerprint"),
            "version_fingerprint": summary.get("version_fingerprint"),
            "numeric_verification": summary.get("numeric_verification"),
            "nonfinite_blocks": summary.get("nonfinite_blocks"),
        }
        if summary
        else None,
    }


def _add_manifest_case(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--manifest", required=True, help="Path to samples/manifest.json")
    parser.add_argument("--case", required=True, help="Sample case id")


def _add_execution_options(parser: argparse.ArgumentParser, *, include_stop: bool) -> None:
    parser.add_argument("--workspace", required=True, help="Durable checkpoint workspace")
    parser.add_argument("--l1-cache-kib", type=int, default=DEFAULT_L1_CACHE_KIB)
    parser.add_argument("--l2-cache-kib", type=int, default=DEFAULT_L2_CACHE_KIB)
    parser.add_argument("--verify-blocks", type=int, default=3)
    parser.add_argument("--atol", type=float, default=None)
    parser.add_argument("--rtol", type=float, default=None)
    if include_stop:
        parser.add_argument(
            "--stop-after-blocks",
            type=int,
            default=None,
            help="Deterministic local stop request after publishing N blocks",
        )
    parser.add_argument(
        "--accumulation-strategy",
        choices=("k-ascending", "k-descending"),
        default="k-ascending",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="blockgemm",
        description="Deterministic three-level GEMM/BMM with block checkpoints",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    generate = subparsers.add_parser("generate", help="Generate deterministic local .npy samples")
    generate.add_argument("--out", default="samples", help="Sample output directory")
    generate.add_argument("--force", action="store_true", help="Overwrite an existing manifest")

    plan = subparsers.add_parser("plan", help="Query the selected tiling plan and score details")
    _add_manifest_case(plan)
    plan.add_argument("--l1-cache-kib", type=int, default=DEFAULT_L1_CACHE_KIB)
    plan.add_argument("--l2-cache-kib", type=int, default=DEFAULT_L2_CACHE_KIB)

    run = subparsers.add_parser("run", help="Run or continue a checkpointed sample")
    _add_manifest_case(run)
    _add_execution_options(run, include_stop=True)

    resume = subparsers.add_parser("resume", help="Resume from the latest valid contiguous prefix")
    _add_manifest_case(resume)
    _add_execution_options(resume, include_stop=True)

    inspect = subparsers.add_parser(
        "inspect", help="Diagnose resume/invalidation state for current inputs without mutation"
    )
    _add_manifest_case(inspect)
    inspect.add_argument("--workspace", required=True)
    inspect.add_argument("--l1-cache-kib", type=int, default=DEFAULT_L1_CACHE_KIB)
    inspect.add_argument("--l2-cache-kib", type=int, default=DEFAULT_L2_CACHE_KIB)
    inspect.add_argument("--verify-blocks", type=int, default=3)
    inspect.add_argument("--atol", type=float, default=None)
    inspect.add_argument("--rtol", type=float, default=None)
    inspect.add_argument(
        "--accumulation-strategy",
        choices=("k-ascending", "k-descending"),
        default="k-ascending",
    )

    status = subparsers.add_parser("status", help="Query checkpoint prefix, fingerprint, and last invalidation")
    status.add_argument("--workspace", required=True)

    report = subparsers.add_parser("report", help="Print the deterministic final summary report")
    report.add_argument("--workspace", required=True)

    diagnostics = subparsers.add_parser("diagnostics", help="Print per-block numeric and NaN/Inf diagnostics")
    diagnostics.add_argument("--workspace", required=True)

    stop = subparsers.add_parser("stop", help="Request graceful stop after the current output block")
    stop.add_argument("--workspace", required=True)
    stop.add_argument("--reason", default="external_request")

    clear = subparsers.add_parser("clear-stop", help="Remove a pending stop marker")
    clear.add_argument("--workspace", required=True)

    demo = subparsers.add_parser("demo", help="Run the complete local deterministic demonstration")
    demo.add_argument("--manifest", default="samples/manifest.json")
    demo.add_argument("--out", default="runs/demo")
    demo.add_argument("--force-samples", action="store_true")

    return parser


def _load_sample(args: argparse.Namespace):
    return load_case(args.manifest, args.case)


def _run_command(args: argparse.Namespace, *, command: str) -> dict[str, Any]:
    loaded = _load_sample(args)
    stop_signal = {"requested": False}
    previous_handler = None

    def handle_stop(signum: int, frame: Any) -> None:  # noqa: ARG001
        stop_signal["requested"] = True
        try:
            CheckpointStore(args.workspace).request_stop(f"signal_{signum}")
        except Exception:
            pass

    try:
        previous_handler = signal.signal(signal.SIGINT, handle_stop)
        if hasattr(signal, "SIGTERM"):
            signal.signal(signal.SIGTERM, handle_stop)
    except (ValueError, OSError):
        previous_handler = None
    try:
        result = execute_problem(
            workspace=args.workspace,
            a=loaded.a,
            b=loaded.b,
            a_transpose=bool(loaded.entry["a_transpose"]),
            b_transpose=bool(loaded.entry["b_transpose"]),
            expected=loaded.expected,
            l1_cache_kib=args.l1_cache_kib,
            l2_cache_kib=args.l2_cache_kib,
            verify_blocks=args.verify_blocks,
            atol=args.atol,
            rtol=args.rtol,
            case_id=str(loaded.entry["id"]),
            source=_sample_source(loaded),
            consume_stop=True,
            stop_after_blocks=args.stop_after_blocks,
            external_stop=lambda: stop_signal["requested"],
            accumulation_strategy=args.accumulation_strategy,
        )
    finally:
        if previous_handler is not None:
            try:
                signal.signal(signal.SIGINT, previous_handler)
                if hasattr(signal, "SIGTERM"):
                    signal.signal(signal.SIGTERM, previous_handler)
            except (ValueError, OSError):
                pass
    return _execution_payload(result, command)


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "generate":
            manifest = generate_samples(args.out, force=args.force)
            _print_json(
                {
                    "ok": True,
                    "command": "generate",
                    "output": str(Path(args.out)),
                    "case_count": len(manifest["cases"]),
                    "cases": [entry["id"] for entry in manifest["cases"]],
                }
            )
        elif args.command == "plan":
            manifest = load_manifest(args.manifest)
            entry = case_entry(manifest, args.case)
            plan = query_plan(
                m=int(entry["M"]),
                n=int(entry["N"]),
                k=int(entry["K"]),
                batch=int(entry["batch"]),
                dtype=str(entry["dtype"]),
                l1_cache_kib=args.l1_cache_kib,
                l2_cache_kib=args.l2_cache_kib,
            )
            _print_json({"ok": True, "command": "plan", "case": args.case, "plan_selection": plan})
        elif args.command in {"run", "resume"}:
            _print_json(_run_command(args, command=args.command))
        elif args.command == "inspect":
            loaded = _load_sample(args)
            context, decision = describe_execution(
                workspace=args.workspace,
                a=loaded.a,
                b=loaded.b,
                a_transpose=bool(loaded.entry["a_transpose"]),
                b_transpose=bool(loaded.entry["b_transpose"]),
                expected=loaded.expected,
                l1_cache_kib=args.l1_cache_kib,
                l2_cache_kib=args.l2_cache_kib,
                verify_blocks=args.verify_blocks,
                atol=args.atol,
                rtol=args.rtol,
                accumulation_strategy=args.accumulation_strategy,
            )
            _print_json(
                {
                    "ok": True,
                    "command": "inspect",
                    "case": args.case,
                    "workspace": str(context.store.paths.root),
                    "version_fingerprint": context.version_fingerprint,
                    "static_version": context.static_version,
                    "plan_fingerprint": context.plan.fingerprint(),
                    "block_count": context.block_count,
                    "resume_decision": decision.to_dict(),
                    "checkpoint_scan": {
                        "valid_prefix": decision.scan.valid_prefix,
                        "issues": decision.scan.issue_summary(),
                    },
                }
            )
        elif args.command == "status":
            _print_json({"ok": True, "command": "status", "status": CheckpointStore(args.workspace).status()})
        elif args.command == "report":
            summary = CheckpointStore(args.workspace).read_summary()
            if summary is None:
                raise BlockGemmError(
                    E_INVALID_SHAPE,
                    "final summary report is not available",
                    reason="summary_missing",
                    details={"workspace": args.workspace},
                )
            _print_json({"ok": True, "command": "report", "summary": summary})
        elif args.command == "diagnostics":
            summary = CheckpointStore(args.workspace).read_summary()
            if summary is None:
                raise BlockGemmError(
                    E_INVALID_SHAPE,
                    "final summary report is not available",
                    reason="summary_missing",
                    details={"workspace": args.workspace},
                )
            blocks = summary.get("blocks", [])
            _print_json(
                {
                    "ok": True,
                    "command": "diagnostics",
                    "numeric_verification": summary.get("numeric_verification", {}),
                    "nonfinite_blocks": summary.get("nonfinite_blocks", []),
                    "not_ok_blocks": [
                        block
                        for block in blocks
                        if block.get("numeric_status") not in {"ok", "skipped_nonfinite", "not_sampled"}
                        or (block.get("nonfinite") or {}).get("affected")
                    ],
                }
            )
        elif args.command == "stop":
            marker = CheckpointStore(args.workspace).request_stop(args.reason)
            _print_json(
                {
                    "ok": True,
                    "command": "stop",
                    "workspace": args.workspace,
                    "stop_marker": marker,
                    "message": "the executor completes the current indivisible output block, then exits",
                }
            )
        elif args.command == "clear-stop":
            CheckpointStore(args.workspace).clear_stop()
            _print_json({"ok": True, "command": "clear-stop", "workspace": args.workspace})
        elif args.command == "demo":
            from .demo import run_demo

            result = run_demo(
                manifest_path=args.manifest,
                output_dir=args.out,
                force_samples=args.force_samples,
            )
            _print_json({"ok": True, "command": "demo", **result})
        else:
            parser.error(f"unknown command: {args.command}")
        return 0
    except BlockGemmError as error:
        _print_json({"ok": False, "error": error.to_dict()})
        return EXIT_CODES.get(error.code, 1)
    except KeyboardInterrupt:
        _print_json(
            {
                "ok": False,
                "error": {
                    "code": "E_INTERRUPTED",
                    "message": "interrupted before the executor could complete an atomic block publish",
                    "reason": "keyboard_interrupt_outside_graceful_handler",
                    "shard_id": None,
                    "block_coord": [0, 0],
                    "details": {},
                },
            }
        )
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
