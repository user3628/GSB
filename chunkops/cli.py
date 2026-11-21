"""Command-line interface for chunkops."""

from __future__ import annotations

import argparse
import json
import math
import shutil
import sys
from pathlib import Path
from typing import Any

from .data import generate_datasets
from .errors import ChunkOpsError, E_NUMERIC_MISMATCH
from .fingerprint import sha256_file
from .http_server import serve
from .pipeline import (
    Pipeline,
    import_fragment,
    request_stop,
)
from .storage import atomic_write_json


def _safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_safe(item) for item in value]
    if isinstance(value, float):
        if math.isnan(value):
            return "NaN"
        if math.isinf(value):
            return "Infinity" if value > 0 else "-Infinity"
    if hasattr(value, "item"):
        try:
            return _safe(value.item())
        except Exception:
            pass
    return value


def _emit(value: Any, *, compact: bool = False) -> None:
    if compact:
        text = json.dumps(_safe(value), ensure_ascii=False, sort_keys=True, allow_nan=False)
    else:
        text = json.dumps(
            _safe(value), ensure_ascii=False, indent=2, sort_keys=True, allow_nan=False
        )
    print(text)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="chunkops",
        description="Blocked Softmax/LayerNorm operator library, pipeline, HTTP and CLI",
    )
    parser.add_argument("--compact", action="store_true", help="emit compact JSON")
    parser.add_argument("--version", action="version", version="chunkops 1.0.0")
    sub = parser.add_subparsers(dest="command", required=True)

    init = sub.add_parser("init-data", help="generate deterministic .npz fragments")
    init.add_argument("--root", default="data")
    init.add_argument("--seed", type=int, default=20261122)
    init.add_argument("--no-large", action="store_true")
    init.add_argument("--overwrite", action="store_true")

    imp = sub.add_parser("import", help="import/register a fragment in a run workspace")
    imp.add_argument("--workspace", required=True)
    imp.add_argument("--fragment", required=True)
    imp.add_argument("--fragment-id")
    imp.add_argument("--op", choices=("softmax", "layernorm"), default="softmax")
    imp.add_argument("--axis", type=int, choices=(0, 1), default=1)
    imp.add_argument("--cache-budget-bytes", type=int, default=4 * 1024 * 1024)
    imp.add_argument("--memory-budget-bytes", type=int, default=512 * 1024 * 1024)
    imp.add_argument("--eps", type=float, default=1e-5)
    imp.add_argument("--atol", type=float, default=1e-4)
    imp.add_argument("--rtol", type=float, default=1e-4)
    imp.add_argument("--output-dtype")
    imp.add_argument("--affine", action="store_true")
    imp.add_argument("--row-block", type=int)
    imp.add_argument("--col-block", type=int)
    imp.add_argument("--sum-strategy", choices=("sequential", "pairwise", "compensated"))
    imp.add_argument("--validation-sample-blocks", type=int, default=10)
    imp.add_argument("--validation-seed", type=int)
    imp.add_argument("--reject-zero-rows", action="store_true")
    imp.add_argument("--reject-nonfinite", action="store_true")

    run = sub.add_parser("run", help="execute stages up to completion")
    run.add_argument("--workspace", required=True)
    run.add_argument("--stage", choices=("plan", "reduce", "validate", "summary"))
    run.add_argument("--until", choices=("plan", "reduce", "validate", "summary"))
    run.add_argument("--max-stages", type=int)
    run.add_argument("--stop-after", choices=("plan", "reduce", "validate", "summary"))
    run.add_argument("--clear-stop", action="store_true")

    resume = sub.add_parser("resume", help="clear stop request and resume")
    resume.add_argument("--workspace", required=True)
    resume.add_argument("--max-stages", type=int)
    resume.add_argument("--stop-after", choices=("plan", "reduce", "validate", "summary"))

    stop = sub.add_parser("stop", help="request graceful stop")
    stop.add_argument("--workspace", required=True)
    stop.add_argument("--reason", default="cli_request")

    status = sub.add_parser("status")
    status.add_argument("--workspace", required=True)

    plan = sub.add_parser("plan")
    plan.add_argument("--workspace", required=True)

    versions = sub.add_parser("versions")
    versions.add_argument("--workspace", required=True)

    errors = sub.add_parser("errors")
    errors.add_argument("--workspace", required=True)
    errors.add_argument("--output")

    summary = sub.add_parser("summary")
    summary.add_argument("--workspace", required=True)
    summary.add_argument("--output")

    compare = sub.add_parser("compare", help="compare two output artifacts byte-for-byte")
    compare.add_argument("--left-workspace", required=True)
    compare.add_argument("--right-workspace", required=True)

    server = sub.add_parser("serve", help="run local HTTP service")
    server.add_argument("--host", default="127.0.0.1")
    server.add_argument("--port", type=int, default=8765)

    demo = sub.add_parser("demo", help="run deterministic end-to-end demonstration")
    demo.add_argument("--root", default=".")
    demo.add_argument("--no-large", action="store_true")
    demo.add_argument("--overwrite", action="store_true")
    return parser


def _plan_override(args: argparse.Namespace) -> dict[str, Any] | None:
    supplied = [args.row_block is not None, args.col_block is not None, args.sum_strategy is not None]
    if not any(supplied):
        return None
    if not all(supplied):
        raise ChunkOpsError(
            "E_INVALID_CONFIG",
            "row-block, col-block, and sum-strategy must be supplied together",
        )
    return {
        "row_block": int(args.row_block),
        "col_block": int(args.col_block),
        "sum_strategy": args.sum_strategy,
    }


def _copy_json_command(path: Path, output: str | None) -> Any:
    value = json.loads(path.read_text(encoding="utf-8"))
    if output:
        atomic_write_json(output, value)
        return {"source": str(path), "destination": str(Path(output).resolve())}
    return value


def _compare_outputs(left_workspace: str, right_workspace: str) -> dict[str, Any]:
    left = Path(left_workspace).resolve() / "outputs" / "output.npy"
    right = Path(right_workspace).resolve() / "outputs" / "output.npy"
    for path in (left, right):
        if not path.exists():
            raise ChunkOpsError(
                "E_INVALID_STATE",
                "output artifact is missing",
                {"path": str(path), "position": "output"},
            )
    left_digest = sha256_file(left)
    right_digest = sha256_file(right)
    result = {
        "left": {"path": str(left), "sha256": left_digest, "size_bytes": left.stat().st_size},
        "right": {"path": str(right), "sha256": right_digest, "size_bytes": right.stat().st_size},
        "byte_identical": left_digest == right_digest and left.stat().st_size == right.stat().st_size,
    }
    if not result["byte_identical"]:
        raise ChunkOpsError(
            E_NUMERIC_MISMATCH,
            "output artifacts are not byte-identical",
            result,
        )
    return result


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "init-data":
            result = generate_datasets(
                args.root,
                seed=args.seed,
                include_large=not args.no_large,
                overwrite=args.overwrite,
            )
        elif args.command == "import":
            config = import_fragment(
                args.workspace,
                args.fragment,
                op=args.op,
                axis=args.axis,
                fragment_id=args.fragment_id,
                cache_budget_bytes=args.cache_budget_bytes,
                memory_budget_bytes=args.memory_budget_bytes,
                eps=args.eps,
                tolerance={"atol": args.atol, "rtol": args.rtol},
                output_dtype=args.output_dtype,
                affine=args.affine,
                plan_override=_plan_override(args),
                validation_max_sample_blocks=args.validation_sample_blocks,
                validation_seed=args.validation_seed,
                reject_zero_rows=args.reject_zero_rows,
                reject_nonfinite=args.reject_nonfinite,
            )
            result = {"config": config, "status": Pipeline(args.workspace).status()}
        elif args.command == "run":
            pipeline = Pipeline(args.workspace)
            if args.stage:
                result = pipeline.run_stage(args.stage)
            else:
                result = pipeline.run(
                    until_stage=args.until,
                    max_stages=args.max_stages,
                    stop_after_stage=args.stop_after,
                    clear_stop_request=args.clear_stop,
                )
        elif args.command == "resume":
            result = Pipeline(args.workspace).resume(
                max_stages=args.max_stages, stop_after_stage=args.stop_after
            )
        elif args.command == "stop":
            marker = request_stop(args.workspace, reason=args.reason)
            result = {"stop": marker, "status": Pipeline(args.workspace).status()}
        elif args.command == "status":
            result = Pipeline(args.workspace).status()
        elif args.command == "plan":
            pipeline = Pipeline(args.workspace)
            result = {"status": pipeline.status(), "plan": pipeline.plan_info()}
        elif args.command == "versions":
            pipeline = Pipeline(args.workspace)
            result = {
                "plans": pipeline.plan_versions.inspect(),
                "results": pipeline.result_versions.inspect(),
                "checkpoints": pipeline.checkpoints.discover(),
            }
        elif args.command == "errors":
            pipeline = Pipeline(args.workspace)
            report_path = pipeline.workspace / "reports" / "errors.json"
            if not report_path.exists():
                pipeline.error_report()
            result = _copy_json_command(report_path, args.output)
        elif args.command == "summary":
            pipeline = Pipeline(args.workspace)
            summary_path = pipeline.workspace / "summary.json"
            if not summary_path.exists():
                pipeline.summary()
            result = _copy_json_command(summary_path, args.output)
        elif args.command == "compare":
            result = _compare_outputs(args.left_workspace, args.right_workspace)
        elif args.command == "serve":
            serve(args.host, args.port)
            return 0
        elif args.command == "demo":
            from .demo import run_demo

            result = run_demo(
                args.root,
                include_large=not args.no_large,
                overwrite=args.overwrite,
            )
        else:
            parser.error("unknown command")
            return 2
        _emit(result, compact=args.compact)
        return 0
    except ChunkOpsError as exc:
        _emit(exc.to_dict(), compact=args.compact)
        return 1
    except KeyboardInterrupt:
        _emit(
            ChunkOpsError("E_INTERRUPTED", "interrupted by user", {"graceful": False}).to_dict(),
            compact=args.compact,
        )
        return 130
    except Exception as exc:
        _emit(
            ChunkOpsError(
                "E_INTERNAL", str(exc), {"exception_type": type(exc).__name__}
            ).to_dict(),
            compact=args.compact,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
