from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from pathlib import Path
from typing import Any, Sequence

import numpy as np

from .api import OperatorWorkspace, bmm, gemm, select_plan
from .blocking import DEFAULT_L1_BYTES, DEFAULT_L2_BYTES, BlockPlan
from .errors import BusinessCode, GemmError
from .storage import json_safe


EXIT_CODES = {
    BusinessCode.E_INVALID_SHAPE: 2,
    BusinessCode.E_BATCH_MISMATCH: 3,
    BusinessCode.E_NUMERIC_MISMATCH: 4,
    BusinessCode.E_STALE_CONFIG: 5,
    BusinessCode.E_STALE_PUBLISH: 6,
    BusinessCode.E_MEMORY_BUDGET: 7,
}


def _json_print(value: Any) -> None:
    print(json.dumps(json_safe(value), sort_keys=True, indent=2, allow_nan=False))


def _parse_plan(value: str | None) -> BlockPlan | None:
    if value is None:
        return None
    parts = [part.strip() for part in value.split(",")]
    if len(parts) != 5:
        raise GemmError(
            BusinessCode.E_INVALID_SHAPE,
            "--plan must be MR,NR,MC,KC,NC",
            details={"value": value},
        )
    try:
        mr, nr, mc, kc, nc = (int(part) for part in parts)
    except ValueError as exc:
        raise GemmError(
            BusinessCode.E_INVALID_SHAPE,
            "--plan contains a non-integer dimension",
            details={"value": value, "error": str(exc)},
        )
    return BlockPlan(mr=mr, nr=nr, mc=mc, kc=kc, nc=nc)


def _load_manifest(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        manifest = json.load(handle)
    if not isinstance(manifest, dict) or not isinstance(manifest.get("samples"), list):
        raise GemmError(
            BusinessCode.E_INVALID_SHAPE,
            "manifest must contain a samples list",
            details={"path": str(path)},
        )
    return manifest


def _sample_entry(manifest: dict[str, Any], name: str) -> dict[str, Any]:
    for record in manifest["samples"]:
        if record.get("name") == name:
            return record
    raise GemmError(
        BusinessCode.E_INVALID_SHAPE,
        "sample name is not present in manifest",
        details={"sample": name},
    )


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_sample(
    *,
    manifest_path: Path,
    sample: str,
    verify_digests: bool,
) -> tuple[dict[str, Any], np.ndarray, np.ndarray]:
    manifest = _load_manifest(manifest_path)
    record = _sample_entry(manifest, sample)
    root = manifest_path.parent
    a_path = root / record["a_file"]
    b_path = root / record["b_file"]
    if verify_digests:
        if _file_sha256(a_path) != record["a_sha256"]:
            raise GemmError(
                BusinessCode.E_INVALID_SHAPE,
                "A sample file digest does not match manifest",
                details={"path": str(a_path)},
            )
        if _file_sha256(b_path) != record["b_sha256"]:
            raise GemmError(
                BusinessCode.E_INVALID_SHAPE,
                "B sample file digest does not match manifest",
                details={"path": str(b_path)},
            )
    return record, np.load(a_path, allow_pickle=False), np.load(b_path, allow_pickle=False)


def _workspace(args: argparse.Namespace) -> OperatorWorkspace | None:
    if getattr(args, "workspace", None):
        return OperatorWorkspace(args.workspace)
    return None


def _selection_payload(selection: Any) -> dict[str, Any]:
    return selection.to_dict()


def command_plan(args: argparse.Namespace) -> int:
    if args.sample:
        manifest = _load_manifest(args.manifest)
        record = _sample_entry(manifest, args.sample)
        m, n, k, batch = record["m"], record["n"], record["k"], record["batch"]
        dtype = record.get("dtype", "float64")
    else:
        if args.m is None or args.n is None or args.k is None:
            raise GemmError(
                BusinessCode.E_INVALID_SHAPE,
                "provide --sample or all of --m/--n/--k",
            )
        m, n, k, batch, dtype = args.m, args.n, args.k, args.batch, args.dtype
    selection = select_plan(
        m,
        n,
        k,
        batch=batch,
        dtype=dtype,
        plan=_parse_plan(args.plan),
        l1_bytes=args.l1_bytes,
        l2_bytes=args.l2_bytes,
        parallel_workers=args.parallel_workers,
    )
    _json_print(
        {
            "ok": True,
            "code": "OK",
            "input": {"m": m, "n": n, "k": k, "batch": batch, "dtype": dtype},
            "selection": _selection_payload(selection),
        }
    )
    return 0


def _run_compute(args: argparse.Namespace) -> Any:
    record, a, b = _load_sample(
        manifest_path=args.manifest,
        sample=args.sample,
        verify_digests=not args.skip_input_digest,
    )
    workspace = _workspace(args)
    kwargs = {
        "trans_a": record["trans_a"],
        "trans_b": record["trans_b"],
        "plan": _parse_plan(args.plan),
        "sum_strategy": args.strategy,
        "l1_bytes": args.l1_bytes,
        "l2_bytes": args.l2_bytes,
        "accumulation_mode": args.accumulation_mode,
        "verify": not args.no_verify,
        "rtol": args.rtol,
        "atol": args.atol,
        "memory_budget_bytes": args.memory_budget,
        "workspace": workspace,
        "use_cache": args.use_cache,
        "publish_config": args.publish_config,
        "expected_config_seq": args.expected_config_seq,
        "expected_cache_seq": args.expected_cache_seq,
        "adopt_stored_plan": not args.no_adopt_stored_plan,
        "selection_metadata": {"sample": args.sample, "cli": True},
    }
    if record["mode"] == "bmm":
        return bmm(a, b, **kwargs)
    return gemm(a, b, **kwargs)


def command_run(args: argparse.Namespace) -> int:
    result = _run_compute(args)
    summary = result.summary()
    summary["sample"] = args.sample
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(
            json.dumps(json_safe(summary), sort_keys=True, indent=2, allow_nan=False) + "\n",
            encoding="utf-8",
        )
    _json_print(summary)
    return 0


def command_report(args: argparse.Namespace) -> int:
    result = _run_compute(args)
    _json_print(
        {
            "ok": True,
            "code": "OK",
            "sample": args.sample,
            "sum_strategy": result.kernel.sum_strategy,
            "error_report": result.error_report,
            "nonfinite": result.nonfinite,
        }
    )
    return 0


def command_config(args: argparse.Namespace) -> int:
    workspace = OperatorWorkspace(getattr(args, "workspace_inner", None) or args.workspace)
    if args.config_command == "show":
        _json_print({"ok": True, "code": "OK", "config": workspace.config_state()})
        return 0
    if args.config_command == "verify":
        state = workspace.config_state()
        _json_print(
            {
                "ok": True,
                "code": "OK",
                "integrity": "valid" if state["status"] in {"valid", "empty"} else state["status"],
                "config": state,
            }
        )
        return 0
    if args.config_command == "publish":
        manifest = _load_manifest(args.manifest)
        record = _sample_entry(manifest, args.sample)
        selection = select_plan(
            record["m"],
            record["n"],
            record["k"],
            batch=record["batch"],
            dtype=record.get("dtype", "float64"),
            plan=_parse_plan(args.plan),
            l1_bytes=args.l1_bytes,
            l2_bytes=args.l2_bytes,
        )
        fingerprint = _sample_fingerprint(record)
        published = workspace.config_store.publish(
            fingerprint=fingerprint,
            plan=selection.plan,
            sum_strategy=args.strategy,
            cache_budget={"l1_bytes": args.l1_bytes, "l2_bytes": args.l2_bytes},
            selection_source=selection.workload_class,
            metadata={"sample": args.sample, "cli": True},
            expected_seq=args.expected_seq,
        )
        _json_print({"ok": True, "code": "OK", "published": published.to_public_dict()})
        return 0
    raise AssertionError("unreachable config subcommand")


def _sample_fingerprint(record: dict[str, Any]) -> str:
    from .shapes import shape_fingerprint

    return shape_fingerprint(
        batch=record["batch"],
        m=record["m"],
        n=record["n"],
        k=record["k"],
        trans_a=record["trans_a"],
        trans_b=record["trans_b"],
        dtype=record.get("dtype", "float64"),
        is_batched=record["mode"] == "bmm",
    )


def command_cache(args: argparse.Namespace) -> int:
    workspace = OperatorWorkspace(getattr(args, "workspace_inner", None) or args.workspace)
    if args.cache_command == "status":
        _json_print({"ok": True, "code": "OK", "cache": workspace.cache_status()})
        return 0
    if args.cache_command == "clear":
        result = workspace.clear_cache(expected_seq=args.expected_seq)
        _json_print({"ok": True, "code": "OK", "cache": result})
        return 0
    raise AssertionError("unreachable cache subcommand")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="gemm-lab", description="Blocked GEMM/BMM local laboratory")
    subparsers = parser.add_subparsers(dest="command", required=True)

    plan_parser = subparsers.add_parser("plan", help="query an auto or explicit block plan")
    plan_parser.add_argument("--sample")
    plan_parser.add_argument("--manifest", type=Path, default=Path("data/manifest.json"))
    plan_parser.add_argument("--m", type=int)
    plan_parser.add_argument("--n", type=int)
    plan_parser.add_argument("--k", type=int)
    plan_parser.add_argument("--batch", type=int, default=1)
    plan_parser.add_argument("--dtype", default="float64")
    plan_parser.add_argument("--plan", help="explicit MR,NR,MC,KC,NC")
    plan_parser.add_argument("--l1-bytes", type=int, default=DEFAULT_L1_BYTES)
    plan_parser.add_argument("--l2-bytes", type=int, default=DEFAULT_L2_BYTES)
    plan_parser.add_argument("--parallel-workers", type=int)
    plan_parser.set_defaults(handler=command_plan)

    def add_compute_arguments(target: argparse.ArgumentParser) -> None:
        target.add_argument("--sample", required=True)
        target.add_argument("--manifest", type=Path, default=Path("data/manifest.json"))
        target.add_argument("--strategy", choices=("sequential", "pairwise", "compensated"), default="sequential")
        target.add_argument("--accumulation-mode", choices=("auto", "panel", "scalar"), default="auto")
        target.add_argument("--plan", help="explicit MR,NR,MC,KC,NC")
        target.add_argument("--l1-bytes", type=int, default=DEFAULT_L1_BYTES)
        target.add_argument("--l2-bytes", type=int, default=DEFAULT_L2_BYTES)
        target.add_argument("--rtol", type=float)
        target.add_argument("--atol", type=float)
        target.add_argument("--memory-budget", type=int)
        target.add_argument("--workspace", type=Path)
        target.add_argument("--use-cache", action="store_true")
        target.add_argument("--publish-config", action="store_true")
        target.add_argument("--expected-config-seq", type=int)
        target.add_argument("--expected-cache-seq", type=int)
        target.add_argument("--no-adopt-stored-plan", action="store_true")
        target.add_argument("--no-verify", action="store_true")
        target.add_argument("--skip-input-digest", action="store_true")

    run_parser = subparsers.add_parser("run", help="compute one local sample")
    add_compute_arguments(run_parser)
    run_parser.add_argument("--output", type=Path)
    run_parser.set_defaults(handler=command_run)

    report_parser = subparsers.add_parser("report", help="compute and print only accuracy/nonfinite reports")
    add_compute_arguments(report_parser)
    report_parser.set_defaults(handler=command_report)

    config_parser = subparsers.add_parser("config", help="query or publish versioned tuning configuration")
    config_parser.add_argument("--workspace", type=Path, default=Path("data"))
    config_sub = config_parser.add_subparsers(dest="config_command", required=True)
    show_parser = config_sub.add_parser("show")
    show_parser.add_argument("--workspace", dest="workspace_inner", type=Path)
    show_parser.set_defaults(handler=command_config)
    verify_parser = config_sub.add_parser("verify")
    verify_parser.add_argument("--workspace", dest="workspace_inner", type=Path)
    verify_parser.set_defaults(handler=command_config)
    publish_parser = config_sub.add_parser("publish")
    publish_parser.add_argument("--workspace", dest="workspace_inner", type=Path)
    publish_parser.add_argument("--sample", required=True)
    publish_parser.add_argument("--manifest", type=Path, default=Path("data/manifest.json"))
    publish_parser.add_argument("--strategy", choices=("sequential", "pairwise", "compensated"), default="sequential")
    publish_parser.add_argument("--plan")
    publish_parser.add_argument("--l1-bytes", type=int, default=DEFAULT_L1_BYTES)
    publish_parser.add_argument("--l2-bytes", type=int, default=DEFAULT_L2_BYTES)
    publish_parser.add_argument("--expected-seq", type=int)
    publish_parser.set_defaults(handler=command_config)

    cache_parser = subparsers.add_parser("cache", help="inspect or clear the versioned result cache")
    cache_parser.add_argument("--workspace", type=Path, default=Path("data"))
    cache_sub = cache_parser.add_subparsers(dest="cache_command", required=True)
    status_parser = cache_sub.add_parser("status")
    status_parser.add_argument("--workspace", dest="workspace_inner", type=Path)
    status_parser.set_defaults(handler=command_cache)
    clear_parser = cache_sub.add_parser("clear")
    clear_parser.add_argument("--workspace", dest="workspace_inner", type=Path)
    clear_parser.add_argument("--expected-seq", type=int)
    clear_parser.set_defaults(handler=command_cache)

    subparsers.add_parser("version", help="print package version").set_defaults(handler=command_version)
    return parser


def command_version(args: argparse.Namespace) -> int:
    from . import __version__

    _json_print({"ok": True, "code": "OK", "version": __version__, "python": sys.version.split()[0], "numpy": np.__version__})
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.handler(args))
    except GemmError as exc:
        _json_print(exc.to_dict())
        return EXIT_CODES.get(exc.code, 1)
    except (OSError, ValueError) as exc:
        _json_print(
            {
                "ok": False,
                "code": "E_INVALID_SHAPE",
                "message": str(exc),
                "details": {"exception": type(exc).__name__},
            }
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

