"""Command-line interface for library integration and local acceptance checks."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys
from typing import Any

import numpy as np

from .blocks import BlockScheme, select_block_scheme
from .csr import load_csr_npz, load_dense_npy
from .engine import RunOptions, spmm
from .errors import SpMMError
from .samples import generate_samples, load_manifest
from .util import atomic_write_bytes, json_safe
from .version_store import VersionStore


EXIT_CODES = {
    "E_SHAPE_INVALID": 10,
    "E_BAD_CSR": 11,
    "E_DUP_INDEX": 12,
    "E_NUMERIC_MISMATCH": 13,
    "E_MEMORY_BUDGET": 14,
    "E_STALE_PUBLISH": 15,
    "E_REFERENCE_MISSING": 16,
    "E_REFERENCE_INVALID": 17,
    "E_DTYPE_UNSUPPORTED": 18,
    "E_DTYPE_MISMATCH": 19,
    "E_LOCK_TIMEOUT": 20,
}


def _emit(value: Any) -> None:
    encoded = json.dumps(json_safe(value), sort_keys=True, ensure_ascii=False, allow_nan=False, indent=2)
    sys.stdout.write(encoded + "\n")


def _write_json(path: str | os.PathLike[str], value: Any) -> None:
    encoded = json.dumps(json_safe(value), sort_keys=True, ensure_ascii=False, allow_nan=False, indent=2).encode("utf-8") + b"\n"
    atomic_write_bytes(path, encoded)


def _add_run_options(parser: argparse.ArgumentParser, *, audit_default: bool = True) -> None:
    parser.add_argument("--sparse", required=True, help="CSR .npz file")
    parser.add_argument("--dense", required=True, help="right-hand dense .npy file")
    parser.add_argument("--strategy", choices=("sequential", "compensated"), default="sequential")
    parser.add_argument("--order", choices=("ascending", "block"), default="ascending")
    parser.add_argument("--row-block", type=int, default=None)
    parser.add_argument("--col-block", type=int, default=None)
    parser.add_argument("--cache-budget", type=int, default=512 * 1024)
    parser.add_argument("--memory-limit", type=int, default=4 * 1024 * 1024)
    parser.add_argument("--duplicate-tolerance", type=int, default=1)
    parser.add_argument("--reference", default=None)
    parser.add_argument("--no-audit", action="store_true", default=not audit_default)
    parser.add_argument("--sampling-seed", type=int, default=20261121)
    parser.add_argument("--sampling-count", type=int, default=64)
    parser.add_argument("--self-check-blocks", type=int, default=3)
    parser.add_argument("--atol", type=float, default=1e-10)
    parser.add_argument("--rtol", type=float, default=1e-10)
    parser.add_argument("--store", default=None, help="version-store directory")
    parser.add_argument("--no-cache", action="store_true")
    parser.add_argument("--expected-sequence", type=int, default=None)
    parser.add_argument("--expected-digest", default=None)
    parser.add_argument("--lock-timeout", type=float, default=30.0)
    parser.add_argument("--max-nonfinite-sources", type=int, default=10_000)
    parser.add_argument("--output", default=None, help="optional result .npy path")
    parser.add_argument("--report", default=None, help="optional full audit report JSON path")
    parser.add_argument("--summary", default=None, help="optional summary JSON path")
    parser.add_argument("--include-plan-candidates", action="store_true")


def _options_from_args(args: argparse.Namespace) -> RunOptions:
    block_scheme: BlockScheme | tuple[int, int] | None = None
    if args.row_block is not None or args.col_block is not None:
        if args.row_block is None or args.col_block is None:
            raise SpMMError("E_USAGE", "--row-block and --col-block must be supplied together")
        block_scheme = (args.row_block, args.col_block)
    return RunOptions(
        sum_strategy=args.strategy,
        order=args.order,
        block_scheme=block_scheme,
        cache_budget_bytes=args.cache_budget,
        memory_limit_bytes=args.memory_limit,
        duplicate_tolerance=args.duplicate_tolerance,
        audit=not args.no_audit,
        reference_path=args.reference,
        sampling_seed=args.sampling_seed,
        sampling_count=args.sampling_count,
        self_check_blocks=args.self_check_blocks,
        absolute_tolerance=args.atol,
        relative_tolerance=args.rtol,
        version_store=args.store,
        use_cache=not args.no_cache,
        expected_version=args.expected_sequence,
        expected_digest=args.expected_digest,
        lock_timeout_seconds=args.lock_timeout,
        max_nonfinite_sources=args.max_nonfinite_sources,
        output_path=args.output,
    )


def command_generate(args: argparse.Namespace) -> int:
    manifest = generate_samples(args.out)
    _emit(
        {
            "ok": True,
            "manifest": str(Path(args.out) / "manifest.json"),
            "manifest_digest": manifest["manifest_digest"],
            "sample_count": len(manifest["samples"]),
            "samples": [
                {
                    "name": item["name"],
                    "shape": item["shape"],
                    "density": item["actual_density"],
                    "nnz": item["nnz"],
                }
                for item in manifest["samples"]
            ],
        }
    )
    return 0


def command_info(args: argparse.Namespace) -> int:
    if args.sparse:
        matrix = load_csr_npz(args.sparse)
        duplicate = matrix.validate_duplicates(args.duplicate_tolerance)
        _emit({"ok": True, "features": matrix.features(), "duplicate_validation": duplicate})
    else:
        manifest = load_manifest(args.manifest)
        _emit({"ok": True, "manifest": manifest})
    return 0


def command_select(args: argparse.Namespace) -> int:
    matrix = load_csr_npz(args.sparse)
    if args.dense:
        dense_cols = int(np.load(args.dense, mmap_mode="r", allow_pickle=False).shape[1])
    elif args.dense_cols is not None:
        dense_cols = int(args.dense_cols)
    else:
        raise SpMMError("E_USAGE", "provide --dense or --dense-cols")
    matrix.validate_duplicates(args.duplicate_tolerance)
    plan = select_block_scheme(
        matrix,
        dense_cols,
        cache_budget_bytes=args.cache_budget,
        sum_strategy=args.strategy,
    )
    _emit({"ok": True, "plan": plan.to_dict()})
    return 0


def command_run(args: argparse.Namespace) -> int:
    options = _options_from_args(args)
    result = spmm(args.sparse, args.dense, options=options)
    summary = result.to_summary(include_plan_candidates=args.include_plan_candidates)
    _emit(summary)
    if args.report and result.audit_report is not None:
        _write_json(args.report, {"ok": True, "audit": result.audit_report})
    if args.summary:
        _write_json(args.summary, summary)
    return 0


def command_audit(args: argparse.Namespace) -> int:
    options = _options_from_args(args)
    options.audit = True
    result = spmm(args.sparse, args.dense, options=options)
    if result.audit_report is None:
        raise SpMMError("E_REFERENCE_MISSING", "audit did not produce a report")
    _emit({"ok": True, "audit": result.audit_report})
    if args.report:
        _write_json(args.report, {"ok": True, "audit": result.audit_report})
    return 0


def command_version_status(args: argparse.Namespace) -> int:
    store = VersionStore(args.store, lock_timeout=args.lock_timeout)
    status = store.status(
        expected_sequence=args.expected_sequence,
        expected_digest=args.expected_digest,
    )
    _emit({"ok": True, "version_status": status})
    return 0


def command_cache_list(args: argparse.Namespace) -> int:
    store = VersionStore(args.store, lock_timeout=args.lock_timeout)
    _emit({"ok": True, "cache": store.list_cache(), "version_status": store.status()})
    return 0


def command_cache_clear(args: argparse.Namespace) -> int:
    store = VersionStore(args.store, lock_timeout=args.lock_timeout)
    _emit({"ok": True, "cache_clear": store.clear_cache()})
    return 0


def command_cache_prune(args: argparse.Namespace) -> int:
    store = VersionStore(args.store, lock_timeout=args.lock_timeout)
    _emit({"ok": True, "cache_prune": store.prune_cache()})
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="spmm-ops", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    sample = sub.add_parser("generate-samples", help="generate deterministic local matrices and Decimal references")
    sample.add_argument("--out", default="samples")
    sample.set_defaults(handler=command_generate, lock_timeout=30.0)

    info = sub.add_parser("info", help="show sample manifest or CSR features")
    info.add_argument("--manifest", default="samples/manifest.json")
    info.add_argument("--sparse", default=None)
    info.add_argument("--duplicate-tolerance", type=int, default=1)
    info.set_defaults(handler=command_info, lock_timeout=30.0)

    select = sub.add_parser("select", help="show automatic block scoring and chosen plan")
    select.add_argument("--sparse", required=True)
    select.add_argument("--dense", default=None)
    select.add_argument("--dense-cols", type=int, default=None)
    select.add_argument("--strategy", choices=("sequential", "compensated"), default="sequential")
    select.add_argument("--cache-budget", type=int, default=512 * 1024)
    select.add_argument("--duplicate-tolerance", type=int, default=1)
    select.add_argument("--lock-timeout", type=float, default=30.0)
    select.set_defaults(handler=command_select)

    run = sub.add_parser("run", help="execute SpMM, optional audit, cache, and version publication")
    _add_run_options(run)
    run.set_defaults(handler=command_run)

    audit = sub.add_parser("audit", help="execute and print the full high-precision error report")
    _add_run_options(audit)
    audit.set_defaults(handler=command_audit, no_audit=False)

    status = sub.add_parser("version-status", help="inspect version sequence, digest chain, and cache state")
    status.add_argument("--store", required=True)
    status.add_argument("--expected-sequence", type=int, default=None)
    status.add_argument("--expected-digest", default=None)
    status.add_argument("--lock-timeout", type=float, default=30.0)
    status.set_defaults(handler=command_version_status)

    cache = sub.add_parser("cache", help="cache management")
    cache_sub = cache.add_subparsers(dest="cache_command", required=True)
    for name, handler in (("list", command_cache_list), ("clear", command_cache_clear), ("prune", command_cache_prune)):
        item = cache_sub.add_parser(name)
        item.add_argument("--store", required=True)
        item.add_argument("--lock-timeout", type=float, default=30.0)
        item.set_defaults(handler=handler)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.handler(args))
    except SpMMError as exc:
        _emit(exc.to_dict())
        return EXIT_CODES.get(exc.code, 1)
    except (OSError, ValueError) as exc:
        error = SpMMError("E_IO", str(exc), {"cause": str(exc)})
        _emit(error.to_dict())
        return 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

