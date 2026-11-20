"""Command line interface for the quantization operator library."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Dict

from .audit import export_audit_report, query_fingerprints, query_status
from .errors import QopError
from .fingerprints import canonical_json_text
from .io_util import load_npz
from .manifest import load_manifest, manifest_config_fingerprint
from .runner import request_stop, run
from .scheduler import plan_schedule


def _print(obj: Dict[str, Any]) -> None:
    sys.stdout.write(canonical_json_text(obj) + "\n")


def _cmd_generate(args: argparse.Namespace) -> int:
    from scripts.generate_data import generate_demo_dataset

    generate_demo_dataset(Path(args.data_dir), seed=int(args.seed))
    _print({"status": "generated", "data_dir": args.data_dir})
    return 0


def _cmd_schedule(args: argparse.Namespace) -> int:
    manifest = load_manifest(args.manifest)
    decision = plan_schedule(manifest, max_workers=args.workers)
    _print(decision.to_dict())
    return 0


def _cmd_run(args: argparse.Namespace) -> int:
    outcome = run(
        args.manifest, args.data_dir, args.run_dir,
        workers=args.workers, serial=args.serial,
    )
    _print(outcome.to_dict())
    return 0


def _cmd_stop(args: argparse.Namespace) -> int:
    path = request_stop(args.run_dir)
    _print({"status": "stop_requested", "path": str(path)})
    return 0


def _cmd_resume(args: argparse.Namespace) -> int:
    # Resume is the same entry point: completed stage checkpoints are reused
    # and only missing shards are re-dispatched.
    outcome = run(
        args.manifest, args.data_dir, args.run_dir,
        workers=args.workers, serial=args.serial,
    )
    _print(outcome.to_dict())
    return 0


def _cmd_status(args: argparse.Namespace) -> int:
    _print(query_status(args.run_dir))
    return 0


def _cmd_fingerprints(args: argparse.Namespace) -> int:
    _print({"fingerprints": query_fingerprints(args.run_dir)})
    return 0


def _cmd_audit(args: argparse.Namespace) -> int:
    path = export_audit_report(args.run_dir)
    _print({"status": "exported", "path": str(path)})
    return 0


def _cmd_manifest_fp(args: argparse.Namespace) -> int:
    manifest = load_manifest(args.manifest)
    _print({"manifest_fingerprint": manifest_config_fingerprint(manifest),
            "config_version": manifest.config_version})
    return 0


def _cmd_inspect_npz(args: argparse.Namespace) -> int:
    arrays = load_npz(args.npz)
    _print({name: {"shape": list(array.shape), "dtype": str(array.dtype)}
            for name, array in arrays.items()})
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="qoplib",
        description="Deterministic low-precision quantization operator library")
    sub = parser.add_subparsers(dest="command", required=True)

    gen = sub.add_parser("generate", help="generate deterministic demo data")
    gen.add_argument("--data-dir", required=True)
    gen.add_argument("--seed", type=int, default=20261121)
    gen.set_defaults(func=_cmd_generate)

    sched = sub.add_parser("schedule", help="show scheduling decision")
    sched.add_argument("--manifest", required=True)
    sched.add_argument("--workers", type=int, default=None)
    sched.set_defaults(func=_cmd_schedule)

    run_cmd = sub.add_parser("run", help="run all accepted shards")
    run_cmd.add_argument("--manifest", required=True)
    run_cmd.add_argument("--data-dir", required=True)
    run_cmd.add_argument("--run-dir", required=True)
    run_cmd.add_argument("--workers", type=int, default=None)
    run_cmd.add_argument("--serial", action="store_true")
    run_cmd.set_defaults(func=_cmd_run)

    stop = sub.add_parser("stop", help="request graceful stop")
    stop.add_argument("--run-dir", required=True)
    stop.set_defaults(func=_cmd_stop)

    resume = sub.add_parser("resume", help="resume a stopped/partial run")
    resume.add_argument("--manifest", required=True)
    resume.add_argument("--data-dir", required=True)
    resume.add_argument("--run-dir", required=True)
    resume.add_argument("--workers", type=int, default=None)
    resume.add_argument("--serial", action="store_true")
    resume.set_defaults(func=_cmd_resume)

    status = sub.add_parser("status", help="query run state")
    status.add_argument("--run-dir", required=True)
    status.set_defaults(func=_cmd_status)

    fps = sub.add_parser("fingerprints", help="list stage fingerprints")
    fps.add_argument("--run-dir", required=True)
    fps.set_defaults(func=_cmd_fingerprints)

    audit = sub.add_parser("audit", help="export audit report")
    audit.add_argument("--run-dir", required=True)
    audit.set_defaults(func=_cmd_audit)

    mfp = sub.add_parser("manifest-fingerprint", help="manifest version fp")
    mfp.add_argument("--manifest", required=True)
    mfp.set_defaults(func=_cmd_manifest_fp)

    inspect = sub.add_parser("inspect-npz", help="inspect a shard npz file")
    inspect.add_argument("--npz", required=True)
    inspect.set_defaults(func=_cmd_inspect_npz)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except QopError as exc:
        sys.stdout.write(canonical_json_text(exc.to_dict()) + "\n")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
