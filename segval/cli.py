"""Command-line interface for the local validation workbench."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from .canonical import atomic_write_bytes, canonical_bytes, canonical_text
from .config import DEFAULT_CONFIG
from .demo_generator import generate_demo_shards
from .diagnostics import checkpoint_fault_probe, doctor
from .engine import (
    collect_exports,
    fingerprints_payload,
    request_stop,
    resume_workbench,
    run_workbench,
    sample_view,
    status_payload,
    update_shard,
    verify_incremental,
)
from .errors import WorkbenchError
from .http_server import serve
from .shards import import_shard, load_shard_file
from .workspace import create_workspace, Workspace


def _print_json(value: Any) -> None:
    print(canonical_text(value, pretty=True))


def _workspace(args: argparse.Namespace) -> Workspace:
    return Workspace(Path(args.workspace)).initialize()


def cmd_init(args: argparse.Namespace) -> int:
    workspace = create_workspace(Path(args.workspace), config=DEFAULT_CONFIG)
    _print_json({"ok": True, "workspace": str(workspace.root), "config": workspace.config()})
    return 0


def cmd_generate_demo(args: argparse.Namespace) -> int:
    generated = generate_demo_shards(Path(args.out), seed=args.seed, shard_count=args.shards)
    _print_json({"ok": True, "generated": generated})
    return 0


def cmd_setup_demo(args: argparse.Namespace) -> int:
    workspace = _workspace(args)
    data_dir = Path(args.data_dir)
    if not any(data_dir.glob("shard-*.json")):
        generate_demo_shards(data_dir, seed=args.seed, shard_count=args.shards)
    imported = []
    for path in sorted(data_dir.glob("shard-*.json")):
        imported.append(import_shard(workspace.root, load_shard_file(path)))
    result: dict[str, Any] = {"ok": True, "workspace": str(workspace.root), "imported": imported}
    if args.run:
        result["run"] = run_workbench(
            workspace.root,
            target_stage="all",
            workers=args.workers,
            clear_stop=True,
        )
    _print_json(result)
    return 0


def cmd_import_shard(args: argparse.Namespace) -> int:
    workspace = _workspace(args)
    shard = load_shard_file(Path(args.file))
    _print_json({"ok": True, "import": import_shard(workspace.root, shard)})
    return 0


def cmd_update_shard(args: argparse.Namespace) -> int:
    workspace = _workspace(args)
    shard = load_shard_file(Path(args.file))
    result = update_shard(workspace.root, shard, workers=args.workers)
    _print_json(result)
    return 0 if result.get("ok") else 2


def cmd_run(args: argparse.Namespace) -> int:
    result = run_workbench(
        Path(args.workspace),
        target_stage=args.stage,
        workers=args.workers,
        only_shard_ids=args.shard,
        stop_after_samples=args.stop_after_samples,
        clear_stop=args.clear_stop,
    )
    _print_json(result)
    return 0 if result.get("ok") else 2


def cmd_resume(args: argparse.Namespace) -> int:
    result = resume_workbench(Path(args.workspace), workers=args.workers)
    _print_json(result)
    return 0 if result.get("ok") else 2


def cmd_stop(args: argparse.Namespace) -> int:
    _print_json(request_stop(Path(args.workspace), args.reason))
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    _print_json(status_payload(Path(args.workspace)))
    return 0


def cmd_fingerprints(args: argparse.Namespace) -> int:
    _print_json(fingerprints_payload(Path(args.workspace)))
    return 0


def cmd_budget(args: argparse.Namespace) -> int:
    status = status_payload(Path(args.workspace))
    _print_json(
        {
            "budget": status["budget"],
            "accepted": status["accepted"],
            "unprocessed": status["unprocessed"],
            "worker_lanes": status["worker_lanes"],
            "planned_concurrency": status["planned_concurrency"],
            "schedule_fingerprint": status["schedule_fingerprint"],
        }
    )
    return 0


def cmd_samples(args: argparse.Namespace) -> int:
    _print_json(sample_view(Path(args.workspace), shard_id=args.shard, image_id=args.image))
    return 0


def cmd_export(args: argparse.Namespace) -> int:
    payloads = collect_exports(Path(args.workspace), write=True)
    if args.kind not in payloads:
        raise WorkbenchError("E_INVALID_REQUEST", "Unknown export kind.", {"kind": args.kind}, 400)
    data = canonical_bytes(payloads[args.kind]) + b"\n"
    if args.output:
        atomic_write_bytes(Path(args.output), data)
        _print_json(
            {
                "ok": True,
                "kind": args.kind,
                "output": str(Path(args.output).resolve()),
                "bytes": len(data),
            }
        )
    else:
        sys.stdout.buffer.write(data)
    return 0


def cmd_verify_incremental(args: argparse.Namespace) -> int:
    report = verify_incremental(
        Path(args.workspace),
        auto_rebuild=not args.no_auto_rebuild,
        workers=args.workers,
        inject_mismatch=args.inject_mismatch,
    )
    _print_json(report)
    return 0


def cmd_config_show(args: argparse.Namespace) -> int:
    _print_json(_workspace(args).config())
    return 0


def cmd_config_set(args: argparse.Namespace) -> int:
    patch: dict[str, Any] = {}
    if args.iou_threshold is not None:
        patch.setdefault("matching", {})["iou_threshold"] = args.iou_threshold
    if args.duplicate_confidence_threshold is not None:
        patch.setdefault("matching", {})["duplicate_confidence_threshold"] = args.duplicate_confidence_threshold
    if args.match_strategy is not None:
        patch.setdefault("matching", {})["strategy"] = args.match_strategy
    if args.out_of_bounds_policy is not None:
        patch.setdefault("decode", {})["out_of_bounds_policy"] = args.out_of_bounds_policy
    if args.precision is not None:
        patch.setdefault("decode", {})["precision"] = args.precision
    if args.max_evaluations is not None:
        patch.setdefault("budget", {})["max_evaluations"] = args.max_evaluations
    if args.per_worker_quota is not None:
        patch.setdefault("budget", {})["per_worker_quota"] = args.per_worker_quota
    if args.max_workers is not None:
        patch.setdefault("budget", {})["max_workers"] = args.max_workers
    if args.config_version is not None:
        patch["config_version"] = args.config_version
    if not patch:
        raise WorkbenchError("E_INVALID_REQUEST", "No configuration fields were provided.")
    workspace = _workspace(args)
    config = workspace.update_config(patch)
    _print_json({"ok": True, "config": config, "config_fingerprint": fingerprints_payload(workspace.root)["config_fingerprint"]})
    return 0


def cmd_serve(args: argparse.Namespace) -> int:
    workspace = _workspace(args)
    print(
        json.dumps(
            {
                "ok": True,
                "url": f"http://{args.host}:{args.port}/",
                "workspace": str(workspace.root),
                "bind": "loopback-only",
            },
            ensure_ascii=False,
        ),
        flush=True,
    )
    serve(workspace.root, host=args.host, port=args.port)
    return 0


def cmd_doctor(args: argparse.Namespace) -> int:
    report = doctor()
    _print_json(report)
    return 0 if report["ok"] else 1


def cmd_probe(args: argparse.Namespace) -> int:
    report = checkpoint_fault_probe(Path(args.workspace) if args.workspace else None)
    _print_json(report)
    return 0 if report["ok"] else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Local deterministic instance-segmentation validation workbench")
    subparsers = parser.add_subparsers(dest="command", required=True)

    init = subparsers.add_parser("init", help="Initialize an empty local workspace")
    init.add_argument("--workspace", required=True)
    init.set_defaults(func=cmd_init)

    generate = subparsers.add_parser("generate-demo", help="Generate deterministic local JSON shards")
    generate.add_argument("--out", default="demo_data")
    generate.add_argument("--seed", type=int, default=20261123)
    generate.add_argument("--shards", type=int, default=4)
    generate.set_defaults(func=cmd_generate_demo)

    setup = subparsers.add_parser("setup-demo", help="Generate/import demo shards into a workspace")
    setup.add_argument("--workspace", required=True)
    setup.add_argument("--data-dir", default="demo_data")
    setup.add_argument("--seed", type=int, default=20261123)
    setup.add_argument("--shards", type=int, default=4)
    setup.add_argument("--run", action="store_true")
    setup.add_argument("--workers", type=int, default=None)
    setup.set_defaults(func=cmd_setup_demo)

    imp = subparsers.add_parser("import-shard", help="Validate and atomically import one shard")
    imp.add_argument("--workspace", required=True)
    imp.add_argument("--file", required=True)
    imp.set_defaults(func=cmd_import_shard)

    update = subparsers.add_parser("update-shard", help="Import one shard and rebuild only its invalidated chain")
    update.add_argument("--workspace", required=True)
    update.add_argument("--file", required=True)
    update.add_argument("--workers", type=int, default=None)
    update.set_defaults(func=cmd_update_shard)

    run = subparsers.add_parser("run", help="Trigger pipeline stages under budget constraints")
    run.add_argument("--workspace", required=True)
    run.add_argument("--stage", choices=("all", "decode", "match", "aggregate", "schedule"), default="all")
    run.add_argument("--workers", type=int, default=None)
    run.add_argument("--shard", action="append", default=None, help="Limit execution to a shard id; repeatable")
    run.add_argument("--stop-after-samples", type=int, default=None)
    run.add_argument("--clear-stop", action="store_true")
    run.set_defaults(func=cmd_run)

    resume = subparsers.add_parser("resume", help="Clear stop request and continue outstanding work")
    resume.add_argument("--workspace", required=True)
    resume.add_argument("--workers", type=int, default=None)
    resume.set_defaults(func=cmd_resume)

    stop = subparsers.add_parser("stop", help="Cooperative stop: exit after the current sample unit")
    stop.add_argument("--workspace", required=True)
    stop.add_argument("--reason", default="cli_stop")
    stop.set_defaults(func=cmd_stop)

    status = subparsers.add_parser("status", help="Show status, fingerprints, budget, and diagnostics")
    status.add_argument("--workspace", required=True)
    status.set_defaults(func=cmd_status)

    fp = subparsers.add_parser("fingerprints", help="Show stage fingerprints and invalidation chains")
    fp.add_argument("--workspace", required=True)
    fp.set_defaults(func=cmd_fingerprints)

    budget = subparsers.add_parser("budget", help="Show budget admission decisions and unprocessed reasons")
    budget.add_argument("--workspace", required=True)
    budget.set_defaults(func=cmd_budget)

    samples = subparsers.add_parser("samples", help="Show decoded polygons with match classifications")
    samples.add_argument("--workspace", required=True)
    samples.add_argument("--shard", default=None)
    samples.add_argument("--image", default=None)
    samples.set_defaults(func=cmd_samples)

    export = subparsers.add_parser("export", help="Export canonical results")
    export.add_argument("--workspace", required=True)
    export.add_argument("--kind", choices=("matches", "aggregates", "schedule", "diagnostics", "bundle"), default="bundle")
    export.add_argument("--output", default=None)
    export.set_defaults(func=cmd_export)

    verify = subparsers.add_parser("verify-incremental", help="Compare incremental results byte-for-byte with a fresh full build")
    verify.add_argument("--workspace", required=True)
    verify.add_argument("--workers", type=int, default=1)
    verify.add_argument("--no-auto-rebuild", action="store_true")
    verify.add_argument("--inject-mismatch", action="store_true", help="Fault-inject one byte difference for E_INCREMENTAL_MISMATCH")
    verify.set_defaults(func=cmd_verify_incremental)

    config_show = subparsers.add_parser("config-show", help="Show current config")
    config_show.add_argument("--workspace", required=True)
    config_show.set_defaults(func=cmd_config_show)

    config_set = subparsers.add_parser("config-set", help="Atomically update one or more config fields")
    config_set.add_argument("--workspace", required=True)
    config_set.add_argument("--iou-threshold", type=float, default=None)
    config_set.add_argument("--duplicate-confidence-threshold", type=float, default=None)
    config_set.add_argument(
        "--match-strategy",
        choices=("greedy_global_iou", "greedy_global_iou_confidence_tiebreak"),
        default=None,
    )
    config_set.add_argument("--out-of-bounds-policy", choices=("clamp", "reject"), default=None)
    config_set.add_argument("--precision", type=int, default=None)
    config_set.add_argument("--max-evaluations", type=int, default=None)
    config_set.add_argument("--per-worker-quota", type=int, default=None)
    config_set.add_argument("--max-workers", type=int, default=None)
    config_set.add_argument("--config-version", default=None)
    config_set.set_defaults(func=cmd_config_set)

    serve_parser = subparsers.add_parser("serve", help="Start the loopback-only HTTP workbench")
    serve_parser.add_argument("--workspace", required=True)
    serve_parser.add_argument("--host", default="127.0.0.1")
    serve_parser.add_argument("--port", type=int, default=8765)
    serve_parser.set_defaults(func=cmd_serve)

    doctor_parser = subparsers.add_parser("doctor", help="Check Python, NumPy, locks, spawn, atomic replace, and loopback")
    doctor_parser.set_defaults(func=cmd_doctor)

    probe = subparsers.add_parser("probe-checkpoints", help="Inject stale/missing-stage faults and concurrent publication")
    probe.add_argument("--workspace", default=None)
    probe.set_defaults(func=cmd_probe)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except WorkbenchError as exc:
        print(
            json.dumps({"ok": False, "error": exc.to_dict()}, ensure_ascii=False, indent=2),
            file=sys.stdout,
        )
        return 2
    except KeyboardInterrupt:
        print(json.dumps({"ok": False, "error": {"code": "E_INTERRUPTED", "message": "Interrupted"}}))
        return 130


if __name__ == "__main__":
    raise SystemExit(main())

