"""Command-line entry point for the local anomaly diagnostics workbench."""

from __future__ import annotations

import argparse
import json
from typing import Any, Sequence

from .config import parse_set_overrides
from .data import DEMO_SEED, generate_demo
from .demo import run_demo
from .errors import WorkbenchError
from .pipeline import Pipeline
from .reference import generate_reference, verify_reference
from .util import read_json
from .web import serve


def _json_print(value: Any) -> None:
    print(json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False))


def _tiers(value: str | None) -> list[str] | None:
    if value is None or value == "all":
        return None
    return [part.strip() for part in value.split(",") if part.strip()]


def _stop_after(values: list[str] | None) -> set[tuple[str, str]] | None:
    if not values:
        return None
    result: set[tuple[str, str]] = set()
    for value in values:
        if ":" not in value:
            raise WorkbenchError("E_DATA", "--stop-after expects tier:stage", details={"value": value})
        tier, stage = value.split(":", 1)
        result.add((tier, stage))
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="anomaly-workbench",
        description="Deterministic local anomaly detection and incremental diagnostics workbench",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    init = subparsers.add_parser("init", help="initialize an empty workspace")
    init.add_argument("--root", default=".workbench")

    generate = subparsers.add_parser("generate", help="generate fixed-seed demo data")
    generate.add_argument("--root", default=".workbench")
    generate.add_argument("--seed", type=int, default=DEMO_SEED)
    generate.add_argument("--force", action="store_true")

    import_parser = subparsers.add_parser("import", help="import local NPZ shards and declaration")
    import_parser.add_argument("--root", default=".workbench")
    import_parser.add_argument("--tier", required=True)
    import_parser.add_argument("--declaration", required=True)
    import_parser.add_argument("--shard", action="append", required=True)
    import_parser.add_argument("--append", action="store_true", help="reject duplicate batch ids")
    import_parser.add_argument("--execute", action="store_true")
    import_parser.add_argument("--verify-full", action="store_true")

    replace = subparsers.add_parser("replace-shard", help="atomically replace one batch shard")
    replace.add_argument("--root", default=".workbench")
    replace.add_argument("--tier", required=True)
    replace.add_argument("--shard", required=True)
    replace.add_argument("--declaration")
    replace.add_argument("--execute", action="store_true")
    replace.add_argument("--verify-full", action="store_true")

    config = subparsers.add_parser("config", help="change configuration")
    config.add_argument("--root", default=".workbench")
    config.add_argument("--set", action="append", default=[])
    config.add_argument("--json", dest="patch_json")
    config.add_argument("--file")
    config.add_argument("--execute", action="store_true")
    config.add_argument("--tiers")
    config.add_argument("--verify-full", action="store_true")

    run = subparsers.add_parser("run", help="execute four stages")
    run.add_argument("--root", default=".workbench")
    run.add_argument("--tiers")
    run.add_argument("--verify-reference", action="store_true")
    run.add_argument("--stop-after", action="append")
    run.add_argument("--through-stage", choices=("preprocess", "fit", "score", "diagnostics"))

    resume = subparsers.add_parser("resume", help="clear stop request and continue")
    resume.add_argument("--root", default=".workbench")
    resume.add_argument("--tiers")
    resume.add_argument("--verify-reference", action="store_true")
    resume.add_argument("--through-stage", choices=("preprocess", "fit", "score", "diagnostics"))

    stop = subparsers.add_parser("stop", help="request a graceful stop at the next stage boundary")
    stop.add_argument("--root", default=".workbench")

    status = subparsers.add_parser("status", help="show stage status and version fingerprints")
    status.add_argument("--root", default=".workbench")
    status.add_argument("--tier")
    status.add_argument("--include-records", action="store_true")

    sensitivity = subparsers.add_parser("sensitivity", help="show threshold sensitivity curve")
    sensitivity.add_argument("--root", default=".workbench")
    sensitivity.add_argument("--tier", default="small")

    summary = subparsers.add_parser("summary", help="show the deterministic two-tier summary")
    summary.add_argument("--root", default=".workbench")

    invalidations = subparsers.add_parser("invalidations", help="show invalidation reasons")
    invalidations.add_argument("--root", default=".workbench")
    invalidations.add_argument("--tier")

    export = subparsers.add_parser("export", help="export scores and diagnostics")
    export.add_argument("--root", default=".workbench")
    export.add_argument("--tier", default="all")
    export.add_argument("--out")

    reference = subparsers.add_parser("reference", help="generate local numerical reference truth")
    reference.add_argument("--root", default=".workbench")
    reference.add_argument("--tiers")

    verify_reference_parser = subparsers.add_parser(
        "verify-reference", help="compare current artifacts with local reference truth"
    )
    verify_reference_parser.add_argument("--root", default=".workbench")

    verify_full = subparsers.add_parser(
        "verify-full", help="compare incremental state with a from-zero build"
    )
    verify_full.add_argument("--root", default=".workbench")
    verify_full.add_argument("--tier", required=True)

    serve_parser = subparsers.add_parser("serve", help="start the local HTTP workbench")
    serve_parser.add_argument("--root", default=".workbench")
    serve_parser.add_argument("--host", default="127.0.0.1")
    serve_parser.add_argument("--port", type=int, default=8765)

    demo = subparsers.add_parser("demo", help="run the complete deterministic acceptance demo")
    demo.add_argument("--root", default=".demo-workspace")
    demo.add_argument("--seed", type=int, default=DEMO_SEED)
    demo.add_argument("--force", action="store_true")
    demo.add_argument("--report")

    return parser


def _merge_patch(args: argparse.Namespace) -> dict[str, Any]:
    patch: dict[str, Any] = {}
    if args.file:
        loaded = read_json(args.file)
        if not isinstance(loaded, dict):
            raise WorkbenchError("E_INVALID_CONFIG", "--file must contain a JSON object")
        patch.update(loaded)
    if args.patch_json:
        loaded = json.loads(args.patch_json)
        if not isinstance(loaded, dict):
            raise WorkbenchError("E_INVALID_CONFIG", "--json must contain a JSON object")
        from .config import merge_config

        patch = merge_config(patch, loaded)
    from .config import merge_config

    patch = merge_config(patch, parse_set_overrides(args.set))
    return patch


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "init":
            pipeline = Pipeline(args.root)
            _json_print(
                {
                    "ok": True,
                    "root": str(pipeline.root),
                    "config": pipeline.config,
                }
            )
            return 0
        if args.command == "generate":
            _json_print(
                {
                    "ok": True,
                    "manifest": generate_demo(args.root, seed=args.seed, force=args.force),
                }
            )
            return 0
        if args.command == "import":
            pipeline = Pipeline(args.root)
            result = pipeline.import_dataset(
                args.tier, args.declaration, args.shard, replace=not args.append
            )
            execution = pipeline.run(tiers=[args.tier]) if args.execute else None
            verification = (
                pipeline.verify_full_equivalence(args.tier)
                if args.verify_full and args.execute
                else None
            )
            _json_print({"ok": True, "import": result, "execution": execution, "verification": verification})
            return 0
        if args.command == "replace-shard":
            pipeline = Pipeline(args.root)
            result = pipeline.replace_shard(args.tier, args.shard, args.declaration)
            execution = pipeline.run(tiers=[args.tier]) if args.execute else None
            verification = (
                pipeline.verify_full_equivalence(args.tier)
                if args.verify_full and args.execute
                else None
            )
            _json_print(
                {
                    "ok": True,
                    "replacement": result,
                    "execution": execution,
                    "verification": verification,
                }
            )
            return 0
        if args.command == "config":
            pipeline = Pipeline(args.root)
            patch = _merge_patch(args)
            result = pipeline.update_config(patch)
            execution = None
            verification = None
            if args.execute:
                execution = pipeline.run(tiers=_tiers(args.tiers))
                if args.verify_full:
                    verification = {
                        tier: pipeline.verify_full_equivalence(tier)
                        for tier in (_tiers(args.tiers) or ["small", "medium"])
                    }
            _json_print(
                {"ok": True, "config_change": result, "execution": execution, "verification": verification}
            )
            return 0
        if args.command == "run":
            pipeline = Pipeline(args.root)
            _json_print(
                {
                    "ok": True,
                    "execution": pipeline.run(
                        tiers=_tiers(args.tiers),
                        verify_reference=args.verify_reference,
                        stop_after=_stop_after(args.stop_after),
                        through_stage=args.through_stage,
                    ),
                }
            )
            return 0
        if args.command == "resume":
            pipeline = Pipeline(args.root)
            _json_print(
                {
                    "ok": True,
                    "execution": pipeline.resume(
                        tiers=_tiers(args.tiers),
                        verify_reference=args.verify_reference,
                        through_stage=args.through_stage,
                    ),
                }
            )
            return 0
        if args.command == "stop":
            _json_print({"ok": True, "stop": Pipeline(args.root).request_stop()})
            return 0
        if args.command == "status":
            status = Pipeline(args.root).status(include_records=args.include_records)
            if args.tier:
                if args.tier not in status["tiers"]:
                    raise WorkbenchError("E_NOT_FOUND", f"tier {args.tier!r} is not imported")
                status = {**status, "tiers": {args.tier: status["tiers"][args.tier]}}
            _json_print({"ok": True, "status": status})
            return 0
        if args.command == "sensitivity":
            _json_print({"ok": True, "sensitivity": Pipeline(args.root).sensitivity(args.tier)})
            return 0
        if args.command == "summary":
            _json_print({"ok": True, "summary": Pipeline(args.root).summary()})
            return 0
        if args.command == "invalidations":
            _json_print(
                {"ok": True, "invalidations": Pipeline(args.root).invalidations(args.tier)}
            )
            return 0
        if args.command == "export":
            pipeline = Pipeline(args.root)
            tiers = _tiers(args.tier) or list(pipeline._select_tiers(None))
            exports = {
                tier: pipeline.export_tier(tier, args.out)
                for tier in tiers
            }
            _json_print({"ok": True, "exports": exports})
            return 0
        if args.command == "reference":
            _json_print({"ok": True, "reference": generate_reference(args.root, tiers=_tiers(args.tiers))})
            return 0
        if args.command == "verify-reference":
            _json_print({"ok": True, "verification": verify_reference(args.root)})
            return 0
        if args.command == "verify-full":
            _json_print(
                {
                    "ok": True,
                    "verification": Pipeline(args.root).verify_full_equivalence(args.tier),
                }
            )
            return 0
        if args.command == "serve":
            serve(args.root, host=args.host, port=args.port)
            return 0
        if args.command == "demo":
            _json_print(
                {
                    "ok": True,
                    "demo": run_demo(
                        args.root,
                        seed=args.seed,
                        force=args.force,
                        report_path=args.report,
                    ),
                }
            )
            return 0
        raise WorkbenchError("E_DATA", f"unknown command: {args.command}")
    except WorkbenchError as exc:
        print(json.dumps({"ok": False, "error": exc.to_dict()}, ensure_ascii=False, sort_keys=True, indent=2))
        return 2
    except Exception as exc:
        print(
            json.dumps(
                {
                    "ok": False,
                    "error": {
                        "code": "E_INTERNAL",
                        "message": str(exc),
                        "stage": None,
                        "sample_id": None,
                        "details": {"exception_type": type(exc).__name__},
                    },
                },
                ensure_ascii=False,
                sort_keys=True,
                indent=2,
            )
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
