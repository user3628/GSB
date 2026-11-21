from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from .canonical import canonical_bytes, digest_obj
from .config import ZONE_ORDER
from .errors import CabinError
from .service import CabinService


def _print(value: Any) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2))


def _service(args: argparse.Namespace) -> CabinService:
    return CabinService(args.workspace)


def _fetch_responsible(state: dict[str, Any], shard_id: str, word: str) -> dict[str, Any]:
    assignments = state["stages"]["arbitrate"]["artifact"]["shards"][shard_id]["assignments"]
    for assignment in assignments:
        if assignment["word"] == word:
            return {
                "word": word,
                "status": assignment["status"],
                "responsible_zone": assignment["responsible_zone"],
                "top_candidate": assignment["top_candidate"],
                "score_gap": assignment["score_gap"],
            }
    return {"word": word, "status": "missing", "responsible_zone": None}


def run_demo(args: argparse.Namespace) -> int:
    service = _service(args)
    workspace = service.workspace
    if workspace.state_path.exists() and not args.keep:
        workspace.reset_runtime()
    initialized = service.initialize_demo(seed=args.seed, reset=True)
    initial_run = service.run(verify=True)
    initial_state = workspace.load_state()
    initial_assignment = _fetch_responsible(initial_state, "shard-003", "打开座椅加热")

    first_query = service.query("active", shard_id="shard-001", time_ms=350, budget_ms=args.query_budget_ms)
    second_query = service.query("active", shard_id="shard-001", time_ms=350, budget_ms=args.query_budget_ms)
    cache_equal = canonical_bytes(first_query["result"]) == canonical_bytes(second_query["result"])

    latency_error = None
    try:
        service.query("active", shard_id="shard-001", time_ms=350, budget_ms=0)
    except CabinError as exc:
        latency_error = {
            "code": exc.code,
            "budget_ms": exc.details.get("budget_ms"),
            "query_kind": exc.details.get("query_kind"),
        }

    scope_edit = service.edit(
        "select_scope",
        parameters={},
        expected_version=initial_state["edit"]["version"],
        shard_id="shard-003",
    )
    priority_edit = service.edit(
        "set_priority",
        parameters={"zone_id": "rear_right", "priority": 1},
        expected_version=scope_edit["edit_version"],
        shard_id="shard-003",
    )
    incremental_run = service.run(verify=True)
    final_state = workspace.load_state()
    final_assignment = _fetch_responsible(final_state, "shard-003", "打开座椅加热")

    stop_result = service.stop("demo_graceful_stop")
    stopped_run = service.run(verify=True)
    resume_result = service.resume(verify=True)
    snapshot_verification = service.verify_snapshots()
    export_result = service.export("result.json")
    final_versions = service.versions()

    summary = {
        "demo": "cabin_voice_arbiter",
        "seed": args.seed,
        "shard_count": len(initialized["shards"]),
        "shards": initialized["shards"],
        "zone_order": list(ZONE_ORDER),
        "manifest_digest": initialized["manifest_digest"],
        "initial_result_digest": initial_run["result_digest"],
        "initial_assignment": initial_assignment,
        "cache": {
            "first_hit": first_query["cache_hit"],
            "second_hit": second_query["cache_hit"],
            "results_byte_equal": cache_equal,
            "result_digest": first_query["result_digest"],
        },
        "latency_budget_demo": latency_error,
        "priority_edit": {
            "scope_edit_version": scope_edit["edit_version"],
            "priority_edit_version": priority_edit["edit_version"],
            "zone": "rear_right",
            "priority": 1,
            "assignment_after": final_assignment,
        },
        "incremental": {
            "recomputed_stages": incremental_run["recomputed_stages"],
            "reused_stages": incremental_run["reused_stages"],
            "result_digest": incremental_run["result_digest"],
            "verified_full_digest": incremental_run["verified_full_digest"],
            "byte_equal": incremental_run["result_digest"] == incremental_run["verified_full_digest"],
        },
        "stop_resume": {
            "stop_requested": stop_result["status"],
            "stopped_run_status": stopped_run["status"],
            "resume_status": resume_result["status"],
            "resume_result_digest": resume_result["result_digest"],
            "byte_equal_to_incremental": resume_result["result_digest"] == incremental_run["result_digest"],
        },
        "snapshots": {
            "verified": snapshot_verification["status"] == "verified",
            "seq": snapshot_verification["seq"],
            "records": snapshot_verification["records"],
        },
        "export": {
            "digest": export_result["result_digest"],
            "edit_version": export_result["edit_version"],
            "config_version": export_result["config_version"],
        },
        "final_result_digest": final_versions["result_digest"],
        "final_edit_version": final_versions["edit_version"],
    }
    summary["summary_digest"] = digest_obj(summary)
    summary_path = Path(args.summary).expanduser().resolve()
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    _print(summary)
    checks = [
        initial_run["verified_full_digest"] == initial_run["result_digest"],
        cache_equal,
        latency_error is not None and latency_error["code"] == "E_LATENCY_BUDGET",
        incremental_run["reused_stages"] == ["vad"],
        incremental_run["recomputed_stages"] == ["arbitrate"],
        incremental_run["result_digest"] == incremental_run["verified_full_digest"],
        initial_assignment["responsible_zone"] == "rear_left",
        final_assignment["responsible_zone"] == "rear_right",
        stopped_run["status"] == "stopped",
        resume_result["result_digest"] == incremental_run["result_digest"],
        snapshot_verification["status"] == "verified",
    ]
    if not all(checks):
        print("demo checks failed", file=sys.stderr)
        return 1
    print(f"demo summary: {summary_path}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="cabin-voice")
    parser.add_argument("--workspace", default="workspace", help="workspace directory")
    subparsers = parser.add_subparsers(dest="command", required=True)

    serve = subparsers.add_parser("serve", help="run the loopback HTTP workbench")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8765)

    demo = subparsers.add_parser("demo", help="run the deterministic end-to-end demo")
    demo.add_argument("--seed", type=int, default=20261122)
    demo.add_argument("--summary", default="artifacts/demo_summary.json")
    demo.add_argument("--query-budget-ms", type=int, default=200)
    demo.add_argument("--keep", action="store_true")

    generate_data = subparsers.add_parser("generate-data", help="write the fixed-seed WAV dataset only")
    generate_data.add_argument("--root", default="demo_data")
    generate_data.add_argument("--seed", type=int, default=20261122)

    import_manifest = subparsers.add_parser("import-manifest", help="import a manifest and its local WAV shards")
    import_manifest.add_argument("--manifest", required=True)
    import_manifest.add_argument("--reset", action="store_true")
    import_manifest.add_argument("--config-json", help="optional configuration JSON object")

    init_demo = subparsers.add_parser("init-demo", help="generate and import the demo dataset")
    init_demo.add_argument("--seed", type=int, default=20261122)
    init_demo.add_argument("--reset", action="store_true")

    run = subparsers.add_parser("run", help="run or incrementally refresh stages")
    run.add_argument("--stage", choices=["all", "vad", "arbitrate"], default="all")
    run.add_argument("--full", action="store_true")
    run.add_argument("--no-verify", action="store_true")

    query = subparsers.add_parser("query", help="query active zones, TDOA or assignments")
    query.add_argument("kind", choices=["active", "tdoa", "assignment"])
    query.add_argument("--shard", required=True)
    query.add_argument("--at", type=int, required=True)
    query.add_argument("--budget-ms", type=int)

    status = subparsers.add_parser("status", help="show stage and version status")
    status.add_argument("--versions", action="store_true")

    edit_priority = subparsers.add_parser("edit-priority", help="apply a priority edit")
    edit_priority.add_argument("--shard", required=True)
    edit_priority.add_argument("--zone", choices=list(ZONE_ORDER), required=True)
    edit_priority.add_argument("--priority", type=int, required=True)
    edit_priority.add_argument("--expected-version", type=int, required=True)
    edit_priority.add_argument("--switch-scope", action="store_true")

    edit = subparsers.add_parser("edit", help="apply any supported edit operation")
    edit.add_argument("--operation", required=True)
    edit.add_argument("--parameters-json", default="{}")
    edit.add_argument("--expected-version", type=int, required=True)
    edit.add_argument("--shard", required=True)
    edit.add_argument("--edit-id")

    export = subparsers.add_parser("export", help="export activity, TDOA and assignment results")
    export.add_argument("--name", default="result.json")

    subparsers.add_parser("stop", help="request graceful stop")
    subparsers.add_parser("resume", help="clear stop request and resume")

    snapshots = subparsers.add_parser("snapshots", help="inspect or verify snapshots")
    snapshots.add_argument("action", choices=["list", "verify"], default="verify", nargs="?")
    snapshots.add_argument("--all-records", action="store_true")

    args = parser.parse_args(argv)
    service = _service(args)
    try:
        if args.command == "serve":
            import uvicorn

            from .api import create_app

            if args.host not in {"127.0.0.1", "localhost", "::1"}:
                raise SystemExit("serve refuses non-loopback hosts")
            uvicorn.run(create_app(args.workspace), host=args.host, port=args.port)
            return 0
        if args.command == "demo":
            return run_demo(args)
        if args.command == "generate-data":
            from .audio import generate_demo_dataset

            _print(generate_demo_dataset(args.root, seed=args.seed))
        elif args.command == "import-manifest":
            config = json.loads(args.config_json) if args.config_json else None
            _print(
                service.import_manifest(
                    args.manifest,
                    reset=args.reset,
                    config=config,
                )
            )
        elif args.command == "init-demo":
            _print(service.initialize_demo(seed=args.seed, reset=True))
        elif args.command == "run":
            _print(service.run(stage=args.stage, force_full=args.full, verify=not args.no_verify))
        elif args.command == "query":
            _print(
                service.query(
                    args.kind,
                    shard_id=args.shard,
                    time_ms=args.at,
                    budget_ms=args.budget_ms,
                )
            )
        elif args.command == "status":
            _print(service.versions() if args.versions else service.status())
        elif args.command == "edit-priority":
            version = args.expected_version
            if args.switch_scope:
                result = service.edit(
                    "select_scope",
                    parameters={},
                    expected_version=version,
                    shard_id=args.shard,
                )
                version = result["edit_version"]
                _print(result)
            _print(
                service.edit(
                    "set_priority",
                    parameters={"zone_id": args.zone, "priority": args.priority},
                    expected_version=version,
                    shard_id=args.shard,
                )
            )
        elif args.command == "edit":
            parameters = json.loads(args.parameters_json)
            _print(
                service.edit(
                    args.operation,
                    parameters=parameters,
                    expected_version=args.expected_version,
                    shard_id=args.shard,
                    edit_id=args.edit_id,
                )
            )
        elif args.command == "export":
            _print(service.export(args.name))
        elif args.command == "stop":
            _print(service.stop("cli"))
        elif args.command == "resume":
            _print(service.resume())
        elif args.command == "snapshots":
            _print(service.verify_snapshots() if args.action == "verify" else service.snapshots())
        return 0
    except CabinError as exc:
        _print(exc.to_dict())
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
