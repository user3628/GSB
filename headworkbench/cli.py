from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

import uvicorn

from .config import current_config_path, load_config
from .data import load_dataset
from .demo import create_mismatch_fixture, run_demo
from .errors import WorkbenchError
from .generator import generate_demo
from .pipeline import Pipeline
from .service import create_app
from .versioning import VersionStore


def emit(value: Any) -> None:
    print(json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False))


def parse_widths(value: str) -> dict[str, int]:
    try:
        payload = json.loads(value)
    except json.JSONDecodeError as exc:
        raise WorkbenchError("E_ARGUMENT", f"--widths must be JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise WorkbenchError("E_ARGUMENT", "--widths must be a JSON object")
    return {str(key): int(bits) for key, bits in payload.items()}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="headworkbench")
    parser.add_argument("--workspace", default=".", help="workspace root (default: current directory)")
    sub = parser.add_subparsers(dest="command", required=True)

    generate = sub.add_parser("generate", help="generate deterministic demo shards and config")
    generate.add_argument("--force", action="store_true")
    generate.add_argument("--seed", type=int, default=20261122)

    sub.add_parser("diagnostics", help="validate data/config and workspace writability")
    sub.add_parser("history", help="show version history")
    sub.add_parser("rollbacks", help="show rollback records")
    sub.add_parser("recover", help="recover HEAD from immutable version records")
    sub.add_parser("demo", help="run the complete acceptance demonstration")

    run = sub.add_parser("run", help="run adjudication for a sampling window")
    run.add_argument("--window", default=None)
    run.add_argument("--mode", choices=("incremental", "full"), default="incremental")

    verify = sub.add_parser("verify", help="compare incremental output with a from-scratch full run")
    verify.add_argument("--window", default=None)

    commit = sub.add_parser("commit", help="atomically commit a per-head width composition")
    commit.add_argument("--widths", required=True, help="JSON object, e.g. '{\"0\":14,\"1\":12}'")
    commit.add_argument("--expected-version", type=int, required=True)
    commit.add_argument("--window", default=None)
    commit.add_argument("--adjudication-digest", default=None)
    commit.add_argument("--note", default=None)
    commit.add_argument("--no-verify-full", action="store_true")

    auto_commit = sub.add_parser("commit-suggested", help="commit the current pipeline's suggested composition")
    auto_commit.add_argument("--window", default=None)
    auto_commit.add_argument("--note", default="CLI suggested composition")

    rollback = sub.add_parser("rollback-to", help="create a new manual rollback record to an earlier active version")
    rollback.add_argument("--target-version", type=int, required=True)
    rollback.add_argument("--expected-version", type=int, required=True)
    rollback.add_argument("--note", default=None)

    mismatch = sub.add_parser("make-mismatch", help="create and validate an injected head-dimension mismatch fixture")
    mismatch.add_argument("--output", default=None)

    serve = sub.add_parser("serve", help="start the local HTTP service")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8765)
    serve.add_argument("--log-level", default="info")

    stop = sub.add_parser("stop", help="gracefully stop a local HTTP service")
    stop.add_argument("--url", default="http://127.0.0.1:8765")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    root = Path(args.workspace).resolve()
    try:
        if args.command == "generate":
            result = generate_demo(root, seed=args.seed, force=args.force)
        elif args.command == "diagnostics":
            dataset = load_dataset(root)
            config = load_config(current_config_path(root))
            layout = VersionStore(root).ensure_layout()
            result = {
                "workspace": str(root),
                "layout": layout,
                "config_version": config.version,
                "config_digest": config.config_digest,
                "data_digest": dataset.data_digest,
                "manifest_digest": dataset.manifest["manifest_digest"],
                "head_count": len(dataset.heads),
                "heads": [
                    {
                        "head_id": head.head_id,
                        "activation_shape": list(head.activation.shape),
                        "query_proj_shape": list(head.query_proj.shape),
                        "key_proj_shape": list(head.key_proj.shape),
                    }
                    for head in dataset.heads
                ],
            }
        elif args.command == "run":
            config = load_config(current_config_path(root))
            window = args.window or ("compact" if "compact" in config.sampling_windows else sorted(config.sampling_windows)[0])
            pipeline = Pipeline(root, config)
            run_result = pipeline.run(window, mode=args.mode)
            payload = run_result["payload"]
            result = {
                "window": payload["window"],
                "mode": args.mode,
                "result_digest": run_result["result_digest"],
                "stage_digests": {name: entry["digest"] for name, entry in payload["stages"].items()},
                "recomputed_stages": run_result["recomputed_stages"],
                "cache_hits": run_result["cache_hits"],
                "ranking": payload["adjudication"]["ranking"],
                "error_upper_bounds": {
                    str(head["head_id"]): head["error_upper_bound"] for head in payload["adjudication"]["heads_by_id"]
                },
                "sample_intervals": {
                    str(head["head_id"]): head["sample_interval"] for head in payload["adjudication"]["heads_by_id"]
                },
                "suggested_head_widths": payload["allocation"]["suggested_head_widths"],
                "total_error_estimate": payload["allocation"]["total_error_estimate"],
                "global_error_budget": payload["allocation"]["global_error_budget"],
                "budget_limited_head_ids": payload["allocation"]["budget_limited_head_ids"],
                "downgraded_head_ids": payload["allocation"]["downgraded_head_ids"],
                "allocation_diagnostics": payload["allocation"]["diagnostics"],
            }
        elif args.command == "verify":
            config = load_config(current_config_path(root))
            window = args.window or ("compact" if "compact" in config.sampling_windows else sorted(config.sampling_windows)[0])
            result = Pipeline(root, config).verify_full_consistency(window)
            result["window"] = window
        elif args.command == "commit":
            store = VersionStore(root)
            config = load_config(current_config_path(root))
            pipeline = Pipeline(root, config)
            record = store.current()
            window = args.window or (record["sampling_window_name"] if record else "compact")
            result = store.commit(
                pipeline,
                parse_widths(args.widths),
                expected_base_version=args.expected_version,
                window_name=window,
                adjudication_digest=args.adjudication_digest,
                note=args.note,
                verify_full=not args.no_verify_full,
            )
        elif args.command == "commit-suggested":
            store = VersionStore(root)
            config = load_config(current_config_path(root))
            pipeline = Pipeline(root, config)
            record = store.current()
            base_version = record["version"] if record else 0
            window = args.window or (record["sampling_window_name"] if record else "compact")
            run_result = pipeline.run(window, mode="incremental")
            result = store.commit(
                pipeline,
                run_result["payload"]["allocation"]["suggested_head_widths"],
                expected_base_version=base_version,
                window_name=window,
                adjudication_digest=run_result["payload"]["stages"]["adjudication"]["digest"],
                note=args.note,
                verify_full=True,
            )
        elif args.command == "history":
            result = VersionStore(root).history()
        elif args.command == "rollbacks":
            result = VersionStore(root).rollback_records()
        elif args.command == "rollback-to":
            result = VersionStore(root).rollback_to(
                target_version=args.target_version,
                expected_base_version=args.expected_version,
                note=args.note,
            )
        elif args.command == "recover":
            result = VersionStore(root).recover()
        elif args.command == "demo":
            result = run_demo(root, force_generate=True)
        elif args.command == "make-mismatch":
            manifest, error = create_mismatch_fixture(root)
            result = {"manifest": str(manifest.relative_to(root)), "expected_error": error}
        elif args.command == "serve":
            app = create_app(root)
            server = uvicorn.Server(
                uvicorn.Config(app, host=args.host, port=args.port, log_level=args.log_level)
            )
            app.state.uvicorn_server = server
            server.run()
            result = {"status": "stopped", "host": args.host, "port": args.port}
        elif args.command == "stop":
            body = json.dumps({}).encode("utf-8")
            request = urllib.request.Request(
                args.url.rstrip("/") + "/api/control/stop",
                data=body,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            try:
                with urllib.request.urlopen(request, timeout=10) as response:
                    result = json.loads(response.read().decode("utf-8"))
            except urllib.error.URLError as exc:
                raise WorkbenchError("E_HTTP", f"cannot reach local service: {exc}") from exc
        else:
            parser.error("unknown command")
            return 2
    except WorkbenchError as exc:
        emit(exc.as_dict())
        return 1
    emit(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

