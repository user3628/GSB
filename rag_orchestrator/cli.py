"""Command-line entry point."""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any

from .config import load_config
from .engine import Orchestrator, stable_bytes, stable_projection
from .errors import E_REQUEST_NOT_FOUND, EngineError
from .samplegen import DEFAULT_SEED, generate_samples
from .util import canonical_json_text, read_jsonl
from .web import WorkspaceService, create_app


def _print_json(value: Any, *, pretty: bool) -> None:
    if pretty:
        print(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True))
    else:
        print(canonical_json_text(value))


def _load_samples(args: argparse.Namespace) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    return load_config(args.config), read_jsonl(args.docs), read_jsonl(args.requests)


def cmd_generate(args: argparse.Namespace) -> int:
    manifest = generate_samples(args.out, seed=args.seed)
    _print_json(manifest, pretty=not args.compact)
    return 0


def cmd_run(args: argparse.Namespace) -> int:
    config, documents, requests = _load_samples(args)
    request_record = next((item for item in requests if item.get("query_id") == args.query_id), None)
    if request_record is None:
        error = EngineError(
            E_REQUEST_NOT_FOUND,
            "query_id not found",
            query_id=args.query_id,
            details={"available_query_ids": sorted(item.get("query_id") for item in requests)},
        )
        _print_json({"status": "error", "error": error.to_dict()}, pretty=not args.compact)
        return 2
    engine = Orchestrator(
        config,
        documents,
        args.workspace,
        mode=args.mode,
        stop_after_stage=args.stop_after_stage,
    )
    result = engine.run(request_record)
    if args.stable:
        sys.stdout.buffer.write(stable_bytes(result) + b"\n")
    else:
        _print_json(result, pretty=not args.compact)
    return 0 if result.get("status") in {"ok", "degraded", "stopped"} else 2


def cmd_run_all(args: argparse.Namespace) -> int:
    config, documents, requests = _load_samples(args)
    engine = Orchestrator(
        config,
        documents,
        args.workspace,
        mode=args.mode,
        stop_after_stage=args.stop_after_stage,
    )
    result = engine.run_all(requests)
    if args.stable:
        projections = [stable_projection(item) for item in result["results"]]
        sys.stdout.buffer.write(canonical_json_text({"stable_projections": projections, "summary_sha256": result["summary_sha256"]}).encode("utf-8") + b"\n")
    else:
        _print_json(result, pretty=not args.compact)
    return 0


def cmd_checkpoints(args: argparse.Namespace) -> int:
    _, _, _ = _load_samples(args)
    service = WorkspaceService(
        config_path=args.config,
        docs_path=args.docs,
        requests_path=args.requests,
        workspace=args.workspace,
    )
    _print_json(service.store.summary(args.query_id), pretty=not args.compact)
    return 0


def cmd_serve(args: argparse.Namespace) -> int:
    service = WorkspaceService(
        config_path=args.config,
        docs_path=args.docs,
        requests_path=args.requests,
        workspace=args.workspace,
    )
    app = create_app(service)
    print(f"RAG diagnostics workbench: http://{args.host}:{args.port}/", flush=True)
    app.run(host=args.host, port=args.port, debug=False, threaded=True, use_reloader=False)
    return 0


def build_parser() -> argparse.ArgumentParser:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--config", dest="config", default=argparse.SUPPRESS)
    common.add_argument("--docs", dest="docs", default=argparse.SUPPRESS)
    common.add_argument("--requests", dest="requests", default=argparse.SUPPRESS)
    common.add_argument("--workspace", dest="workspace", default=argparse.SUPPRESS)
    common.add_argument("--compact", dest="compact", action="store_true", default=argparse.SUPPRESS)

    parser = argparse.ArgumentParser(
        prog="rag-orchestrator",
        description="Local deterministic RAG parallel orchestration and diagnostics workbench.",
    )
    parser.add_argument("--config", default="data/config.yaml")
    parser.add_argument("--docs", default="data/docs.jsonl")
    parser.add_argument("--requests", default="data/requests.jsonl")
    parser.add_argument("--workspace", default="runtime")
    parser.add_argument("--compact", action="store_true", help="emit one-line canonical JSON")
    subparsers = parser.add_subparsers(dest="command", required=True)

    generate = subparsers.add_parser("generate", help="write deterministic JSONL/YAML samples", parents=[common])
    generate.add_argument("--out", default="data")
    generate.add_argument("--seed", type=int, default=DEFAULT_SEED)
    generate.set_defaults(func=cmd_generate)

    run = subparsers.add_parser("run", help="run one request", parents=[common])
    run.add_argument("query_id")
    run.add_argument("--mode", choices=("parallel", "serial"), default="parallel")
    run.add_argument("--stop-after-stage", choices=("normalize", "dispatch", "rerank", "assemble"))
    run.add_argument("--stable", action="store_true", help="emit only stable, cache-independent bytes")
    run.set_defaults(func=cmd_run)

    run_all = subparsers.add_parser("run-all", help="run all local requests", parents=[common])
    run_all.add_argument("--mode", choices=("parallel", "serial"), default="parallel")
    run_all.add_argument("--stop-after-stage", choices=("normalize", "dispatch", "rerank", "assemble"))
    run_all.add_argument("--stable", action="store_true")
    run_all.set_defaults(func=cmd_run_all)

    checkpoints = subparsers.add_parser("checkpoints", help="print checkpoint ledger and stage records", parents=[common])
    checkpoints.add_argument("query_id")
    checkpoints.set_defaults(func=cmd_checkpoints)

    serve = subparsers.add_parser("serve", help="start the local HTTP workbench", parents=[common])
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8765)
    serve.set_defaults(func=cmd_serve)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
