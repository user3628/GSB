"""Command-line interface for build, status, query, verification, and serve."""

from __future__ import annotations

import argparse
import json
import signal
import sys
import threading
from pathlib import Path
from typing import Any

from .config import IndexConfig
from .errors import GracefulStop, PipelineError, as_error_payload
from .pipeline import Pipeline
from .query import QueryEngine
from .reference import reference_query
from .stages import verify_compressed_postings
from .utils import canonical_json_bytes


def _print(value: Any, *, pretty: bool = False) -> None:
    if pretty:
        print(json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2))
    else:
        sys.stdout.buffer.write(canonical_json_bytes(value))


class StopController:
    def __init__(self, stop_after_units: int | None = None):
        self.event = threading.Event()
        self.stop_after_units = stop_after_units
        self.units = 0

    def install_signals(self) -> None:
        def request_stop(signum: int, frame: Any) -> None:
            self.event.set()

        signal.signal(signal.SIGINT, request_stop)
        if hasattr(signal, "SIGTERM"):
            signal.signal(signal.SIGTERM, request_stop)

    def check(self) -> bool:
        self.units += 1
        if self.stop_after_units is not None and self.units > self.stop_after_units:
            return True
        return self.event.is_set()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Deterministic Chinese sparse index pipeline")
    parser.add_argument("--workspace", default=".", help="workspace containing artifacts/")
    subparsers = parser.add_subparsers(dest="command", required=True)

    build = subparsers.add_parser("build", help="build or resume all pipeline stages")
    build.add_argument("--workspace", default=argparse.SUPPRESS)
    build.add_argument("--config", default="data/index.yaml")
    build.add_argument("--force", action="store_true")
    build.add_argument("--stop-after-units", type=int, default=None, help="deterministic graceful-stop test hook")
    build.add_argument("--pretty", action="store_true")

    status = subparsers.add_parser("status", help="show checkpoint and invalidation status")
    status.add_argument("--workspace", default=argparse.SUPPRESS)
    status.add_argument("--config", default="data/index.yaml")
    status.add_argument("--pretty", action="store_true")

    query = subparsers.add_parser("query", help="execute a compressed query")
    query.add_argument("--workspace", default=argparse.SUPPRESS)
    query.add_argument("--config", default="data/index.yaml")
    query.add_argument("--query-id", default="cli-query")
    query.add_argument("--terms", nargs="+", required=True)
    query.add_argument("--mode", choices=["and", "or"], default="and")
    query.add_argument("--limit", type=int, default=None)
    query.add_argument("--no-cache", action="store_true")
    query.add_argument("--no-meta", action="store_true")
    query.add_argument("--pretty", action="store_true")

    reference = subparsers.add_parser("reference-query", help="run the uncompressed sequential reference")
    reference.add_argument("--workspace", default=argparse.SUPPRESS)
    reference.add_argument("--config", default="data/index.yaml")
    reference.add_argument("--terms", nargs="+", required=True)
    reference.add_argument("--mode", choices=["and", "or"], default="and")
    reference.add_argument("--limit", type=int, default=None)
    reference.add_argument("--pretty", action="store_true")

    verify = subparsers.add_parser("verify", help="verify block round trips and benchmark equality")
    verify.add_argument("--workspace", default=argparse.SUPPRESS)
    verify.add_argument("--config", default="data/index.yaml")
    verify.add_argument("--pretty", action="store_true")

    serve = subparsers.add_parser("serve", help="start a local HTTP query service")
    serve.add_argument("--workspace", default=argparse.SUPPRESS)
    serve.add_argument("--config", default="data/index.yaml")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8765)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    workspace = Path(args.workspace).resolve()
    try:
        config = IndexConfig.load(args.config)
        pipeline = Pipeline(config, workspace)
        if args.command == "build":
            controller = StopController(args.stop_after_units)
            controller.install_signals()
            try:
                result = pipeline.build(force=args.force, stop_check=controller.check)
            except GracefulStop as exc:
                _print(
                    {
                        "ok": False,
                        "status": "stopped",
                        "message": str(exc),
                        "completed_stages": [record["stage"] for record in pipeline.checkpoints.active()[0]],
                        "resume_command": f"python -m sparse_index.cli --workspace {workspace} build --config {args.config}",
                    },
                    pretty=args.pretty,
                )
                return 130
            _print(result, pretty=args.pretty)
            return 0
        if args.command == "status":
            _print(pipeline.status(), pretty=args.pretty)
            return 0
        if args.command == "query":
            limit = args.limit if args.limit is not None else int(config.value("query.default_limit", 20))
            engine = QueryEngine(config, pipeline.artifacts_dir)
            result = engine.execute(
                query_id=args.query_id,
                terms=args.terms,
                mode=args.mode,
                limit=limit,
                use_cache=not args.no_cache,
            )
            _print(result["result"] if args.no_meta else result, pretty=args.pretty)
            return 0
        if args.command == "reference-query":
            limit = args.limit if args.limit is not None else int(config.value("query.default_limit", 20))
            result = reference_query(
                config,
                pipeline.artifacts_dir,
                terms=args.terms,
                mode=args.mode,
                limit=limit,
            )
            _print(result, pretty=args.pretty)
            return 0
        if args.command == "verify":
            block_report = verify_compressed_postings(
                config,
                pipeline.stage_dir(2),
                pipeline.stage_dir(3),
            )
            engine = QueryEngine(config, pipeline.artifacts_dir)
            query_payload = json.loads(
                (pipeline.stage_dir(4) / "query_results.json").read_text(encoding="utf-8")
            )
            mismatches = []
            for item in query_payload["queries"]:
                query_id = item["query_id"]
                compressed = item["result"]
                sequential = reference_query(
                    config,
                    pipeline.artifacts_dir,
                    terms=[row["term"] for row in compressed["terms"]],
                    mode=compressed["mode"],
                    limit=compressed["limit"],
                )
                if compressed != sequential:
                    mismatches.append({"query_id": query_id})
            if mismatches:
                raise PipelineError(
                    "E_QUERY_MISMATCH",
                    "验证发现压缩查询与参考结果差异",
                    details={"queries": mismatches},
                )
            _print(
                {
                    "ok": True,
                    "compressed_blocks": block_report,
                    "queries_verified": len(query_payload["queries"]),
                    "query_mismatches": [],
                    "status": pipeline.status()["up_to_date"],
                },
                pretty=args.pretty,
            )
            return 0
        if args.command == "serve":
            from .http_api import serve

            serve(config, workspace, args.host, args.port)
            return 0
    except PipelineError as exc:
        _print(exc.to_dict(), pretty=getattr(args, "pretty", False))
        return exc.exit_code
    except KeyboardInterrupt:
        _print({"ok": False, "status": "interrupted"})
        return 130
    except Exception as exc:
        _print(as_error_payload(exc))
        return 1
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
