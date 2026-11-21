from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Sequence

from .config import Settings
from .errors import PipelineError
from .models import BatchSubmitRequest, InjectionOptions
from .policy import PolicySnapshot, parse_policy_snapshot
from .service import PipelineService
from .verifier import ResultVerifier


def _print(value: Any) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True))


def _settings(args: argparse.Namespace) -> Settings:
    settings = Settings.from_env(args.root)
    if args.db_path:
        settings.db_path = Path(args.db_path).resolve()
    if args.artifact_dir:
        settings.artifact_dir = Path(args.artifact_dir).resolve()
        settings.db_path = settings.artifact_dir / "pipeline.sqlite3" if not args.db_path else settings.db_path
        settings.staging_dir = settings.artifact_dir / "staging"
        settings.published_dir = settings.artifact_dir / "batches"
        settings.verification_dir = settings.artifact_dir / "verification"
    return settings


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="prep-pipeline")
    parser.add_argument("--root", default=None, help="workspace root (default: cwd)")
    parser.add_argument("--db-path", default=None)
    parser.add_argument("--artifact-dir", default=None)
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init", help="initialize SQLite and run startup recovery")
    sub.add_parser("bootstrap", help="submit bundled data/policy_v*.json snapshots")
    sub.add_parser("recover", help="scan and abort incomplete persisted batches")

    checksum = sub.add_parser("policy-checksum", help="compute the canonical checksum of a snapshot file")
    checksum.add_argument("--file", required=True)

    serve = sub.add_parser("serve", help="run the local HTTP API")
    serve.add_argument("--host", default=None)
    serve.add_argument("--port", type=int, default=None)

    run = sub.add_parser("run-batch", help="run one deterministic local batch")
    run.add_argument("--batch-id", required=True)
    run.add_argument("--policy-version", required=True, type=int)
    run.add_argument("--input", required=True)
    run.add_argument("--inject-invalid-label", action="store_true")
    run.add_argument("--inject-missing-shard-key", action="store_true")
    run.add_argument("--inject-sample-ratio", type=float, default=None)
    run.add_argument("--inject-duplicate-conflict", action="store_true")
    run.add_argument("--inject-interrupt-stage", default=None)
    run.add_argument("--interrupt-after-records", type=int, default=1)
    run.add_argument("--target-record-index", type=int, default=0)
    run.add_argument(
        "--hard-crash-after-stage",
        default=None,
        help="TEST ONLY: os._exit(71) after a completed stage fact is persisted",
    )

    query = sub.add_parser("query", help="query one committed batch")
    query.add_argument("--batch-id", required=True)

    verify = sub.add_parser("verify", help="recompute and compare one committed batch")
    verify.add_argument("--batch-id", required=True)
    verify.add_argument("--policy-version", type=int, default=None)
    verify.add_argument("--baseline-batch-id", default=None)

    return parser


def _injection_from_args(args: argparse.Namespace) -> InjectionOptions:
    return InjectionOptions(
        invalid_label=args.inject_invalid_label,
        missing_shard_key=args.inject_missing_shard_key,
        sample_ratio=args.inject_sample_ratio,
        duplicate_conflict=args.inject_duplicate_conflict,
        interrupt_at_stage=args.inject_interrupt_stage,
        interrupt_after_records=args.interrupt_after_records,
        target_record_index=args.target_record_index,
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    settings = _settings(args)
    try:
        if args.command == "serve":
            import uvicorn

            from .api import create_app

            host = args.host or settings.host
            port = args.port or settings.port
            _print({"status": "starting", "host": host, "port": port, "database": str(settings.db_path)})
            uvicorn.run(create_app(settings), host=host, port=port, log_level="info")
            return 0

        if args.command == "policy-checksum":
            value = json.loads(Path(args.file).read_text(encoding="utf-8"))
            value["checksum"] = value.get("checksum", "")
            try:
                snapshot = PolicySnapshot.model_validate(value)
            except Exception as exc:
                raise PipelineError(
                    "E_SNAPSHOT_INVALID",
                    "policy snapshot schema is invalid",
                    {"file": args.file, "detail": str(exc)},
                ) from exc
            _print(
                {
                    "file": str(Path(args.file).resolve()),
                    "version": snapshot.version,
                    "checksum": snapshot.expected_checksum(),
                }
            )
            return 0

        service = PipelineService(settings)
        if args.command == "init":
            reports = service.initialize(recover=True)
            _print({"status": "initialized", "db_path": str(settings.db_path), "recovery_reports": reports})
            return 0
        if args.command == "bootstrap":
            reports = service.initialize(recover=True)
            policies = service.bootstrap_policies(sorted(settings.data_dir.glob("policy_v*.json")))
            _print(
                {
                    "status": "bootstrapped",
                    "recovery_reports": reports,
                    "policies": policies,
                    "policy_head": service.db.policy_head(),
                }
            )
            return 0
        if args.command == "recover":
            reports = service.recover()
            _print({"recovered": reports, "recovered_count": len(reports)})
            return 0
        if args.command == "run-batch":
            request = BatchSubmitRequest(
                batch_id=args.batch_id,
                policy_version=args.policy_version,
                input_path=args.input,
                injections=_injection_from_args(args),
            )
            result = service.submit_batch(
                request, hard_crash_after_stage=args.hard_crash_after_stage
            )
            _print(result)
            return 0
        if args.command == "query":
            _print(service.get_batch(args.batch_id))
            return 0
        if args.command == "verify":
            report = ResultVerifier(settings, service.db).verify(
                args.batch_id,
                policy_version=args.policy_version,
                baseline_batch_id=args.baseline_batch_id,
            )
            _print(report)
            return 0
        parser.error(f"unknown command: {args.command}")
        return 2
    except PipelineError as exc:
        print(json.dumps(exc.as_dict(), ensure_ascii=False, indent=2, sort_keys=True), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
