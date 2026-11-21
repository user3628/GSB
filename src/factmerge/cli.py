"""Command-line interface for the fact-merge component."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from .config import load_config
from .errors import FactMergeError
from .evidence import export_manifest_bytes, load_manifest, rebuild_manifest, validate_manifest
from .migration import export_legacy_v1, migrate_legacy_v1_file
from .pipeline import FactMergeEngine, write_run_outputs
from .state import StateStore
from .util import atomic_write_bytes, canonical_json


def _default_config_path() -> str | None:
    candidate = Path("configs/default.json")
    return str(candidate) if candidate.exists() else None


def _print_json(value: Any, *, compact: bool = False) -> None:
    if compact:
        text = canonical_json(value)
    else:
        text = json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True)
    print(text)


def _engine(args: argparse.Namespace) -> FactMergeEngine:
    return FactMergeEngine(
        load_config(args.config),
        args.corpus,
        state_dir=args.state_dir,
        expected=args.expected,
    )


def _find_claim(manifest: dict[str, Any], claim_id: str) -> dict[str, Any] | None:
    for entity in manifest.get("entities", []):
        for claim in entity.get("claims", []):
            if claim.get("claim_id") == claim_id:
                return claim
    return None


def _find_entity(manifest: dict[str, Any], entity_id: str) -> dict[str, Any] | None:
    for entity in manifest.get("entities", []):
        if entity.get("entity_id") == entity_id:
            return entity
    return None


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="factmerge",
        description="Deterministic cross-language factual claim reconciliation",
    )
    parser.add_argument("--config", default=_default_config_path(), help="JSON configuration file")
    parser.add_argument("--corpus", default="samples/corpus", help="JSONL corpus file or shard directory")
    parser.add_argument("--state-dir", default=".factmerge-state", help="atomic checkpoint state directory")
    parser.add_argument("--expected", default="samples/expected.json", help="optional evaluation ground truth JSON")
    sub = parser.add_subparsers(dest="command", required=True)

    verify = sub.add_parser("verify", help="verify one entity and print its evidence-bearing decision")
    verify.add_argument("entity_id")
    verify.add_argument("--no-state", action="store_true", help="disable checkpoint reuse")

    batch = sub.add_parser("batch", help="run a deterministic batch and export versioned evidence")
    batch.add_argument("--out-dir", default="artifacts")
    batch.add_argument("--stop-after-entities", type=int, default=None)
    batch.add_argument("--resume", action="store_true")
    batch.add_argument("--no-state", action="store_true")

    resume = sub.add_parser("resume", help="resume a stopped batch from checkpoints")
    resume.add_argument("--out-dir", default="artifacts")
    resume.add_argument("--no-state", action="store_true")

    query = sub.add_parser("query", help="query a demotion reason and full evidence chain")
    query.add_argument("--manifest", default="artifacts/evidence_manifest.json")
    group = query.add_mutually_exclusive_group(required=True)
    group.add_argument("--claim-id")
    group.add_argument("--entity-id")

    version = sub.add_parser("version", help="show fingerprints, invalidation reasons and checkpoints")
    version.add_argument("--compact", action="store_true")

    stop = sub.add_parser("stop", help="atomically request a stop at the next entity boundary")
    stop.add_argument("--compact", action="store_true")

    evidence = sub.add_parser("evidence", help="validate or rebuild a versioned evidence manifest")
    evidence_sub = evidence.add_subparsers(dest="evidence_command", required=True)
    validate = evidence_sub.add_parser("validate")
    validate.add_argument("manifest")
    rebuild = evidence_sub.add_parser("rebuild")
    rebuild.add_argument("manifest")
    rebuild.add_argument("--out", required=True)

    migrate = sub.add_parser("migrate-v1", help="import legacy v1 evidence and export equivalent v1")
    migrate.add_argument("file")
    migrate.add_argument("--out-manifest", required=True)
    migrate.add_argument("--out-v1", required=True)

    selfcheck = sub.add_parser("selfcheck", help="check repeatability and batch/single-entity equivalence")
    selfcheck.add_argument("--compact", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "verify":
            result = _engine(args).verify_entity(args.entity_id, use_state=not args.no_state)
            entity = next(
                (item for item in result["entity_results"] if item["entity_id"] == args.entity_id), None
            )
            _print_json(
                {
                    "status": result["status"],
                    "entity": entity,
                    "manifest_sha256": result["manifest_sha256"],
                    "version": result["version_status"].get("current_version"),
                }
            )
            return 0

        if args.command in {"batch", "resume"}:
            engine = _engine(args)
            result = engine.run(
                resume=(args.command == "resume" or getattr(args, "resume", False)),
                stop_after_entities=getattr(args, "stop_after_entities", None),
                use_state=not args.no_state,
            )
            if result["status"] == "complete":
                write_run_outputs(result, args.out_dir)
            _print_json(
                {
                    "status": result["status"],
                    "next_entity_id": result["next_entity_id"],
                    "summary": result["summary"],
                    "manifest_sha256": result["manifest_sha256"],
                    "version_status": result["version_status"],
                }
            )
            return 0

        if args.command == "query":
            manifest = load_manifest(args.manifest)
            if args.claim_id:
                value = _find_claim(manifest, args.claim_id)
                if value is None:
                    raise FactMergeError("E_NOT_FOUND", f"claim not found: {args.claim_id}", claim_id=args.claim_id)
                _print_json(value)
            else:
                value = _find_entity(manifest, args.entity_id)
                if value is None:
                    raise FactMergeError("E_NOT_FOUND", f"entity not found: {args.entity_id}", entity_id=args.entity_id)
                _print_json(value)
            return 0

        if args.command == "version":
            _print_json(_engine(args).version_status(), compact=args.compact)
            return 0

        if args.command == "stop":
            store = StateStore(args.state_dir)
            store.request_stop()
            _print_json(
                {
                    "status": "stop_requested",
                    "state_dir": str(store.root),
                    "stop_file": str(store.stop_path),
                    "next_seq": int(store.state.get("next_seq", 1)),
                },
                compact=args.compact,
            )
            return 0

        if args.command == "evidence":
            if args.evidence_command == "validate":
                report = validate_manifest(load_manifest(args.manifest))
                _print_json(report)
                return 0
            rebuilt = rebuild_manifest(load_manifest(args.manifest))
            atomic_write_bytes(args.out, export_manifest_bytes(rebuilt))
            _print_json(
                {
                    "status": "rebuilt",
                    "out": str(args.out),
                    "manifest_id": rebuilt["manifest_id"],
                }
            )
            return 0

        if args.command == "migrate-v1":
            config = load_config(args.config)
            manifest = migrate_legacy_v1_file(args.file, config)
            atomic_write_bytes(args.out_manifest, export_manifest_bytes(manifest))
            export_legacy_v1(manifest, args.out_v1)
            _print_json(
                {
                    "status": "migrated",
                    "source_records": manifest["migration"]["record_count"],
                    "downgraded_records": sum(
                        1 for item in manifest["migration"]["downgrades"] if item["downgrade_codes"]
                    ),
                    "manifest_id": manifest["manifest_id"],
                    "out_manifest": str(args.out_manifest),
                    "out_v1": str(args.out_v1),
                }
            )
            return 0

        if args.command == "selfcheck":
            result = _engine(args).consistency_check()
            _print_json(result, compact=args.compact)
            return 0

        parser.error("unknown command")
        return 1
    except FactMergeError as exc:
        print(json.dumps(exc.as_dict(), ensure_ascii=False, sort_keys=True), file=sys.stderr)
        return exc.exit_code
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        error = FactMergeError("E_INVALID_FORMAT", str(exc))
        print(json.dumps(error.as_dict(), ensure_ascii=False, sort_keys=True), file=sys.stderr)
        return error.exit_code
