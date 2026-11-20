"""Command line application for the deterministic sampling engine."""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path
from typing import Any, Iterable

from .config import EngineConfig
from .data import generate_demo_data
from .engine import SamplingEngine
from .errors import EngineError
from .manifest import ManifestCodec
from .storage import atomic_write_json, atomic_write_text


DEFAULT_CONFIG = "config/config.json"
DATA_MANIFESTS = (
    "manifest_v2.json",
    "manifest_v1.json",
    "manifest_v2_large.json",
    "manifest_v1_large.json",
)


def _json_print(payload: Any) -> None:
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2))


def _config_and_workspace(args: argparse.Namespace) -> tuple[EngineConfig, Path]:
    config = EngineConfig.load(args.config) if getattr(args, "config", None) else EngineConfig()
    workspace = Path(getattr(args, "workspace", None) or config.workspace).resolve()
    return config, workspace


def _engine(args: argparse.Namespace) -> SamplingEngine:
    config, workspace = _config_and_workspace(args)
    return SamplingEngine(workspace, config)


def _reset_workspace(workspace: Path) -> None:
    if workspace.exists():
        shutil.rmtree(workspace)
    guard = workspace.parent / f".{workspace.name}.sampling-engine-guard.json"
    if guard.exists():
        guard.unlink()


def _import_all(engine: SamplingEngine, manifest_paths: Iterable[str]) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for path in manifest_paths:
        results.append(engine.import_manifest_path(path))
    return results


def cmd_generate_data(args: argparse.Namespace) -> int:
    metadata = generate_demo_data(args.data_dir, seed=args.seed)
    _json_print({"ok": True, "data_dir": str(Path(args.data_dir).resolve()), **metadata})
    return 0


def cmd_validate_manifest(args: argparse.Namespace) -> int:
    config, _ = _config_and_workspace(args)
    codec = ManifestCodec(config)
    manifest = codec.decode_path(args.manifest)
    _json_print(
        {
            "ok": True,
            "manifest_version": manifest.version,
            "manifest_digest": manifest.manifest_digest,
            "source_path": manifest.source_path,
            "vocab_size": len(manifest.vocab),
            "request_ids": list(manifest.request_ids),
            "conversions": manifest.conversions,
            "warnings": list(manifest.warnings),
        }
    )
    return 0


def cmd_convert(args: argparse.Namespace) -> int:
    config, _ = _config_and_workspace(args)
    codec = ManifestCodec(config)
    manifest = codec.decode_path(args.manifest)
    document = codec.export_document(manifest, args.target_version)
    text = json.dumps(document, ensure_ascii=False, indent=2) + "\n"
    atomic_write_text(args.output, text)
    roundtrip = codec.decode_document(
        document,
        base_dir=str(Path(args.manifest).resolve().parent),
        source_path=str(Path(args.output).resolve()),
    )
    mismatches: list[dict[str, Any]] = []
    original_by_id = {request.request_id: request for request in manifest.requests}
    for request in roundtrip.requests:
        original = original_by_id[request.request_id]
        for field in ("temperature", "top_k", "top_p", "seed"):
            if getattr(original, field) != getattr(request, field):
                mismatches.append(
                    {
                        "request_id": request.request_id,
                        "field": field,
                        "original": getattr(original, field),
                        "roundtrip": getattr(request, field),
                    }
                )
    output = {
        "ok": not mismatches,
        "input": str(Path(args.manifest).resolve()),
        "output": str(Path(args.output).resolve()),
        "target_version": args.target_version,
        "derived_manifest_version": roundtrip.version,
        "roundtrip_mismatches": mismatches,
        "conversion_details": roundtrip.conversions,
    }
    _json_print(output)
    return 0 if not mismatches else 1


def cmd_import(args: argparse.Namespace) -> int:
    engine = _engine(args)
    imports = _import_all(engine, args.manifest)
    _json_print(
        {
            "ok": True,
            "workspace": str(engine.workspace),
            "imports": imports,
            "requests": engine.list_requests(),
        }
    )
    return 0


def cmd_import_logits(args: argparse.Namespace) -> int:
    engine = _engine(args)
    _json_print(engine.import_logits(args.path))
    return 0


def cmd_run(args: argparse.Namespace) -> int:
    engine = _engine(args)
    imports = _import_all(engine, args.manifest) if args.manifest else []
    result = engine.run(
        request_ids=args.request_id,
        stop_after_requests=args.stop_after_requests,
    )
    output = {
        "ok": True,
        "workspace": str(engine.workspace),
        "imports": imports,
        **result,
    }
    if args.output:
        atomic_write_json(args.output, output)
    _json_print(output)
    return 0


def cmd_resume(args: argparse.Namespace) -> int:
    engine = _engine(args)
    result = engine.resume(request_ids=args.request_id)
    if args.output:
        atomic_write_json(args.output, result)
    _json_print(result)
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    engine = _engine(args)
    if args.request_id:
        payload = engine.request_status(args.request_id)
    else:
        payload = {
            "workspace": str(engine.workspace),
            "workspace_metadata": engine.workspace_metadata(),
            "manifests": engine.manifest_groups(),
            "requests": [engine.request_status(item["request_id"]) for item in engine.list_requests()],
        }
    _json_print(payload)
    return 0


def cmd_tokens(args: argparse.Namespace) -> int:
    engine = _engine(args)
    _json_print(engine.request_tokens(args.request_id))
    return 0


def cmd_summary(args: argparse.Namespace) -> int:
    engine = _engine(args)
    if args.request_id:
        _json_print(engine.request_summary(args.request_id))
    else:
        _json_print(engine.corpus_summary())
    return 0


def cmd_export_manifest(args: argparse.Namespace) -> int:
    engine = _engine(args)
    document = engine.export_manifest(args.manifest_id, args.target_version)
    text = json.dumps(document, ensure_ascii=False, indent=2) + "\n"
    if args.output:
        atomic_write_text(args.output, text)
    _json_print(document)
    return 0


def cmd_snapshot_export(args: argparse.Namespace) -> int:
    engine = _engine(args)
    snapshot = engine.export_workspace_snapshot(args.output)
    _json_print(
        {
            "ok": True,
            "output": str(Path(args.output).resolve()),
            "generation": snapshot.get("generation"),
            "snapshot_digest": snapshot.get("snapshot_digest"),
            "file_count": len(snapshot.get("files", {})),
        }
    )
    return 0


def cmd_snapshot_validate(args: argparse.Namespace) -> int:
    engine = _engine(args)
    snapshot = engine.validate_workspace_snapshot(
        args.snapshot, allow_rollback=args.allow_rollback
    )
    _json_print(
        {
            "ok": True,
            "snapshot_digest": snapshot.get("snapshot_digest"),
            "generation": snapshot.get("generation"),
            "workspace_id": snapshot.get("workspace_id"),
            "file_count": len(snapshot.get("files", {})),
        }
    )
    return 0


def cmd_serve(args: argparse.Namespace) -> int:
    from .service import serve

    config, workspace = _config_and_workspace(args)
    engine = SamplingEngine(workspace, config)
    if args.manifest:
        _import_all(engine, args.manifest)
    elif config.initial_manifests:
        if not engine.list_requests():
            _import_all(engine, config.initial_manifests)
    serve(engine, config)
    return 0


def _run_demo_engine(
    workspace: Path,
    config: EngineConfig,
    manifests: Iterable[str],
    *,
    stop_after: int | None = None,
) -> tuple[SamplingEngine, dict[str, Any], dict[str, Any] | None]:
    engine = SamplingEngine(workspace, config)
    imports = _import_all(engine, manifests)
    first = engine.run(stop_after_requests=stop_after)
    second = engine.resume() if stop_after is not None else None
    return engine, {"imports": imports, "first_run": first}, second


def cmd_demo(args: argparse.Namespace) -> int:
    data_dir = Path(args.data_dir).resolve()
    if args.generate_data or not (data_dir / "manifest_v2.json").exists():
        generate_demo_data(data_dir, seed=args.seed)
    config = EngineConfig.load(args.config)
    manifests = [str(data_dir / name) for name in DATA_MANIFESTS]
    demo_root = Path(args.workspace).resolve()
    full_workspace = demo_root.parent / f"{demo_root.name}_full"
    if not args.keep:
        _reset_workspace(full_workspace)
        _reset_workspace(demo_root)
    full_engine, full_trace, _ = _run_demo_engine(
        full_workspace, config, manifests
    )
    stop_engine, stop_trace, resume_trace = _run_demo_engine(
        demo_root, config, manifests, stop_after=args.stop_after
    )
    full_result = full_trace["first_run"]["result"]
    resumed_result = (resume_trace or stop_trace["first_run"])["result"]
    stop_resume_equal = full_result["result_digest"] == resumed_result["result_digest"]
    sequence_checks: dict[str, Any] = {}
    sequences_contiguous = True
    for item in full_engine.list_requests():
        status = full_engine.request_status(item["request_id"])
        if status["status"] in {"complete", "error"}:
            contiguous = bool(status["sequence"]["contiguous"])
            sequence_checks[item["request_id"]] = status["sequence"]
            sequences_contiguous = sequences_contiguous and contiguous
    expected_path = Path(args.expected)
    expected_digest = None
    expected_match: bool | None = None
    if expected_path.exists():
        try:
            expected_payload = json.loads(expected_path.read_text(encoding="utf-8"))
            expected_digest = expected_payload.get("result_digest")
            expected_match = expected_digest == full_result["result_digest"]
        except (OSError, json.JSONDecodeError):
            expected_match = False
    if args.write_expected or expected_digest is None:
        atomic_write_json(
            expected_path,
            {
                "expected_schema": 1,
                "result_digest": full_result["result_digest"],
                "corpus": full_result["corpus"],
                "request_count": len(full_result["requests"]),
            },
        )
        expected_digest = full_result["result_digest"]
        expected_match = True
    output = {
        "ok": bool(stop_resume_equal and sequences_contiguous and expected_match),
        "full_workspace": str(full_workspace),
        "stop_resume_workspace": str(demo_root),
        "full_run": full_result,
        "stop_run": stop_trace["first_run"],
        "resume_run": resume_trace,
        "verification": {
            "full_result_digest": full_result["result_digest"],
            "resumed_result_digest": resumed_result["result_digest"],
            "stop_resume_equal": stop_resume_equal,
            "checkpoint_sequences_contiguous": sequences_contiguous,
            "sequence_checks": sequence_checks,
            "expected_digest": expected_digest,
            "expected_match": expected_match,
            "total_tokens": full_result["corpus"]["token_count"],
            "successful_requests": full_result["corpus"]["successful_request_count"],
            "failed_requests": full_result["corpus"]["failed_request_count"],
        },
    }
    atomic_write_json(args.output, output)
    _json_print(output)
    return 0 if output["ok"] else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m sampling_engine",
        description="Deterministic local LLM sampling and decoding engine",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    def add_workspace_options(sub: argparse.ArgumentParser) -> None:
        sub.add_argument("--workspace", help="writable local workspace directory")
        sub.add_argument("--config", help=f"engine config JSON (default for serve: {DEFAULT_CONFIG})")

    sub = subparsers.add_parser("generate-data", help="generate deterministic data")
    sub.add_argument("--data-dir", default="data")
    sub.add_argument("--seed", type=int, default=20261121)
    sub.set_defaults(func=cmd_generate_data)

    sub = subparsers.add_parser("validate-manifest", help="decode and validate a manifest")
    sub.add_argument("--manifest", required=True)
    sub.add_argument("--config")
    sub.set_defaults(func=cmd_validate_manifest)

    sub = subparsers.add_parser("convert", help="convert a manifest to v1 or v2")
    sub.add_argument("--manifest", required=True)
    sub.add_argument("--target-version", type=int, choices=(1, 2), required=True)
    sub.add_argument("--output", required=True)
    sub.add_argument("--config")
    sub.set_defaults(func=cmd_convert)

    sub = subparsers.add_parser("import", help="import one or more manifests")
    sub.add_argument("--manifest", action="append", required=True)
    add_workspace_options(sub)
    sub.set_defaults(func=cmd_import)

    sub = subparsers.add_parser("import-logits", help="copy a logits shard into the workspace")
    sub.add_argument("--path", required=True)
    add_workspace_options(sub)
    sub.set_defaults(func=cmd_import_logits)

    sub = subparsers.add_parser("run", help="run all or selected requests")
    sub.add_argument("--manifest", action="append", default=[])
    sub.add_argument("--request-id", action="append", default=None)
    sub.add_argument("--stop-after-requests", type=int)
    sub.add_argument("--output")
    add_workspace_options(sub)
    sub.set_defaults(func=cmd_run)

    sub = subparsers.add_parser("resume", help="resume from active checkpoints")
    sub.add_argument("--request-id", action="append", default=None)
    sub.add_argument("--output")
    add_workspace_options(sub)
    sub.set_defaults(func=cmd_resume)

    sub = subparsers.add_parser("status", help="show request/stage status")
    sub.add_argument("--request-id")
    add_workspace_options(sub)
    sub.set_defaults(func=cmd_status)

    sub = subparsers.add_parser("tokens", help="show step-by-step sampled tokens")
    sub.add_argument("--request-id", required=True)
    add_workspace_options(sub)
    sub.set_defaults(func=cmd_tokens)

    sub = subparsers.add_parser("summary", help="show request or corpus summary")
    sub.add_argument("--request-id")
    add_workspace_options(sub)
    sub.set_defaults(func=cmd_summary)

    sub = subparsers.add_parser("export-manifest", help="export an imported manifest group")
    sub.add_argument("--manifest-id", required=True)
    sub.add_argument("--target-version", type=int, choices=(1, 2), required=True)
    sub.add_argument("--output")
    add_workspace_options(sub)
    sub.set_defaults(func=cmd_export_manifest)

    sub = subparsers.add_parser("snapshot-export", help="export a workspace JSON snapshot")
    sub.add_argument("--output", required=True)
    add_workspace_options(sub)
    sub.set_defaults(func=cmd_snapshot_export)

    sub = subparsers.add_parser("snapshot-validate", help="validate a workspace snapshot")
    sub.add_argument("--snapshot", required=True)
    sub.add_argument("--allow-rollback", action="store_true")
    add_workspace_options(sub)
    sub.set_defaults(func=cmd_snapshot_validate)

    sub = subparsers.add_parser("serve", help="start the loopback HTTP service")
    sub.add_argument("--manifest", action="append")
    sub.add_argument("--workspace")
    sub.add_argument("--config", default=DEFAULT_CONFIG)
    sub.set_defaults(func=cmd_serve)

    sub = subparsers.add_parser("demo", help="run deterministic full/stop/resume demonstration")
    sub.add_argument("--workspace", default="work/demo")
    sub.add_argument("--data-dir", default="data")
    sub.add_argument("--config", default=DEFAULT_CONFIG)
    sub.add_argument("--seed", type=int, default=20261121)
    sub.add_argument("--stop-after", type=int, default=2)
    sub.add_argument("--output", default="artifacts/demo_result.json")
    sub.add_argument("--expected", default="artifacts/demo_expected.json")
    sub.add_argument("--write-expected", action="store_true")
    sub.add_argument("--generate-data", action="store_true")
    sub.add_argument("--keep", action="store_true", help="reuse existing demo workspaces")
    sub.set_defaults(func=cmd_demo)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except EngineError as exc:
        print(json.dumps({"ok": False, **exc.to_dict()}, ensure_ascii=False, indent=2), file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print(json.dumps({"ok": False, "code": "E_INTERRUPTED"}), file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
