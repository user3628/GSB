"""Command-line workbench for deterministic builds and operations."""

from __future__ import annotations

import argparse
import json
from typing import Any

from .config import validate_config
from .engine import Engine
from .errors import CabinVoiceError
from .manifest import zone_ids as manifest_zone_ids
from .samplegen import generate_demo
from .storage import Workspace
from .timeline import EditModel
from .util import nested_set
from .validation import run_validation


def _print(value: Any) -> None:
    print(json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2))


def _load_json_argument(raw: str) -> Any:
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise CabinVoiceError("E_BAD_REQUEST", "argument is not valid JSON", {"value": raw}) from exc


def _workspace(args: argparse.Namespace) -> Workspace:
    return Workspace(args.workspace).ensure()


def _engine(args: argparse.Namespace) -> Engine:
    return Engine(_workspace(args))


def _edit_model(workspace: Workspace) -> EditModel:
    manifest = workspace.require_manifest()
    return EditModel(workspace.edits_path, manifest)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="cabin-voice", description="In-cabin multi-zone voice arbitration")
    subparsers = parser.add_subparsers(dest="command", required=True)

    generate = subparsers.add_parser("generate", help="generate deterministic demo WAVs and manifests")
    generate.add_argument("--output", default="demo")
    generate.add_argument("--seed", type=int, default=20261123)

    imp = subparsers.add_parser("import", help="import a manifest and its WAVs into a workspace")
    imp.add_argument("--workspace", default="workspace")
    imp.add_argument("manifest")

    build = subparsers.add_parser("build", help="run the four-stage build")
    build.add_argument("--workspace", default="workspace")
    build.add_argument("--manifest", help="import this manifest before building")
    build.add_argument("--mode", choices=["incremental", "full"], default="incremental")
    build.add_argument("--resume", action="store_true")
    build.add_argument("--verify", action="store_true", help="compare final bytes with a clean full build")
    build.add_argument("--stop-after-units", type=int, default=None, help=argparse.SUPPRESS)

    status = subparsers.add_parser("status")
    status.add_argument("--workspace", default="workspace")

    fingerprints = subparsers.add_parser("fingerprints")
    fingerprints.add_argument("--workspace", default="workspace")

    result = subparsers.add_parser("result")
    result.add_argument("--workspace", default="workspace")

    stop = subparsers.add_parser("stop")
    stop.add_argument("--workspace", default="workspace")

    recover = subparsers.add_parser("recover")
    recover.add_argument("--workspace", default="workspace")
    recover.add_argument("--verify", action="store_true")

    verify = subparsers.add_parser("verify-full")
    verify.add_argument("--workspace", default="workspace")

    replace = subparsers.add_parser("replace-sample")
    replace.add_argument("--workspace", default="workspace")
    replace.add_argument("--chunk-id", required=True)
    replace.add_argument("--wav", required=True)
    replace.add_argument("--verify", dest="verify", action="store_true", default=True)
    replace.add_argument("--no-verify", dest="verify", action="store_false")

    config = subparsers.add_parser("set-config")
    config.add_argument("--workspace", default="workspace")
    config.add_argument("--path", required=True, help="dotted path, e.g. detection.frame_ms")
    config.add_argument("--value", required=True, help="JSON value")
    config.add_argument("--build", action="store_true")
    config.add_argument("--verify", action="store_true")

    priority = subparsers.add_parser("set-priority")
    priority.add_argument("--workspace", default="workspace")
    priority.add_argument("--zone-id", required=True)
    priority.add_argument("--priority", required=True, type=int)

    hits = subparsers.add_parser("set-hits")
    hits.add_argument("--workspace", default="workspace")
    hits.add_argument("--chunk-id", required=True)
    hits.add_argument("--zone-id", required=True)
    hits.add_argument("--hits", required=True, help="JSON list of {term,strength,at_ms?}")

    split = subparsers.add_parser("split-activity")
    split.add_argument("--workspace", default="workspace")
    split.add_argument("--chunk-id", required=True)
    split.add_argument("--zone-id", required=True)
    split.add_argument("--at-ms", required=True, type=int)

    merge = subparsers.add_parser("merge-activity")
    merge.add_argument("--workspace", default="workspace")
    merge.add_argument("--chunk-id", required=True)
    merge.add_argument("--zone-id", required=True)
    merge.add_argument("--left-start-ms", required=True, type=int)
    merge.add_argument("--right-start-ms", required=True, type=int)

    serve = subparsers.add_parser("serve", help="serve the local HTTP workbench")
    serve.add_argument("--workspace", default="workspace")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8765)
    serve.add_argument("--manifest", help="import this manifest before serving")

    validate = subparsers.add_parser("validate", help="run the reproducible end-to-end acceptance suite")
    validate.add_argument("--output", default="validation_run")
    validate.add_argument("--seed", type=int, default=20261123)
    return parser


def dispatch(args: argparse.Namespace) -> dict[str, Any]:
    if args.command == "generate":
        return {"ok": True, **generate_demo(args.output, seed=args.seed)}
    if args.command == "validate":
        return {"ok": True, **run_validation(args.output, seed=args.seed)}

    workspace = _workspace(args)
    if args.command == "import":
        return {"ok": True, **workspace.import_bundle(args.manifest)}
    if args.command == "build":
        if getattr(args, "manifest", None):
            workspace.import_bundle(args.manifest)
        return _engine(args).build(
            mode=args.mode,
            resume=args.resume,
            verify=args.verify,
            stop_after_units=args.stop_after_units,
        )
    if args.command == "status":
        return _engine(args).status()
    if args.command == "fingerprints":
        return _engine(args).fingerprints()
    if args.command == "result":
        return {"ok": True, "result": _engine(args).result()}
    if args.command == "stop":
        return {"ok": True, **workspace.request_stop()}
    if args.command == "recover":
        return _engine(args).build(mode="incremental", resume=True, verify=args.verify)
    if args.command == "verify-full":
        return {"ok": True, **_engine(args).verify_full_consistency()}
    if args.command == "replace-sample":
        replacement = workspace.replace_chunk_sample_file(args.chunk_id, args.wav)
        build = _engine(args).build(mode="incremental", verify=args.verify)
        return {"ok": True, "replacement": replacement, "build": build}
    if args.command == "set-config":
        manifest = workspace.require_manifest()
        config = nested_set(workspace.load_config(), args.path, _load_json_argument(args.value))
        validate_config(config, manifest_zone_ids(manifest))
        workspace.save_config(config)
        response: dict[str, Any] = {"ok": True, "path": args.path, "value": _load_json_argument(args.value)}
        if args.build:
            response["build"] = _engine(args).build(mode="incremental", verify=args.verify)
        return response
    if args.command == "set-priority":
        return {"ok": True, "edit": _edit_model(workspace).set_priority(args.zone_id, args.priority)}
    if args.command == "set-hits":
        return {
            "ok": True,
            "edit": _edit_model(workspace).set_hits(
                args.chunk_id, args.zone_id, _load_json_argument(args.hits)
            ),
        }
    if args.command == "split-activity":
        return {
            "ok": True,
            "edit": _edit_model(workspace).split_activity(args.chunk_id, args.zone_id, args.at_ms),
        }
    if args.command == "merge-activity":
        return {
            "ok": True,
            "edit": _edit_model(workspace).merge_activity(
                args.chunk_id, args.zone_id, args.left_start_ms, args.right_start_ms
            ),
        }
    if args.command == "serve":
        from .http_api import serve

        if getattr(args, "manifest", None):
            workspace.import_bundle(args.manifest)
        serve(workspace, args.host, args.port)
        return {"ok": True, "status": "server_stopped"}
    raise CabinVoiceError("E_BAD_REQUEST", "unknown command", {"command": args.command})


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        response = dispatch(args)
        _print(response)
        return 0
    except CabinVoiceError as exc:
        _print(exc.as_dict())
        return 2
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
