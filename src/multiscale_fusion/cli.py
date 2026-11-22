from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from .engine import FusionEngine
from .errors import FusionError
from .http_api import serve_http
from .samplegen import generate_demo_bundle
from .workspace import import_bundle


def _print_json(value: Any) -> None:
    print(json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False, allow_nan=False))


def _workspace(args: argparse.Namespace) -> Path:
    return Path(args.workspace).resolve()


def _engine(args: argparse.Namespace) -> FusionEngine:
    return FusionEngine(_workspace(args))


def _cmd_generate(args: argparse.Namespace) -> int:
    result = generate_demo_bundle(args.output, seed=args.seed, force=args.force)
    _print_json(result)
    return 0


def _cmd_import(args: argparse.Namespace) -> int:
    overrides = {}
    for entry in args.shard_override or []:
        if "=" not in entry:
            raise FusionError("E_INVALID_PARAM", "--shard-override must be RELATIVE_PATH=LOCAL_PATH")
        name, value = entry.split("=", 1)
        overrides[name] = value
    result = import_bundle(args.bundle, _workspace(args), shard_overrides=overrides, force=args.force)
    _print_json(result)
    return 0


def _cmd_demo(args: argparse.Namespace) -> int:
    root = Path(args.dir).resolve()
    bundle_dir = root / "bundle"
    workspace = root / "workspace"
    generated = generate_demo_bundle(bundle_dir, seed=args.seed, force=args.force)
    imported = import_bundle(bundle_dir, workspace, force=args.force)
    run = FusionEngine(workspace).run(args.task, verify_full=args.verify_full)
    _print_json({"generated": generated, "imported": imported, "run": run})
    return 0


def _cmd_run(args: argparse.Namespace) -> int:
    result = _engine(args).run(
        args.task,
        stage_delay=args.stage_delay,
        crash_after_layer=args.crash_after,
        verify_full=args.verify_full,
    )
    _print_json(result)
    return 0


def _cmd_resume(args: argparse.Namespace) -> int:
    result = _engine(args).resume(
        args.task,
        stage_delay=args.stage_delay,
        crash_after_layer=args.crash_after,
        verify_full=args.verify_full,
    )
    _print_json(result)
    return 0


def _cmd_stage(args: argparse.Namespace) -> int:
    result = _engine(args).run_until(args.layer, args.task, stage_delay=args.stage_delay)
    _print_json(result)
    return 0


def _cmd_status(args: argparse.Namespace) -> int:
    _print_json(_engine(args).status(args.task))
    return 0


def _cmd_sensitivity(args: argparse.Namespace) -> int:
    _print_json(_engine(args).sensitivity(args.task))
    return 0


def _cmd_stop(args: argparse.Namespace) -> int:
    _print_json(_engine(args).stop(args.task, reason=args.reason))
    return 0


def _cmd_verify_checkpoints(args: argparse.Namespace) -> int:
    _print_json(_engine(args).verify_checkpoints(args.task))
    return 0


def _cmd_verify_incremental(args: argparse.Namespace) -> int:
    _print_json(_engine(args).verify_incremental(args.task))
    return 0


def _cmd_serve(args: argparse.Namespace) -> int:
    server = serve_http(_workspace(args), host=args.host, port=args.port)
    host, port = server.server_address[:2]
    print(json.dumps({"ok": True, "url": f"http://{host}:{port}", "workspace": str(_workspace(args))}, ensure_ascii=False))
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


def _common_workspace(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--workspace", required=True, help="workspace root containing input/manifest.json")
    parser.add_argument("--task", default=None, help="task id (optional for single-task manifests)")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="msfp", description="Deterministic multi-scale feature pyramid fusion")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("generate", help="generate deterministic demo shards, manifest and scalar references")
    p.add_argument("output")
    p.add_argument("--seed", type=int, default=20261123)
    p.add_argument("--force", action="store_true")
    p.set_defaults(func=_cmd_generate)

    p = sub.add_parser("import", help="import a local bundle into a workspace")
    p.add_argument("--workspace", required=True)
    p.add_argument("--bundle", required=True)
    p.add_argument("--shard-override", action="append", help="RELATIVE_PATH=LOCAL_PATH override")
    p.add_argument("--force", action="store_true")
    p.set_defaults(func=_cmd_import)

    p = sub.add_parser("demo", help="generate, import and run the bundled demo")
    p.add_argument("--dir", default="demo")
    p.add_argument("--seed", type=int, default=20261123)
    p.add_argument("--task", default="demo_fpn")
    p.add_argument("--force", action="store_true")
    p.add_argument("--verify-full", action="store_true")
    p.set_defaults(func=_cmd_demo)

    p = sub.add_parser("run", help="run or incrementally continue all stages")
    _common_workspace(p)
    p.add_argument("--stage-delay", type=float, default=0.0, help="artificial delay before a stage commit (stop testing)")
    p.add_argument("--crash-after", default=None, help="simulate forced termination after a layer checkpoint")
    p.add_argument("--verify-full", action="store_true")
    p.set_defaults(func=_cmd_run)

    p = sub.add_parser("resume", help="clear a stop request and resume from committed checkpoints")
    _common_workspace(p)
    p.add_argument("--stage-delay", type=float, default=0.0)
    p.add_argument("--crash-after", default=None)
    p.add_argument("--verify-full", action="store_true")
    p.set_defaults(func=_cmd_resume)

    p = sub.add_parser("stage", help="run through one named layer")
    _common_workspace(p)
    p.add_argument("--layer", required=True)
    p.add_argument("--stage-delay", type=float, default=0.0)
    p.set_defaults(func=_cmd_stage)

    p = sub.add_parser("status", help="query checkpoints and current stage state")
    _common_workspace(p)
    p.set_defaults(func=_cmd_status)

    p = sub.add_parser("sensitivity", help="query the current sensitivity report")
    _common_workspace(p)
    p.set_defaults(func=_cmd_sensitivity)

    p = sub.add_parser("stop", help="request graceful stop after the current layer")
    _common_workspace(p)
    p.add_argument("--reason", default="cli_request")
    p.set_defaults(func=_cmd_stop)

    p = sub.add_parser("verify-checkpoints", help="verify checkpoint chain, HEAD and artifact digests")
    _common_workspace(p)
    p.set_defaults(func=_cmd_verify_checkpoints)

    p = sub.add_parser("verify-incremental", help="compare current outputs with a from-scratch rebuild")
    _common_workspace(p)
    p.set_defaults(func=_cmd_verify_incremental)

    p = sub.add_parser("serve", help="start the local HTTP API")
    p.add_argument("--workspace", required=True)
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8765)
    p.set_defaults(func=_cmd_serve)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except FusionError as exc:
        _print_json(exc.to_dict())
        return 2
    except BrokenPipeError:
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
