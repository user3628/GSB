"""Command-line entry point for the rolling-backtest workbench.

Run ``python -m src.cli --help`` for the full command list.  All paths are
local and no network access is performed by the CLI.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any, Dict

from .checkpoints import (list_checkpoints, read_state, request_stop,
                          reset_workspace)
from .config import load_config
from .errors import WorkbenchError
from .pipeline import run_pipeline, STAGES
from .report import export_all


def _print(payload: Dict[str, Any]) -> None:
    print(json.dumps(payload, sort_keys=True, indent=2, ensure_ascii=False))


def _cmd_run(args) -> int:
    cfg = load_config(args.config)
    state = run_pipeline(args.workspace, args.manifest, cfg,
                         parallel=not args.serial,
                         stop_after_folds=args.stop_after_folds)
    if args.export:
        if state.get("run_status") == "completed":
            paths = export_all(args.workspace, state)
            state["exports"] = paths
    _print({"run_status": state["run_status"], "stage": state["stage"],
            "stop_requested": state.get("stop_requested", False),
            "accepted": state.get("scheduler", {}).get("accepted_folds"),
            "rejected": state.get("scheduler", {}).get("rejected_folds")})
    return 0


def _cmd_stop(args) -> int:
    request_stop(args.workspace)
    _print({"requested": True, "workspace": args.workspace})
    return 0


def _cmd_resume(args) -> int:
    cfg = load_config(args.config)
    state = run_pipeline(args.workspace, args.manifest, cfg,
                         parallel=not args.serial)
    if args.export:
        paths = export_all(args.workspace, state)
        state["exports"] = paths
    _print({"run_status": state["run_status"], "stage": state["stage"]})
    return 0


def _cmd_status(args) -> int:
    state = read_state(args.workspace)
    if state is None:
        _print({"run_status": "never_run", "stages": STAGES})
        return 0
    _print(state)
    return 0


def _cmd_fingerprints(args) -> int:
    state = read_state(args.workspace) or {}
    _print({
        "config_fingerprint": state.get("config_fingerprint"),
        "manifest_fingerprint": state.get("manifest_fingerprint"),
        "aggregate_fingerprint": state.get("aggregate_fingerprint"),
        "checkpoints": state.get("checkpoints", []),
        "invalidation": state.get("invalidation", []),
    })
    return 0


def _cmd_unprocessed(args) -> int:
    state = read_state(args.workspace) or {}
    _print({
        "rejected_folds": state.get("rejected_folds", []),
        "not_started_folds": state.get("not_started_folds", []),
        "series_errors": state.get("series_errors", []),
        "failures": state.get("failures", []),
    })
    return 0


def _cmd_export(args) -> int:
    state = read_state(args.workspace)
    if state is None:
        raise WorkbenchError("no pipeline state found; run the pipeline first",
                             code="E_NOT_FOUND")
    paths = export_all(args.workspace, state)
    _print(paths)
    return 0


def _cmd_reset(args) -> int:
    reset_workspace(args.workspace)
    _print({"reset": True, "workspace": args.workspace})
    return 0


def _cmd_checkpoints(args) -> int:
    _print({"checkpoints": list_checkpoints(args.workspace)})
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="workbench",
        description="Local deterministic time-series rolling backtest "
                    "and parallel scheduling workbench")
    parser.add_argument("--workspace", default="workbench",
                        help="local workspace for state and checkpoints")
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="run the rolling backtest pipeline")
    run.add_argument("--manifest", default="samples/manifest.json")
    run.add_argument("--config", default="config/backtest.yaml")
    run.add_argument("--serial", action="store_true",
                     help="force in-process serial execution")
    run.add_argument("--stop-after-folds", type=int, default=None,
                     help="test hook: stop cooperatively after N folds")
    run.add_argument("--export", action="store_true",
                     help="export reports at the end of the run")
    run.set_defaults(func=_cmd_run)

    stop = sub.add_parser("stop", help="request a cooperative stop")
    stop.set_defaults(func=_cmd_stop)

    resume = sub.add_parser("resume", help="resume a stopped/incomplete run")
    resume.add_argument("--manifest", default="samples/manifest.json")
    resume.add_argument("--config", default="config/backtest.yaml")
    resume.add_argument("--serial", action="store_true")
    resume.add_argument("--export", action="store_true")
    resume.set_defaults(func=_cmd_resume)

    status = sub.add_parser("status", help="show pipeline state")
    status.set_defaults(func=_cmd_status)

    fp = sub.add_parser("fingerprints", help="show version fingerprints")
    fp.set_defaults(func=_cmd_fingerprints)

    un = sub.add_parser("unprocessed", help="show rejected/unprocessed folds")
    un.set_defaults(func=_cmd_unprocessed)

    export = sub.add_parser("export", help="export JSON/CSV/HTML reports")
    export.set_defaults(func=_cmd_export)

    reset = sub.add_parser("reset", help="delete generated workspace state")
    reset.set_defaults(func=_cmd_reset)

    cps = sub.add_parser("checkpoints", help="list shard checkpoints")
    cps.set_defaults(func=_cmd_checkpoints)
    return parser


def main(argv=None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    os.makedirs(args.workspace, exist_ok=True)
    try:
        return args.func(args)
    except WorkbenchError as exc:
        print(json.dumps(exc.to_dict(), sort_keys=True, ensure_ascii=False),
              file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
