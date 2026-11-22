"""Command-line interface for the deterministic retrieval-answer workbench."""
from __future__ import annotations

import argparse
import sys
from typing import Any, Dict, List

from . import generator
from .errors import WorkbenchError
from .incremental import detect_change, rebuild_incremental, write_report
from .io_utils import canonical_pretty_json, read_json
from .pipeline import Pipeline
from .server import serve
from . import query as query_mod


def _print(payload: Any) -> None:
    print(canonical_pretty_json(payload))


def _cmd_generate(args: argparse.Namespace) -> int:
    outputs = generator.generate(args.data_dir)
    _print({"ok": True, "outputs": outputs})
    return 0


def _cmd_run(args: argparse.Namespace) -> int:
    pipeline = Pipeline(args.data_dir, args.run_dir)
    summary = pipeline.run(session_ids=args.session,
                           max_turns=args.max_turns)
    _print({"ok": True, "summary": summary})
    return 0


def _cmd_stop(args: argparse.Namespace) -> int:
    pipeline = Pipeline(args.data_dir, args.run_dir)
    pipeline.stop.request_stop()
    _print({"ok": True, "stop_requested": True})
    return 0


def _cmd_recover(args: argparse.Namespace) -> int:
    pipeline = Pipeline(args.data_dir, args.run_dir)
    summary = pipeline.run(session_ids=args.session)
    _print({"ok": True, "summary": summary})
    return 0


def _cmd_status(args: argparse.Namespace) -> int:
    pipeline = Pipeline(args.data_dir, args.run_dir)
    path = f"{args.run_dir}/summary.json"
    payload: Dict[str, Any] = {
        "sessions": {},
        "config_fingerprint": pipeline.config_fingerprint,
    }
    for session_id in sorted(pipeline.sessions):
        payload["sessions"][session_id] = pipeline.store.load_state(
            session_id)
    try:
        payload["summary"] = read_json(path)
    except FileNotFoundError:
        payload["summary"] = None
    _print(payload)
    return 0


def _cmd_fingerprints(args: argparse.Namespace) -> int:
    pipeline = Pipeline(args.data_dir, args.run_dir)
    diagnostics = read_json(
        pipeline._diagnostics_path(args.session_id))
    _print({"session_id": args.session_id,
            "turns": [
                {"turn_index": turn["turn_index"],
                 "input_fingerprints": turn["input_fingerprints"],
                 "stage_fingerprints": turn["stage_fingerprints"]}
                for turn in diagnostics["turns"]]})
    return 0


def _cmd_evidence(args: argparse.Namespace) -> int:
    pipeline = Pipeline(args.data_dir, args.run_dir)
    diagnostics = read_json(
        pipeline._diagnostics_path(args.session_id))
    for turn in diagnostics["turns"]:
        if turn["turn_index"] == args.turn:
            _print(turn)
            return 0
    raise WorkbenchError("E_INVALID_STAGE",
                         f"turn {args.turn} not found",
                         session_id=args.session_id, turn_index=args.turn)


def _cmd_export(args: argparse.Namespace) -> int:
    pipeline = Pipeline(args.data_dir, args.run_dir)
    _print(read_json(pipeline._answer_path(args.session_id)))
    return 0


def _cmd_invalidate(args: argparse.Namespace) -> int:
    pipeline = Pipeline(args.data_dir, args.run_dir)
    reports = []
    for session_id, turns in sorted(pipeline.sessions.items()):
        focus = query_mod.Focus()
        for turn in sorted(turns, key=lambda row: row["turn_index"]):
            report = detect_change(pipeline, session_id, turn,
                                   focus.to_dict())
            reports.append(report)
            if turn.get("focus_hint"):
                focus = query_mod.Focus(entity=turn["focus_hint"])
    path = write_report(args.run_dir, reports)
    _print({"ok": True, "report_path": path,
            "reports": [report.to_dict() for report in reports]})
    return 0


def _cmd_verify_incremental(args: argparse.Namespace) -> int:
    result = rebuild_incremental(
        Pipeline(args.data_dir, args.clean_run_dir),
        data_dir_baseline=args.data_dir,
        run_dir_clean=args.clean_run_dir,
        changed_data_dir=args.changed_data_dir,
        changed_run_dir=args.changed_run_dir)
    _print({"ok": True, "verification": result})
    return 0


def _cmd_serve(args: argparse.Namespace) -> int:
    serve(args.host, args.port, args.data_dir, args.run_dir)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ragwb",
        description="Local deterministic conversational retrieval workbench")
    sub = parser.add_subparsers(dest="command", required=True)

    gen = sub.add_parser("generate", help="generate deterministic demo data")
    gen.add_argument("--data-dir", required=True)
    gen.set_defaults(func=_cmd_generate)

    def add_run_dir(target: argparse.ArgumentParser) -> None:
        target.add_argument("--data-dir", required=True)
        target.add_argument("--run-dir", required=True)

    run = sub.add_parser("run", help="execute the four-stage pipeline")
    add_run_dir(run)
    run.add_argument("--session", action="append")
    run.add_argument("--max-turns", type=int)
    run.set_defaults(func=_cmd_run)

    stop = sub.add_parser("stop", help="request a graceful stop")
    add_run_dir(stop)
    stop.set_defaults(func=_cmd_stop)

    recover = sub.add_parser("recover", help="resume after interruption")
    add_run_dir(recover)
    recover.add_argument("--session", action="append")
    recover.set_defaults(func=_cmd_recover)

    status = sub.add_parser("status", help="show checkpoint state")
    add_run_dir(status)
    status.set_defaults(func=_cmd_status)

    fp = sub.add_parser("fingerprints", help="show stage fingerprints")
    add_run_dir(fp)
    fp.add_argument("--session-id", required=True)
    fp.set_defaults(func=_cmd_fingerprints)

    ev = sub.add_parser("evidence", help="show evidence chain for a turn")
    add_run_dir(ev)
    ev.add_argument("--session-id", required=True)
    ev.add_argument("--turn", type=int, required=True)
    ev.set_defaults(func=_cmd_evidence)

    export = sub.add_parser("export", help="export session answers")
    add_run_dir(export)
    export.add_argument("--session-id", required=True)
    export.set_defaults(func=_cmd_export)

    invalid = sub.add_parser(
        "invalidate", help="detect version changes and invalidation scope")
    add_run_dir(invalid)
    invalid.set_defaults(func=_cmd_invalidate)

    verify = sub.add_parser(
        "verify-incremental",
        help="byte-compare incremental rebuild with a clean full run")
    verify.add_argument("--data-dir", required=True)
    verify.add_argument("--clean-run-dir", required=True)
    verify.add_argument("--changed-data-dir", required=True)
    verify.add_argument("--changed-run-dir", required=True)
    verify.set_defaults(func=_cmd_verify_incremental)

    server = sub.add_parser("serve", help="start the local HTTP interface")
    server.add_argument("--host", default="127.0.0.1")
    server.add_argument("--port", type=int, default=8765)
    server.add_argument("--data-dir")
    server.add_argument("--run-dir")
    server.set_defaults(func=_cmd_serve)
    return parser


def main(argv: List[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except WorkbenchError as exc:
        print(canonical_pretty_json({"ok": False, "error": exc.to_dict()}),
              file=sys.stderr)
        return 2


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
