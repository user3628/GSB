"""Command-line interface with JSON-only success/error contracts."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from .engine import Engine, StopController
from .errors import EngineError
from .generator import generate_workspace
from .util import pretty_json


EXIT_CODES = {
    "E_STALE_CHECKPOINT": 73,
    "E_INVALID_STAGE": 64,
    "E_INCREMENTAL_MISMATCH": 65,
    "E_INVALID_CONFIG": 78,
    "E_STOPPED": 75,
}


def _emit(value: Any, *, pretty: bool) -> None:
    if pretty:
        print(pretty_json(value))
    else:
        print(json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")))


def _add_common(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--pretty", action="store_true", help="pretty-print JSON output")


def _records_from_jsonl(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise EngineError(
            "E_INVALID_STAGE",
            "replacement records file cannot be read",
            details={"path": str(path), "error": str(exc)},
        ) from exc
    for line_number, line in enumerate(lines, 1):
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except ValueError as exc:
            raise EngineError(
                "E_INVALID_STAGE",
                "replacement records file contains invalid JSON",
                details={"path": str(path), "line": line_number, "error": str(exc)},
            ) from exc
        if not isinstance(record, dict):
            raise EngineError(
                "E_INVALID_STAGE",
                "each replacement record must be an object",
                details={"path": str(path), "line": line_number},
            )
        records.append(record)
    return records


def _load_answers(path: Path) -> dict[str, str]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise EngineError(
            "E_INVALID_STAGE",
            "answer update file is not valid JSON",
            details={"path": str(path), "error": str(exc)},
        ) from exc
    if isinstance(raw, dict) and isinstance(raw.get("answers"), dict):
        raw = raw["answers"]
    if not isinstance(raw, dict) or not all(
        isinstance(key, str) and isinstance(value, str) for key, value in raw.items()
    ):
        raise EngineError(
            "E_INVALID_STAGE",
            "answer update file must be an object mapping sample ids to strings",
            details={"path": str(path)},
        )
    return raw


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m eval_engine")
    subparsers = parser.add_subparsers(dest="command", required=True)

    generate_parser = subparsers.add_parser("generate")
    _add_common(generate_parser)
    generate_parser.add_argument("--workspace", default=".")
    generate_parser.add_argument("--seed", type=int, default=20261121)
    generate_parser.add_argument("--force", action="store_true")

    build = subparsers.add_parser("build")
    _add_common(build)
    build.add_argument("--workspace", default=".")
    build.add_argument("--verify", action="store_true")
    build.add_argument("--stop-file", default=None)

    status = subparsers.add_parser("status")
    _add_common(status)
    status.add_argument("--workspace", default=".")

    query = subparsers.add_parser("query")
    _add_common(query)
    query.add_argument("--workspace", default=".")
    query.add_argument("--stage", required=True)
    query.add_argument("--allow-stale", action="store_true")

    update_answers = subparsers.add_parser("update-answers")
    _add_common(update_answers)
    update_answers.add_argument("--workspace", default=".")
    update_answers.add_argument("--benchmark", required=True)
    update_answers.add_argument("--answers-file", required=True)
    update_answers.add_argument("--replace", action="store_true")

    replace_batch = subparsers.add_parser("replace-batch")
    _add_common(replace_batch)
    replace_batch.add_argument("--workspace", default=".")
    replace_batch.add_argument("--batch", required=True)
    replace_batch.add_argument("--records-file", required=True)

    verify = subparsers.add_parser("verify")
    _add_common(verify)
    verify.add_argument("--workspace", default=".")
    return parser


def run(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    workspace = Path(args.workspace).resolve()
    if args.command == "generate":
        return (
            generate_workspace(workspace, seed=args.seed, force=args.force),
            0,
        )
    engine = Engine(workspace)
    if args.command == "build":
        controller = (
            StopController(stop_file=Path(args.stop_file))
            if args.stop_file
            else StopController(stop_file=engine.stop_file)
        )
        result = engine.build(stop_controller=controller, verify=args.verify)
        return result.as_dict(), 75 if result.status == "stopped" else 0
    if args.command == "status":
        return engine.status(), 0
    if args.command == "query":
        return engine.query_stage(args.stage, allow_stale=args.allow_stale), 0
    if args.command == "update-answers":
        return (
            engine.update_answers(
                args.benchmark,
                _load_answers(Path(args.answers_file)),
                replace=args.replace,
            ),
            0,
        )
    if args.command == "replace-batch":
        return (
            engine.replace_batch(
                args.batch, _records_from_jsonl(Path(args.records_file))
            ),
            0,
        )
    if args.command == "verify":
        result = engine.build(verify=False)
        verification = engine.verify_consistency()
        return {
            "ok": True,
            "build_status": result.status,
            "verification": verification,
        }, 0
    raise EngineError("E_INVALID_STAGE", "unknown command", details={"command": args.command})


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        result, exit_code = run(args)
    except EngineError as exc:
        _emit(exc.as_dict(), pretty=getattr(args, "pretty", False))
        return EXIT_CODES.get(exc.code, 1)
    _emit(result, pretty=getattr(args, "pretty", False))
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
