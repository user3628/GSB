"""Command-line experiment interface."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from .config import parse_yaml
from .engine import SummaryEngine
from .errors import EngineError
from .samples import load_documents, write_samples
from .utils import atomic_write_bytes, canonical_json


def _engine(args: argparse.Namespace) -> SummaryEngine:
    return SummaryEngine(
        args.workspace,
        documents_path=args.docs,
        config_path=args.config,
    )


def _print(value: Any) -> None:
    print(canonical_json(value, indent=2))


def _compact_run(result: dict[str, Any]) -> dict[str, Any]:
    return {
        "ok": result.get("ok"),
        "status": result.get("status"),
        "config_version": result.get("config_version"),
        "admission_order": result.get("schedule", {}).get("admission_order"),
        "accepted_doc_ids": result.get("schedule", {}).get("accepted_doc_ids"),
        "unprocessed": result.get("schedule", {}).get("unprocessed"),
        "deferred": result.get("deferred"),
        "budget_usage": result.get("budget_usage"),
        "documents": [
            {
                "doc_id": doc["doc_id"],
                "summary": doc["summary"],
                "route": doc["final"]["candidate_route"],
                "fusion_score": doc["final"]["fusion_score"],
                "changed_stages": doc["changed_stages"],
                "reused_stages": doc["reused_stages"],
                "needs_review": doc["final"]["needs_review"],
            }
            for doc in result.get("documents", [])
        ],
        "documents_with_errors": result.get("documents_with_errors"),
        "error": result.get("error"),
    }


def _load_config_argument(path: str | None) -> dict[str, Any] | None:
    if not path:
        return None
    file_path = Path(path)
    if file_path.suffix.lower() == ".json":
        with file_path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    return parse_yaml(file_path.read_text(encoding="utf-8"), str(file_path))


def cmd_init(args: argparse.Namespace) -> int:
    workspace = Path(args.workspace).resolve()
    sample_dir = workspace / "samples"
    docs = sample_dir / "documents.jsonl"
    config = sample_dir / "config.yaml"
    if args.force or not docs.exists() or not config.exists():
        result = write_samples(sample_dir)
    else:
        result = {"documents": str(docs), "config": str(config), "document_count": len(docs.read_text(encoding="utf-8").splitlines())}
    engine = SummaryEngine(workspace, documents_path=docs, config_path=config)
    engine.store.initialize()
    _print({"ok": True, "workspace": str(workspace), **result, "state": str(engine.store.state_dir), "exports": str(engine.store.exports_dir)})
    return 0


def cmd_run(args: argparse.Namespace) -> int:
    engine = _engine(args)
    result = engine.run(mode=args.mode, stop_after_docs=args.stop_after)
    if args.out:
        atomic_write_bytes(Path(args.out), canonical_json(result, indent=2).encode("utf-8") + b"\n")
    _print(result if args.full else _compact_run(result))
    return 0 if result.get("ok") else 3


def cmd_resume(args: argparse.Namespace) -> int:
    engine = _engine(args)
    result = engine.resume(mode=args.mode, stop_after_docs=args.stop_after)
    if args.out:
        atomic_write_bytes(Path(args.out), canonical_json(result, indent=2).encode("utf-8") + b"\n")
    _print(result if args.full else _compact_run(result))
    return 0 if result.get("ok") else 3


def cmd_status(args: argparse.Namespace) -> int:
    result = _engine(args).status()
    if not args.full:
        result = {
            "ok": result["ok"],
            "config_version": result["config_version"],
            "checkpoint_count": result["checkpoint_count"],
            "last_checkpoint_sequence": result["last_checkpoint_sequence"],
            "documents": [
                {
                    "doc_id": doc["doc_id"],
                    "accepted": doc["accepted"],
                    "ready": doc["ready"],
                    "cached_stages": doc["cached_stages"],
                    "affected_stages": doc["affected_stages"],
                    "earliest_affected_stage": doc["earliest_affected_stage"],
                    "version_fingerprint": doc["version_fingerprint"],
                    "summary": doc["summary"],
                    "needs_review": doc["needs_review"],
                    "unprocessed": doc["unprocessed"],
                }
                for doc in result["documents"]
            ],
        }
    _print(result)
    return 0


def cmd_budget(args: argparse.Namespace) -> int:
    _print(_engine(args).budget_status())
    return 0


def cmd_candidates(args: argparse.Namespace) -> int:
    _print(_engine(args).candidates(args.doc_id))
    return 0


def cmd_edit(args: argparse.Namespace) -> int:
    if args.text_file:
        text = Path(args.text_file).read_text(encoding="utf-8")
    elif args.text is not None:
        text = args.text
    else:
        raise EngineError("E_INVALID_INPUT", "Provide --text or --text-file")
    _print(_engine(args).edit_summary(args.doc_id, text))
    return 0


def cmd_stop(args: argparse.Namespace) -> int:
    _print(_engine(args).request_stop(args.reason))
    return 0


def cmd_export(args: argparse.Namespace) -> int:
    engine = _engine(args)
    result = engine.export()
    if args.print:
        _print(engine.export_set())
    else:
        _print(result)
    return 0


def cmd_import_docs(args: argparse.Namespace) -> int:
    engine = _engine(args)
    file_path = Path(args.file)
    documents: Any
    if file_path.suffix.lower() == ".jsonl":
        documents = load_documents(file_path)
    else:
        with file_path.open("r", encoding="utf-8") as handle:
            value = json.load(handle)
        documents = value.get("documents") if isinstance(value, dict) else value
    _print(engine.import_documents(documents))
    return 0


def cmd_config(args: argparse.Namespace) -> int:
    engine = _engine(args)
    raw = _load_config_argument(args.file)
    _print({"ok": True, "config": engine.save_config(raw or {})})
    return 0


def cmd_serve(args: argparse.Namespace) -> int:
    from .http_api import run_http_server

    engine = _engine(args)
    run_http_server(engine, host=args.host, port=args.port)
    return 0


def cmd_demo(args: argparse.Namespace) -> int:
    from .demo import run_demo

    result = run_demo(args.workspace)
    _print(result)
    return 0 if result.get("ok") else 3


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Deterministic local news summary and rewriting engine")
    parser.add_argument("--workspace", default=".", help="Working directory containing samples/, state/, summaries/ and exports/")
    parser.add_argument("--docs", default=None, help="Override document shard JSONL path")
    parser.add_argument("--config", default=None, help="Override YAML/JSON configuration path")
    sub = parser.add_subparsers(dest="command", required=True)

    init = sub.add_parser("init", help="Create fixed-seed samples and output directories")
    init.add_argument("--force", action="store_true")
    init.set_defaults(func=cmd_init)

    run = sub.add_parser("run", help="Run admitted documents")
    run.add_argument("--mode", choices=("serial", "parallel"), default="parallel")
    run.add_argument("--stop-after", type=int, default=None, help="Inject a stop request after N indivisible document units")
    run.add_argument("--full", action="store_true", help="Print full run result")
    run.add_argument("--out", default=None, help="Write full run result JSON here")
    run.set_defaults(func=cmd_run)

    resume = sub.add_parser("resume", help="Clear the stop request and resume with cached stages")
    resume.add_argument("--mode", choices=("serial", "parallel"), default="parallel")
    resume.add_argument("--stop-after", type=int, default=None)
    resume.add_argument("--full", action="store_true")
    resume.add_argument("--out", default=None)
    resume.set_defaults(func=cmd_resume)

    status = sub.add_parser("status", help="Show stage state and version fingerprints")
    status.add_argument("--full", action="store_true")
    status.set_defaults(func=cmd_status)

    budget = sub.add_parser("budget", help="Show budget usage and unprocessed reasons")
    budget.set_defaults(func=cmd_budget)

    candidates = sub.add_parser("candidates", help="Show candidate scores and elimination reasons")
    candidates.add_argument("--doc-id", required=True)
    candidates.set_defaults(func=cmd_candidates)

    edit = sub.add_parser("edit", help="Submit a human summary edit")
    edit.add_argument("--doc-id", required=True)
    edit.add_argument("--text", default=None)
    edit.add_argument("--text-file", default=None)
    edit.set_defaults(func=cmd_edit)

    stop = sub.add_parser("stop", help="Write a graceful stop request")
    stop.add_argument("--reason", default="cli")
    stop.set_defaults(func=cmd_stop)

    export = sub.add_parser("export", help="Write the deterministic final summary set")
    export.add_argument("--print", action="store_true", help="Also print the exported set")
    export.set_defaults(func=cmd_export)

    imp = sub.add_parser("import-docs", help="Import a JSON array or {'documents': [...]} object")
    imp.add_argument("--file", required=True)
    imp.set_defaults(func=cmd_import_docs)

    config = sub.add_parser("config", help="Normalize and save a YAML/JSON configuration")
    config.add_argument("--file", required=True)
    config.set_defaults(func=cmd_config)

    serve = sub.add_parser("serve", help="Run the local HTTP API")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8765)
    serve.set_defaults(func=cmd_serve)

    demo = sub.add_parser("demo", help="Run the complete reproducibility demonstration")
    demo.set_defaults(func=cmd_demo)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except EngineError as exc:
        _print(exc.to_dict())
        return 2
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
