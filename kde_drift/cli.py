"""Command-line interface for generation, evaluation, inspection, and HTTP serving."""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
from pathlib import Path
from typing import Any, Sequence

import numpy as np

from .canonical import atomic_write_bytes, file_sha256, json_safe, read_json
from .config import DEFAULT_CONFIG, normalize_config
from .data import DatasetRegistry, load_dataset
from .error_demos import collect_error_demonstrations
from .errors import GracefulStop, WorkbenchError
from .generator import BASE_SEED, generate_dataset
from .http_server import create_server
from .service import ERROR_CATALOG, WorkbenchService
from .stop import StopController
from .workbench import Workbench


def _read_config(path: str | None) -> dict[str, Any]:
    if not path:
        return normalize_config(DEFAULT_CONFIG)
    try:
        return normalize_config(read_json(path))
    except OSError as exc:
        raise WorkbenchError(
            "E_NOT_FOUND", f"configuration file not found: {path}", location=str(path)
        ) from exc
    except ValueError as exc:
        raise WorkbenchError(
            "E_CONFIG", f"configuration file is not valid JSON: {path}", location=str(path)
        ) from exc


def _emit(value: Any) -> None:
    sys.stdout.write(json.dumps(json_safe(value), sort_keys=True, indent=2, ensure_ascii=False) + "\n")


def _public_run_result(result: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": result["status"],
        "dataset": result["dataset"],
        "config_version": result["config_version"],
        "pipeline_fingerprint": result["pipeline_fingerprint"],
        "outer_sequence": result["outer_sequence"],
        "batch_evaluation": result["batch_evaluation"],
        "summary": result["summary"],
        "stages": {
            stage: {
                "sequence": record["sequence"],
                "stage_revision": record["stage_revision"],
                "result_fingerprint": record["result_fingerprint"],
                "stage_input_fingerprint": record["stage_input_fingerprint"],
            }
            for stage, record in result["records"].items()
            if record is not None
        },
    }


def cmd_generate(args: argparse.Namespace) -> int:
    index = generate_dataset(args.out, seed=args.seed)
    _emit({"ok": True, "output": str(Path(args.out).resolve()), "index": index})
    return 0


def cmd_validate(args: argparse.Namespace) -> int:
    registry = DatasetRegistry(args.data_dir)
    summaries = registry.summaries()
    _emit({"ok": True, "dataset_count": len(summaries), "datasets": summaries})
    return 0


def cmd_run(args: argparse.Namespace) -> int:
    dataset = load_dataset(args.data_dir, args.dataset)
    config = _read_config(args.config)
    workbench = Workbench(dataset, config, args.workdir)
    stop = StopController(stop_file=args.stop_file, stop_after_units=args.stop_after_units)
    try:
        result = workbench.run(
            resume=not args.no_resume,
            force=args.force,
            stop=stop,
            batch_size=args.batch_size,
            batch_order=args.batch_order,
        )
    except GracefulStop as exc:
        _emit({"ok": True, **exc.to_dict(), "analysis": workbench.analyze()})
        return 3
    _emit({"ok": True, **_public_run_result(result)})
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    dataset = load_dataset(args.data_dir, args.dataset)
    workbench = Workbench(dataset, _read_config(args.config), args.workdir)
    _emit({"ok": True, **workbench.status()})
    return 0


def cmd_invalidation(args: argparse.Namespace) -> int:
    return cmd_status(args)


def cmd_stop(args: argparse.Namespace) -> int:
    path = Path(args.stop_file) if args.stop_file else Path(args.workdir) / "STOP"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("stop\n", encoding="utf-8")
    _emit({"ok": True, "stop_file": str(path.resolve()), "message": "cooperative stop requested"})
    return 0


def cmd_export(args: argparse.Namespace) -> int:
    dataset = load_dataset(args.data_dir, args.dataset)
    workbench = Workbench(dataset, _read_config(args.config), args.workdir)
    data = workbench.export_bytes(args.kind)
    output = Path(args.out)
    atomic_write_bytes(output, data)
    _emit(
        {
            "ok": True,
            "kind": args.kind,
            "output": str(output.resolve()),
            "bytes": len(data),
            "sha256": file_sha256(output),
        }
    )
    return 0


def cmd_errors(args: argparse.Namespace) -> int:
    dataset = load_dataset(args.data_dir, "small")
    demonstrations = collect_error_demonstrations(dataset)
    _emit(
        {
            "ok": all(item.get("raised") for item in demonstrations),
            "catalog": ERROR_CATALOG,
            "demonstrations": demonstrations,
        }
    )
    return 0 if all(item.get("raised") for item in demonstrations) else 1


def cmd_serve(args: argparse.Namespace) -> int:
    service = WorkbenchService(
        args.data_dir,
        args.workdir_base,
        dataset=args.dataset,
        config=_read_config(args.config),
    )
    server = create_server(service, host="127.0.0.1", port=args.port)
    host, port = server.server_address[:2]
    print(f"KDE Drift Lab listening on http://{host}:{port}")
    if args.auto_run:
        service.start_run(resume=True)
    try:
        server.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        print("\nshutting down")
    finally:
        service.stop()
        service.wait(timeout=5)
        server.server_close()
    return 0


def cmd_demo(args: argparse.Namespace) -> int:
    from .demo import run_demo

    output = run_demo(
        data_dir=args.data_dir,
        output_dir=args.out,
        seed=args.seed,
        regenerate=not args.no_generate,
    )
    _emit(output)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="kde-drift",
        description="Deterministic local KDE drift detection and distribution comparison workbench",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    generate = subparsers.add_parser("generate", help="generate reproducible small/large NPZ datasets")
    generate.add_argument("--out", default="data")
    generate.add_argument("--seed", type=int, default=BASE_SEED)
    generate.set_defaults(func=cmd_generate)

    validate = subparsers.add_parser("validate", help="validate and summarize local datasets")
    validate.add_argument("--data-dir", default="data")
    validate.set_defaults(func=cmd_validate)

    run = subparsers.add_parser("run", help="run or resume the four-stage evaluation")
    run.add_argument("--dataset", default="small")
    run.add_argument("--config", default=None)
    run.add_argument("--data-dir", default="data")
    run.add_argument("--workdir", default="runs/small")
    run.add_argument("--batch-size", type=int, default=None)
    run.add_argument("--batch-order", choices=["forward", "reverse"], default="forward")
    run.add_argument("--stop-file", default=None)
    run.add_argument("--stop-after-units", type=int, default=None, help="test/demo cooperative stop hook")
    run.add_argument("--force", action="store_true")
    run.add_argument("--no-resume", action="store_true", help="force a complete rebuild")
    run.set_defaults(func=cmd_run)

    status = subparsers.add_parser("status", help="show stage validity, fingerprints, and revisions")
    status.add_argument("--dataset", default="small")
    status.add_argument("--config", default=None)
    status.add_argument("--data-dir", default="data")
    status.add_argument("--workdir", default="runs/small")
    status.set_defaults(func=cmd_status)

    invalidation = subparsers.add_parser("invalidation", help="show incremental invalidation analysis")
    invalidation.add_argument("--dataset", default="small")
    invalidation.add_argument("--config", default=None)
    invalidation.add_argument("--data-dir", default="data")
    invalidation.add_argument("--workdir", default="runs/small")
    invalidation.set_defaults(func=cmd_invalidation)

    stop = subparsers.add_parser("stop", help="write a cooperative stop request")
    stop.add_argument("--workdir", default="runs/small")
    stop.add_argument("--stop-file", default=None)
    stop.set_defaults(func=cmd_stop)

    export = subparsers.add_parser("export", help="export deterministic density curves or drift report")
    export.add_argument("--kind", choices=["density", "report"], required=True)
    export.add_argument("--dataset", default="small")
    export.add_argument("--config", default=None)
    export.add_argument("--data-dir", default="data")
    export.add_argument("--workdir", default="runs/small")
    export.add_argument("--out", required=True)
    export.set_defaults(func=cmd_export)

    errors = subparsers.add_parser("errors", help="exercise and display all structured error codes")
    errors.add_argument("--data-dir", default="data")
    errors.set_defaults(func=cmd_errors)

    serve = subparsers.add_parser("serve", help="start the local HTTP workstation")
    serve.add_argument("--data-dir", default="data")
    serve.add_argument("--workdir-base", default="runs/http")
    serve.add_argument("--dataset", default="small")
    serve.add_argument("--config", default=None)
    serve.add_argument("--port", type=int, default=8765)
    serve.add_argument("--auto-run", action="store_true")
    serve.set_defaults(func=cmd_serve)

    demo = subparsers.add_parser("demo", help="run the repeatable end-to-end demonstration")
    demo.add_argument("--data-dir", default="data")
    demo.add_argument("--out", default="runs/demo")
    demo.add_argument("--seed", type=int, default=BASE_SEED)
    demo.add_argument("--no-generate", action="store_true")
    demo.set_defaults(func=cmd_demo)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except WorkbenchError as exc:
        sys.stderr.write(json.dumps(exc.to_dict(), ensure_ascii=False, indent=2) + "\n")
        return 2
    except GracefulStop as exc:
        _emit({"ok": True, **exc.to_dict()})
        return 3
