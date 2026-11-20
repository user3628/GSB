"""Command-line interface for the local calibration workbench."""

from __future__ import annotations

import argparse
import json
import socket
import sys
from pathlib import Path
from typing import Any

from .data import generate_datasets, verify_generated_data
from .errors import WorkbenchError
from .service import WorkbenchService
from .util import atomic_temp_is_same_directory


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True, allow_nan=False)


def _service(args: argparse.Namespace) -> WorkbenchService:
    state_dir = Path(args.state_dir) if getattr(args, "state_dir", None) else None
    data_dir = Path(args.data_dir) if getattr(args, "data_dir", None) else None
    return WorkbenchService(Path(args.root), state_dir=state_dir, data_dir=data_dir)


def _parse_patch(items: list[str]) -> dict[str, Any]:
    patch: dict[str, Any] = {}
    for item in items:
        if "=" not in item:
            raise WorkbenchError("E_INVALID_PARAM", "--set expects key=value", {"item": item})
        key, value = item.split("=", 1)
        key = key.strip()
        if not key:
            raise WorkbenchError("E_INVALID_PARAM", "--set key must not be empty", {"item": item})
        patch[key] = value
    return patch


def _free_port(host: str = "127.0.0.1") -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((host, 0))
        return int(sock.getsockname()[1])


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="gsb", description="Deterministic GBDT probability calibration workbench")
    parser.add_argument("--root", default=str(Path.cwd()), help="project root (all paths must remain inside)")
    parser.add_argument("--state-dir", default=None, help="optional state directory inside the project")
    parser.add_argument("--data-dir", default=None, help="optional data directory inside the project")
    subparsers = parser.add_subparsers(dest="command", required=True)

    generate = subparsers.add_parser("generate-data", help="generate deterministic small/medium NPZ datasets")
    generate.add_argument("--force", action="store_true", help="overwrite existing generated data")

    subparsers.add_parser("verify-data", help="verify manifest, NPZ shape, missing columns and checksums")
    subparsers.add_parser("self-check", help="check same-directory atomic replacement support")

    imported = subparsers.add_parser("import", help="import one generated dataset into the workspace")
    imported.add_argument("--dataset", default="small", choices=["small", "medium"])

    config = subparsers.add_parser("config", help="patch configuration and optionally rebuild + verify")
    config.add_argument("--set", action="append", default=[], metavar="KEY=VALUE")
    config.add_argument("--verify", action="store_true", help="rebuild invalid chain and compare with from-scratch build")

    train = subparsers.add_parser("train", help="run staged training to completion or a selected stage")
    train.add_argument("--until-stage", default="diagnostics", choices=["binning", "boosting", "calibration", "diagnostics"])
    train.add_argument("--resume", action="store_true", help="clear a stop request and resume persisted progress")
    train.add_argument("--stop-after-rounds", type=int, default=None, help="deterministic demonstration stop boundary")

    resume = subparsers.add_parser("resume", help="resume from persisted stage/round progress")
    resume.add_argument("--until-stage", default="diagnostics", choices=["binning", "boosting", "calibration", "diagnostics"])

    subparsers.add_parser("status", help="show stages, fingerprints, invalidation reasons and progress")
    subparsers.add_parser("stop", help="request graceful stop after the current boosting round")

    compare = subparsers.add_parser("compare-batches", help="compare forward/reverse batch reductions")
    compare.add_argument("--batch-sizes", default="7,64,257", help="comma-separated batch sizes")

    predict = subparsers.add_parser("predict", help="run one batch-size prediction reduction")
    predict.add_argument("--batch-size", type=int, default=257)
    predict.add_argument("--reverse", action="store_true")

    export = subparsers.add_parser("export", help="export probabilities CSV and diagnostics JSON")
    export.add_argument("--out-dir", default="out")

    subparsers.add_parser("reliability", help="show reliability table and numerical diagnostics")
    subparsers.add_parser("verify-incremental", help="compare current artifacts with a from-scratch rebuild")

    port = subparsers.add_parser("free-port", help="find an available local HTTP port without starting a server")
    port.add_argument("--host", default="127.0.0.1")

    serve = subparsers.add_parser("serve", help="start the local Flask workbench (bind loopback by default)")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=None)

    demo = subparsers.add_parser("demo", help="run the complete deterministic two-scale demonstration")
    demo.add_argument("--output", default="out/demo_report.json")

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "generate-data":
            data_dir = Path(args.data_dir) if args.data_dir else Path(args.root) / "data"
            print(_json(generate_datasets(data_dir, force=args.force)))
            return 0
        if args.command == "verify-data":
            data_dir = Path(args.data_dir) if args.data_dir else Path(args.root) / "data"
            print(_json(verify_generated_data(data_dir)))
            return 0
        if args.command == "self-check":
            target = Path(args.root) / "state" / ".atomic-check"
            target.parent.mkdir(parents=True, exist_ok=True)
            print(_json({"same_directory_atomic_temp": atomic_temp_is_same_directory(target), "platform": sys.platform}))
            return 0
        if args.command == "free-port":
            print(_json({"host": args.host, "port": _free_port(args.host)}))
            return 0
        if args.command == "serve":
            from .app import create_app

            port = args.port if args.port is not None else _free_port(args.host)
            app = create_app(Path(args.root), state_dir=args.state_dir, data_dir=args.data_dir)
            print(_json({"host": args.host, "port": port, "url": f"http://{args.host}:{port}/"}))
            app.run(host=args.host, port=port, debug=False, use_reloader=False)
            return 0
        if args.command == "demo":
            from .demo import run_demo

            result = run_demo(Path(args.root), output_path=Path(args.output))
            print(_json(result))
            return 0

        service = _service(args)
        if args.command == "import":
            result = service.import_dataset(args.dataset)
        elif args.command == "config":
            patch = _parse_patch(args.set)
            if not patch:
                raise WorkbenchError("E_INVALID_PARAM", "provide at least one --set key=value")
            if args.verify:
                result = service.reconfigure(patch, rebuild=True, verify_incremental=True)
            else:
                result = service.set_config(patch)
        elif args.command == "train":
            result = service.build(
                resume=args.resume,
                until_stage=args.until_stage,
                stop_after_rounds=args.stop_after_rounds,
            )
        elif args.command == "resume":
            result = service.build(resume=True, until_stage=args.until_stage)
        elif args.command == "status":
            result = service.status()
        elif args.command == "stop":
            result = service.request_stop("cli_stop")
        elif args.command == "compare-batches":
            sizes = [int(value.strip()) for value in args.batch_sizes.split(",") if value.strip()]
            result = service.compare_batches(sizes)
        elif args.command == "predict":
            dataset = service.load_current_dataset()
            from .pipeline import Pipeline

            result = Pipeline(service.workspace, dataset).predict_batches(args.batch_size, reverse=args.reverse)
        elif args.command == "export":
            result = service.export(Path(args.out_dir))
        elif args.command == "reliability":
            result = service.reliability()
        elif args.command == "verify-incremental":
            result = service.verify_incremental()
        else:  # pragma: no cover - argparse guards this path
            raise WorkbenchError("E_INVALID_PARAM", f"unknown command: {args.command}")
        print(_json(result))
        return 0
    except WorkbenchError as exc:
        print(_json(exc.to_dict()), file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print(_json({"ok": False, "error": {"code": "E_INTERRUPTED", "message": "interrupted"}}), file=sys.stderr)
        return 130


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
