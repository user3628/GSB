from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any

from .demo import run_demo
from .generator import DEFAULT_SEED, generate_demo
from .importer import import_dataset
from .injection import append_injection, build_operation, clear_injections, list_injections
from .pipeline import (
    read_conflicts,
    read_failures,
    read_report,
    request_stop,
    run_pipeline,
    sample_detail,
    workspace_status,
)
from .store import CheckpointStore
from .util import WorkbenchError, atomic_write_bytes, pretty_json, resolve_workspace_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m workbench",
        description="Deterministic local image classification / multi-label validation workbench",
    )
    parser.add_argument("--root", default=".", help="workspace root (default: current directory)")
    sub = parser.add_subparsers(dest="command", required=True)

    generate = sub.add_parser("generate", help="generate deterministic PNG samples and manifest")
    generate.add_argument("--seed", type=int, default=DEFAULT_SEED)
    generate.add_argument("--no-clean", action="store_true", help="keep existing image directory")

    for name in ("run", "resume"):
        command = sub.add_parser(name, help=f"{name} the four-stage pipeline")
        command.add_argument("--config", default="configs/default.json")
        command.add_argument("--stop-after", type=int, default=None, help="stop after N newly published checkpoints")

    status = sub.add_parser("status", help="show stage status, fingerprints, and run state")
    status.add_argument("--config", default="configs/default.json")

    sub.add_parser("stages", help="show checkpoint stage sequence and integrity")

    sample = sub.add_parser("sample", help="show one sample's sources and checkpoints")
    sample.add_argument("sample_key")
    sample.add_argument("--config", default="configs/default.json")

    sub.add_parser("failures", help="show located failure samples")
    sub.add_parser("conflicts", help="show label-exclusivity conflicts")

    invalidations = sub.add_parser("invalidations", help="show incremental invalidation audit")
    invalidations.add_argument("--limit", type=int, default=50)

    sub.add_parser("stop", help="write a cooperative stop request")

    inject = sub.add_parser("inject", help="append a declarative anomaly injection")
    inject.add_argument("type", choices=["dim_mismatch", "unknown_label", "numeric", "missing_field", "score_length"])
    inject.add_argument("--sample", required=True)
    inject.add_argument("--width", type=int)
    inject.add_argument("--height", type=int)
    inject.add_argument("--label")
    inject.add_argument("--value")
    inject.add_argument("--field")
    inject.add_argument("--lower-bound", type=float)
    inject.add_argument("--config", default="configs/default.json")

    clear = sub.add_parser("clear-injection", help="remove all declarative injections")
    clear.add_argument("--config", default="configs/default.json")

    injections = sub.add_parser("injection", help="show current declarative injections")
    injections.add_argument("--config", default="configs/default.json")

    import_cmd = sub.add_parser("import", help="import a local manifest and image files into samples/imports")
    import_cmd.add_argument("--manifest", required=True)
    import_cmd.add_argument("--name", required=True)
    import_cmd.add_argument("--overwrite", action="store_true")

    export = sub.add_parser("export", help="export a byte-identical validation report snapshot")
    export.add_argument("--output", default="reports/exported_report.json")

    serve = sub.add_parser("serve", help="start the local HTTP workbench")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8765)

    demo = sub.add_parser("demo", help="run deterministic baseline, stop/resume, and injection demonstration")
    demo.add_argument("--seed", type=int, default=DEFAULT_SEED)
    demo.add_argument("--stop-after", type=int, default=5)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    root = Path(args.root).resolve()
    try:
        payload: Any
        if args.command == "generate":
            payload = generate_demo(root, seed=args.seed, clean=not args.no_clean)
        elif args.command in {"run", "resume"}:
            result = run_pipeline(
                root,
                config_path=args.config,
                mode="resume" if args.command == "resume" else "run",
                clear_stop_flag=True,
                stop_after_checkpoints=args.stop_after,
            )
            payload = result.as_dict()
        elif args.command == "status":
            payload = workspace_status(root, config_path=args.config)
        elif args.command == "stages":
            payload = CheckpointStore(root).verify_all()
        elif args.command == "sample":
            payload = sample_detail(root, args.sample_key, config_path=args.config)
        elif args.command == "failures":
            payload = {"failures": read_failures(root)}
        elif args.command == "conflicts":
            payload = {"conflicts": read_conflicts(root)}
        elif args.command == "invalidations":
            payload = {"invalidations": CheckpointStore(root).invalidations(args.limit)}
        elif args.command == "stop":
            payload = request_stop(root, "cli_stop_request")
        elif args.command == "inject":
            operation = build_operation(
                args.type,
                args.sample,
                width=args.width,
                height=args.height,
                label=args.label,
                value=args.value,
                field=args.field,
                lower_bound=args.lower_bound,
            )
            payload = append_injection(root, operation, config_path=root / args.config)
        elif args.command == "clear-injection":
            payload = clear_injections(root, config_path=root / args.config)
        elif args.command == "injection":
            payload = list_injections(root, config_path=root / args.config)
        elif args.command == "import":
            payload = import_dataset(root, args.manifest, name=args.name, overwrite=args.overwrite)
        elif args.command == "export":
            source = root / "reports" / "report.json"
            if not source.exists():
                raise WorkbenchError("E_NOT_FOUND", "run the pipeline before exporting a report")
            target = resolve_workspace_path(root, args.output)
            data = source.read_bytes()
            atomic_write_bytes(target, data)
            digest = hashlib.sha256(data).hexdigest()
            atomic_write_bytes(target.with_name(target.name + ".sha256"), f"{digest}\n".encode("ascii"))
            payload = {
                "source": "reports/report.json",
                "output": str(target.relative_to(root)),
                "sha256": digest,
            }
        elif args.command == "serve":
            from .api import serve

            serve(root, host=args.host, port=args.port)
            return 0
        elif args.command == "demo":
            payload = run_demo(root, seed=args.seed, stop_after=args.stop_after)
        else:
            raise AssertionError(args.command)
        print(pretty_json(payload), end="")
        return 0
    except WorkbenchError as exc:
        print(pretty_json({"ok": False, "error": exc.as_dict()}), end="", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print(json.dumps({"ok": False, "error": {"code": "E_INTERRUPTED"}}), file=sys.stderr)
        return 130


__all__ = ["build_parser", "main"]
