from __future__ import annotations

import argparse
import json
import signal
import sys
from pathlib import Path
from typing import Any

from .errors import WorkbenchError
from .service import WorkbenchService


def _print(value: Any) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False))


def _load_json_file(path: str) -> Any:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _service(args: argparse.Namespace) -> WorkbenchService:
    return WorkbenchService(args.root, runtime_dir=args.runtime)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="iwb", description="Deterministic image repair and format interoperability workbench")
    parser.add_argument("--root", default=".", help="workspace root (default: current directory)")
    parser.add_argument("--runtime", default=None, help="runtime directory (default: <root>/runtime)")
    subparsers = parser.add_subparsers(dest="command", required=True)

    generate = subparsers.add_parser("generate-sample", help="generate the deterministic demo dataset")
    generate.add_argument("--out", default="data")

    import_parser = subparsers.add_parser("import", help="import a schema-v2 or legacy schema-v1 manifest")
    import_parser.add_argument("--manifest", required=True)
    import_parser.add_argument("--legacy", action="store_true")
    import_parser.add_argument("--legacy-config", help="path to a JSON compatibility rule object")

    run_parser = subparsers.add_parser("run", help="run repair, style transfer and export with incremental invalidation")
    run_parser.add_argument("--force", action="store_true")

    subparsers.add_parser("status", help="show stage status, fingerprints and snapshot state")

    fragment_parser = subparsers.add_parser("fragment", help="show or update one fragment")
    fragment_parser.add_argument("fragment_id")
    fragment_parser.add_argument("--set", help="JSON object of supported updates")

    convert_parser = subparsers.add_parser("convert", help="export one processed fragment as PNG, JPEG or JSON descriptor")
    convert_parser.add_argument("--fragment", required=True)
    convert_parser.add_argument("--to", required=True, choices=["png", "jpeg", "jpg", "json"])
    convert_parser.add_argument("--out", default=None)

    import_exchange = subparsers.add_parser("import-exchange", help="import a PNG/JPEG plus optional JSON descriptor")
    import_exchange.add_argument("--image", required=True)
    import_exchange.add_argument("--descriptor", default=None)

    export_project = subparsers.add_parser("export-project", help="write the lossless internal project JSON")
    export_project.add_argument("--out", required=True)
    export_project.add_argument("--artifact", choices=["source", "repair", "style"], default="style")

    import_project = subparsers.add_parser("import-project", help="load a lossless internal project JSON")
    import_project.add_argument("--project", required=True)

    config_parser = subparsers.add_parser("config", help="replace the active format capability configuration")
    config_parser.add_argument("--file", required=True, help="JSON object containing format configuration")

    subparsers.add_parser("stop", help="request a graceful stop after the current fragment")
    subparsers.add_parser("resume", help="clear the graceful stop request")

    snapshot = subparsers.add_parser("snapshot", help="inspect or publish snapshots")
    snapshot_sub = snapshot.add_subparsers(dest="snapshot_command", required=True)
    snapshot_sub.add_parser("status")
    inspect = snapshot_sub.add_parser("inspect")
    inspect.add_argument("--candidate", required=True)
    publish = snapshot_sub.add_parser("publish")
    publish.add_argument("--candidate", required=True)

    serve = subparsers.add_parser("serve", help="start the local FastAPI application")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8000)

    subparsers.add_parser("demo", help="run the complete deterministic acceptance demonstration")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "generate-sample":
            from .demo_data import generate_demo_data

            result = generate_demo_data(Path(args.root) / args.out)
            _print(result)
            return 0
        if args.command == "demo":
            from .demo import run_demo

            report = run_demo(Path(args.root), runtime_dir=Path(args.runtime) if args.runtime else None)
            _print(report)
            return 0 if report.get("ok") else 1
        if args.command == "serve":
            import uvicorn

            from .api import create_app

            uvicorn.run(
                create_app(Path(args.root).resolve(), args.runtime),
                host=args.host,
                port=args.port,
                reload=False,
            )
            return 0
        service = _service(args)
        if args.command == "import":
            legacy_config = _load_json_file(args.legacy_config) if args.legacy_config else None
            _print({"ok": True, "project": service.import_manifest(args.manifest, legacy=args.legacy, legacy_config=legacy_config)})
        elif args.command == "run":
            def stop_handler(signum: int, frame: Any) -> None:
                service.request_stop()
                print("stop requested; waiting for the current fragment to finish", file=sys.stderr)

            previous_handler = signal.signal(signal.SIGINT, stop_handler)
            try:
                _print({"ok": True, "run": service.run(force=args.force)})
            finally:
                signal.signal(signal.SIGINT, previous_handler)
        elif args.command == "status":
            _print({"ok": True, "status": service.status()})
        elif args.command == "fragment":
            if args.set:
                _print({"ok": True, "fragment": service.update_fragment(args.fragment_id, _load_json_file(args.set))})
            else:
                _print({"ok": True, "fragment": service.fragment_status(args.fragment_id)})
        elif args.command == "convert":
            _print({"ok": True, "conversion": service.convert(args.fragment, args.to, args.out)})
        elif args.command == "import-exchange":
            _print({"ok": True, "fragment": service.import_exchange(args.image, descriptor_path=args.descriptor)})
        elif args.command == "export-project":
            _print({"ok": True, "export": service.export_project(args.out, artifact=args.artifact)})
        elif args.command == "import-project":
            _print({"ok": True, "project": service.load_project_file(args.project)})
        elif args.command == "config":
            _print({"ok": True, "format_config": service.set_format_config(_load_json_file(args.file))})
        elif args.command == "stop":
            _print({"ok": True, "control": service.request_stop()})
        elif args.command == "resume":
            _print({"ok": True, "control": service.resume()})
        elif args.command == "snapshot":
            if args.snapshot_command == "status":
                _print({"ok": True, "snapshot": service.snapshot_status()})
            elif args.snapshot_command == "inspect":
                _print({"ok": True, "inspection": service.inspect_snapshot(_load_json_file(args.candidate))})
            else:
                _print({"ok": True, "publication": service.publish_snapshot(_load_json_file(args.candidate))})
        return 0
    except WorkbenchError as exc:
        _print({"ok": False, "error": exc.to_dict()})
        return 2
    except (OSError, ValueError) as exc:
        _print({"ok": False, "error": {"code": "E_CLI", "message": str(exc), "details": {}}})
        return 2

