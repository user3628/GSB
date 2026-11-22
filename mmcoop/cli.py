from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

from .canonical import canonical_text
from .config import load_config, write_projection_matrix
from .demo import run_demo
from .errors import AppError
from .service import WorkspaceService


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="mmcoop", description="Deterministic local multimodal cooperative workspace")
    parser.add_argument("--config", default="config/default.yaml", help="YAML configuration path")
    parser.add_argument("--data", default="data", help="dataset directory")
    parser.add_argument("--workspace", default="workspace", help="checkpoint/artifact workspace directory")
    subparsers = parser.add_subparsers(dest="command", required=True)

    demo_init = subparsers.add_parser("init-demo", help="generate deterministic local demo data")
    demo_init.add_argument("--seed", type=int, default=20261123)
    demo_init.add_argument("--fresh-workspace", action="store_true")

    importer = subparsers.add_parser("import", help="import canonical text JSONL and image JSON files")
    importer.add_argument("--texts", required=True)
    importer.add_argument("--images", required=True)

    text_edit = subparsers.add_parser("edit-text", help="edit a text record")
    text_edit.add_argument("text_id")
    text_edit.add_argument("--text")
    text_edit.add_argument("--lang")
    text_edit.add_argument("--source-channel")

    image_edit = subparsers.add_parser("edit-image", help="edit an image record")
    image_edit.add_argument("image_id")
    image_edit.add_argument("--feature", help="JSON array")
    image_edit.add_argument("--feature-file")
    image_edit.add_argument("--operator")
    image_edit.add_argument("--text-id")

    build = subparsers.add_parser("build", help="build or incrementally rebuild all stages")
    build.add_argument("--inject-kind", choices=["projection_dim", "invalid_operator", "text_too_long", "similarity_nan", "missing_ref"])
    build.add_argument("--inject-sample")
    build.add_argument("--through-stage", choices=["text_encoding", "projection_alignment", "fusion_scoring", "aggregation"], default="aggregation")
    build.add_argument("--no-verify", action="store_true")

    inject = subparsers.add_parser("inject", help="run one parameter injection and return its stable business error")
    inject.add_argument("--kind", required=True, choices=["projection_dim", "invalid_operator", "text_too_long", "similarity_nan", "missing_ref"])
    inject.add_argument("--sample-id", required=True)

    subparsers.add_parser("status", help="show stage state, fingerprints and invalidation reasons")
    subparsers.add_parser("version", help="show the current deterministic version fingerprint")
    subparsers.add_parser("results", help="show aggregation and cooperative conclusions")
    subparsers.add_parser("invalidations", help="show incremental invalidation diagnostics")
    subparsers.add_parser("checkpoints", help="show checkpoint scan and integrity")
    subparsers.add_parser("integrity", help="adjudicate snapshot rollback/tamper/advance")
    subparsers.add_parser("stop", help="request graceful stop before the next indivisible unit")

    resume = subparsers.add_parser("resume", help="clear stop request and resume from the latest consistent checkpoint")
    resume.add_argument("--through-stage", choices=["text_encoding", "projection_alignment", "fusion_scoring", "aggregation"], default="aggregation")
    resume.add_argument("--no-verify", action="store_true")

    demo = subparsers.add_parser("demo", help="run the full deterministic acceptance demonstration")
    demo.add_argument("--seed", type=int, default=20261123)
    demo.add_argument("--report", default="artifacts/demo_report.json")

    serve = subparsers.add_parser("serve", help="run the local FastAPI/uvicorn server")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8000)

    config_init = subparsers.add_parser("init-config", help="write the configured projection matrix file")
    config_init.add_argument("--output")
    return parser


def _service(args: argparse.Namespace) -> WorkspaceService:
    return WorkspaceService(args.config, args.data, args.workspace)


def _success(value: Any) -> dict[str, Any]:
    return {"ok": True, "data": value}


def _print(value: Any) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True, allow_nan=False))


def _feature_from_args(args: argparse.Namespace) -> list[float] | None:
    if args.feature_file:
        value = json.loads(Path(args.feature_file).read_text(encoding="utf-8"))
    elif args.feature is not None:
        value = json.loads(args.feature)
    else:
        return None
    if not isinstance(value, list):
        raise AppError("E_INVALID_INPUT", "data_edit", "feature must be a JSON array", field="feature", http_status=400)
    return value


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    service = _service(args)
    try:
        if args.command == "init-demo":
            config = load_config(args.config)
            matrix_file = config.projection.get("matrix_file")
            if matrix_file:
                target = Path(str(matrix_file))
                if not target.is_absolute():
                    target = config.path.parent / target
                if not target.exists():
                    write_projection_matrix(args.config, target)
            _print(_success(service.generate_demo(seed=args.seed, fresh_workspace=args.fresh_workspace)))
        elif args.command == "import":
            _print(_success(service.import_dataset(args.texts, args.images)))
        elif args.command == "edit-text":
            changes = {"text": args.text, "lang": args.lang, "source_channel": args.source_channel}
            if all(value is None for value in changes.values()):
                raise AppError("E_INVALID_INPUT", "data_edit", "at least one text field must be supplied", http_status=400)
            _print(_success(service.edit_text(args.text_id, changes)))
        elif args.command == "edit-image":
            feature = _feature_from_args(args)
            changes = {"feature": feature, "source_operator": args.operator, "text_id": args.text_id}
            if all(value is None for value in changes.values()):
                raise AppError("E_INVALID_INPUT", "data_edit", "at least one image field must be supplied", http_status=400)
            _print(_success(service.edit_image(args.image_id, changes)))
        elif args.command == "build":
            injections = None
            if args.inject_kind:
                injections = [{"kind": args.inject_kind, "sample_id": args.inject_sample}]
                if args.inject_sample is None:
                    injections[0].pop("sample_id")
            _print(
                _success(
                    service.build(
                        injections=injections,
                        verify_incremental=not args.no_verify,
                        through_stage=args.through_stage,
                    )
                )
            )
        elif args.command == "inject":
            _print(_success(service.build(injections=[{"kind": args.kind, "sample_id": args.sample_id}], verify_incremental=True)))
        elif args.command == "status":
            _print(_success(service.status()))
        elif args.command == "version":
            status = service.status()
            _print(
                _success(
                    {
                        "status": status["status"],
                        "config_version": status["config"]["config_version"],
                        "data_fingerprint": status["data_fingerprint"],
                        **status["version"],
                    }
                )
            )
        elif args.command == "results":
            _print(_success(service.results()))
        elif args.command == "invalidations":
            _print(_success(service.invalidations()))
        elif args.command == "checkpoints":
            _print(_success(service.checkpoints()))
        elif args.command == "integrity":
            _print(_success(service.integrity()))
        elif args.command == "stop":
            _print(_success(service.request_stop()))
        elif args.command == "resume":
            _print(
                _success(
                    service.resume(
                        verify_incremental=not args.no_verify,
                        through_stage=args.through_stage,
                    )
                )
            )
        elif args.command == "demo":
            _print(_success(run_demo(service, seed=args.seed, report_path=args.report)))
        elif args.command == "serve":
            import uvicorn

            os.environ["MMCOOP_CONFIG"] = str(Path(args.config).resolve())
            os.environ["MMCOOP_DATA"] = str(Path(args.data).resolve())
            os.environ["MMCOOP_WORKSPACE"] = str(Path(args.workspace).resolve())
            from .api import create_app

            uvicorn.run(
                create_app(config_path=args.config, data_dir=args.data, workspace_dir=args.workspace),
                host=args.host,
                port=args.port,
            )
        elif args.command == "init-config":
            _print(_success({"written": str(write_projection_matrix(args.config, args.output))}))
        else:  # pragma: no cover
            parser.error(f"unknown command {args.command}")
        return 0
    except AppError as error:
        _print(error.to_dict())
        return 2
    except (OSError, ValueError, json.JSONDecodeError) as error:
        _print({"ok": False, "error": {"code": "E_INVALID_INPUT", "stage": "cli", "message": str(error)}})
        return 2
    except KeyboardInterrupt:
        return 130
