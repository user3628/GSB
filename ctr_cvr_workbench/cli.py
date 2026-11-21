"""Command-line workbench for the local CTR/CVR pipeline."""

from __future__ import annotations

import argparse
import json
import platform
import shutil
import sys
from pathlib import Path
from typing import Any

from .config import Config
from .data import load_manifest, load_rows, register_manifest_observation, validate_rows
from .errors import PipelineError
from .generator import write_dataset
from .pipeline import Pipeline
from .util import resolve_under


def _print(value: Any) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True))


def _config(args: argparse.Namespace) -> Config:
    return Config.load(args.config, args.base_dir)


def cmd_preflight(args: argparse.Namespace) -> int:
    checks: dict[str, Any] = {
        "python_version": platform.python_version(),
        "python_ge_310": sys.version_info >= (3, 10),
        "operating_system": platform.system(),
        "platform": platform.platform(),
        "cwd": Path.cwd().as_posix(),
        "gpu_required": False,
        "network_required": False,
        "container_required": False,
    }
    try:
        import numpy

        checks["numpy_version"] = numpy.__version__
    except Exception as exc:
        checks["numpy_error"] = str(exc)
    try:
        import yaml

        checks["pyyaml_version"] = yaml.__version__
    except Exception as exc:
        checks["pyyaml_error"] = str(exc)
    checks["ok"] = "numpy_version" in checks and "pyyaml_version" in checks and checks["python_ge_310"]
    if platform.system() != "Windows":
        checks["runtime_note"] = "actual runner is WSL2/Linux; code paths are Windows-compatible and use relative paths"
    _print(checks)
    return 0 if checks["ok"] else 1


def cmd_generate(args: argparse.Namespace) -> int:
    _print(
        write_dataset(
            args.out,
            seed=args.seed,
            shards=args.shards,
            rows_per_shard=args.rows_per_shard,
        )
    )
    return 0


def cmd_register(args: argparse.Namespace) -> int:
    config = _config(args)
    manifest = load_manifest(resolve_under(Path(args.base_dir), config.manifest_path, "paths.manifest"))
    observation = register_manifest_observation(manifest, config.state_root)
    shards = load_rows(manifest)
    report = validate_rows(shards)
    _print(
        {
            "status": "registered" if report.ok else "invalid",
            "manifest_sha256": manifest.digest,
            "observation": observation.as_posix(),
            "read_only_observation": True,
            "label_error_count": len(report.label_errors),
            "funnel_violation_count": len(report.funnel_violations),
            "shards": len(manifest.shards),
            "declared_rows": sum(shard.rows for shard in manifest.shards),
        }
    )
    return 0 if report.ok else 2


def cmd_run(args: argparse.Namespace) -> int:
    try:
        result = Pipeline(
            args.config,
            base_dir=args.base_dir,
            injection=args.inject,
            stop_after_epochs=args.stop_after_epochs,
            worker_override=args.workers,
            execution_mode=args.execution_mode,
        ).run()
    except PipelineError as exc:
        _print({"ok": False, "error": exc.to_dict()})
        return 2

    if args.full:
        _print(result)
    elif result["status"] == "stopped":
        _print(result)
    else:
        _print(
            {
                "status": result["status"],
                "config_version": result["config_version"],
                "summary_digest": result["summary_digest"],
                "summary_path": result["summary_path"],
                "accepted_shards": len(result["schedule"]["accepted"]),
                "unprocessed_shards": len(result["schedule"]["unprocessed"]),
                "objective_metrics": result["summary"]["inference"],
            }
        )
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    _print(Pipeline.status(args.config, base_dir=args.base_dir))
    return 0


def cmd_losses(args: argparse.Namespace) -> int:
    config = _config(args)
    path = config.outputs_root / "train" / "metrics.json"
    if not path.exists():
        _print({"ok": False, "error": {"code": "E_NOT_FOUND", "message": "training metrics are not available"}})
        return 2
    metrics = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                metrics.append(json.loads(line))
    if args.limit and args.limit > 0:
        metrics = metrics[-args.limit :]
    if not args.include_traversal:
        for metric in metrics:
            traversal = metric.get("sample_traversal", {})
            metric["traversal_digest"] = traversal.get("digest")
            metric.pop("sample_traversal", None)
    _print({"metrics_path": path.as_posix(), "epochs": len(metrics), "losses": metrics})
    return 0


def cmd_funnel(args: argparse.Namespace) -> int:
    config = _config(args)
    path = config.outputs_root / "funnel" / "diagnostics.json"
    if not path.exists():
        path = config.outputs_root / "funnel" / "label-diagnostics.json"
    if not path.exists():
        _print({"ok": False, "error": {"code": "E_NOT_FOUND", "message": "funnel diagnostics are not available"}})
        return 2
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not args.all:
        payload["violations"] = payload.get("violations", [])[: args.limit]
    _print(payload)
    return 0


def cmd_summary(args: argparse.Namespace) -> int:
    config = _config(args)
    path = config.outputs_root / "experiment_summary.json"
    if not path.exists():
        _print({"ok": False, "error": {"code": "E_NOT_FOUND", "message": "experiment summary is not available"}})
        return 2
    with path.open("r", encoding="utf-8") as handle:
        _print(json.load(handle))
    return 0


def _export(args: argparse.Namespace, source: Path) -> int:
    if not source.exists():
        _print({"ok": False, "error": {"code": "E_NOT_FOUND", "message": f"artifact not found: {source}"}})
        return 2
    destination = Path(args.out)
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, destination)
    _print({"status": "exported", "source": source.as_posix(), "destination": destination.as_posix()})
    return 0


def cmd_export_model(args: argparse.Namespace) -> int:
    config = _config(args)
    return _export(args, config.outputs_root / "train" / "model.json")


def cmd_export_scores(args: argparse.Namespace) -> int:
    config = _config(args)
    return _export(args, config.outputs_root / "infer" / "scores.jsonl")


def cmd_serve(args: argparse.Namespace) -> int:
    from .server import serve

    serve(args.base_dir, config_path=args.config, host=args.host, port=args.port)
    return 0


def cmd_demo(args: argparse.Namespace) -> int:
    from .demo import run_demo

    report = run_demo(Path(args.out), config_path=args.config, base_dir=args.base_dir)
    _print(report)
    return 0 if report.get("ok") else 2


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ctr-cvr", description="Local deterministic CTR/CVR workbench")
    parser.add_argument("--base-dir", default=".", help="workspace base for relative paths")
    subparsers = parser.add_subparsers(dest="command", required=True)

    preflight = subparsers.add_parser("preflight", help="print Python/dependency versions")
    preflight.set_defaults(func=cmd_preflight)

    generate = subparsers.add_parser("generate", help="generate deterministic local shards")
    generate.add_argument("--out", default="data/shards")
    generate.add_argument("--seed", type=int, default=20261122)
    generate.add_argument("--shards", type=int, default=6)
    generate.add_argument("--rows-per-shard", type=int, default=220)
    generate.set_defaults(func=cmd_generate)

    register = subparsers.add_parser("register", help="read-only register manifest truth")
    register.add_argument("--config", default="configs/default.yaml")
    register.set_defaults(func=cmd_register)

    run = subparsers.add_parser("run", help="run the four pipeline stages")
    run.add_argument("--config", default="configs/default.yaml")
    run.add_argument(
        "--inject",
        choices=[
            "zero_feature_dim",
            "manifest_mismatch",
            "invalid_ctr_label",
            "cvr_without_ctr",
            "negative_lr",
            "negative_epochs",
        ],
    )
    run.add_argument("--workers", type=int)
    run.add_argument("--execution-mode", choices=["sequential", "parallel"])
    run.add_argument("--stop-after-epochs", type=int)
    run.add_argument("--full", action="store_true")
    run.set_defaults(func=cmd_run)

    status = subparsers.add_parser("status", help="show stage/version status")
    status.add_argument("--config", default="configs/default.yaml")
    status.set_defaults(func=cmd_status)

    losses = subparsers.add_parser("losses", help="show the loss curve")
    losses.add_argument("--config", default="configs/default.yaml")
    losses.add_argument("--limit", type=int, default=0)
    losses.add_argument("--include-traversal", action="store_true")
    losses.set_defaults(func=cmd_losses)

    funnel = subparsers.add_parser("funnel", help="show funnel diagnostics")
    funnel.add_argument("--config", default="configs/default.yaml")
    funnel.add_argument("--limit", type=int, default=20)
    funnel.add_argument("--all", action="store_true")
    funnel.set_defaults(func=cmd_funnel)

    summary = subparsers.add_parser("summary", help="show reproducible experiment summary")
    summary.add_argument("--config", default="configs/default.yaml")
    summary.set_defaults(func=cmd_summary)

    export_model = subparsers.add_parser("export-model", help="export fixed-point model JSON")
    export_model.add_argument("--config", default="configs/default.yaml")
    export_model.add_argument("--out", default="outputs/export/model.json")
    export_model.set_defaults(func=cmd_export_model)

    export_scores = subparsers.add_parser("export-scores", help="export inference score JSONL")
    export_scores.add_argument("--config", default="configs/default.yaml")
    export_scores.add_argument("--out", default="outputs/export/scores.jsonl")
    export_scores.set_defaults(func=cmd_export_scores)

    serve_parser = subparsers.add_parser("serve", help="start the local HTTP workbench")
    serve_parser.add_argument("--config", default="configs/default.yaml")
    serve_parser.add_argument("--host", default=None)
    serve_parser.add_argument("--port", type=int, default=None)
    serve_parser.set_defaults(func=cmd_serve)

    demo = subparsers.add_parser("demo", help="run the reproducible acceptance demonstration")
    demo.add_argument("--config", default="configs/default.yaml")
    demo.add_argument("--out", default="artifacts/demo")
    demo.set_defaults(func=cmd_demo)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except PipelineError as exc:
        _print({"ok": False, "error": exc.to_dict()})
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
