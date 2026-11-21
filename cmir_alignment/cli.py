"""Command-line interface for the reference component and local demo."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from .configs import AlignmentConfig, default_config
from .datagen import generate_dataset
from .engine import RetrievalEngine
from .errors import CMIRError


def _print(value: Any) -> None:
    print(json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2))


def _engine(args: argparse.Namespace) -> RetrievalEngine:
    return RetrievalEngine(args.workspace)


def _load_json_file(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def _config_patch(args: argparse.Namespace) -> dict[str, Any]:
    patch: dict[str, Any] = {}
    if getattr(args, "config_file", None):
        return _load_json_file(args.config_file)
    if getattr(args, "bits", None) is not None:
        patch["bits"] = args.bits
    if getattr(args, "granularity", None) is not None:
        patch["granularity"] = args.granularity
    projection: dict[str, Any] = {}
    if getattr(args, "projection_seed", None) is not None:
        projection["seed"] = args.projection_seed
    if getattr(args, "visual_matrix_file", None):
        projection["visual_matrix"] = _load_json_file(args.visual_matrix_file)
    if getattr(args, "text_matrix_file", None):
        projection["text_matrix"] = _load_json_file(args.text_matrix_file)
    if projection:
        patch["projection"] = projection
    ranking: dict[str, Any] = {}
    for name in (
        "cosine_weight",
        "inverted_hit_weight",
        "t2i_visual_weight",
        "t2i_text_weight",
        "i2t_visual_weight",
        "i2t_text_weight",
        "index_dims",
        "default_k",
    ):
        value = getattr(args, name, None)
        if value is not None:
            ranking[name] = value
    if ranking:
        patch["ranking"] = ranking
    recall: dict[str, Any] = {}
    for name in ("max_error_bound", "probe_count", "probe_k"):
        value = getattr(args, name, None)
        if value is not None:
            recall[name] = value
    if recall:
        patch["recall"] = recall
    return patch


def _add_workspace(parser: argparse.ArgumentParser, default: str = "runtime/workspace") -> None:
    parser.add_argument("--workspace", default=default, help="persistent runtime workspace")


def _add_config_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--config-file", help="full AlignmentConfig JSON file")
    parser.add_argument("--bits", type=int, choices=(4, 8))
    parser.add_argument("--granularity", choices=("per_dim", "per_vector"))
    parser.add_argument("--projection-seed", type=int)
    parser.add_argument("--visual-matrix-file")
    parser.add_argument("--text-matrix-file")
    parser.add_argument("--cosine-weight", type=float)
    parser.add_argument("--inverted-hit-weight", dest="inverted_hit_weight", type=float)
    parser.add_argument("--t2i-visual-weight", type=float)
    parser.add_argument("--t2i-text-weight", type=float)
    parser.add_argument("--i2t-visual-weight", type=float)
    parser.add_argument("--i2t-text-weight", type=float)
    parser.add_argument("--index-dims", type=int)
    parser.add_argument("--default-k", type=int)
    parser.add_argument("--max-error-bound", type=float)
    parser.add_argument("--probe-count", type=int)
    parser.add_argument("--probe-k", type=int)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="cmir", description="Deterministic cross-modal alignment retrieval")
    subparsers = parser.add_subparsers(dest="command", required=True)

    generate = subparsers.add_parser("generate", help="generate deterministic local shards")
    generate.add_argument("--output", default="data/sample")
    generate.add_argument("--seed", type=int, default=20261122)
    generate.add_argument("--shards", type=int, default=3)
    generate.add_argument("--items-per-shard", type=int, default=6)
    generate.add_argument("--width", type=int, default=64)
    generate.add_argument("--height", type=int, default=48)
    generate.add_argument("--write-configs", default="configs")

    import_cmd = subparsers.add_parser("import", help="import a dataset root")
    _add_workspace(import_cmd)
    import_cmd.add_argument("--dataset", required=True)

    config = subparsers.add_parser("config", help="update projection/quantization/ranking config")
    _add_workspace(config)
    _add_config_options(config)

    build = subparsers.add_parser("build", help="build or resume all stage plans")
    _add_workspace(build)
    build.add_argument("--stop-after-units", type=int)

    update = subparsers.add_parser("update-shard", help="single-shard incremental update")
    _add_workspace(update)
    update.add_argument("--shard", required=True)
    update.add_argument("--stop-after-units", type=int)

    search = subparsers.add_parser("search", help="run bidirectional retrieval")
    _add_workspace(search)
    search.add_argument("--mode", required=True, choices=("text2image", "image2text"))
    search.add_argument("--query")
    search.add_argument("--query-item-id")
    search.add_argument("--query-image-file")
    search.add_argument("--query-vector-file")
    search.add_argument("--k", type=int)

    status = subparsers.add_parser("status", help="show dataset/config/checkpoint status")
    _add_workspace(status)

    stop = subparsers.add_parser("stop", help="request graceful stop after current unit")
    _add_workspace(stop)

    resume = subparsers.add_parser("resume", help="clear stop request and continue build")
    _add_workspace(resume)
    resume.add_argument("--stop-after-units", type=int)

    verify = subparsers.add_parser("verify", help="compare incremental state with a full rebuild")
    _add_workspace(verify)

    export = subparsers.add_parser("export", help="export search results and diagnostic report")
    _add_workspace(export)
    export.add_argument("--output", required=True)
    export.add_argument("--queries-file")

    audit = subparsers.add_parser("checkpoint-audit", help="audit checkpoint chain integrity")
    _add_workspace(audit)

    recover = subparsers.add_parser("recover", help="rebuild head pointer from valid checkpoint prefix")
    _add_workspace(recover)

    serve = subparsers.add_parser("serve", help="start local HTTP app")
    _add_workspace(serve)
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8765)

    demo = subparsers.add_parser("demo", help="run the deterministic end-to-end demonstration")
    demo.add_argument("--workspace", default="runtime/demo-workspace")
    demo.add_argument("--data-dir", default="data/sample")
    demo.add_argument("--report", default="artifacts/demo-report.json")

    http_demo = subparsers.add_parser("http-demo", help="exercise local HTTP endpoints end-to-end")
    http_demo.add_argument("--workspace", default="runtime/http-demo-workspace")
    http_demo.add_argument("--data-dir", default="data/sample")
    http_demo.add_argument("--port", type=int, default=0)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "generate":
            result = generate_dataset(
                args.output,
                seed=args.seed,
                shards=args.shards,
                items_per_shard=args.items_per_shard,
                width=args.width,
                height=args.height,
            )
            if args.write_configs:
                config_dir = Path(args.write_configs)
                config_dir.mkdir(parents=True, exist_ok=True)
                for bits in (8, 4):
                    cfg = default_config(bits, "per_dim")
                    from .canonical import atomic_write_json

                    atomic_write_json(config_dir / f"quant{bits}.json", cfg.to_dict())
            _print(result)
            return 0
        if args.command == "import":
            _print(_engine(args).import_dataset(args.dataset))
            return 0
        if args.command == "config":
            patch = _config_patch(args)
            _print(_engine(args).set_config(patch=patch))
            return 0
        if args.command == "build":
            _print(_engine(args).build(stop_after_units=args.stop_after_units))
            return 0
        if args.command == "update-shard":
            _print(
                _engine(args).update_shard(
                    args.shard, stop_after_units=args.stop_after_units
                )
            )
            return 0
        if args.command == "search":
            query_vector = (
                _load_json_file(args.query_vector_file)
                if args.query_vector_file
                else None
            )
            _print(
                _engine(args).search(
                    mode=args.mode,
                    query=args.query,
                    query_item_id=args.query_item_id,
                    query_image_file=args.query_image_file,
                    query_vector=query_vector,
                    k=args.k,
                )
            )
            return 0
        if args.command == "status":
            _print(_engine(args).status())
            return 0
        if args.command == "stop":
            _print(_engine(args).request_stop())
            return 0
        if args.command == "resume":
            _print(_engine(args).resume(stop_after_units=args.stop_after_units))
            return 0
        if args.command == "verify":
            _print(_engine(args).verify_full_consistency())
            return 0
        if args.command == "export":
            queries = _load_json_file(args.queries_file) if args.queries_file else None
            _print(_engine(args).export_report(args.output, queries=queries))
            return 0
        if args.command == "checkpoint-audit":
            _print(_engine(args).checkpoints.audit())
            return 0
        if args.command == "recover":
            _print(_engine(args).checkpoints.repair_head())
            return 0
        if args.command == "serve":
            from .http_app import serve

            serve(args.workspace, host=args.host, port=args.port)
            return 0
        if args.command == "demo":
            from .demo import run_demo

            _print(
                run_demo(
                    workspace=args.workspace,
                    data_dir=args.data_dir,
                    report_path=args.report,
                )
            )
            return 0
        if args.command == "http-demo":
            from .http_demo import run_http_demo

            _print(
                run_http_demo(
                    workspace=args.workspace,
                    data_dir=args.data_dir,
                    port=args.port,
                )
            )
            return 0
        parser.error("unknown command")
        return 2
    except CMIRError as exc:
        _print(exc.to_dict())
        return 2


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
