from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from .canonical import read_json, read_jsonl
from .demo import generate_demo_dataset, run_demo
from .engine import Engine
from .errors import EngineError
from .layout import normalize_layout
from .workspace import Workspace


def _json_dump(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True, allow_nan=False)


def _ids(value: str) -> list[str]:
    values = [item.strip() for item in value.split(",") if item.strip()]
    if not values:
        raise EngineError("E_INVALID_INPUT", "--text-ids must contain at least one id", "request_validation")
    return values


def _print_error(exc: EngineError) -> None:
    print(_json_dump({"ok": False, "error": exc.to_dict()}), file=sys.stderr)


def _engine(args: argparse.Namespace) -> Engine:
    return Engine(Path(args.workspace))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="mmbackfill", description="Deterministic multimodal backfill engine")
    parser.add_argument("--workspace", default=".mmbackfill-work", help="workspace directory (default: .mmbackfill-work)")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("doctor", help="verify atomic replace, UTF-8 and real file locks")

    gen = sub.add_parser("generate-demo", help="write fixed-seed demo inputs and fault fixtures")
    gen.add_argument("--out", required=True)
    gen.add_argument("--seed", type=int, default=42)

    demo = sub.add_parser("demo", help="run the full deterministic acceptance demo")
    demo.add_argument("--out", required=True)
    demo.add_argument("--seed", type=int, default=42)

    layout = sub.add_parser("import-layout", help="validate, install and reindex a layout configuration")
    layout.add_argument("--layout", required=True)

    validate = sub.add_parser("validate-layout", help="validate a layout configuration without changing state")
    validate.add_argument("--layout", required=True)

    images = sub.add_parser("import-images", help="replace the image catalog and stage PNG files")
    images.add_argument("--jsonl", required=True)
    images.add_argument("--images-dir")

    texts = sub.add_parser("import-texts", help="replace the text catalog")
    texts.add_argument("--jsonl", required=True)

    imported = sub.add_parser("import-inputs", help="import layout, images and texts in one operation")
    imported.add_argument("--images-jsonl", required=True)
    imported.add_argument("--texts-jsonl", required=True)
    imported.add_argument("--images-dir")
    imported.add_argument("--layout", required=True)

    backfill = sub.add_parser("backfill", help="atomically commit one batch")
    backfill.add_argument("--batch-id", required=True)
    backfill.add_argument("--text-ids", required=True)
    backfill.add_argument("--expected-seq", required=True, type=int)
    backfill.add_argument("--order", choices=["arrival", "id"], default="id")
    backfill.add_argument("--vector-overrides", help="JSON object mapping text_id to a raw vector")

    all_cmd = sub.add_parser("backfill-all", help="commit multiple atomic batches")
    all_cmd.add_argument("--text-ids", required=True)
    all_cmd.add_argument("--batch-size", type=int, default=2)
    all_cmd.add_argument("--expected-seq", type=int, default=0)
    all_cmd.add_argument("--order", choices=["arrival", "id"], default="id")
    all_cmd.add_argument("--batch-prefix", default="batch")

    sub.add_parser("status", help="show sequence, stop and index summary")
    sub.add_parser("index", help="dump the canonical shard layout")
    sub.add_parser("batches", help="list batch records and sequence numbers")
    sub.add_parser("invalidations", help="show invalidation events and failed shards")
    sub.add_parser("vectors", help="dump the deterministic vector cache")
    sub.add_parser("diagnostics", help="list persisted structured diagnostics")
    sub.add_parser("recover", help="clear stop marker and recover the latest consistent checkpoint")
    stop = sub.add_parser("stop", help="request graceful stop between atomic batches")
    stop.add_argument("--reason", default="operator request")

    serve = sub.add_parser("serve", help="run the local HTTP API")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8765)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "generate-demo":
            result = generate_demo_dataset(Path(args.out), args.seed)
        elif args.command == "demo":
            result = run_demo(Path(args.out), args.seed)
        elif args.command == "doctor":
            result = _engine(args).workspace.doctor()
        elif args.command == "import-layout":
            result = _engine(args).install_layout(read_json(Path(args.layout)))
        elif args.command == "validate-layout":
            normalized = normalize_layout(read_json(Path(args.layout)))
            result = {"ok": True, "layout": normalized}
        elif args.command == "import-images":
            result = _engine(args).import_images(
                read_jsonl(Path(args.jsonl)),
                Path(args.images_dir) if args.images_dir else None,
            )
        elif args.command == "import-texts":
            result = _engine(args).import_texts(read_jsonl(Path(args.jsonl)))
        elif args.command == "import-inputs":
            engine = _engine(args)
            engine.install_layout(read_json(Path(args.layout)))
            result = engine.import_inputs(
                read_jsonl(Path(args.images_jsonl)),
                read_jsonl(Path(args.texts_jsonl)),
                Path(args.images_dir) if args.images_dir else None,
            )
        elif args.command == "backfill":
            overrides = read_json(Path(args.vector_overrides)) if args.vector_overrides else None
            result = _engine(args).submit_batch(
                batch_id=args.batch_id,
                text_ids=_ids(args.text_ids),
                expected_sequence=args.expected_seq,
                order=args.order,
                vector_overrides=overrides,
            )
        elif args.command == "backfill-all":
            result = _engine(args).backfill_all(
                text_ids=_ids(args.text_ids),
                expected_sequence=args.expected_seq,
                batch_size=args.batch_size,
                batch_prefix=args.batch_prefix,
                order=args.order,
            )
        elif args.command == "status":
            result = _engine(args).status()
        elif args.command == "index":
            result = _engine(args).index()
        elif args.command == "batches":
            result = _engine(args).batches()
        elif args.command == "invalidations":
            result = _engine(args).invalidations()
        elif args.command == "vectors":
            result = _engine(args).vectors()
        elif args.command == "diagnostics":
            result = _engine(args).diagnostics()
        elif args.command == "recover":
            result = _engine(args).recover(clear_stop=True)
        elif args.command == "stop":
            result = {"ok": True, "stop": _engine(args).workspace.request_stop(args.reason)}
        elif args.command == "serve":
            import uvicorn

            from .api import create_app

            app = create_app(Path(args.workspace))
            uvicorn.run(app, host=args.host, port=args.port)
            return 0
        else:
            parser.error(f"unknown command {args.command}")
            return 2
    except EngineError as exc:
        _print_error(exc)
        return 1
    print(_json_dump(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
