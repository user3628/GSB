"""Command line interface for the OCR diagnostic workbench."""

from __future__ import annotations

import argparse
import json
import os
import sys

from .checkpoint import CheckpointStore
from .config import WorkbenchConfig
from .errors import WorkbenchError
from .hashing import canonical_json
from .pipeline import Pipeline
from .samplegen.generator import generate_dataset


def _print(payload) -> None:
    print(canonical_json(payload) if isinstance(payload, (dict, list)) else str(payload))


def _load_pipeline(args) -> Pipeline:
    config = WorkbenchConfig()
    config_path = os.path.join(args.workspace, "config.json")
    if os.path.exists(config_path):
        config = CheckpointStore(args.workspace).load_config()
    return Pipeline(args.workspace, args.samples, config)


def cmd_generate(args) -> None:
    manifest = generate_dataset(args.samples, seed=args.seed)
    _print(
        {
            "samples": args.samples,
            "groups": [
                {"name": group["name"], "shards": group["shard_count"], "chars": group["total_chars"]}
                for group in manifest["groups"]
            ],
            "total_shards": len(manifest["shards"]),
        }
    )


def cmd_import(args) -> None:
    with open(os.path.join(args.samples, "manifest.json"), "r", encoding="utf-8") as handle:
        manifest = json.load(handle)
    pipeline = _load_pipeline(args)
    pipeline.import_sample(manifest)
    _print({"imported": len(manifest["shards"]), "workspace": args.workspace})


def cmd_run(args) -> None:
    pipeline = _load_pipeline(args)
    if not getattr(args, "allow_stop", False):
        pipeline.clear_stop()
    stats = pipeline.run()
    _print(stats.to_dict())


def cmd_status(args) -> None:
    store = CheckpointStore(args.workspace)
    status = store.status()
    if args.image_id:
        status["shards"] = [shard for shard in status["shards"] if shard["image_id"] == args.image_id]
    _print(status)


def cmd_fingerprints(args) -> None:
    store = CheckpointStore(args.workspace)
    payload = {
        "config_version": store.load_config().version() if os.path.exists(store.config_path) else None,
        "input_fingerprints": store.load_input_fingerprints(),
        "journal_fingerprints": {
            image_id: [
                {
                    "stage": record["stage"],
                    "sequence": record["sequence"],
                    "input_fingerprint": record["input_fingerprint"],
                    "upstream_fingerprint": record.get("upstream_fingerprint"),
                    "stage_fingerprint": record["stage_fingerprint"],
                    "artifact_summary": record["artifact_summary"],
                }
                for record in store.read_journal(image_id)
            ]
            for image_id in (
                os.path.splitext(name)[0] for name in os.listdir(store.journal_dir)
            )
        },
    }
    _print(payload)


def cmd_decision(args) -> None:
    store = CheckpointStore(args.workspace)
    payload = store.load_artifact(args.image_id, "decision")
    _print(payload)


def cmd_report(args) -> None:
    report = _load_pipeline(args).load_report()
    _print(report)


def cmd_stop(args) -> None:
    pipeline = _load_pipeline(args)
    pipeline.request_stop()
    _print({"stop_requested": True, "flag": pipeline.stop_flag})


def cmd_resume(args) -> None:
    pipeline = _load_pipeline(args)
    pipeline.clear_stop()
    stats = pipeline.run()
    _print(stats.to_dict())


def cmd_export(args) -> None:
    store = CheckpointStore(args.workspace)
    manifest = store.load_manifest()
    export = []
    for shard in manifest["shards"]:
        decision = store.load_artifact(shard["image_id"], "decision")
        recognition = store.load_artifact(shard["image_id"], "recognize")
        export.append(
            {
                "image_id": shard["image_id"],
                "group": shard["group"],
                "detection": decision["refined_detection"],
                "decision": decision["decision"],
                "recognition": recognition["recognition"],
            }
        )
    with open(args.output, "w", encoding="utf-8") as handle:
        handle.write(canonical_json(export))
    _print({"exported": len(export), "path": os.path.abspath(args.output)})


def cmd_serve(args) -> None:
    from .http_server import build_server

    pipeline = _load_pipeline(args)
    server = build_server(args.workspace, args.samples, pipeline.config, port=args.port)
    print(f"OCR workbench listening on http://127.0.0.1:{args.port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.shutdown()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ocrwb", description="OCR diagnostic workbench")
    parser.add_argument("--workspace", default="workspace", help="checkpoint workspace directory")
    parser.add_argument("--samples", default="samples", help="directory containing manifest.json and images")
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--workspace", default="workspace", help="checkpoint workspace directory")
    common.add_argument("--samples", default="samples", help="directory containing manifest.json and images")
    sub = parser.add_subparsers(dest="command", required=True)

    generated = sub.add_parser("generate", help="generate deterministic sample dataset", parents=[common])
    generated.add_argument("--seed", type=int, default=20261121)
    generated.set_defaults(func=cmd_generate)

    sub.add_parser("import", help="import manifest + shards into the workspace", parents=[common]).set_defaults(func=cmd_import)
    run_parser = sub.add_parser("run", help="run the full four-stage pipeline", parents=[common])
    run_parser.add_argument(
        "--allow-stop",
        action="store_true",
        help="honor an existing STOP flag instead of clearing it (used to demonstrate graceful stop)",
    )
    run_parser.set_defaults(func=cmd_run)

    status = sub.add_parser("status", help="show per-shard stage status", parents=[common])
    status.add_argument("--image-id", default=None)
    status.set_defaults(func=cmd_status)

    sub.add_parser("fingerprints", help="show input and stage version fingerprints", parents=[common]).set_defaults(
        func=cmd_fingerprints
    )

    decision = sub.add_parser("decision", help="show one shard's strategy decision record", parents=[common])
    decision.add_argument("image_id")
    decision.set_defaults(func=cmd_decision)

    sub.add_parser("report", help="show the aggregated diagnostic report", parents=[common]).set_defaults(func=cmd_report)
    sub.add_parser("stop", help="request a graceful stop", parents=[common]).set_defaults(func=cmd_stop)
    sub.add_parser("resume", help="clear the stop flag and resume", parents=[common]).set_defaults(func=cmd_resume)

    export = sub.add_parser("export", help="export candidates + recognition results", parents=[common])
    export.add_argument("--output", default="export.json")
    export.set_defaults(func=cmd_export)

    served = sub.add_parser("serve", help="start the local HTTP diagnostic workbench", parents=[common])
    served.add_argument("--port", type=int, default=8088)
    served.set_defaults(func=cmd_serve)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        args.func(args)
    except WorkbenchError as exc:
        print(canonical_json(exc.to_dict()), file=sys.stderr)
        return 2
    except FileNotFoundError as exc:
        print(canonical_json({"code": "E_INVALID_STAGE", "message": str(exc)}), file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
