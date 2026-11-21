"""Command-line interface for the deterministic IQA component."""

import argparse
import json
import os
import sys

from .config import Config
from .injection import Injection
from .pipeline import Pipeline
from .checkpoints import STAGE_ORDER
from .hashing import canonical


def _load_json(path):
    with open(path, "r", encoding="utf-8") as handle:
        return json.loads(handle.read())


def _config_from_args(args):
    weights = None
    thresholds = None
    if args.weights:
        weights = _load_json(args.weights)
    if args.thresholds:
        thresholds = _load_json(args.thresholds)
    config = Config(
        weights=weights or Config().weights,
        thresholds=thresholds or Config().thresholds,
        fusion_domain=args.fusion_domain,
    )
    return config.validate()


def _injection_from_args(args):
    return Injection(
        kind=args.inject or "none",
        image_id=args.inject_image or "",
        value=args.inject_value,
    )


def build_parser():
    parser = argparse.ArgumentParser(description="Deterministic generated-image quality assessment")
    parser.add_argument("--work-dir", default="work")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("generate", help="regenerate deterministic samples and metadata")

    run_p = sub.add_parser("run", help="run the four-stage pipeline")
    run_p.add_argument("--weights")
    run_p.add_argument("--thresholds")
    run_p.add_argument("--fusion-domain", choices=["log", "linear"], default="log")
    run_p.add_argument("--inject", choices=["dimension_mismatch", "missing_reference", "nan_pixels", "negative_weight"])
    run_p.add_argument("--inject-image", default="")
    run_p.add_argument("--inject-value", type=float, default=-1.0)

    sub.add_parser("status", help="show stage status and version fingerprints")
    sub.add_parser("scores", help="show per-dimension scores and skip reasons")
    sub.add_parser("diagnostics", help="show diagnostics and out-of-range report")

    stop_p = sub.add_parser("stop", help="request a graceful stop after the current unit")
    resume_p = sub.add_parser("resume", help="clear the stop flag and continue")
    serve_p = sub.add_parser("serve", help="start the loopback HTTP diagnostic interface")
    serve_p.add_argument("--host", default="127.0.0.1")
    serve_p.add_argument("--port", type=int, default=8765)
    return parser


def _print(payload):
    sys.stdout.write(canonical(payload))
    sys.stdout.write("\n")


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    work_dir = os.path.abspath(args.work_dir)

    if args.command == "generate":
        sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "scripts"))
        from generate_samples import main as generate_main

        generate_main(work_dir)
        return 0

    if args.command == "stop":
        Pipeline(work_dir).store.request_stop()
        _print({"stop_requested": True, "work_dir": work_dir})
        return 0

    if args.command == "resume":
        pipeline = Pipeline(work_dir)
        pipeline.store.clear_stop()
        summary = pipeline.run()
        _print(summary)
        return 0

    if args.command == "status":
        _print(Pipeline(work_dir).status())
        return 0

    if args.command == "scores":
        pipeline = Pipeline(work_dir)
        payload = []
        for metadata in pipeline.load_metadata():
            image_id = metadata["image_id"]
            entry = {"image_id": image_id, "stages": {}}
            for stage in STAGE_ORDER:
                record = pipeline.store.load(image_id, stage)
                entry["stages"][stage] = None if record is None else record["product"]
            payload.append(entry)
        _print(payload)
        return 0

    if args.command == "diagnostics":
        path = os.path.join(work_dir, "outputs", "diagnostics.json")
        if not os.path.exists(path):
            raise SystemExit("no diagnostics report found; run the pipeline first")
        with open(path, "r", encoding="utf-8") as handle:
            sys.stdout.write(handle.read())
        return 0

    if args.command == "run":
        pipeline = Pipeline(work_dir, _config_from_args(args), _injection_from_args(args))
        summary = pipeline.run()
        _print(summary)
        return 0

    if args.command == "serve":
        from .http_app import serve

        serve(work_dir, args.host, args.port)
        return 0

    parser.error(f"unknown command {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
