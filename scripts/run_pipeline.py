#!/usr/bin/env python3
"""Command line entry point for the matching/scheduling pipeline."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from detpipe.config import PipelineConfig
from detpipe.errors import PipeError
from detpipe.pipeline import Pipeline
from detpipe.validator import report_bytes, validate_run, write_report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the detection candidate pipeline")
    parser.add_argument("--samples", required=True, help="path to deterministic samples JSON")
    parser.add_argument("--workspace", required=True, help="checkpoint workspace directory")
    parser.add_argument("--iou-threshold", type=float, default=0.5)
    parser.add_argument("--score-threshold", type=float, default=0.5)
    parser.add_argument("--max-evaluations", type=int, default=1_000_000)
    parser.add_argument("--worker-quota", type=int, default=1_000_000)
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--shard-size", type=int, default=2)
    parser.add_argument("--fresh", action="store_true",
                        help="fail instead of resuming when a workspace already exists")
    parser.add_argument("--report", default=None, help="write validation report JSON here")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    with open(args.samples, "r", encoding="utf-8") as fh:
        dataset = json.load(fh)
    config = PipelineConfig(
        iou_threshold=args.iou_threshold,
        score_threshold=args.score_threshold,
        max_evaluations=args.max_evaluations,
        worker_quota=args.worker_quota,
        workers=args.workers,
        shard_size=args.shard_size,
    )
    try:
        result = Pipeline(dataset, config, args.workspace).run(resume=not args.fresh)
        report = validate_run(args.workspace)
    except PipeError as exc:
        print(json.dumps({"error": exc.to_dict()}, ensure_ascii=False, indent=2))
        return 2
    if args.report:
        write_report(report, args.report)
    print(json.dumps(
        {
            "run_status": result["status"],
            "shards_published": result["shards_published"],
            "shards_total": result["shards_total"],
            "scheduler_state": result["scheduler_state"],
            "errors": result["errors"],
            "global_statistics": report["global_statistics"],
        },
        ensure_ascii=False, indent=2,
    ))
    if result["errors"]:
        return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
