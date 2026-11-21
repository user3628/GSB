"""命令行入口：导入样本、分阶段重建、状态/指纹查询、停止、恢复、导出。"""

from __future__ import annotations

import argparse
import json
import os
import sys

from .canonical import dumps
from .errors import ReconError
from .generator import generate_samples
from .pipeline import Pipeline


def _print(obj) -> None:
    sys.stdout.write(dumps(obj).decode("utf-8") + "\n")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="recon3d", description="本地确定性多视角三维重建管线")
    sub = parser.add_subparsers(dest="command", required=True)

    p_gen = sub.add_parser("generate", help="生成确定性样本")
    p_gen.add_argument("--out", default="samples")
    p_gen.add_argument("--seed", type=int, default=20261121)

    p_run = sub.add_parser("run", help="全量/恢复重建")
    p_run.add_argument("--samples", default="samples")
    p_run.add_argument("--run", dest="run_dir", default="runs/default")
    p_run.add_argument("--force", action="store_true")
    p_run.add_argument("--stop-after", default=None)

    p_status = sub.add_parser("status", help="查看阶段状态与版本指纹")
    p_status.add_argument("--samples", default="samples")
    p_status.add_argument("--run", dest="run_dir", default="runs/default")

    p_invalid = sub.add_parser("invalidate", help="查看相对旧指纹的失效原因")
    p_invalid.add_argument("--samples", default="samples")
    p_invalid.add_argument("--run", dest="run_dir", default="runs/default")
    p_invalid.add_argument("--previous", required=True)

    p_stop = sub.add_parser("stop", help="请求优雅停止")
    p_stop.add_argument("--run", dest="run_dir", default="runs/default")

    p_resume = sub.add_parser("resume", help="从最近一致检查点恢复")
    p_resume.add_argument("--samples", default="samples")
    p_resume.add_argument("--run", dest="run_dir", default="runs/default")

    p_export = sub.add_parser("export", help="导出点云与验证摘要路径")
    p_export.add_argument("--run", dest="run_dir", default="runs/default")

    p_query = sub.add_parser("query", help="交互查询")
    p_query.add_argument("--samples", default="samples")
    p_query.add_argument("--run", dest="run_dir", default="runs/default")
    p_query.add_argument("--type", dest="query_type", required=True,
                         choices=["cloud_summary", "rejection_reasons", "error_stats", "stage_status"])
    p_query.add_argument("--budget", type=float, default=1.0)
    p_query.add_argument("--query-id", default=None)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "generate":
            manifest = generate_samples(args.out, args.seed)
            _print({"generated": args.out, "tiles": len(manifest["tiles"]),
                    "truth_points": len(manifest["truth_points"])})
            return 0

        if args.command == "run":
            pipeline = Pipeline(args.samples, args.run_dir)
            result = pipeline.run(stop_after=args.stop_after, force=args.force)
            _print(result)
            return 0

        if args.command == "status":
            pipeline = Pipeline(args.samples, args.run_dir)
            _print({"sample_fingerprint": pipeline.sample_fp,
                    "stages": pipeline.stage_fingerprints(),
                    "journal": pipeline.store.stage_status()})
            return 0

        if args.command == "invalidate":
            pipeline = Pipeline(args.samples, args.run_dir)
            previous = json.loads(open(args.previous, "rb").read())
            _print({"reasons": pipeline.invalidation_reason(previous)})
            return 0

        if args.command == "stop":
            Pipeline(args.samples if hasattr(args, "samples") else "samples", args.run_dir).request_stop()
            _print({"stop_requested": True, "run": args.run_dir})
            return 0

        if args.command == "resume":
            pipeline = Pipeline(args.samples, args.run_dir)
            result = pipeline.run(force=False)
            _print(result)
            return 0

        if args.command == "export":
            ply = os.path.join(args.run_dir, "exports", "point_cloud.ply")
            summary = os.path.join(args.run_dir, "exports", "validation_summary.json")
            _print({"point_cloud": ply, "validation_summary": summary,
                    "exists": os.path.exists(ply) and os.path.exists(summary)})
            return 0

        if args.command == "query":
            from .query import QueryService

            pipeline = Pipeline(args.samples, args.run_dir)
            service = QueryService(args.run_dir, pipeline.sample_fp,
                                   pipeline.manifest["config_version"], args.budget)
            service.stage_digests = {r["stage"]: r["artifact_digest"]
                                     for r in pipeline.store.stage_status()}
            _print(service.query(args.query_type, budget=args.budget, query_id=args.query_id))
            return 0
    except ReconError as error:
        body = {"error": error.to_dict()}
        sys.stdout.write(dumps(body).decode("utf-8") + "\n")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
