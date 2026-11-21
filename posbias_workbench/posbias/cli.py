"""Command-line interface for the position-bias workbench."""

from __future__ import annotations

import argparse
import json
import os
import sys

from .api.server import WorkbenchService, port_available, serve
from .core.config import (AppConfig, convert_legacy_v1,
                          export_legacy_v1, content_fingerprint)
from .core.injection import INJECTION_KINDS, run_injection
from .core.pipeline import Pipeline, StopFlag
from .core.shards import write_shard
from .errors import WorkbenchError


def _load(args):
    return AppConfig.load(args.config)


def cmd_generate(args):
    cfg = _load(args)
    os.makedirs(args.shard_dir, exist_ok=True)
    for idx in range(args.shards):
        meta = write_shard(
            args.shard_dir, seed=cfg.seed + args.seed_offset,
            shard_index=idx, batch=args.batch, heads=args.heads,
            seq_len=args.seq_len, head_dim=args.head_dim,
            inject_nan=args.inject_nan)
        print(json.dumps({"written": meta["q_file"],
                          "shape": meta["shape"]}, ensure_ascii=False))


def cmd_run(args):
    cfg = _load(args)
    pipeline = Pipeline(cfg, args.workdir,
                        shard_dir=args.shard_dir,
                        reference_path=args.reference,
                        stop_flag=StopFlag())
    report = pipeline.run(report_path=args.report)
    print(json.dumps({"status": "completed",
                      "report_path": args.report,
                      "config_fingerprint": report["config_fingerprint"],
                      "reused_stages": report["reused_stages"],
                      "passed": report["tolerance"]["passed"]},
                     ensure_ascii=False, indent=2))


def cmd_reference(args):
    cfg = _load(args)
    pipeline = Pipeline(cfg, args.workdir, shard_dir=args.shard_dir,
                        reference_path=args.reference, stop_flag=StopFlag())
    path = pipeline.ensure_reference()
    print(json.dumps({"reference_written": path}, ensure_ascii=False))


def cmd_stop(args):
    flag_file = os.path.join(args.workdir, "stop.flag")
    open(flag_file, "w", encoding="utf-8").write("stop\n")
    print(json.dumps({"stop_flag": flag_file}, ensure_ascii=False))


def cmd_resume(args):
    cfg = _load(args)
    pipeline = Pipeline(cfg, args.workdir, shard_dir=args.shard_dir,
                        reference_path=args.reference, stop_flag=StopFlag())
    report = pipeline.run(report_path=args.report)
    print(json.dumps({"status": "resumed",
                      "sequences": report["checkpoint_sequences"],
                      "reused_stages": report["reused_stages"]},
                     ensure_ascii=False, indent=2))


def cmd_inject(args):
    cfg = _load(args)
    outcome = run_injection(kind=args.kind, config=cfg,
                            work_dir=args.workdir)
    print(json.dumps(outcome, ensure_ascii=False, indent=2))


def cmd_legacy_import(args):
    cfg, provenance = convert_legacy_v1(
        __import__("yaml").safe_load(open(args.legacy, encoding="utf-8")))
    cfg.dump(args.output)
    print(json.dumps({"converted": args.output,
                      "provenance": provenance}, ensure_ascii=False,
                     indent=2)[:2000])


def cmd_legacy_export(args):
    cfg = _load(args)
    legacy = export_legacy_v1(cfg)
    with open(args.output, "w", encoding="utf-8") as handle:
        __import__("yaml").safe_dump(legacy, handle, sort_keys=True,
                                     allow_unicode=True)
    print(json.dumps({"legacy_written": args.output}, ensure_ascii=False))


def cmd_serve(args):
    if not port_available(args.host, args.port):
        raise OSError(f"port {args.port} unavailable on {args.host}")
    service = WorkbenchService(config_path=args.config,
                               work_dir=args.workdir,
                               reference_path=args.reference)
    httpd = serve(service, host=args.host, port=args.port)
    print(json.dumps({"serving": f"http://{args.host}:{args.port}"},
                     ensure_ascii=False))
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        httpd.shutdown()


def build_parser():
    parser = argparse.ArgumentParser(
        prog="posbias",
        description="Long-context position-encoding bias workbench")
    parser.add_argument("--config", default="configs/config_v2.yaml")
    parser.add_argument("--workdir", default="data")
    parser.add_argument("--shard-dir", default=None)
    parser.add_argument("--reference", default=None)
    parser.add_argument("--report", default=None)
    sub = parser.add_subparsers(dest="command", required=True)

    gen = sub.add_parser("generate", help="generate deterministic shards")
    gen.add_argument("--shards", type=int, default=3)
    gen.add_argument("--batch", type=int, default=2)
    gen.add_argument("--heads", type=int, default=4)
    gen.add_argument("--seq-len", type=int, default=12)
    gen.add_argument("--head-dim", type=int, default=16)
    gen.add_argument("--seed-offset", type=int, default=0)
    gen.add_argument("--inject-nan", action="store_true")
    gen.set_defaults(func=cmd_generate)

    run = sub.add_parser("run", help="run the four-stage pipeline")
    run.set_defaults(func=cmd_run)

    ref = sub.add_parser("reference", help="write golden reference metrics")
    ref.set_defaults(func=cmd_reference)

    stop = sub.add_parser("stop", help="write a stop flag marker")
    stop.set_defaults(func=cmd_stop)

    resume = sub.add_parser("resume", help="resume from checkpoints")
    resume.set_defaults(func=cmd_resume)

    inject = sub.add_parser("inject", help="run a fault injection case")
    inject.add_argument("kind", choices=INJECTION_KINDS)
    inject.set_defaults(func=cmd_inject)

    imp = sub.add_parser("legacy-import", help="convert v1 manifest to v2")
    imp.add_argument("--legacy", default="configs/legacy_manifest_v1.yaml")
    imp.add_argument("--output", default="data/config_converted_v2.yaml")
    imp.set_defaults(func=cmd_legacy_import)

    exp = sub.add_parser("legacy-export", help="export v2 back to v1")
    exp.add_argument("--output", default="data/legacy_roundtrip_v1.yaml")
    exp.set_defaults(func=cmd_legacy_export)

    server = sub.add_parser("serve", help="start loopback HTTP service")
    server.add_argument("--host", default="127.0.0.1")
    server.add_argument("--port", type=int, default=8765)
    server.set_defaults(func=cmd_serve)
    return parser


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.shard_dir is None:
        args.shard_dir = os.path.join(args.workdir, "shards")
    if args.reference is None:
        args.reference = os.path.join(args.workdir, "reference",
                                      "reference_metrics.yaml")
    if args.report is None:
        args.report = os.path.join(args.workdir, "reports",
                                   "diagnostic_report.json")
    try:
        args.func(args)
    except WorkbenchError as exc:
        print(json.dumps(exc.to_dict(), ensure_ascii=False, indent=2),
              file=sys.stderr)
        return 2
    except OSError as exc:
        print(json.dumps({"code": "E_PORT_UNAVAILABLE",
                          "message": str(exc)}), file=sys.stderr)
        return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
