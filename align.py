#!/usr/bin/env python3
"""Command line diagnostic workbench for the alignment engine.

All commands run fully locally (no network access required).  Business
errors are reported as JSON with stable error codes on stderr, exit code 2.
"""

import argparse
import json
import os
import sys

from align_engine import service
from align_engine.errors import EngineError
from align_engine.pipeline import STAGE_ORDER


def _print(obj):
    print(json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True))


def _add_workspace(parser):
    parser.add_argument("--workspace", "-w", required=True,
                        help="workspace directory")


def build_parser():
    parser = argparse.ArgumentParser(prog="align.py", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("gen-samples", help="generate deterministic samples")
    p.add_argument("--out", default="samples")

    p = sub.add_parser("import", help="import shards + manifest")
    _add_workspace(p)
    p.add_argument("--manifest", required=True)
    p.add_argument("--shards-dir", required=True)
    p.add_argument("--extra-shards-dir", default=None)

    p = sub.add_parser("run", help="run/resume the alignment pipeline")
    _add_workspace(p)
    p.add_argument("--stage", default=None,
                   help="stage name or 1-based index (default: all stages)")
    p.add_argument("--stop-after-units", type=int, default=None,
                   help="deterministic stop hook for tests/demo")

    p = sub.add_parser("stop", help="request graceful stop")
    _add_workspace(p)

    p = sub.add_parser("resume", help="resume after stop (same as run)")
    _add_workspace(p)

    p = sub.add_parser("reset", help="clear checkpoints and artifacts")
    _add_workspace(p)

    p = sub.add_parser("status", help="stage status and fingerprints")
    _add_workspace(p)

    p = sub.add_parser("summary", help="alignment summary")
    _add_workspace(p)

    p = sub.add_parser("uncovered", help="uncovered / degraded samples")
    _add_workspace(p)

    p = sub.add_parser("fingerprints", help="version fingerprints")
    _add_workspace(p)

    p = sub.add_parser("checkpoints", help="checkpoint records")
    _add_workspace(p)

    p = sub.add_parser("invalidations", help="invalidation events")
    _add_workspace(p)

    p = sub.add_parser("convert", help="convert between the three formats")
    p.add_argument("--src", required=True)
    p.add_argument("--src-format", required=True,
                   choices=["json_project", "npz_pack", "npy_dir"])
    p.add_argument("--dst", required=True)
    p.add_argument("--dst-format", required=True,
                   choices=["json_project", "npz_pack", "npy_dir"])
    p.add_argument("--verify", action="store_true")

    p = sub.add_parser("verify-roundtrip",
                       help="convert to all three formats and re-import")
    p.add_argument("--src", required=True)
    p.add_argument("--src-format", required=True,
                   choices=["json_project", "npz_pack", "npy_dir"])
    p.add_argument("--work-dir", required=True)

    p = sub.add_parser("upgrade",
                       help="upgrade v1 manifest; verify v2 equivalence")
    p.add_argument("--v1", required=True)
    p.add_argument("--v2", required=True)
    p.add_argument("--shards-dir", required=True)
    p.add_argument("--work-dir", required=True)
    p.add_argument("--out", default=None,
                   help="write re-exported v2 manifest here")

    p = sub.add_parser("export", help="run the export stage")
    _add_workspace(p)

    p = sub.add_parser("config-show", help="show engine config")
    _add_workspace(p)

    p = sub.add_parser("config-set", help="set a config value (dot path)")
    _add_workspace(p)
    p.add_argument("--key", required=True)
    p.add_argument("--value", required=True,
                   help="JSON value, e.g. 0.9 or '\"text_then_vision\"'")

    p = sub.add_parser("serve", help="start the HTTP diagnostic workbench")
    _add_workspace(p)
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8765)

    p = sub.add_parser("demo", help="deterministic end-to-end demonstration")
    p.add_argument("--root", default="demo_run")
    return parser


def _resolve_stage(value):
    if value is None:
        return None
    if value.isdigit():
        index = int(value)
        if 1 <= index <= len(STAGE_ORDER):
            return STAGE_ORDER[index - 1]
        return str(index)  # let the pipeline raise E_INVALID_STAGE
    return value


def main(argv=None):
    args = build_parser().parse_args(argv)
    try:
        if args.command == "gen-samples":
            import sample_gen
            _print(sample_gen.generate(args.out))
        elif args.command == "import":
            _print(service.do_import(args.workspace, args.manifest,
                                     args.shards_dir,
                                     args.extra_shards_dir))
        elif args.command == "run":
            _print(service.do_run(args.workspace,
                                  stage=_resolve_stage(args.stage),
                                  stop_after_units=args.stop_after_units))
        elif args.command == "stop":
            _print(service.do_stop(args.workspace))
        elif args.command == "resume":
            _print(service.do_run(args.workspace))
        elif args.command == "reset":
            _print(service.do_reset(args.workspace))
        elif args.command == "status":
            _print(service.do_status(args.workspace))
        elif args.command == "summary":
            _print(service.do_summary(args.workspace))
        elif args.command == "uncovered":
            _print(service.do_uncovered(args.workspace))
        elif args.command == "fingerprints":
            _print(service.do_fingerprints(args.workspace))
        elif args.command == "checkpoints":
            _print(service.do_checkpoints(args.workspace))
        elif args.command == "invalidations":
            _print(service.do_invalidations(args.workspace))
        elif args.command == "convert":
            _print(service.do_convert(args.src, args.src_format,
                                      args.dst, args.dst_format,
                                      verify=args.verify))
        elif args.command == "verify-roundtrip":
            _print(service.do_verify_roundtrip(args.src, args.src_format,
                                               args.work_dir))
        elif args.command == "upgrade":
            _print(service.do_upgrade(args.v1, args.v2, args.shards_dir,
                                      args.work_dir, out_path=args.out))
        elif args.command == "export":
            _print(service.do_export(args.workspace))
        elif args.command == "config-show":
            _print(service.do_config_show(args.workspace))
        elif args.command == "config-set":
            _print(service.do_config_set(args.workspace, args.key,
                                         json.loads(args.value)))
        elif args.command == "serve":
            from align_engine.workbench.server import serve
            serve(args.workspace, args.host, args.port)
        elif args.command == "demo":
            from align_engine.workbench.demo import run_demo
            _print(run_demo(args.root))
        else:  # pragma: no cover
            raise SystemExit("unknown command")
    except EngineError as exc:
        print(json.dumps({"error": exc.to_dict()}, ensure_ascii=False,
                         indent=2, sort_keys=True), file=sys.stderr)
        raise SystemExit(2)


if __name__ == "__main__":
    main()
