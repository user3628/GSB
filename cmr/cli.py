"""Command line entry: init / build / query / status / inject / stop / serve."""

import argparse
import json
import sys

from .errors import CmrError
from .inject import INJECTION_KINDS
from .service import Workbench


def _print(obj):
    print(json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True))


def main(argv=None):
    parser = argparse.ArgumentParser(prog="cmr", description="cross-modal retrieval workbench")
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--state-dir", default="state")
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("init", help="create config, projection and deterministic samples")

    p_build = sub.add_parser("build", help="run the four-stage build (incremental)")
    p_build.add_argument("--from-stage", choices=["features", "projection", "index", "recall"])
    p_build.add_argument("--stop-after", type=int, default=None,
                         help="gracefully stop after N indivisible units (demo hook)")

    p_qt = sub.add_parser("query-text", help="text query -> top-k images")
    p_qt.add_argument("text")
    p_qt.add_argument("-k", type=int, default=None)

    p_qi = sub.add_parser("query-image", help="image query -> top-k captions")
    p_qi.add_argument("image", help="PNG path or pair:<pair_id>")
    p_qi.add_argument("-k", type=int, default=None)

    sub.add_parser("status", help="stage status, fingerprints, invalidation, injections")

    p_inj = sub.add_parser("inject", help="run a fault injection")
    p_inj.add_argument("kind", choices=INJECTION_KINDS)

    p_imp = sub.add_parser("import-pairs", help="import a JSONL manifest")
    p_imp.add_argument("path")

    sub.add_parser("stop", help="request graceful stop")
    sub.add_parser("resume", help="clear stop flag and resume build")

    p_pub = sub.add_parser("publish", help="publish a checkpoint record (protected)")
    p_pub.add_argument("record_json")
    p_pub.add_argument("--expected-seq", type=int, default=None)

    p_serve = sub.add_parser("serve", help="start the local HTTP workbench")
    p_serve.add_argument("--host", default="127.0.0.1")
    p_serve.add_argument("--port", type=int, default=8371)

    sub.add_parser("verify", help="run the acceptance verification suite")

    args = parser.parse_args(argv)
    wb = Workbench(args.data_dir, args.state_dir)

    try:
        if args.cmd == "init":
            _print(wb.init())
        elif args.cmd == "build":
            _print(wb.build(from_stage=args.from_stage, stop_after=args.stop_after))
        elif args.cmd == "query-text":
            _print(wb.query_text(args.text, args.k))
        elif args.cmd == "query-image":
            _print(wb.query_image(args.image, args.k))
        elif args.cmd == "status":
            _print(wb.status())
        elif args.cmd == "inject":
            _print(wb.inject(args.kind))
        elif args.cmd == "import-pairs":
            with open(args.path, encoding="utf-8") as fh:
                _print(wb.import_pairs(fh.read()))
        elif args.cmd == "stop":
            _print(wb.stop())
        elif args.cmd == "resume":
            _print(wb.resume())
        elif args.cmd == "publish":
            _print(wb.publish(json.loads(args.record_json), args.expected_seq))
        elif args.cmd == "serve":
            import uvicorn

            from .server import create_app

            app = create_app(args.data_dir, args.state_dir)
            uvicorn.run(app, host=args.host, port=args.port, log_level="warning")
        elif args.cmd == "verify":
            from .verify import run_verification

            _print(run_verification())
    except CmrError as exc:
        _print(exc.to_dict())
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
