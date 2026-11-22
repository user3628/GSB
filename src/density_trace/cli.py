from __future__ import annotations

import argparse
import json
import sys
from typing import Any

from .demo import generate_demo_data, run_demo
from .engine import Engine, import_inputs
from .errors import BusinessError
from .http_server import serve


def _emit(value: Any) -> None:
    print(
        json.dumps(value, ensure_ascii=False, allow_nan=False, indent=2, sort_keys=True)
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="density-trace")
    subparsers = parser.add_subparsers(dest="command", required=True)

    generate = subparsers.add_parser(
        "generate-demo", help="generate deterministic local NPZ demo data"
    )
    generate.add_argument("--out", required=True)
    generate.add_argument("--keep-existing", action="store_true")

    import_parser = subparsers.add_parser(
        "import", help="validate and snapshot local batch inputs"
    )
    import_parser.add_argument("--workspace", required=True)
    import_parser.add_argument("--manifest", required=True)
    import_parser.add_argument("--config", required=True)
    import_parser.add_argument("--reference", required=True)

    run = subparsers.add_parser(
        "run", help="execute the next batch or resume the batch stream"
    )
    run.add_argument("--workspace", required=True)
    run.add_argument("--batch", type=int)
    run.add_argument("--max-batches", type=int)
    run.add_argument("--expected-version", type=int)
    run.add_argument("--stop-after", type=int)

    resume = subparsers.add_parser(
        "resume", help="resume from the latest valid checkpoint"
    )
    resume.add_argument("--workspace", required=True)
    resume.add_argument("--max-batches", type=int)
    resume.add_argument("--expected-version", type=int)

    reconcile = subparsers.add_parser(
        "reconcile",
        help="detect version changes and invalidate from the earliest affected batch",
    )
    reconcile.add_argument("--workspace", required=True)

    status = subparsers.add_parser("status", help="show final or partial run status")
    status.add_argument("--workspace", required=True)

    version = subparsers.add_parser(
        "version", help="show fingerprints, generation, and invalidation observability"
    )
    version.add_argument("--workspace", required=True)

    state = subparsers.add_parser(
        "state", help="export an independently decodable intermediate density state"
    )
    state.add_argument("--workspace", required=True)
    state.add_argument("--batch", type=int, required=True)
    state.add_argument("--expected-version", type=int)

    curve = subparsers.add_parser("curve", help="export a batch density curve")
    curve.add_argument("--workspace", required=True)
    curve.add_argument("--batch", type=int, required=True)
    curve.add_argument("--expected-version", type=int)

    verdict = subparsers.add_parser("verdict", help="export a batch drift adjudication")
    verdict.add_argument("--workspace", required=True)
    verdict.add_argument("--batch", type=int, required=True)
    verdict.add_argument("--expected-version", type=int)

    errors = subparsers.add_parser(
        "errors", help="export a batch numeric error report and block partial sums"
    )
    errors.add_argument("--workspace", required=True)
    errors.add_argument("--batch", type=int, required=True)
    errors.add_argument("--expected-version", type=int)

    stop = subparsers.add_parser(
        "request-stop", help="request graceful stop at the next batch boundary"
    )
    stop.add_argument("--workspace", required=True)
    stop.add_argument("--reason", default="cli")

    batches = subparsers.add_parser(
        "batches", help="list imported batches and input fingerprints"
    )
    batches.add_argument("--workspace", required=True)

    server = subparsers.add_parser("serve", help="run the local HTTP API")
    server.add_argument("--workspace", required=True)
    server.add_argument("--host", default="127.0.0.1")
    server.add_argument("--port", type=int, default=8765)

    demo = subparsers.add_parser(
        "demo", help="run the complete deterministic demonstration"
    )
    demo.add_argument("--out", required=True)
    demo.add_argument("--keep-existing", action="store_true")
    return parser


def _run_command(args: argparse.Namespace) -> Any:
    if args.command == "generate-demo":
        return generate_demo_data(args.out, overwrite=not args.keep_existing)
    if args.command == "import":
        return import_inputs(args.workspace, args.manifest, args.config, args.reference)
    if args.command == "demo":
        return run_demo(args.out, overwrite=not args.keep_existing)
    if args.command == "serve":
        serve(args.workspace, args.host, args.port)
        return {"status": "stopped"}

    engine = Engine.from_workspace(args.workspace)
    if args.command == "run":
        callback = None
        if args.stop_after is not None:
            published = {"count": 0}

            def callback(record: dict[str, Any]) -> None:
                published["count"] += 1
                if published["count"] >= args.stop_after:
                    engine.request_stop(
                        {
                            "reason": "cli-stop-after",
                            "batch_id": int(record["batch_id"]),
                        }
                    )

        if args.batch is not None:
            return engine.run(
                batch_id=args.batch,
                expected_version=args.expected_version,
                on_batch=callback,
            )
        return engine.resume(
            expected_version=args.expected_version,
            max_batches=args.max_batches,
            on_batch=callback,
        )
    if args.command == "resume":
        return engine.resume(
            expected_version=args.expected_version,
            max_batches=args.max_batches,
        )
    if args.command == "reconcile":
        return engine.reconcile()
    if args.command == "status":
        return engine.final_summary()
    if args.command == "version":
        return engine.version_info()
    if args.command == "state":
        return engine.get_state(args.batch, expected_version=args.expected_version)
    if args.command == "curve":
        return engine.get_curve(args.batch, expected_version=args.expected_version)
    if args.command == "verdict":
        return engine.get_verdict(args.batch, expected_version=args.expected_version)
    if args.command == "errors":
        return engine.get_error_report(
            args.batch, expected_version=args.expected_version
        )
    if args.command == "request-stop":
        return engine.request_stop({"reason": args.reason})
    if args.command == "batches":
        return {
            "batch_count": len(engine.manifest.batches),
            "batches": [
                {
                    "spec": spec.to_dict(),
                    "input_digest": engine.manifest.input_digest(spec),
                }
                for spec in engine.manifest.batches
            ],
        }
    raise BusinessError("E_INVALID_STAGE", f"unknown command: {args.command}")


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        result = _run_command(args)
        if args.command != "serve":
            _emit(result)
        return 0
    except BusinessError as exc:
        print(
            json.dumps(
                {"ok": False, "error": exc.to_dict()},
                ensure_ascii=False,
                allow_nan=False,
                indent=2,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
