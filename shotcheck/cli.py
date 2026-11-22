from __future__ import annotations

import argparse
import json
import signal
import sys
from pathlib import Path
from typing import Any

from .checkpoints import CheckpointStore
from .errors import BusinessError
from .generator import generate_all
from .pipeline import GracefulStop, Pipeline, StopController, load_inputs
from .server import serve
from .util import canonical_bytes, read_json
from .verification import verify_all
from .version import PIPELINE_VERSION


def _print_json(value: Any) -> None:
    sys.stdout.buffer.write(canonical_bytes(value))


def _cmd_doctor(_args: argparse.Namespace) -> int:
    result: dict[str, Any] = {
        "ok": True,
        "pipeline_version": PIPELINE_VERSION,
        "python_version": sys.version.split()[0],
        "python_supported": sys.version_info >= (3, 10),
    }
    try:
        import numpy

        result["numpy_version"] = numpy.__version__
        result["numpy_importable"] = True
    except Exception as exc:
        result["numpy_importable"] = False
        result["numpy_error"] = str(exc)
        result["ok"] = False
    try:
        import fcntl  # noqa: F401

        result["file_lock"] = "fcntl.flock"
    except Exception as exc:
        result["file_lock"] = None
        result["file_lock_error"] = str(exc)
        result["ok"] = False
    if not result["python_supported"]:
        result["ok"] = False
    _print_json(result)
    return 0 if result["ok"] else 1


def _cmd_generate(args: argparse.Namespace) -> int:
    summary = generate_all(args.root, seed=args.seed)
    _print_json({"ok": True, "root": str(Path(args.root)), "generation": summary})
    return 0


def _cmd_run(args: argparse.Namespace) -> int:
    controller = StopController()
    if args.stop_after_stage or args.stop_after_shots is not None:
        controller.configure_target(args.stop_after_stage, args.stop_after_shots)
    previous_sigint = None

    def request_stop(_signum: int, _frame: Any) -> None:
        controller.request()

    try:
        previous_sigint = signal.signal(signal.SIGINT, request_stop)
    except (ValueError, OSError):
        previous_sigint = None
    try:
        run = Pipeline(args.workspace, stop_controller=controller).run(force=args.force)
    except GracefulStop as exc:
        _print_json(
            {
                "ok": True,
                "status": "stopped",
                "stage": exc.stage,
                "completed_shots": exc.completed_shots,
                "shot_id": exc.shot_id,
            }
        )
        return 130
    except BusinessError as exc:
        _print_json({"ok": False, "error": exc.to_dict()})
        return 2
    finally:
        if previous_sigint is not None:
            try:
                signal.signal(signal.SIGINT, previous_sigint)
            except (ValueError, OSError):
                pass
    _print_json(
        {
            "ok": True,
            "status": "complete",
            "workspace": str(Path(args.workspace)),
            "result": str(Path(args.workspace) / "exports/result.json"),
            "run_meta": run["run_meta"],
            "summary": run["result"]["summary"],
        }
    )
    return 0


def _cmd_status(args: argparse.Namespace) -> int:
    workspace = Path(args.workspace)
    store = CheckpointStore(workspace)
    manifest_path = workspace / "inputs/manifest.json"
    result_path = workspace / "exports/result.json"
    _print_json(
        {
            "ok": True,
            "workspace": str(workspace),
            "checkpoints": store.status(),
            "input_manifest": read_json(manifest_path) if manifest_path.exists() else None,
            "result_exists": result_path.exists(),
        }
    )
    return 0


def _cmd_verify(args: argparse.Namespace) -> int:
    report = verify_all(args.root, seed=args.seed)
    _print_json(
        {
            "ok": report["all_passed"],
            "report": str(Path(args.root) / "verification/report.json"),
            "markdown": str(Path(args.root) / "verification/report.md"),
            "all_passed": report["all_passed"],
            "tiers": {
                tier: {
                    "all_passed": item["all_passed"],
                    "shot_count": item["shot_count"],
                    "summary": item["summary"],
                    "strategy": item["strategy"]["selected"],
                }
                for tier, item in report["tiers"].items()
            },
            "incremental_passed": all(
                check["passed"] for check in report["incremental"]["checks"].values()
            ),
            "resume_passed": all(
                check["passed"] for check in report["resume"]["checks"].values()
            ),
            "checkpoint_concurrency_passed": report["checkpoint_concurrency"][
                "all_passed"
            ],
            "fingerprint_matrix_passed": report["fingerprint_matrix"]["all_passed"],
        }
    )
    return 0 if report["all_passed"] else 1


def _cmd_serve(args: argparse.Namespace) -> int:
    try:
        server = serve(args.workspace, host=args.host, port=args.port)
    except OSError as exc:
        _print_json(
            {
                "ok": False,
                "error": {"code": "E_HTTP_BIND", "message": str(exc)},
            }
        )
        return 2
    host, port = server.server_address[:2]
    print(f"ShotCheck HTTP listening on http://{host}:{port}", file=sys.stderr)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("ShotCheck HTTP stopped", file=sys.stderr)
    finally:
        server.server_close()
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="shotcheck",
        description="Deterministic local storyboard semantic verification pipeline",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    doctor = subparsers.add_parser("doctor", help="check Python, NumPy, and file locks")
    doctor.set_defaults(func=_cmd_doctor)

    generate = subparsers.add_parser("generate", help="generate deterministic samples")
    generate.add_argument("--root", default="runtime")
    generate.add_argument("--seed", type=int, default=20261123)
    generate.set_defaults(func=_cmd_generate)

    run = subparsers.add_parser("run", help="run the four-stage pipeline")
    run.add_argument("--workspace", default="runtime/large")
    run.add_argument("--force", action="store_true", help="recompute all stages")
    run.add_argument(
        "--stop-after-stage",
        choices=["normalize", "duration", "coverage", "strategy"],
    )
    run.add_argument("--stop-after-shots", type=int)
    run.set_defaults(func=_cmd_run)

    status = subparsers.add_parser("status", help="show checkpoint and result status")
    status.add_argument("--workspace", default="runtime/large")
    status.set_defaults(func=_cmd_status)

    verify = subparsers.add_parser("verify", help="run all deterministic verifications")
    verify.add_argument("--root", default="runtime")
    verify.add_argument("--seed", type=int, default=20261123)
    verify.set_defaults(func=_cmd_verify)

    server = subparsers.add_parser("serve", help="start the local JSON HTTP server")
    server.add_argument("--workspace", default="runtime/large")
    server.add_argument("--host", default="127.0.0.1")
    server.add_argument("--port", type=int, default=18765)
    server.set_defaults(func=_cmd_serve)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
