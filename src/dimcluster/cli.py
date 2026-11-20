from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from .config import load_config
from .envcheck import environment_check
from .errors import WorkbenchError
from .pipeline import Workbench
from .samples import generate_samples, verify_samples
from .state import StateStore
from .util import canonical_json_text


def _emit(value: Any, pretty: bool = True) -> None:
    print(canonical_json_text(value, pretty=pretty))


def _add_config_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--config", type=Path, help="JSON config overlay")
    parser.add_argument(
        "--set",
        dest="overrides",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="Dotted config override, JSON-decoded when possible",
    )


def _config_from_args(
    args: argparse.Namespace,
    use_active_state_config: bool = True,
) -> dict[str, Any]:
    path = args.config
    if path is None and use_active_state_config:
        active = Path(args.root).resolve() / "state" / "active_config.json"
        if active.exists():
            path = active
    return load_config(path, args.overrides)


def _workbench(args: argparse.Namespace, config: dict[str, Any] | None = None) -> Workbench:
    return Workbench(
        Path(args.root),
        config if config is not None else _config_from_args(args),
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="dimcluster",
        description="Local deterministic tabular reduction/clustering diagnostics workbench",
    )
    parser.add_argument("--root", default=".", help="Workspace root (default: cwd)")
    sub = parser.add_subparsers(dest="command", required=True)

    generate = sub.add_parser("generate-samples", help="Generate the three deterministic CSV tiers")
    generate.add_argument("--samples-dir", type=Path, default=None)
    generate.add_argument("--seed", type=int, default=20261120)
    generate.add_argument("--pretty", action="store_true")

    verify_samples_parser = sub.add_parser("verify-samples", help="Verify CSV row/column counts and digests")
    verify_samples_parser.add_argument("--manifest", type=Path, default=None)
    verify_samples_parser.add_argument("--pretty", action="store_true")

    env = sub.add_parser("env-check", help="Check writable paths, atomic replace, file lock, and loopback bind")
    env.add_argument("--pretty", action="store_true")

    validate = sub.add_parser("validate-config", help="Validate config and print its fingerprint")
    _add_config_args(validate)
    validate.add_argument("--pretty", action="store_true")

    run = sub.add_parser("run", help="Run/reuse the staged pipeline")
    _add_config_args(run)
    run.add_argument("--mode", choices=["auto", "full"], default="auto")
    run.add_argument("--reset", action="store_true")
    run.add_argument("--verify-full", action="store_true")
    run.add_argument("--stop-after-tier", default=None, help="Deterministic stop after a tier unit")
    run.add_argument("--pretty", action="store_true")

    resume = sub.add_parser("resume", help="Clear the stop request and continue incrementally")
    _add_config_args(resume)
    resume.add_argument("--verify-full", action="store_true")
    resume.add_argument("--pretty", action="store_true")

    status = sub.add_parser("status", help="Show stage status and sequence head")
    _add_config_args(status)
    status.add_argument("--verify", action="store_true")
    status.add_argument("--pretty", action="store_true")

    fingerprints = sub.add_parser("fingerprints", help="Show config/input/checkpoint fingerprints")
    _add_config_args(fingerprints)
    fingerprints.add_argument("--pretty", action="store_true")

    rationale = sub.add_parser("rationale", help="Show reduction/clustering rationale for a tier")
    _add_config_args(rationale)
    rationale.add_argument("--tier", required=True)
    rationale.add_argument("--pretty", action="store_true")

    report = sub.add_parser("report", help="Print the diagnosis report")
    _add_config_args(report)
    report.add_argument("--pretty", action="store_true")

    stop = sub.add_parser("stop", help="Request a graceful stop after the current tier unit")
    _add_config_args(stop)
    stop.add_argument("--reason", default="cli_requested")
    stop.add_argument("--pretty", action="store_true")

    integrity = sub.add_parser("verify-integrity", help="Verify checkpoint/ledger/artifact integrity")
    _add_config_args(integrity)
    integrity.add_argument("--pretty", action="store_true")

    import_parser = sub.add_parser("import-samples", help="Verify and import a local manifest")
    _add_config_args(import_parser)
    import_parser.add_argument("--manifest", type=Path, default=None)
    import_parser.add_argument("--pretty", action="store_true")

    serve = sub.add_parser("serve", help="Start the loopback HTTP workbench")
    _add_config_args(serve)
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=0)
    serve.add_argument("--pretty", action="store_true")

    demo = sub.add_parser("demo", help="Run deterministic three-tier stop/resume/incremental/invalid-config demo")
    _add_config_args(demo)
    demo.add_argument("--pretty", action="store_true")

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        root = Path(args.root).resolve()
        if args.command == "generate-samples":
            samples_dir = args.samples_dir or (root / "samples")
            summary = generate_samples(samples_dir, seed=args.seed)
            _emit({"ok": True, "samples_dir": str(samples_dir), "summary": summary}, pretty=args.pretty)
            return 0
        if args.command == "verify-samples":
            manifest = args.manifest or (root / "samples" / "manifest.json")
            _emit(verify_samples(manifest), pretty=args.pretty)
            return 0
        if args.command == "env-check":
            result = environment_check(root)
            _emit(result, pretty=args.pretty)
            return 0 if result["ok"] else 2
        if args.command == "validate-config":
            config = _config_from_args(args, use_active_state_config=False)
            from .config import config_fingerprint

            _emit({"ok": True, "config_sha256": config_fingerprint(config), "config": config}, pretty=args.pretty)
            return 0
        if args.command == "serve":
            from .server import serve_forever

            config = _config_from_args(args)
            return serve_forever(
                root=root,
                config=config,
                host=args.host,
                port=args.port,
                pretty=args.pretty,
            )
        if args.command == "demo":
            from .demo import run_demo

            config = _config_from_args(args, use_active_state_config=False)
            result = run_demo(root, config)
            _emit(result, pretty=args.pretty)
            return 0 if result.get("ok") else 2

        config = _config_from_args(args)
        workbench = Workbench(root, config)
        if args.command == "run":
            result = workbench.run(
                mode=args.mode,
                reset=args.reset,
                verify_full=args.verify_full,
                stop_after_tier=args.stop_after_tier,
            )
            _emit(result, pretty=args.pretty)
            return 0
        if args.command == "resume":
            result = workbench.run(mode="auto", verify_full=args.verify_full)
            _emit(result, pretty=args.pretty)
            return 0
        if args.command == "status":
            result = workbench.state.status()
            if args.verify:
                result["integrity"] = workbench.state.verify_integrity()
            _emit(result, pretty=args.pretty)
            return 0
        if args.command == "fingerprints":
            _emit(workbench.fingerprints(), pretty=args.pretty)
            return 0
        if args.command == "rationale":
            _emit(workbench.rationale(args.tier), pretty=args.pretty)
            return 0
        if args.command == "report":
            report = workbench.report()
            _emit(report, pretty=args.pretty)
            return 0
        if args.command == "stop":
            del workbench
            store = StateStore(root / "state", root=root)
            _emit(store.request_stop(args.reason), pretty=args.pretty)
            return 0
        if args.command == "verify-integrity":
            result = workbench.state.verify_integrity()
            _emit(result, pretty=args.pretty)
            return 0 if result["ok"] else 3
        if args.command == "import-samples":
            _emit(workbench.import_samples(args.manifest), pretty=args.pretty)
            return 0
        parser.error(f"Unknown command: {args.command}")
        return 2
    except WorkbenchError as exc:
        _emit(exc.to_dict(), pretty=True)
        return 1 if exc.http_status < 500 else 2
    except FileNotFoundError as exc:
        _emit(
            {
                "ok": False,
                "error": {"code": "E_INVALID_STAGE", "message": str(exc), "details": {}},
            },
            pretty=True,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
