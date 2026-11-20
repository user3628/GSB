"""Command-line interface for the deterministic regularized workbench."""

from __future__ import annotations

import argparse
import json
import signal
import sys
from pathlib import Path
from typing import Any

from .api import create_server
from .canonical import canonical_json
from .errors import WorkbenchError
from .samples import generate_samples
from .workbench import Workbench


def _print(value: Any) -> None:
    print(json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2))


def _read_json_file(path: str) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as handle:
        value = json.load(handle)
    if not isinstance(value, dict):
        raise ValueError("Config file must contain a JSON object")
    return value


def _number_list(value: str) -> list[float]:
    return [float(item.strip()) for item in value.split(",") if item.strip()]


def _patch_from_args(args: argparse.Namespace) -> dict[str, Any]:
    patch: dict[str, Any] = {}
    if getattr(args, "config_json", None):
        patch = _read_json_file(args.config_json)
    if getattr(args, "dataset", None):
        patch["dataset_id"] = args.dataset
    preprocessing: dict[str, Any] = {}
    for attr, key in (
        ("normalization", "numeric_normalization"),
        ("numeric_imputation", "numeric_imputation"),
        ("encoding", "categorical_encoding"),
        ("categorical_imputation", "categorical_imputation"),
        ("smoothing", "target_encoding_smoothing"),
    ):
        value = getattr(args, attr, None)
        if value is not None:
            preprocessing[key] = value
    if preprocessing:
        patch["preprocessing"] = {**patch.get("preprocessing", {}), **preprocessing}
    cv: dict[str, Any] = {}
    for attr in ("folds", "seed", "metric"):
        if getattr(args, attr, None) is not None:
            cv[attr] = getattr(args, attr)
    if getattr(args, "unstratified", False):
        cv["stratify"] = False
    if cv:
        patch["cv"] = {**patch.get("cv", {}), **cv}
    regularization: dict[str, Any] = {}
    if getattr(args, "mode", None):
        regularization["mode"] = args.mode
    if getattr(args, "strength", None):
        regularization["strengths"] = _number_list(args.strength)
    if getattr(args, "l1_ratio", None):
        regularization["l1_ratios"] = _number_list(args.l1_ratio)
    if regularization:
        patch["regularization"] = {**patch.get("regularization", {}), **regularization}
    solver: dict[str, Any] = {}
    for attr in ("solver", "tolerance", "max_iter", "objective_limit"):
        value = getattr(args, attr, None)
        if value is not None:
            solver[attr] = value
    if solver:
        patch["solver"] = {**patch.get("solver", {}), **solver}
    return patch


def _add_common(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--workspace", default="runtime", help="writable workspace directory")
    parser.add_argument("--data-dir", default=None, help="directory containing dataset_declaration.json")
    parser.add_argument("--session", default="default", help="session id")


def _add_config_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--config-json", help="partial or full JSON config patch")
    parser.add_argument("--dataset", help="dataset id")
    parser.add_argument("--normalization", choices=["standard", "minmax", "none"])
    parser.add_argument("--numeric-imputation", choices=["mean", "median", "zero"])
    parser.add_argument("--encoding", choices=["onehot", "target"])
    parser.add_argument("--categorical-imputation", choices=["mode", "missing_token"])
    parser.add_argument("--smoothing", type=float)
    parser.add_argument("--folds", type=int)
    parser.add_argument("--seed", type=int)
    parser.add_argument("--unstratified", action="store_true")
    parser.add_argument("--metric", choices=["log_loss", "accuracy"])
    parser.add_argument("--mode", choices=["l1", "l2", "elasticnet"])
    parser.add_argument("--strength", help="comma-separated regularization strengths")
    parser.add_argument("--l1-ratio", dest="l1_ratio", help="comma-separated L1 ratios")
    parser.add_argument(
        "--solver", choices=["coordinate_descent", "proximal_gradient"]
    )
    parser.add_argument("--tolerance", type=float)
    parser.add_argument("--max-iter", type=int)
    parser.add_argument("--objective-limit", type=float)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m linreg_workbench",
        description="Deterministic L1/L2/elastic-net logistic regression workbench.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    serve = sub.add_parser("serve", help="start the local interactive HTTP application")
    _add_common(serve)
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8765)

    gen = sub.add_parser("generate-data", help="regenerate the deterministic CSV samples")
    gen.add_argument("--output", default="data")

    datasets = sub.add_parser("datasets", help="list registered datasets")
    _add_common(datasets)

    importer = sub.add_parser("import-data", help="import a local CSV and checksum it")
    _add_common(importer)
    importer.add_argument("--csv", required=True, help="path to the local CSV file")
    importer.add_argument("--dataset-id", required=True)
    importer.add_argument("--target", help="target column (default: label/target/last)")
    importer.add_argument("--features-json", help="optional feature declaration JSON array")
    importer.add_argument("--override", action="store_true")

    train = sub.add_parser("train", help="start a new staged training run")
    _add_common(train)
    _add_config_options(train)
    train.add_argument(
        "--stop-after-task",
        type=int,
        default=None,
        help="deterministic demo hook: request stop after this many completed solve tasks",
    )

    resume = sub.add_parser("resume", help="resume the active run from its last checkpoint")
    _add_common(resume)
    resume.add_argument("--stop-after-task", type=int, default=None)

    stop = sub.add_parser("stop", help="request graceful stop of the active run")
    _add_common(stop)

    for name, help_text in (
        ("status", "show run, stage and checkpoint status"),
        ("folds", "show per-fold solve results"),
        ("fingerprints", "show stage and task fingerprints"),
        ("invalidations", "show version invalidation reasons"),
        ("checkpoints", "show valid checkpoints and ignored records"),
        ("summary", "show the fixed run summary"),
    ):
        command = sub.add_parser(name, help=help_text)
        _add_common(command)

    export = sub.add_parser("export", help="export coefficients or a solve trajectory")
    _add_common(export)
    export.add_argument(
        "--kind", choices=["coefficients", "trajectory"], default="coefficients"
    )
    export.add_argument("--task-index", type=int, default=None)
    export.add_argument("--output", help="write JSON to a file instead of stdout")

    demo = sub.add_parser("demo", help="run the deterministic end-to-end demonstration")
    demo.add_argument("--workspace", default="runtime/demo")
    demo.add_argument("--data-dir", default="data")
    demo.add_argument("--write-expected", action="store_true")

    verify = sub.add_parser("verify-demo", help="re-run and compare fixed summaries")
    verify.add_argument("--workspace", default=None)
    verify.add_argument("--data-dir", default="data")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "generate-data":
            declaration = generate_samples(Path(args.output))
            _print(
                {
                    "generated": [
                        {
                            "dataset_id": item["dataset_id"],
                            "rows": item["rows"],
                            "columns": item["columns"],
                            "sha256": item["sha256"],
                        }
                        for item in declaration["datasets"]
                    ]
                }
            )
            return 0

        if args.command == "demo":
            from .demo import run_demo

            result = run_demo(
                workspace=args.workspace,
                data_dir=args.data_dir,
                write_expected=args.write_expected,
            )
            _print(result)
            return 0 if result.get("ok") else 1

        if args.command == "verify-demo":
            from .demo import verify_demo

            result = verify_demo(
                workspace=args.workspace,
                data_dir=args.data_dir,
            )
            _print(result)
            return 0 if result.get("ok") else 1

        wb = Workbench(args.workspace, data_dir=args.data_dir)
        if args.command == "serve":
            server = create_server(
                args.workspace,
                data_dir=args.data_dir,
                host=args.host,
                port=args.port,
            )
            host, port = server.server_address[:2]
            print(f"Workbench listening on http://{host}:{port}", flush=True)
            try:
                server.serve_forever()
            except KeyboardInterrupt:
                print("Shutting down HTTP server.", flush=True)
            finally:
                server.server_close()
            return 0
        if args.command == "datasets":
            _print({"datasets": wb.datasets()})
            return 0
        if args.command == "import-data":
            features = None
            if args.features_json:
                features = json.loads(Path(args.features_json).read_text("utf-8"))
                if not isinstance(features, list):
                    raise ValueError("features-json must contain an array")
            dataset = wb.import_dataset(
                args.csv,
                args.dataset_id,
                target=args.target,
                features=features,
                override=args.override,
            )
            _print({"imported": dataset})
            return 0
        if args.command == "train":
            patch = _patch_from_args(args)
            previous_stop = None

            def request_stop(_signum: int, _frame: Any) -> None:
                nonlocal previous_stop
                if previous_stop is not None:
                    raise KeyboardInterrupt
                previous_stop = True
                try:
                    wb.request_stop(args.session)
                    print("Graceful stop requested; finishing the current solver round.", file=sys.stderr)
                except WorkbenchError:
                    pass

            old_int = signal.signal(signal.SIGINT, request_stop)
            old_term = signal.signal(signal.SIGTERM, request_stop)
            try:
                outcome = wb.start_training(
                    args.session,
                    config_patch=patch,
                    stop_after_tasks=args.stop_after_task,
                    background=False,
                )
            finally:
                signal.signal(signal.SIGINT, old_int)
                signal.signal(signal.SIGTERM, old_term)
            _print({"outcome": outcome, "status": wb.status(args.session)})
            return 0 if outcome.get("status") in {"complete", "stopped"} else 1
        if args.command == "resume":
            outcome = wb.resume(
                args.session,
                stop_after_tasks=args.stop_after_task,
                background=False,
            )
            _print({"outcome": outcome, "status": wb.status(args.session)})
            return 0 if outcome.get("status") in {"complete", "stopped"} else 1
        if args.command == "stop":
            _print(wb.request_stop(args.session))
            return 0
        if args.command == "status":
            _print(wb.status(args.session))
            return 0
        if args.command == "folds":
            _print(wb.folds(args.session))
            return 0
        if args.command == "fingerprints":
            _print(wb.fingerprints(args.session))
            return 0
        if args.command == "invalidations":
            _print(wb.invalidations(args.session))
            return 0
        if args.command == "checkpoints":
            _print(wb.checkpoints(args.session))
            return 0
        if args.command == "summary":
            _print(wb.summary(args.session))
            return 0
        if args.command == "export":
            if args.kind == "coefficients":
                value = wb.result(args.session)
            else:
                value = wb.trajectory(args.session, task_index=args.task_index)
            payload = (
                json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
            )
            if args.output:
                Path(args.output).write_text(payload, encoding="utf-8")
                _print({"written": str(Path(args.output).resolve())})
            else:
                sys.stdout.write(payload)
            return 0
    except WorkbenchError as exc:
        _print({"ok": False, "error": exc.to_dict()})
        return 2
    except (OSError, ValueError) as exc:
        _print({"ok": False, "error": {"code": "E_INVALID_INPUT", "message": str(exc)}})
        return 2
    return 0
