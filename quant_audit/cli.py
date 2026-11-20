"""Command-line interface for the deterministic quantization audit engine."""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any

from .canonical import canonical_json
from .errors import AuditError
from .workspace import Workspace


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m quant_audit",
        description="Deterministic layer-wise quantization audit and calibration rollback engine.",
    )
    parser.add_argument("--pretty", action="store_true", help="pretty-print JSON output")
    subparsers = parser.add_subparsers(dest="command", required=True)

    for command in ("init", "import"):
        init_parser = subparsers.add_parser(command, help="import weights, calibration tensor and YAML config")
        init_parser.add_argument("--workspace", required=True)
        init_parser.add_argument("--weights", required=True)
        init_parser.add_argument("--calibration", required=True)
        init_parser.add_argument("--config", required=True)
        init_parser.add_argument("--force", action="store_true", help="replace imported inputs and reset audit history")

    calibrate = subparsers.add_parser("calibrate", help="run a full or incremental calibration")
    calibrate.add_argument("--workspace", required=True)
    calibrate.add_argument("--mode", choices=("full", "incremental"), default="full")
    calibrate.add_argument("--base-version", type=int, default=None, help="incremental base version (default: active)")
    calibrate.add_argument("--config", default=None, help="override the imported YAML config")
    calibrate.add_argument("--calibration", default=None, help="override the imported calibration NPY")

    query = subparsers.add_parser("query", help="query layer errors, attribution and budget status")
    query.add_argument("--workspace", required=True)
    query.add_argument("--version", type=int, default=None)
    query.add_argument("--layers-only", action="store_true")

    history = subparsers.add_parser("history", help="show version history")
    history.add_argument("--workspace", required=True)

    rollback = subparsers.add_parser("rollback", help="activate the latest or a specified budget-compliant version")
    rollback.add_argument("--workspace", required=True)
    rollback.add_argument("--to-version", type=int, default=None)

    demo = subparsers.add_parser("demo", help="generate deterministic samples and run the complete local demonstration")
    demo.add_argument("--workspace", default="demo_run")
    demo.add_argument("--data-dir", default="demo_data")
    demo.add_argument("--report", default="artifacts/demo_report.json")
    demo.add_argument("--force", action="store_true")
    demo.add_argument("--faults", action="store_true", help="include controlled anomaly-code demonstrations")

    generate = subparsers.add_parser("generate-demo-data", help="generate deterministic NPZ/NPY/config fixtures")
    generate.add_argument("--data-dir", default="demo_data")
    generate.add_argument("--seed", type=int, default=20261120)
    generate.add_argument("--force", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    raw_arguments = list(sys.argv[1:] if argv is None else argv)
    pretty = "--pretty" in raw_arguments
    raw_arguments = [argument for argument in raw_arguments if argument != "--pretty"]
    arguments = parser.parse_args(raw_arguments)
    arguments.pretty = pretty
    try:
        output = dispatch(arguments)
    except AuditError as exc:
        _emit(exc.to_dict(), pretty=arguments.pretty)
        return 2
    except Exception as exc:  # noqa: BLE001  # pragma: no cover - defensive CLI boundary
        _emit(
            {
                "ok": False,
                "error": {
                    "code": "E_UNEXPECTED",
                    "message": f"{type(exc).__name__}: {exc}",
                },
            },
            pretty=arguments.pretty,
        )
        return 3
    _emit({"ok": True, "command": arguments.command, **output}, pretty=arguments.pretty)
    return 0


def dispatch(arguments: argparse.Namespace) -> dict[str, Any]:
    if arguments.command in {"init", "import"}:
        workspace = Workspace.initialize(
            arguments.workspace,
            arguments.weights,
            arguments.calibration,
            arguments.config,
            force=arguments.force,
        )
        return {
            "workspace": str(workspace.root),
            "metadata": workspace.metadata(),
            "latest_version": workspace.store.latest_version(),
        }
    if arguments.command == "demo":
        from .demo import run_demo

        return run_demo(
            workspace_path=arguments.workspace,
            data_dir=arguments.data_dir,
            report_path=arguments.report,
            force=arguments.force,
            include_faults=arguments.faults,
        )
    if arguments.command == "generate-demo-data":
        from .demo import generate_demo_data

        return generate_demo_data(arguments.data_dir, seed=arguments.seed, force=arguments.force)
    workspace = Workspace.open(arguments.workspace)
    if arguments.command == "calibrate":
        result = workspace.calibrate(
            mode=arguments.mode,
            base_version=arguments.base_version,
            config_path=arguments.config,
            calibration_path=arguments.calibration,
        )
        return result
    if arguments.command == "query":
        if arguments.layers_only:
            version = arguments.version
            query = workspace.query(version)
            attribution_by_name = {
                layer["name"]: layer for layer in query["result"]["attribution"]["layers"]
            }
            budget_by_name = {
                layer["name"]: layer for layer in query["result"]["budget"]["per_layer"]
            }
            return {
                "version": query["version"],
                "active_version": query["active_version"],
                "layers": [
                    {
                        "name": layer["name"],
                        "error": layer["error"],
                        "attribution": attribution_by_name[layer["name"]],
                        "budget": budget_by_name[layer["name"]],
                    }
                    for layer in query["result"]["layers"]
                ],
                "attribution_closure": query["result"]["attribution"]["closure"],
                "budget": query["result"]["budget"],
            }
        return workspace.query(arguments.version)
    if arguments.command == "history":
        return workspace.history()
    if arguments.command == "rollback":
        return workspace.rollback(arguments.to_version)
    raise AuditError("E_INVALID_INPUT", f"unsupported command: {arguments.command}")


def _emit(payload: dict[str, Any], pretty: bool = False) -> None:
    if pretty:
        text = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, allow_nan=False)
    else:
        text = canonical_json(payload)
    print(text)


if __name__ == "__main__":
    sys.exit(main())

