"""Command line interface for the local material generator."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Sequence

from .config import load_config
from .errors import E_IO, MatgenError
from .pipeline import (
    clear_stop,
    generate_all,
    get_error_report,
    get_status,
    get_versions,
    load_recipes,
    request_stop,
)


def _add_workspace_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--workspace", default=".", help="workspace root (default: current directory)")


def _add_generation_arguments(parser: argparse.ArgumentParser) -> None:
    _add_workspace_argument(parser)
    parser.add_argument("--recipes", help="recipe JSON path (default: WORKSPACE/samples/recipes.json)")
    parser.add_argument("--cards", help="color-card manifest path (default: WORKSPACE/samples/color_cards.json)")
    parser.add_argument("--config", help="configuration JSON path (default: WORKSPACE/samples/config.json if present)")
    parser.add_argument("--output", help="output directory (default: WORKSPACE/out)")
    parser.add_argument(
        "--stop-after",
        type=int,
        default=None,
        metavar="N",
        help="request a graceful stop after N complete material units (test/demo aid)",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="matgen", description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    generate = subparsers.add_parser("generate", help="generate all material recipes")
    _add_generation_arguments(generate)

    resume = subparsers.add_parser("resume", help="clear a stop request and continue generation")
    _add_generation_arguments(resume)

    demo = subparsers.add_parser("demo", help="run the bundled deterministic sample")
    _add_generation_arguments(demo)

    stop = subparsers.add_parser("stop", help="request a graceful stop")
    _add_workspace_argument(stop)

    status = subparsers.add_parser("status", help="show checkpoint and version state")
    _add_workspace_argument(status)

    report = subparsers.add_parser("report", help="print one material error report")
    _add_workspace_argument(report)
    report.add_argument("--material-id", required=True)

    versions = subparsers.add_parser("versions", help="print algorithm and stage configuration versions")
    _add_workspace_argument(versions)
    versions.add_argument("--config", help="configuration JSON path")

    validate = subparsers.add_parser("validate-recipes", help="import and validate a recipe list")
    _add_workspace_argument(validate)
    validate.add_argument("--recipes", help="recipe JSON path")

    return parser


def _resolve_path(workspace: Path, requested: str | None, default_relative: str) -> Path:
    if requested is not None:
        candidate = Path(requested)
        return candidate if candidate.is_absolute() else (Path.cwd() / candidate).resolve()
    return workspace / default_relative


def _resolve_generation_paths(args: argparse.Namespace) -> tuple[Path, Path, Path | None, Path | None, Path]:
    workspace = Path(args.workspace).resolve()
    recipes = _resolve_path(workspace, args.recipes, "samples/recipes.json")
    cards_candidate = _resolve_path(workspace, args.cards, "samples/color_cards.json")
    cards: Path | None = cards_candidate if cards_candidate.exists() else None
    config_candidate = _resolve_path(workspace, args.config, "samples/config.json")
    config: Path | None = config_candidate if config_candidate.exists() else None
    output = _resolve_path(workspace, args.output, "out")
    return workspace, recipes, cards, config, output


def _run_generation(args: argparse.Namespace, *, resume: bool) -> dict[str, Any]:
    workspace, recipes, cards, config_path, output = _resolve_generation_paths(args)
    if resume:
        clear_stop(workspace)
    config = load_config(config_path)
    result = generate_all(
        workspace=workspace,
        recipes_path=recipes,
        cards_path=cards,
        config=config,
        output_root=output,
        stop_after_materials=args.stop_after,
    )
    return {
        "status": result["status"],
        "run_digest": result.get("run_digest"),
        "completed_material_ids": result["completed_material_ids"],
        "output_root": str(output),
        "reason": result.get("reason"),
    }


def _print_json(value: Any) -> None:
    sys.stdout.write(
        json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False) + "\n"
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "generate":
            _print_json(_run_generation(args, resume=False))
            return 0
        if args.command == "resume":
            _print_json(_run_generation(args, resume=True))
            return 0
        if args.command == "demo":
            _print_json(_run_generation(args, resume=False))
            return 0
        if args.command == "stop":
            workspace = Path(args.workspace).resolve()
            request_stop(workspace)
            _print_json({"status": "stop_requested", "workspace": str(workspace)})
            return 0
        if args.command == "status":
            _print_json(get_status(Path(args.workspace).resolve()))
            return 0
        if args.command == "report":
            workspace = Path(args.workspace).resolve()
            _print_json(get_error_report(workspace, args.material_id))
            return 0
        if args.command == "versions":
            workspace = Path(args.workspace).resolve()
            config_path = _resolve_path(workspace, args.config, "samples/config.json")
            config = load_config(config_path if config_path.exists() else None)
            _print_json(get_versions(config))
            return 0
        if args.command == "validate-recipes":
            workspace = Path(args.workspace).resolve()
            recipes_path = _resolve_path(workspace, args.recipes, "samples/recipes.json")
            recipes = load_recipes(recipes_path)
            _print_json(
                {
                    "status": "valid",
                    "material_count": len(recipes),
                    "material_ids": [recipe["material_id"] for recipe in recipes],
                }
            )
            return 0
    except MatgenError as exc:
        sys.stderr.write(
            json.dumps(exc.to_dict(), ensure_ascii=False, sort_keys=True, indent=2) + "\n"
        )
        return 2
    except OSError as exc:
        error = MatgenError(code=E_IO, message="filesystem operation failed", details={"reason": str(exc)})
        sys.stderr.write(json.dumps(error.to_dict(), ensure_ascii=False, sort_keys=True) + "\n")
        return 2
    parser.error("unknown command")
    return 2


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

