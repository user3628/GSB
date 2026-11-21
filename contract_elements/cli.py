from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

from .canonical import atomic_write_bytes, atomic_write_json, atomic_write_jsonl, canonical_json, read_json
from .config import config_fingerprint, load_config
from .errors import BusinessError
from .interop import V1Interop, load_v1
from .normalization import ClauseNormalizer
from .pipeline import BatchEngine, DatasetPaths


def _paths(args: argparse.Namespace) -> DatasetPaths:
    return DatasetPaths(
        input_path=Path(args.input),
        seed_path=Path(args.seeds),
        dictionary_path=Path(args.dictionary),
        v1_path=Path(args.v1),
        gold_path=Path(args.gold),
        output_dir=Path(args.output),
        runtime_dir=Path(args.runtime),
    )


def _add_data_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--config", default="config/default.json")
    parser.add_argument("--input", default="data/contracts.jsonl")
    parser.add_argument("--seeds", default="data/seeds.jsonl")
    parser.add_argument("--dictionary", default="data/elements.json")
    parser.add_argument("--v1", default="data/annotations_v1.jsonl")
    parser.add_argument("--gold", default="data/gold_conflicts.json")
    parser.add_argument("--output", default="output/default")
    parser.add_argument("--runtime", default="runtime/default")


def _print(value: Any) -> None:
    sys.stdout.write(canonical_json(value) + "\n")


def _engine(args: argparse.Namespace) -> BatchEngine:
    config = load_config(args.config)
    return BatchEngine(config, _paths(args), injection_path=getattr(args, "injections", None))


def command_batch(args: argparse.Namespace) -> int:
    engine = _engine(args)
    result = engine.run(
        resume=bool(getattr(args, "resume", False)),
        stop_after_units=getattr(args, "stop_after_units", None),
        clear_stop=bool(getattr(args, "resume", False)),
    )
    _print(result)
    return 0


def command_parse(args: argparse.Namespace) -> int:
    result = _engine(args).parse_contract(args.contract_id)
    _print(result)
    return 0


def command_candidates(args: argparse.Namespace) -> int:
    result = _engine(args).parse_contract(args.contract_id)
    _print({"contract_id": args.contract_id, "candidates": result["candidates"]})
    return 0


def command_conflicts(args: argparse.Namespace) -> int:
    result = _engine(args).parse_contract(args.contract_id)
    _print({"contract_id": args.contract_id, "conflicts": result["conflicts"]})
    return 0


def command_version(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    runtime = Path(args.runtime)
    state_path = runtime / "version_state.json"
    progress_path = runtime / "progress.json"
    state = read_json(state_path) if state_path.exists() else None
    progress = read_json(progress_path) if progress_path.exists() else None
    _print(
        {
            "config_fingerprint": config_fingerprint(config),
            "runtime": str(runtime),
            "status": "initialized" if state is not None else "uninitialized",
            "version_state": state,
            "progress": progress,
        }
    )
    return 0


def command_stop(args: argparse.Namespace) -> int:
    runtime = Path(args.runtime)
    runtime.mkdir(parents=True, exist_ok=True)
    atomic_write_bytes(runtime / "STOP", b"stop\n")
    _print({"status": "stop_requested", "runtime": str(runtime), "stop_file": str(runtime / "STOP")})
    return 0


def command_interop_import(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    interop = V1Interop(config, ClauseNormalizer(config))
    rows = load_v1(args.source)
    structure = interop.import_rows(rows)
    exported = interop.export_rows(structure)
    compatibility = interop.compatibility(rows)
    atomic_write_json(args.internal_output, structure)
    atomic_write_jsonl(args.output, exported)
    _print(
        {
            "status": "ok" if compatibility["success"] else "failed",
            "source": args.source,
            "v1_output": args.output,
            "internal_output": args.internal_output,
            "compatibility": compatibility,
        }
    )
    return 0 if compatibility["success"] else 1


def command_interop_export(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    interop = V1Interop(config, ClauseNormalizer(config))
    structure = read_json(args.internal_input)
    exported = interop.export_rows(structure)
    atomic_write_jsonl(args.output, exported)
    _print({"status": "ok", "v1_output": args.output, "records": len(exported)})
    return 0


def command_serve(args: argparse.Namespace) -> int:
    from .service import run_server

    config = load_config(args.config)
    run_server(
        config,
        _paths(args),
        host=args.host or config["service"]["host"],
        port=args.port or int(config["service"]["port"]),
        injection_path=args.injections,
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="contract-elements", description="Deterministic contract element extraction")
    subparsers = parser.add_subparsers(dest="command", required=True)

    batch = subparsers.add_parser("batch", help="run deterministic batch extraction and arbitration")
    _add_data_arguments(batch)
    batch.add_argument("--injections")
    batch.add_argument("--resume", action="store_true")
    batch.add_argument("--stop-after-units", type=int)
    batch.set_defaults(func=command_batch)

    experiment = subparsers.add_parser("experiment", help="run a reproducible batch experiment")
    _add_data_arguments(experiment)
    experiment.add_argument("--injections")
    experiment.set_defaults(func=command_batch, resume=False, stop_after_units=None)

    for name, function, help_text in [
        ("parse", command_parse, "parse one contract and show all diagnostics"),
        ("candidates", command_candidates, "show candidates for one contract"),
        ("conflicts", command_conflicts, "show conflict diagnostics for one contract"),
    ]:
        command = subparsers.add_parser(name, help=help_text)
        _add_data_arguments(command)
        command.add_argument("--contract-id", required=True)
        command.add_argument("--injections")
        command.set_defaults(func=function)

    version = subparsers.add_parser("version", help="query version fingerprints and invalidation reasons")
    version.add_argument("--config", default="config/default.json")
    version.add_argument("--runtime", default="runtime/default")
    version.set_defaults(func=command_version)

    stop = subparsers.add_parser("stop", help="request cooperative stop of a running batch")
    stop.add_argument("--runtime", default="runtime/default")
    stop.set_defaults(func=command_stop)

    resume = subparsers.add_parser("resume", help="resume a stopped batch from its atomic checkpoint")
    _add_data_arguments(resume)
    resume.add_argument("--injections")
    resume.set_defaults(func=command_batch, resume=True, stop_after_units=None)

    interop_import = subparsers.add_parser("import-v1", help="import structure version 1 annotations and export canonical v1")
    interop_import.add_argument("--config", default="config/default.json")
    interop_import.add_argument("--source", default="data/annotations_v1.jsonl")
    interop_import.add_argument("--output", default="output/interop/annotations_v1.export.jsonl")
    interop_import.add_argument("--internal-output", default="output/interop/internal_v2.json")
    interop_import.set_defaults(func=command_interop_import)

    interop_export = subparsers.add_parser("export-v1", help="export an internal conversion structure back to structure version 1")
    interop_export.add_argument("--config", default="config/default.json")
    interop_export.add_argument("--internal-input", default="output/interop/internal_v2.json")
    interop_export.add_argument("--output", default="output/interop/annotations_v1.export.jsonl")
    interop_export.set_defaults(func=command_interop_export)

    serve = subparsers.add_parser("serve", help="start the local HTTP service")
    _add_data_arguments(serve)
    serve.add_argument("--injections")
    serve.add_argument("--host")
    serve.add_argument("--port", type=int)
    serve.set_defaults(func=command_serve)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except BusinessError as error:
        _print({"status": "error", "error": error.to_dict()})
        return 2
    except (ValueError, OSError, KeyError) as error:
        _print({"status": "error", "error": {"code": "E_INTEROP", "message": str(error)}})
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

