"""Command-line interface for the local ToneLab workbench."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

from .config import Config
from .engine import BuildEngine
from .errors import ToneLabError
from .formats import load_manifest, import_project_json, import_wav_bundle
from .util import atomic_write_json, canonical_json_bytes, read_json, sha256_bytes
from .workspace import Workspace


def _print(value: Any) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True))


def _workspace(args: argparse.Namespace) -> Workspace:
    config_path = getattr(args, "config", None)
    if config_path:
        return Workspace(args.workspace, config=Config.load(config_path))
    return Workspace(args.workspace)


def _mutation_from_args(args: argparse.Namespace) -> dict[str, Any]:
    kind = args.mutation
    if kind == "sample":
        mutation: dict[str, Any] = {"kind": "sample", "shard_id": args.shard}
        if args.wav:
            mutation["wav"] = args.wav
        if args.shift is not None:
            mutation["shift"] = args.shift
        return mutation
    if kind == "unit":
        mutation = {"kind": "unit", "shard_id": args.shard, "unit_index": args.unit_index}
        for key in ("start_ms", "end_ms", "symbol"):
            value = getattr(args, key)
            if value is not None:
                mutation[key] = value
        return mutation
    if kind == "upgrade":
        return {"kind": "upgrade", "shard_id": args.shard}
    if kind == "pitch":
        return {
            "kind": "pitch",
            "shard_id": args.shard,
            "point_index": args.point_index,
            "value": args.value,
        }
    raise ToneLabError("E_BAD_REQUEST", "unknown mutation", context={"mutation": kind})


def cmd_init(args: argparse.Namespace) -> dict[str, Any]:
    workspace = _workspace(args)
    manifests = args.manifest or []
    if manifests:
        project = workspace.import_manifests(manifests, project_id=args.project_id)
        return {
            "ok": True,
            "workspace": str(workspace.root),
            "project_id": project.project_id,
            "shards": [
                {
                    "shard_id": shard.shard_id,
                    "source_structure_version": shard.source_structure_version,
                    "sample_count": shard.sample_count,
                    "units": [unit.symbol for unit in shard.units],
                }
                for shard in project.shards
            ],
        }
    if args.project_json:
        project = import_project_json(args.project_json, workspace.config)
        workspace.save_project(project)
    elif args.wav_dir:
        project = import_wav_bundle(args.wav_dir, workspace.config)
        workspace.save_project(project)
    else:
        raise ToneLabError("E_BAD_REQUEST", "init requires --manifest, --project-json, or --wav-dir")
    return {"ok": True, "workspace": str(workspace.root), "project_id": project.project_id, "shards": len(project.shards)}


def cmd_import_wav(args: argparse.Namespace) -> dict[str, Any]:
    workspace = _workspace(args)
    project = workspace.add_wav_shard(args.wav, args.sidecar)
    return {
        "ok": True,
        "workspace": str(workspace.root),
        "project_id": project.project_id,
        "shards": [shard.shard_id for shard in project.shards],
    }


def cmd_import_units(args: argparse.Namespace) -> dict[str, Any]:
    workspace = _workspace(args)
    project = workspace.apply_units_file(args.path)
    return {
        "ok": True,
        "workspace": str(workspace.root),
        "project_id": project.project_id,
        "units": {shard.shard_id: len(shard.units) for shard in project.shards},
    }


def cmd_build(args: argparse.Namespace) -> dict[str, Any]:
    workspace = _workspace(args)
    report = BuildEngine(workspace).build(
        args.mode,
        clear_stop=args.clear_stop,
        until_stage=args.until_stage,
    )
    if args.summary_out:
        atomic_write_json(args.summary_out, report)
    return report


def cmd_stage(args: argparse.Namespace) -> dict[str, Any]:
    workspace = _workspace(args)
    report = BuildEngine(workspace).run_stage(args.stage, args.mode)
    if args.summary_out:
        atomic_write_json(args.summary_out, report)
    return report


def cmd_status(args: argparse.Namespace) -> dict[str, Any]:
    return BuildEngine(_workspace(args)).status()


def cmd_fingerprints(args: argparse.Namespace) -> dict[str, Any]:
    return BuildEngine(_workspace(args)).fingerprints()


def cmd_checkpoints(args: argparse.Namespace) -> dict[str, Any]:
    return BuildEngine(_workspace(args)).checkpoints()


def cmd_edit_unit(args: argparse.Namespace) -> dict[str, Any]:
    if (
        args.start_ms is None
        and args.end_ms is None
        and args.symbol is None
        and args.formants is None
        and args.formant_ranges is None
    ):
        raise ToneLabError("E_BAD_REQUEST", "edit-unit requires at least one edited field")
    formants = json.loads(args.formants) if args.formants else None
    formant_ranges = json.loads(args.formant_ranges) if args.formant_ranges else None
    return BuildEngine(_workspace(args)).edit_unit(
        args.shard,
        args.unit_index,
        start_ms=args.start_ms,
        end_ms=args.end_ms,
        symbol=args.symbol,
        formants_hz=formants,
        formant_ranges_hz=formant_ranges,
    )


def cmd_edit_pitch(args: argparse.Namespace) -> dict[str, Any]:
    return BuildEngine(_workspace(args)).edit_pitch_point(
        args.shard,
        args.point_index,
        time=args.time,
        value=args.value,
    )


def cmd_replace_sample(args: argparse.Namespace) -> dict[str, Any]:
    return BuildEngine(_workspace(args)).replace_sample(args.shard, args.wav)


def cmd_upgrade(args: argparse.Namespace) -> dict[str, Any]:
    return BuildEngine(_workspace(args)).upgrade_shard(args.shard, args.to_version)


def cmd_stop(args: argparse.Namespace) -> dict[str, Any]:
    return {"ok": True, **BuildEngine(_workspace(args)).request_stop(args.reason)}


def cmd_resume(args: argparse.Namespace) -> dict[str, Any]:
    return BuildEngine(_workspace(args)).resume()


def cmd_export(args: argparse.Namespace) -> dict[str, Any]:
    return BuildEngine(_workspace(args)).export(args.format, args.out)


def cmd_roundtrip(args: argparse.Namespace) -> dict[str, Any]:
    engine = BuildEngine(_workspace(args))
    formats = args.format or ["json_v2", "wav", "units", "json_v1"]
    return {"ok": True, "results": {fmt: engine.roundtrip(fmt) for fmt in formats}}


def cmd_consistency(args: argparse.Namespace) -> dict[str, Any]:
    return BuildEngine(_workspace(args)).consistency_check(_mutation_from_args(args))


def cmd_validate(args: argparse.Namespace) -> dict[str, Any]:
    config = Config.load(args.config) if args.config else Config()
    data = read_json(args.manifest)
    if data.get("format") in {"tonelab.sound-project", "tonelab.internal-project"}:
        project = import_project_json(args.manifest, config)
        source = "project_json"
    else:
        project = load_manifest(args.manifest, config)
        source = "manifest"
    return {
        "ok": True,
        "source": source,
        "project_id": project.project_id,
        "declared_total_samples": project.declared_total_samples,
        "actual_total_samples": project.total_samples,
        "shards": [
            {
                "shard_id": shard.shard_id,
                "source_structure_version": shard.source_structure_version,
                "sample_count": shard.sample_count,
                "timbre_id": shard.timbre_id,
                "units": [unit.symbol for unit in shard.units],
            }
            for shard in project.shards
        ],
    }


def cmd_serve(args: argparse.Namespace) -> dict[str, Any]:
    from .server import serve

    server_config_path = Path("config/server.json")
    server_config = read_json(server_config_path) if server_config_path.exists() else {}
    host = args.host or server_config.get("host", "127.0.0.1")
    port = args.port if args.port is not None else int(server_config.get("port", 8765))
    workspace = args.workspace or server_config.get("workspace", "workspace/demo")
    serve(workspace, host=host, port=port, config_path=args.config, open_browser=False)
    return {"ok": True, "host": host, "port": port}


def cmd_demo(args: argparse.Namespace) -> dict[str, Any]:
    data_dir = Path(args.data_dir).resolve()
    workspace_root = Path(args.workspace).resolve()
    output = Path(args.output).resolve()
    generator = Path(__file__).resolve().parent.parent / "scripts" / "generate_demo.py"
    subprocess.run(
        [sys.executable, str(generator), "--data-dir", str(data_dir)],
        check=True,
        stdout=subprocess.DEVNULL,
    )
    workspace = Workspace(workspace_root, config=Config.load(args.config) if args.config else Config())
    project = workspace.import_manifests(
        [data_dir / "manifest_v2.json", data_dir / "manifest_v1.json"],
        project_id="demo-aurora",
    )
    engine = BuildEngine(workspace)
    full = engine.build("full")
    incremental = engine.build("incremental")
    roundtrips = {
        fmt: engine.roundtrip(fmt)
        for fmt in ("json_v2", "wav", "units", "json_v1")
    }
    consistency = {}
    v1_shard = next(shard.shard_id for shard in project.shards if shard.source_structure_version == 1)
    mutations = {
        "sample": {"kind": "sample", "shard_id": project.shards[0].shard_id, "shift": 3},
        "unit": {
            "kind": "unit",
            "shard_id": project.shards[1].shard_id,
            "unit_index": 1,
            "end_ms": round(project.shards[1].units[1].end_ms - 5.0, 6),
        },
        "upgrade": {"kind": "upgrade", "shard_id": v1_shard},
    }
    for name, mutation in mutations.items():
        consistency[name] = engine.consistency_check(mutation)
    anomalies: dict[str, Any] = {}
    config = workspace.config
    for anomaly_name in ("timeline", "numeric"):
        try:
            load_manifest(data_dir / f"anomaly_{anomaly_name}.json", config)
            anomalies[anomaly_name] = {"ok": True}
        except ToneLabError as exc:
            anomalies[anomaly_name] = exc.to_dict()
    index = json.loads((data_dir / "index.json").read_text(encoding="utf-8"))
    summary = {
        "format": "tonelab.baseline-summary",
        "schema": 1,
        "seed": index["seed"],
        "data": index,
        "workspace": (
            str(workspace_root.relative_to(Path(__file__).resolve().parent.parent))
            if workspace_root.is_relative_to(Path(__file__).resolve().parent.parent)
            else str(workspace_root)
        ),
        "full_build": {
            key: full[key]
            for key in (
                "project_id",
                "config_version",
                "shard_order",
                "sample_total",
                "truncated_frames_total",
                "project_version_fingerprint",
                "structure_version_fingerprint",
                "deterministic_fingerprint",
                "stage_artifact_sha256",
                "project_exports",
                "checkpoint",
            )
        },
        "incremental_noop": {
            "deterministic_fingerprint": incremental["deterministic_fingerprint"],
            "recomputed_stages": incremental["recomputed_stages"],
            "fingerprint_matches_full": incremental["deterministic_fingerprint"] == full["deterministic_fingerprint"],
        },
        "roundtrips": roundtrips,
        "incremental_consistency": consistency,
        "anomalies": anomalies,
    }
    summary_core = {
        key: value
        for key, value in summary.items()
        if key not in {"workspace", "data"}
    }
    summary["summary_fingerprint"] = sha256_bytes(canonical_json_bytes(summary_core))
    atomic_write_json(output, summary)
    atomic_write_json(workspace.exports_dir / "baseline_summary.json", summary)
    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="tonelab", description="Local deterministic timbre and pronunciation workbench")
    sub = parser.add_subparsers(dest="command", required=True)

    def common(p: argparse.ArgumentParser, *, config: bool = True) -> None:
        p.add_argument("--workspace", default="workspace/demo")
        if config:
            p.add_argument("--config")

    p = sub.add_parser("init", help="create workspace from manifests or project files")
    common(p)
    p.add_argument("--manifest", action="append")
    p.add_argument("--project-json")
    p.add_argument("--wav-dir")
    p.add_argument("--project-id")
    p.set_defaults(func=cmd_init)

    p = sub.add_parser("import", help="import one or more manifests")
    common(p)
    p.add_argument("manifest", nargs="+")
    p.add_argument("--project-id")
    p.set_defaults(func=lambda args: _init_import(args))

    p = sub.add_parser("import-wav", help="merge one WAV + sidecar shard into the workspace")
    common(p)
    p.add_argument("--wav", required=True)
    p.add_argument("--sidecar")
    p.set_defaults(func=cmd_import_wav)

    p = sub.add_parser("import-units", help="apply unit annotations to the current project")
    common(p)
    p.add_argument("--path", required=True)
    p.set_defaults(func=cmd_import_units)

    p = sub.add_parser("build", help="run staged deterministic build")
    common(p)
    p.add_argument("--mode", choices=["incremental", "full"], default="incremental")
    p.add_argument("--until-stage", choices=["feature", "model", "quantize", "export"])
    p.add_argument("--clear-stop", action="store_true")
    p.add_argument("--summary-out")
    p.set_defaults(func=cmd_build)

    p = sub.add_parser("stage", help="execute through one named pipeline stage")
    common(p)
    p.add_argument("--stage", required=True, choices=["feature", "model", "quantize", "export"])
    p.add_argument("--mode", choices=["incremental", "full"], default="incremental")
    p.add_argument("--summary-out")
    p.set_defaults(func=cmd_stage)

    p = sub.add_parser("status", help="query stages, fingerprints, checkpoints, and stop state")
    common(p)
    p.set_defaults(func=cmd_status)

    p = sub.add_parser("fingerprints", help="query detailed version fingerprints")
    common(p)
    p.set_defaults(func=cmd_fingerprints)

    p = sub.add_parser("checkpoints", help="query atomic monotonic checkpoint records")
    common(p)
    p.set_defaults(func=cmd_checkpoints)

    p = sub.add_parser("edit-unit", help="edit one pronunciation unit")
    common(p)
    p.add_argument("--shard", required=True)
    p.add_argument("--unit-index", type=int, required=True)
    p.add_argument("--start-ms", type=float)
    p.add_argument("--end-ms", type=float)
    p.add_argument("--symbol")
    p.add_argument("--formants", help="JSON array, e.g. [500,1500,2500]")
    p.add_argument("--formant-ranges", help="JSON array of [low,high] pairs")
    p.set_defaults(func=cmd_edit_unit)

    p = sub.add_parser("edit-pitch", help="edit one normalized pitch point")
    common(p)
    p.add_argument("--shard", required=True)
    p.add_argument("--point-index", type=int, required=True)
    p.add_argument("--time", type=float)
    p.add_argument("--value", type=float)
    p.set_defaults(func=cmd_edit_pitch)

    p = sub.add_parser("replace-sample", help="replace one shard PCM from WAV")
    common(p)
    p.add_argument("--shard", required=True)
    p.add_argument("--wav", required=True)
    p.set_defaults(func=cmd_replace_sample)

    p = sub.add_parser("upgrade", help="explicitly upgrade one shard to structure version 2")
    common(p)
    p.add_argument("--shard", required=True)
    p.add_argument("--to-version", type=int, default=2)
    p.set_defaults(func=cmd_upgrade)

    p = sub.add_parser("stop", help="request cooperative stop")
    common(p)
    p.add_argument("--reason", default="user requested stop")
    p.set_defaults(func=cmd_stop)

    p = sub.add_parser("resume", help="clear stop marker and resume incremental build")
    common(p)
    p.set_defaults(func=cmd_resume)

    p = sub.add_parser("export", help="export json_v2, wav, units, or json_v1")
    common(p)
    p.add_argument("--format", required=True, choices=["json_v2", "wav", "units", "json_v1", "v2", "v1"])
    p.add_argument("--out", required=True)
    p.set_defaults(func=cmd_export)

    p = sub.add_parser("roundtrip", help="audit external format roundtrips")
    common(p)
    p.add_argument("--format", action="append", choices=["json_v2", "wav", "units", "json_v1", "v2", "v1"])
    p.set_defaults(func=cmd_roundtrip)

    p = sub.add_parser("consistency", help="compare incremental mutation rebuild to full rebuild byte-for-byte")
    common(p)
    p.add_argument("--mutation", required=True, choices=["sample", "unit", "upgrade", "pitch"])
    p.add_argument("--shard", required=True)
    p.add_argument("--unit-index", type=int, default=0)
    p.add_argument("--start-ms", type=float)
    p.add_argument("--end-ms", type=float)
    p.add_argument("--symbol")
    p.add_argument("--point-index", type=int, default=0)
    p.add_argument("--value", type=float)
    p.add_argument("--wav")
    p.add_argument("--shift", type=int)
    p.set_defaults(func=cmd_consistency)

    p = sub.add_parser("validate", help="validate a v1/v2 manifest or project JSON without importing")
    p.add_argument("--manifest", required=True)
    p.add_argument("--config")
    p.set_defaults(func=cmd_validate)

    p = sub.add_parser("serve", help="start the local interactive HTTP workbench")
    p.add_argument("--workspace")
    p.add_argument("--config")
    p.add_argument("--host")
    p.add_argument("--port", type=int)
    p.set_defaults(func=cmd_serve)

    p = sub.add_parser("demo", help="generate fixtures, full build, roundtrips, and consistency proof")
    p.add_argument("--data-dir", default="data")
    p.add_argument("--workspace", default="workspace/demo")
    p.add_argument("--config")
    p.add_argument("--output", default="artifacts/baseline_summary.json")
    p.set_defaults(func=cmd_demo)
    return parser


def _init_import(args: argparse.Namespace) -> dict[str, Any]:
    workspace = _workspace(args)
    project = workspace.import_manifests(args.manifest, project_id=args.project_id)
    return {
        "ok": True,
        "workspace": str(workspace.root),
        "project_id": project.project_id,
        "shards": [
            {
                "shard_id": shard.shard_id,
                "source_structure_version": shard.source_structure_version,
                "sample_count": shard.sample_count,
            }
            for shard in project.shards
        ],
    }


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        result = args.func(args)
        _print(result)
        return 0
    except ToneLabError as exc:
        _print(exc.to_dict())
        return 1
    except (FileNotFoundError, ValueError, KeyError) as exc:
        _print({"ok": False, "error": {"code": "E_BAD_REQUEST", "message": str(exc)}})
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

