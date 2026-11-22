from __future__ import annotations

import argparse
import json
import shutil
import signal
import sys
from pathlib import Path
from typing import Any

from .codec import (
    decode_manifest,
    encode_v1,
    encode_v2,
    manifest_version,
    read_manifest_file,
    semantic_signature,
    write_manifest,
)
from .digest import load_npz, read_json
from .engine import open_engine
from .errors import BusinessError
from .kernel import GatherConfig
from .model import V1UpgradeConfig
from .samplegen import generate_all
from .weights import load_manifest_facts


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _json_print(value: Any) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True))


def _v1_config(args: argparse.Namespace) -> V1UpgradeConfig:
    return V1UpgradeConfig(
        total_rows=getattr(args, "total_rows", None),
        percentage_rounding=getattr(args, "percentage_rounding", "ceil"),
        offset_strategy=getattr(args, "offset_strategy", "row_start"),
        recover_pad_from="weight_metadata",
    )


def _gather_config(args: argparse.Namespace) -> GatherConfig:
    return GatherConfig(
        output_scale=getattr(args, "output_scale", None),
        negative_index_policy=getattr(args, "negative_policy", "wrap"),
        verify_atol_scale=getattr(args, "verify_tolerance_scale", 0.0),
    )


def _safe_batches(manifest: dict[str, Any], data_root: Path) -> list[str]:
    metadata_path = data_root / "sample_metadata.json"
    if metadata_path.exists():
        metadata = read_json(metadata_path)
        normal = metadata.get("normal_batches")
        if isinstance(normal, list):
            return [str(item) for item in normal]
    return [str(item.get("batch_id")) for item in manifest.get("index_batches", [])]


def _error_batches(data_root: Path, manifest: dict[str, Any]) -> list[str]:
    metadata_path = data_root / "sample_metadata.json"
    if metadata_path.exists():
        metadata = read_json(metadata_path)
        errors = metadata.get("error_batches")
        if isinstance(errors, list) and errors:
            return [str(item) for item in errors]
    normal = set(_safe_batches(manifest, data_root))
    return [str(item.get("batch_id")) for item in manifest.get("index_batches", []) if item.get("batch_id") not in normal]


def _parse_batches(values: list[str] | None) -> list[str] | None:
    if not values:
        return None
    parsed: list[str] = []
    for value in values:
        parsed.extend(item.strip() for item in value.split(",") if item.strip())
    return parsed


def command_generate(args: argparse.Namespace) -> int:
    result = generate_all(args.out)
    _json_print(result)
    return 0


def command_codec(args: argparse.Namespace) -> int:
    data_root = Path(args.root)
    manifest = read_manifest_file(args.manifest)
    facts = load_manifest_facts(manifest, data_root)
    project = decode_manifest(manifest, facts=facts, v1_config=_v1_config(args))
    roundtrip_v2 = decode_manifest(encode_v2(project), facts=facts)
    downgraded = encode_v1(project)
    roundtrip_v1 = decode_manifest(downgraded, facts=facts, v1_config=_v1_config(args))
    v2_equal = semantic_signature(project) == semantic_signature(roundtrip_v2)
    v1_equal = semantic_signature(project) == semantic_signature(roundtrip_v1)
    output: dict[str, Any] = {
        "input_schema_version": manifest_version(manifest),
        "decoded_structure_version": project.structure_version,
        "embedding_dim": project.embedding_dim,
        "shards": [
            {
                "shard_id": shard.shard_id,
                "row_range": [shard.row_start, shard.row_end],
                "row_offset": shard.row_offset,
                "embedding_dim": shard.embedding_dim,
                "pad_ranges": [list(item) for item in shard.pad_ranges],
            }
            for shard in project.shards
        ],
        "v1_upgrade_provenance": list(project.provenance),
        "v2_roundtrip_semantics_equal": v2_equal,
        "v1_roundtrip_semantics_equal": v1_equal,
        "v1_has_compat_extension": "_compat" in downgraded,
    }
    if args.out:
        out = Path(args.out)
        out.mkdir(parents=True, exist_ok=True)
        write_manifest(out / "manifest_v2_roundtrip.json", encode_v2(project))
        write_manifest(out / "manifest_v1_downgrade.json", downgraded)
        output["written"] = [str(out / "manifest_v2_roundtrip.json"), str(out / "manifest_v1_downgrade.json")]
    _json_print(output)
    return 0 if v2_equal and v1_equal else 1


def command_inspect(args: argparse.Namespace) -> int:
    engine = open_engine(
        args.manifest,
        args.root,
        args.workspace,
        gather_config=_gather_config(args),
        verify_tolerance_scale=args.verify_tolerance_scale,
        v1_config=_v1_config(args),
        strict_weight_metadata=args.strict_weight_metadata,
    )
    _json_print(engine.inspect(_parse_batches(args.batches)))
    return 0


def command_run(args: argparse.Namespace) -> int:
    if args.reset and Path(args.workspace).exists():
        shutil.rmtree(args.workspace)
    manifest = read_manifest_file(args.manifest)
    data_root = Path(args.root)
    batches = _parse_batches(args.batches)
    if batches is None:
        batches = _safe_batches(manifest, data_root)
        if not batches:
            raise BusinessError("E_BATCH_INVALID", "no batches selected")
    engine = open_engine(
        args.manifest,
        data_root,
        args.workspace,
        gather_config=_gather_config(args),
        verify_tolerance_scale=args.verify_tolerance_scale,
        v1_config=_v1_config(args),
        strict_weight_metadata=args.strict_weight_metadata,
    )
    requested_stop = None if args.stop_file is None else Path(args.stop_file)
    signal_stop = Path(args.workspace) / ".stop-request"
    if requested_stop is None:
        signal_stop.unlink(missing_ok=True)
        stop_path = signal_stop
    else:
        stop_path = requested_stop
    previous_handlers: dict[int, Any] = {}

    def request_stop(signum: int, frame: Any) -> None:
        stop_path.parent.mkdir(parents=True, exist_ok=True)
        stop_path.write_text(f"signal={signum}\n", encoding="utf-8")

    for signum in (getattr(signal, "SIGINT", None), getattr(signal, "SIGTERM", None)):
        if signum is None:
            continue
        try:
            previous_handlers[signum] = signal.signal(signum, request_stop)
        except (OSError, ValueError):
            pass
    try:
        report = engine.run(
            batches,
            stop_after_batches=args.stop_after_batches,
            stop_file=stop_path,
        )
    finally:
        for signum, handler in previous_handlers.items():
            signal.signal(signum, handler)
    _json_print(report.to_dict())
    return 0


def command_errors(args: argparse.Namespace) -> int:
    if args.reset and Path(args.workspace).exists():
        shutil.rmtree(args.workspace)
    manifest = read_manifest_file(args.manifest)
    data_root = Path(args.root)
    batches = _parse_batches(args.batches) or _error_batches(data_root, manifest)
    engine = open_engine(
        args.manifest,
        data_root,
        args.workspace,
        gather_config=_gather_config(args),
        v1_config=_v1_config(args),
        strict_weight_metadata=args.strict_weight_metadata,
    )
    results: list[dict[str, Any]] = []
    for batch_id in batches:
        link = engine.project.batch_map().get(batch_id)
        if link is None:
            results.append({"batch_id": batch_id, "code": "E_BATCH_INVALID", "context": {"reason": "not linked"}})
            continue
        try:
            result = engine.process_batch(link)
            results.append({"batch_id": batch_id, "unexpected_success": result.to_dict()})
        except BusinessError as exc:
            results.append({"batch_id": batch_id, **exc.to_dict()})
    _json_print(results)
    return 0


def _bundle_bytes(path: str | None) -> bytes | None:
    return None if path is None else Path(path).read_bytes()


def command_demo(args: argparse.Namespace) -> int:
    samples_root = Path(args.samples)
    if args.regenerate or not (samples_root / "main" / "manifest_v2.json").exists():
        generate_all(samples_root)
    data_root = samples_root / "main"
    metadata = read_json(data_root / "sample_metadata.json")
    safe_batches = [str(item) for item in metadata["normal_batches"]]
    error_batches = [str(item) for item in metadata["error_batches"]]
    state_root = Path(args.state)
    if args.reset and state_root.exists():
        shutil.rmtree(state_root)
    state_root.mkdir(parents=True, exist_ok=True)

    manifest_v1 = read_manifest_file(data_root / "manifest_v1.json")
    manifest_v2 = read_manifest_file(data_root / "manifest_v2.json")
    facts = load_manifest_facts(manifest_v1, data_root)
    project_v2 = decode_manifest(manifest_v2, facts=facts)
    project_v1 = decode_manifest(manifest_v1, facts=facts, v1_config=V1UpgradeConfig())
    v1_roundtrip = decode_manifest(
        encode_v1(project_v1),
        facts=facts,
        v1_config=V1UpgradeConfig(),
    )
    codec_ok = semantic_signature(project_v1) == semantic_signature(v1_roundtrip)
    v2_to_v1 = decode_manifest(
        encode_v1(project_v2),
        facts=facts,
        v1_config=V1UpgradeConfig(),
    )
    v2_to_v1_ok = semantic_signature(project_v2) == semantic_signature(v2_to_v1)

    gather_config = GatherConfig(output_scale=args.output_scale)
    full_engine = open_engine(
        data_root / "manifest_v2.json",
        data_root,
        state_root / "full",
        gather_config=gather_config,
    )
    full_report = full_engine.run(safe_batches)

    resume_engine = open_engine(
        data_root / "manifest_v2.json",
        data_root,
        state_root / "stop_resume",
        gather_config=gather_config,
    )
    first = resume_engine.run(safe_batches, stop_after_batches=2)
    second = resume_engine.run(safe_batches)
    full_bytes = _bundle_bytes(full_report.bundle_path)
    resumed_bytes = _bundle_bytes(second.bundle_path)
    resume_equal = full_bytes is not None and full_bytes == resumed_bytes

    error_engine = open_engine(
        data_root / "manifest_v2.json",
        data_root,
        state_root / "errors",
        gather_config=gather_config,
    )
    error_results = []
    for batch_id in error_batches:
        try:
            error_engine.process_batch(error_engine.project.batch_map()[batch_id])
            error_results.append({"batch_id": batch_id, "unexpected_success": True})
        except BusinessError as exc:
            error_results.append({"batch_id": batch_id, **exc.to_dict()})

    changed_root = state_root / "changed_data"
    if changed_root.exists():
        shutil.rmtree(changed_root)
    shutil.copytree(data_root, changed_root)
    incremental_workspace = state_root / "incremental"
    baseline_engine = open_engine(
        data_root / "manifest_v2.json",
        data_root,
        incremental_workspace,
        gather_config=gather_config,
    )
    baseline_engine.run(["batch-duplicate"])
    weight_path = changed_root / "weights" / "shard-000.npz"
    arrays = load_npz(weight_path)
    arrays["weights_q"] = arrays["weights_q"].copy()
    arrays["weights_q"][0, 0] = int(arrays["weights_q"][0, 0]) + 11
    from .digest import deterministic_npz_write

    deterministic_npz_write(weight_path, arrays)
    changed_engine = open_engine(
        changed_root / "manifest_v2.json",
        changed_root,
        incremental_workspace,
        gather_config=gather_config,
    )
    incremental = changed_engine.inspect(["batch-duplicate"])[0]

    output = {
        "codec_v1_roundtrip_semantics_equal": codec_ok,
        "codec_v2_to_v1_roundtrip_semantics_equal": v2_to_v1_ok,
        "v1_corrected_embedding_dim": project_v1.embedding_dim,
        "v1_pad_ranges": {shard.shard_id: [list(item) for item in shard.pad_ranges] for shard in project_v1.shards},
        "full_run": full_report.to_dict(),
        "stopped_run": first.to_dict(),
        "resumed_run": second.to_dict(),
        "stop_resume_bundle_byte_equal": resume_equal,
        "bundle_digest": None if full_report.bundle_digest is None else full_report.bundle_digest,
        "error_injection": error_results,
        "incremental_invalidation": {
            "earliest_invalid_stage": incremental.get("earliest_invalid_stage"),
            "reasons": incremental.get("reasons"),
            "reused_stages": incremental.get("reused_stages"),
        },
    }
    _json_print(output)
    return 0 if codec_ok and v2_to_v1_ok and resume_equal else 1


def _add_common(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--manifest", default="samples/main/manifest_v2.json")
    parser.add_argument("--root", default="samples/main")
    parser.add_argument("--workspace", default="state/default")
    parser.add_argument("--output-scale", type=float, default=0.0001)
    parser.add_argument("--negative-policy", choices=("wrap", "reject"), default="wrap")
    parser.add_argument("--verify-tolerance-scale", type=float, default=0.0)
    parser.add_argument("--total-rows", type=int)
    parser.add_argument("--percentage-rounding", choices=("floor", "ceil", "round"), default="ceil")
    parser.add_argument("--offset-strategy", choices=("row_start", "contiguous"), default="row_start")
    parser.add_argument("--strict-weight-metadata", action="store_true")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="embedding-gather", description="Deterministic embedding lookup and gather component")
    subparsers = parser.add_subparsers(dest="command", required=True)

    generate = subparsers.add_parser("generate-samples", help="generate deterministic .npz samples and manifests")
    generate.add_argument("--out", default="samples")
    generate.set_defaults(func=command_generate)

    codec = subparsers.add_parser("codec-check", help="check v1/v2 manifest interoperability")
    codec.add_argument("--manifest", default="samples/main/manifest_v1.json")
    codec.add_argument("--root", default="samples/main")
    codec.add_argument("--total-rows", type=int)
    codec.add_argument("--percentage-rounding", choices=("floor", "ceil", "round"), default="ceil")
    codec.add_argument("--offset-strategy", choices=("row_start", "contiguous"), default="row_start")
    codec.add_argument("--out")
    codec.set_defaults(func=command_codec)

    inspect = subparsers.add_parser("inspect", help="show checkpoint fingerprints and invalidation reasons")
    _add_common(inspect)
    inspect.add_argument("--batches", nargs="*")
    inspect.set_defaults(func=command_inspect)

    run = subparsers.add_parser("run", help="run the four-stage batch pipeline")
    _add_common(run)
    run.add_argument("--batches", nargs="*")
    run.add_argument("--stop-after-batches", type=int)
    run.add_argument("--stop-file")
    run.add_argument("--reset", action="store_true")
    run.set_defaults(func=command_run)

    errors = subparsers.add_parser("errors", help="inject sample error batches and print stable codes")
    _add_common(errors)
    errors.add_argument("--batches", nargs="*")
    errors.add_argument("--reset", action="store_true")
    errors.set_defaults(func=command_errors)

    demo = subparsers.add_parser("demo", help="run the complete local deterministic demonstration")
    demo.add_argument("--samples", default=str(PROJECT_ROOT / "samples"))
    demo.add_argument("--state", default=str(PROJECT_ROOT / "demo_state"))
    demo.add_argument("--output-scale", type=float, default=0.0001)
    demo.add_argument("--regenerate", action="store_true")
    demo.add_argument("--reset", action="store_true", default=True)
    demo.set_defaults(func=command_demo)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except BusinessError as exc:
        _json_print(exc.to_dict())
        return 2
    except (OSError, ValueError) as exc:
        _json_print({"code": "E_IO", "message": str(exc), "context": {}})
        return 2


if __name__ == "__main__":
    sys.exit(main())
