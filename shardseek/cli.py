"""Command-line interface for ShardSeek."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from .config import (
    BASE_FEATURE_CONFIG,
    calibrated_projection_matrices,
    config_fingerprint,
    load_config,
    write_configs,
)
from .demo import run_demo
from .errors import BusinessError, error
from .features import extract_text_features, extract_visual_features
from .generator import DEFAULT_SEED, generate_samples
from .http_server import serve
from .pipeline import Pipeline
from .search import corpus_diagnostics, run_search
from .util import canonical_bytes, write_atomic, write_json_atomic
from .verification import verify_incremental, verify_stop_resume
from .workspace import Workspace


def _emit(value: Any) -> None:
    sys.stdout.buffer.write(canonical_bytes(value, pretty=True))
    sys.stdout.buffer.flush()


def _workspace(args: argparse.Namespace) -> Workspace:
    return Workspace(args.root, project_root=args.project_root)


def _recalibrate(project_root: Path) -> dict[str, str]:
    rows = [
        json.loads(line)
        for line in (project_root / "samples" / "manifest_strict.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    visual_vectors = []
    text_vectors = []
    for row in rows:
        visual_vectors.append(
            extract_visual_features(
                project_root / "samples" / row["image_file"],
                BASE_FEATURE_CONFIG,
                item_id=row["item_id"],
            )["vector"]
        )
        text_vectors.append(
            extract_text_features(
                row["caption"],
                BASE_FEATURE_CONFIG,
                item_id=row["item_id"],
            )["vector"]
        )
    import numpy as np

    calibration = calibrated_projection_matrices(np.vstack(visual_vectors), np.vstack(text_vectors))
    return write_configs(project_root / "configs", calibration=calibration)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="shardseek",
        description="Deterministic local visual representation and cross-modal retrieval lab.",
    )
    parser.add_argument("--root", default="runtime", help="Workspace runtime directory (default: runtime)")
    parser.add_argument(
        "--project-root",
        default=".",
        help="Project directory containing configs/ and samples/ (default: .)",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    init_parser = subparsers.add_parser("init", help="Initialize workspace directories and locks")
    init_parser.add_argument("--clean", action="store_true", help="Clear the workspace before initialization")

    generate_parser = subparsers.add_parser("generate", help="Generate deterministic local PNG shards and JSONL manifests")
    generate_parser.add_argument("--output", default="samples")
    generate_parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    generate_parser.add_argument("--recalibrate", action="store_true", help="Regenerate configs with local CCA calibration")

    config_parser = subparsers.add_parser("config", help="Inspect or select an active projection/quantization config")
    config_sub = config_parser.add_subparsers(dest="config_command", required=True)
    config_use = config_sub.add_parser("use", help="Select a config by name or path")
    config_use.add_argument("config")
    config_sub.add_parser("show", help="Show the active config fingerprint summary")
    config_sub.add_parser("list", help="List checked-in configs")

    import_parser = subparsers.add_parser("import", help="Import a JSONL manifest and copy its PNG shards")
    import_parser.add_argument("--manifest", default="samples/manifest.jsonl")

    reset_parser = subparsers.add_parser("reset", help="Clean the workspace, import samples, and select baseline config")
    reset_parser.add_argument("--manifest", default="samples/manifest.jsonl")
    reset_parser.add_argument("--config", default="baseline_8bit")

    build_parser = subparsers.add_parser("build", help="Run a full or incremental staged build")
    build_parser.add_argument("--mode", choices=("full", "incremental"), default="incremental")
    build_parser.add_argument("--config", help="Select this config before building")
    build_parser.add_argument("--stop-after-shards", type=int, default=None)

    resume_parser = subparsers.add_parser("resume", help="Resume the matching interrupted build session")
    resume_parser.add_argument("--stop-after-shards", type=int, default=None)

    subparsers.add_parser("stop", help="Request a graceful stop at the next shard boundary")
    subparsers.add_parser("status", help="Show config, import, snapshot, and checkpoint status")
    subparsers.add_parser("fingerprints", help="Show all stage and published version fingerprints")
    subparsers.add_parser("checkpoints", help="Show the current checkpoint session")

    search_parser = subparsers.add_parser("search", help="Run deterministic bidirectional retrieval")
    search_parser.add_argument("--direction", choices=("text_to_image", "image_to_text"), required=True)
    search_parser.add_argument("--text", help="Text query for text_to_image")
    search_parser.add_argument("--image-item", help="Imported item_id for image_to_text")
    search_parser.add_argument("--top-k", type=int, default=5)
    search_parser.add_argument("--dense-weight", type=float)
    search_parser.add_argument("--inverted-weight", type=float)
    search_parser.add_argument("--export", help="Write the canonical report JSON to this path")

    diagnostics_parser = subparsers.add_parser("diagnostics", help="Show corpus-level error-bound scaling")
    diagnostics_parser.add_argument("--export", help="Write the canonical diagnostics JSON to this path")

    inc_parser = subparsers.add_parser("verify-incremental", help="Compare incremental change against a fresh full build")
    inc_parser.add_argument("--parent", default="runtime/verification/incremental_cli")
    inc_parser.add_argument("--manifest", default="samples/manifest.jsonl")
    inc_parser.add_argument("--baseline-config", default="baseline_8bit")
    inc_parser.add_argument("--changed-config", default="compact_4bit")
    inc_parser.add_argument("--output")

    stop_parser = subparsers.add_parser("verify-stop-resume", help="Compare stop/resume against a one-shot build")
    stop_parser.add_argument("--parent", default="runtime/verification/stop_resume_cli")
    stop_parser.add_argument("--manifest", default="samples/manifest.jsonl")
    stop_parser.add_argument("--config", default="baseline_8bit")
    stop_parser.add_argument("--stop-after-shards", type=int, default=2)

    demo_parser = subparsers.add_parser("demo", help="Run the deterministic end-to-end demonstration")
    demo_parser.add_argument("--artifacts", default="artifacts")
    demo_parser.add_argument("--seed", type=int, default=DEFAULT_SEED)

    serve_parser = subparsers.add_parser("serve", help="Start the local HTTP application")
    serve_parser.add_argument("--host", default="127.0.0.1")
    serve_parser.add_argument("--port", type=int, default=8767)

    subparsers.add_parser("version", help="Show the ShardSeek version")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    project_root = Path(args.project_root).resolve()
    try:
        if args.command == "init":
            workspace = _workspace(args)
            if args.clean:
                workspace.clean()
            else:
                workspace.init()
            _emit(workspace.status())
        elif args.command == "generate":
            output = Path(args.output)
            summary = generate_samples(output, seed=args.seed)
            result: dict[str, Any] = {"ok": True, "generation": summary}
            if args.recalibrate:
                result["config_hashes"] = _recalibrate(project_root)
            _emit(result)
        elif args.command == "config":
            workspace = _workspace(args)
            if args.config_command == "use":
                config = workspace.set_config(args.config)
                _emit({"ok": True, "selected": config["name"], "fingerprint": config_fingerprint(config)})
            elif args.config_command == "show":
                config = workspace.active_config()
                _emit(
                    {
                        "ok": True,
                        "name": config.get("name"),
                        "fingerprint": config_fingerprint(config),
                        "bits": config["quantization"]["bits"],
                        "granularity": config["quantization"]["granularity"],
                    }
                )
            else:
                entries = []
                for path in sorted((project_root / "configs").glob("*.json")):
                    config = load_config(path, project_root)
                    entries.append(
                        {
                            "file": path.name,
                            "name": config.get("name"),
                            "fingerprint": config_fingerprint(config),
                            "bits": config["quantization"]["bits"],
                        }
                    )
                _emit({"ok": True, "configs": entries})
        elif args.command == "import":
            payload = _workspace(args).import_manifest(args.manifest)
            _emit({"ok": True, "import": payload})
        elif args.command == "reset":
            workspace = _workspace(args)
            workspace.clean()
            imported = workspace.import_manifest(args.manifest)
            config = workspace.set_config(args.config)
            _emit(
                {
                    "ok": True,
                    "imported": {
                        "item_count": imported["item_count"],
                        "enabled_count": imported["enabled_count"],
                        "rejected": imported["rejected"],
                    },
                    "config": {"name": config.get("name"), "fingerprint": config_fingerprint(config)},
                }
            )
        elif args.command == "build":
            result = Pipeline(_workspace(args)).build(
                mode=args.mode,
                stop_after_shards=args.stop_after_shards,
                config_name=args.config,
            )
            _emit(result)
        elif args.command == "resume":
            _emit(Pipeline(_workspace(args)).build(mode="resume", stop_after_shards=args.stop_after_shards))
        elif args.command == "stop":
            _emit({"ok": True, "control": _workspace(args).request_stop()})
        elif args.command == "status":
            _emit(_workspace(args).status())
        elif args.command == "fingerprints":
            _emit(_workspace(args).fingerprints())
        elif args.command == "checkpoints":
            _emit({"ok": True, "checkpoints": _workspace(args).checkpoint_store().status()})
        elif args.command == "search":
            weights = None
            if args.dense_weight is not None or args.inverted_weight is not None:
                weights = {
                    "dense_weight": 0.72 if args.dense_weight is None else args.dense_weight,
                    "inverted_weight": 0.28 if args.inverted_weight is None else args.inverted_weight,
                }
            report = run_search(
                _workspace(args),
                direction=args.direction,
                query_text=args.text,
                image_item_id=args.image_item,
                top_k=args.top_k,
                weights=weights,
            )
            if args.export:
                write_atomic(args.export, canonical_bytes(report))
            _emit(report)
        elif args.command == "diagnostics":
            payload = corpus_diagnostics(_workspace(args))
            if args.export:
                write_atomic(args.export, canonical_bytes(payload))
            _emit(payload)
        elif args.command == "verify-incremental":
            result = verify_incremental(
                parent_root=args.parent,
                manifest_path=args.manifest,
                project_root=project_root,
                baseline_config=args.baseline_config,
                changed_config=args.changed_config,
                output_path=args.output,
            )
            _emit(result)
        elif args.command == "verify-stop-resume":
            _emit(
                verify_stop_resume(
                    parent_root=args.parent,
                    manifest_path=args.manifest,
                    project_root=project_root,
                    config_name=args.config,
                    stop_after_shards=args.stop_after_shards,
                )
            )
        elif args.command == "demo":
            report = run_demo(
                project_root=project_root,
                root=args.root,
                artifacts_dir=args.artifacts,
                seed=args.seed,
            )
            _emit(report)
        elif args.command == "serve":
            serve(args.root, project_root=project_root, host=args.host, port=args.port)
        elif args.command == "version":
            from . import __version__

            _emit({"ok": True, "version": __version__})
        return 0
    except BusinessError as exc:
        sys.stdout.buffer.write(canonical_bytes(exc.to_dict(), pretty=True))
        sys.stdout.buffer.flush()
        return 2
    except KeyboardInterrupt:
        return 130
    except Exception as exc:
        structured = error(
            "E_INTERNAL",
            "Unexpected internal error",
            stage="internal",
            details={"type": type(exc).__name__, "message": str(exc)},
            http_status=500,
        )
        sys.stdout.buffer.write(canonical_bytes(structured.to_dict(), pretty=True))
        sys.stdout.buffer.flush()
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

