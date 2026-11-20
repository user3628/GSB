"""Command-line interface for the preference-alignment editor."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .core.config import Config
from .core.errors import PrefAlignError
from .core.hashing import canonical_json
from .core.workspace import (
    build_workspace,
    edit_sample,
    full_rebuild_digests,
    import_demo_shards,
    import_shards,
    replace_source_shard,
)


def _print(obj) -> None:
    print(json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True))


def _build(args) -> tuple:
    root = Path(args.root).resolve()
    config_path = Path(args.config).resolve() if args.config else None
    return build_workspace(root, config_path)


def cmd_import(args) -> int:
    _, store, _ = _build(args)
    if args.demo:
        result = import_demo_shards(store, Path(args.root).resolve())
    else:
        result = import_shards(store, [Path(item) for item in args.shards])
    _print({"imported": result})
    return 0


def cmd_run(args) -> int:
    _, _, pipeline = _build(args)
    result = pipeline.run(reason=args.reason or "cli_run")
    _print(result)
    return 0


def cmd_stop(args) -> int:
    _, _, pipeline = _build(args)
    pipeline.request_stop()
    _print({"stop_requested": True, "marker": str(pipeline.stop_marker)})
    return 0


def cmd_resume(args) -> int:
    _, _, pipeline = _build(args)
    _print(pipeline.resume())
    return 0


def cmd_status(args) -> int:
    _, _, pipeline = _build(args)
    _print(pipeline.status())
    return 0


def cmd_edit(args) -> int:
    root = Path(args.root).resolve()
    _, store, pipeline = _build(args)
    payload = json.loads(Path(args.file).read_text(encoding="utf-8"))
    sample = edit_sample(store, payload)
    result = pipeline.run(reason=f"edit:{sample.pair_id}")
    _print({"edited": sample.pair_id, "run": result})
    return 0


def cmd_replace(args) -> int:
    _, store, pipeline = _build(args)
    entry = replace_source_shard(store, args.source, Path(args.file), args.alias)
    result = pipeline.run(reason=f"replace_source:{args.source}")
    _print({"replaced": args.source, "entry": entry, "run": result})
    return 0


def cmd_verify(args) -> int:
    root = Path(args.root).resolve()
    cfg, store, pipeline = _build(args)
    baseline_root = Path(args.baseline_root).resolve() if args.baseline_root \
        else root / "workspace" / "verify_full"
    digests, count = full_rebuild_digests(root, baseline_root,
                                          Path(args.config).resolve() if args.config else None)
    report = pipeline.verify_incremental(digests, count)
    _print(report)
    return 0


def cmd_export(args) -> int:
    _, _, pipeline = _build(args)
    target = pipeline.export_mixture()
    _print({"exported": str(target)})
    return 0


def cmd_demo(args) -> int:
    """End-to-end deterministic demo: build, edit, invalidate, verify."""
    from .core.store import load_shard_samples
    root = Path(args.root).resolve()
    cfg, store, pipeline = build_workspace(root)
    import_demo_shards(store, root)
    first = pipeline.run(reason="demo_initial")
    baseline_before = pipeline.stage_digests()

    samples, _ = store.effective_samples()
    target = next(sample for sample in samples if sample.prompt)
    edited = sample_to_edit_dict(target)
    edited["chosen"] = target.chosen + " 【编辑后补充说明】"
    edit_sample(store, edited)
    second = pipeline.run(reason="demo_edit")
    status = pipeline.status()

    baseline_root = root / "workspace" / "verify_full"
    digests, count = full_rebuild_digests(root, baseline_root)
    verify = pipeline.verify_incremental(digests, count)
    _print({
        "initial_run": first,
        "edit_pair_id": target.pair_id,
        "incremental_run": second,
        "invalidation_log": status["invalidation_log"],
        "verification": verify,
    })
    return 0


def sample_to_edit_dict(sample) -> dict:
    data = sample.to_dict()
    data.pop("shard", None)
    return data


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="prefalign",
                                     description="LLM preference data incremental editor")
    parser.add_argument("--root", default=".", help="project root directory")
    parser.add_argument("--config", default=None, help="path to YAML config")
    sub = parser.add_subparsers(dest="command", required=True)

    p_import = sub.add_parser("import", help="import JSONL shards")
    p_import.add_argument("shards", nargs="*", help="shard file paths")
    p_import.add_argument("--demo", action="store_true", help="import data/shards/*.jsonl")
    p_import.set_defaults(func=cmd_import)

    p_run = sub.add_parser("run", help="run/incrementally update the pipeline")
    p_run.add_argument("--reason", default=None)
    p_run.set_defaults(func=cmd_run)

    sub.add_parser("stop", help="request a graceful stop").set_defaults(func=cmd_stop)
    sub.add_parser("resume", help="resume from latest consistent checkpoint"
                   ).set_defaults(func=cmd_resume)
    sub.add_parser("status", help="show stage status and version fingerprints"
                   ).set_defaults(func=cmd_status)

    p_edit = sub.add_parser("edit", help="edit one sample from a JSON payload file")
    p_edit.add_argument("file")
    p_edit.set_defaults(func=cmd_edit)

    p_replace = sub.add_parser("replace", help="replace a source shard")
    p_replace.add_argument("source")
    p_replace.add_argument("file")
    p_replace.add_argument("--alias", required=True)
    p_replace.set_defaults(func=cmd_replace)

    p_verify = sub.add_parser("verify", help="incremental vs full rebuild byte comparison")
    p_verify.add_argument("--baseline-root", default=None)
    p_verify.set_defaults(func=cmd_verify)

    sub.add_parser("export", help="export the final mixture").set_defaults(func=cmd_export)
    sub.add_parser("demo", help="deterministic end-to-end demo").set_defaults(func=cmd_demo)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except PrefAlignError as exc:
        print(canonical_json(exc.to_dict()), file=sys.stderr)
        return 2
    except FileNotFoundError as exc:
        print(json.dumps({"code": "E_IO", "message": str(exc)}), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
