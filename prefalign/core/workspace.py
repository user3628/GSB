"""Workspace construction helpers shared by the CLI, HTTP API and tests."""
from __future__ import annotations

import json
import shutil
from pathlib import Path

from .config import Config
from .model import Sample
from .pipeline import Pipeline
from .store import WorkspaceStore, write_jsonl


def build_workspace(root: Path, config_path: Path | None = None) -> tuple[Config, WorkspaceStore, Pipeline]:
    root = Path(root).resolve()
    if config_path is None:
        config_path = root / "config" / "default.yaml"
    cfg = Config.load(config_path, root)
    store = WorkspaceStore(root, cfg.path("shards_dir"), cfg.path("state_dir"))
    return cfg, store, Pipeline(cfg)


def import_shards(store: WorkspaceStore, shard_paths: list[Path]) -> dict:
    result = {}
    for path in sorted(Path(item) for item in shard_paths):
        result[path.name] = store.import_shard(path, alias=path.name)
    return result


def import_demo_shards(store: WorkspaceStore, root: Path) -> dict:
    shard_paths = sorted((root / "data" / "shards").glob("*.jsonl"))
    return import_shards(store, shard_paths)


def edit_sample(store: WorkspaceStore, sample_dict: dict) -> Sample:
    sample = Sample.from_dict(sample_dict, shard=sample_dict.get("shard"))
    store.upsert_edit(sample)
    return sample


def replace_source_shard(store: WorkspaceStore, source: str, replacement: Path,
                         alias: str) -> dict:
    return store.replace_source_shard(source, Path(replacement), alias)


def fresh_workspace_copy(source_root: Path, target_root: Path) -> Path:
    """Materialize an independent clean workspace for a from-scratch rebuild."""
    target_root = Path(target_root)
    if target_root.exists():
        shutil.rmtree(target_root)
    ignore = shutil.ignore_patterns(
        ".venv", "__pycache__", ".git", ".pytest_cache",
        "checkpoints", "artifacts", "verify_full", "verify_full2", "locks")
    shutil.copytree(source_root, target_root, ignore=ignore)
    return target_root


def full_rebuild_digests(source_root: Path, target_root: Path,
                         config_path: Path | None = None) -> tuple[dict[str, str], int]:
    """Run a clean from-scratch pipeline and return per-stage artifact digests.

    The copied workspace carries the input registry and edit log, so the clean
    rebuild sees exactly the same effective samples as the incremental run.
    """
    fresh_root = fresh_workspace_copy(source_root, target_root)
    cfg, store, pipeline = build_workspace(fresh_root, config_path)
    pipeline.run(reason="clean_full_rebuild")
    count = pipeline.status()["input_summary"]["n_samples"]
    return pipeline.stage_digests(), count


def write_replacement_shard(path: Path, records: list[dict]) -> None:
    write_jsonl(Path(path), records)


def load_json_records(path: Path) -> list[dict]:
    return [json.loads(line) for line in Path(path).read_text(
        encoding="utf-8").splitlines() if line.strip()]
