from __future__ import annotations

import copy
import json
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from .bridge import edit_entry, load_bridge, validate_bridge, write_bridge
from .config import load_config, set_config_value, validate_config, write_config
from .errors import BusinessError, E_INPUT
from .util import atomic_write, atomic_write_json, digest_file, read_json, read_jsonl, safe_name, write_jsonl


REQUIRED_PAIR_FIELDS = {"pair_id", "query", "doc", "lang_query", "lang_doc", "source"}


@dataclass(frozen=True)
class WorkspacePaths:
    root: Path

    @property
    def inputs(self) -> Path:
        return self.root / "inputs"

    @property
    def shards(self) -> Path:
        return self.inputs / "shards"

    @property
    def bridge(self) -> Path:
        return self.inputs / "bridge.json"

    @property
    def scoring(self) -> Path:
        return self.inputs / "scoring.yaml"

    @property
    def state(self) -> Path:
        return self.root / "state"

    @property
    def artifacts(self) -> Path:
        return self.state / "artifacts"

    @property
    def checkpoints(self) -> Path:
        return self.state / "checkpoints"

    @property
    def manifest(self) -> Path:
        return self.state / "manifest.json"

    @property
    def run_status(self) -> Path:
        return self.state / "run_status.json"

    @property
    def stop_request(self) -> Path:
        return self.state / "stop.request.json"

    @property
    def reports(self) -> Path:
        return self.root / "reports"

    @property
    def exports(self) -> Path:
        return self.root / "exports"

    @property
    def build_lock(self) -> Path:
        return self.state / "build.lock"

    @property
    def checkpoint_lock(self) -> Path:
        return self.state / "checkpoint.lock"


def _validate_pairs(records: Iterable[dict[str, Any]], shard: str) -> None:
    seen: set[str] = set()
    for line_number, pair in enumerate(records, 1):
        missing = REQUIRED_PAIR_FIELDS - set(pair)
        if missing:
            raise BusinessError(
                E_INPUT,
                f"{shard}:{line_number}: missing pair fields: {sorted(missing)}",
            )
        pair_id = str(pair["pair_id"])
        if pair_id in seen:
            raise BusinessError(E_INPUT, f"{shard}:{line_number}: duplicate pair_id {pair_id}")
        seen.add(pair_id)
        for field in ("query", "doc", "lang_query", "lang_doc", "source"):
            if not isinstance(pair[field], str):
                raise BusinessError(E_INPUT, f"{shard}:{line_number}: {field} must be a string")
        if "human_relevance" in pair and pair["human_relevance"] is not None:
            value = pair["human_relevance"]
            if not isinstance(value, int) or isinstance(value, bool) or value < 0 or value > 3:
                raise BusinessError(
                    E_INPUT,
                    f"{shard}:{line_number}: human_relevance must be an integer in [0, 3]",
                )


def initialize_workspace(source_data: Path, workspace: Path, *, force: bool = False) -> WorkspacePaths:
    paths = WorkspacePaths(workspace.resolve())
    source_data = source_data.resolve()
    source_shards = source_data / "shards"
    source_bridge = source_data / "bridge.json"
    source_scoring = source_data / "scoring.yaml"
    for required in (source_shards, source_bridge, source_scoring):
        if not required.exists():
            raise BusinessError(E_INPUT, f"source data is incomplete: missing {required}")
    if paths.root.exists() and any(paths.root.iterdir()) and not force:
        raise BusinessError(E_INPUT, f"workspace is not empty: {paths.root}; use --force to replace")
    if paths.root.exists() and force:
        for child in paths.root.iterdir():
            if child.is_dir():
                shutil.rmtree(child)
            else:
                child.unlink()
    for directory in (
        paths.shards,
        paths.artifacts,
        paths.checkpoints,
        paths.reports,
        paths.exports,
    ):
        directory.mkdir(parents=True, exist_ok=True)

    shard_files = sorted(source_shards.glob("*.jsonl"))
    if len(shard_files) < 3:
        raise BusinessError(E_INPUT, "at least three JSONL shards are required")
    for source in shard_files:
        records = read_jsonl(source)
        _validate_pairs(records, source.name)
        write_jsonl(paths.shards / source.name, records)
    bridge = load_bridge(source_bridge)
    write_bridge(paths.bridge, bridge)
    config = load_config(source_scoring)
    write_config(paths.scoring, config)
    return paths


def list_shards(paths: WorkspacePaths) -> list[str]:
    return [path.stem for path in sorted(paths.shards.glob("*.jsonl"))]


def load_all_pairs(paths: WorkspacePaths) -> dict[str, list[dict[str, Any]]]:
    return {shard: read_jsonl(paths.shards / f"{shard}.jsonl") for shard in list_shards(paths)}


def input_digests(paths: WorkspacePaths) -> dict[str, Any]:
    return {
        "shards": {
            shard: digest_file(paths.shards / f"{shard}.jsonl") for shard in list_shards(paths)
        },
        "bridge": digest_file(paths.bridge),
        "scoring": digest_file(paths.scoring),
    }


def import_shard(paths: WorkspacePaths, shard: str, content: str | bytes) -> dict[str, Any]:
    name = safe_name(shard)
    if name.endswith(".jsonl"):
        name = name[:-6]
    target = paths.shards / f"{name}.jsonl"
    try:
        if isinstance(content, bytes):
            content = content.decode("utf-8")
        records = [json.loads(line) for line in content.splitlines() if line.strip()]
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise BusinessError(E_INPUT, f"invalid JSONL import: {exc}") from exc
    if not isinstance(records, list):
        raise BusinessError(E_INPUT, "shard import must be JSONL objects")
    _validate_pairs(records, name)
    existing_ids: set[str] = set()
    for other in list_shards(paths):
        if other == name:
            continue
        for record in read_jsonl(paths.shards / f"{other}.jsonl"):
            pair_id = str(record["pair_id"])
            if pair_id in existing_ids:
                raise BusinessError(E_INPUT, f"duplicate pair_id across shards: {pair_id}")
            existing_ids.add(pair_id)
    for record in records:
        if str(record["pair_id"]) in existing_ids:
            raise BusinessError(E_INPUT, f"duplicate pair_id across shards: {record['pair_id']}")
    write_jsonl(target, records)
    return {"shard": name, "pair_count": len(records), "digest": digest_file(target)}


def edit_pair(
    paths: WorkspacePaths,
    shard: str,
    pair_id: str,
    *,
    query: str | None = None,
    doc: str | None = None,
    human_relevance: int | None = None,
    clear_human_relevance: bool = False,
) -> dict[str, Any]:
    path = paths.shards / f"{shard}.jsonl"
    if not path.exists():
        raise BusinessError(E_INPUT, f"shard not found: {shard}")
    records = read_jsonl(path)
    found = False
    for record in records:
        if str(record["pair_id"]) != str(pair_id):
            continue
        found = True
        if query is not None:
            record["query"] = query
        if doc is not None:
            record["doc"] = doc
        if clear_human_relevance:
            record.pop("human_relevance", None)
        elif human_relevance is not None:
            record["human_relevance"] = int(human_relevance)
    if not found:
        raise BusinessError(E_INPUT, f"pair not found: {shard}/{pair_id}")
    _validate_pairs(records, shard)
    write_jsonl(path, records)
    return {"shard": shard, "pair_id": pair_id, "digest": digest_file(path)}


def import_bridge(paths: WorkspacePaths, content: str | bytes | dict[str, Any]) -> dict[str, Any]:
    if isinstance(content, dict):
        bridge = copy.deepcopy(content)
    else:
        try:
            if isinstance(content, bytes):
                content = content.decode("utf-8")
            bridge = json.loads(content)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise BusinessError(E_INPUT, f"invalid bridge import: {exc}") from exc
    validate_bridge(bridge)
    write_bridge(paths.bridge, bridge)
    return {"entry_count": len(bridge["entries"]), "digest": digest_file(paths.bridge)}


def update_bridge_entry(
    paths: WorkspacePaths,
    entry_id: str,
    *,
    weight: float | None = None,
    en: list[str] | None = None,
    zh: str | None = None,
) -> dict[str, Any]:
    bridge = load_bridge(paths.bridge)
    updated = edit_entry(bridge, entry_id, weight=weight, en=en, zh=zh)
    write_bridge(paths.bridge, updated)
    return {"entry_id": entry_id, "digest": digest_file(paths.bridge)}


def update_config(paths: WorkspacePaths, dotted_path: str, value: Any) -> dict[str, Any]:
    config = load_config(paths.scoring)
    updated = set_config_value(config, dotted_path, value)
    write_config(paths.scoring, updated)
    return {"path": dotted_path, "value": value, "digest": digest_file(paths.scoring)}


def replace_config(paths: WorkspacePaths, config: dict[str, Any]) -> dict[str, Any]:
    validate_config(config)
    write_config(paths.scoring, config)
    return {"digest": digest_file(paths.scoring)}


def copy_inputs(source: WorkspacePaths, destination: WorkspacePaths) -> None:
    destination.shards.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source.bridge, destination.bridge)
    shutil.copyfile(source.scoring, destination.scoring)
    for shard in list_shards(source):
        shutil.copyfile(source.shards / f"{shard}.jsonl", destination.shards / f"{shard}.jsonl")


def read_manifest(paths: WorkspacePaths) -> dict[str, Any]:
    if not paths.manifest.exists():
        return {}
    try:
        return read_json(paths.manifest)
    except (OSError, ValueError) as exc:
        raise BusinessError(E_INPUT, f"invalid workspace manifest: {exc}") from exc


def write_manifest(paths: WorkspacePaths, manifest: dict[str, Any]) -> None:
    atomic_write_json(paths.manifest, manifest, indent=2)


def request_stop(paths: WorkspacePaths, *, reason: str = "operator request") -> None:
    atomic_write_json(
        paths.stop_request,
        {"requested": True, "reason": reason},
        indent=2,
    )


def clear_stop(paths: WorkspacePaths) -> None:
    if paths.stop_request.exists():
        paths.stop_request.unlink()


def stop_requested(paths: WorkspacePaths) -> bool:
    return paths.stop_request.exists()
