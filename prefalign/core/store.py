"""Workspace input storage: shard registry, edits and atomic writes."""
from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

from .hashing import canonical_json, sha256_bytes, sha256_file
from .model import Sample
from .paths import ensure_dir, to_windows


def atomic_write_bytes(path: Path, data: bytes) -> None:
    """Write bytes atomically: temp file in same dir + os.replace + fsync."""
    ensure_dir(path.parent)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_name, path)
    except BaseException:
        try:
            os.unlink(tmp_name)
        except FileNotFoundError:
            pass
        raise


def atomic_write_json(path: Path, obj) -> None:
    atomic_write_bytes(path, (canonical_json(obj) + "\n").encode("utf-8"))


def read_jsonl(path: Path) -> list[dict]:
    records = []
    with open(path, "r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, 1):
            line = line.strip()
            if not line:
                continue
            records.append(json.loads(line))
    return records


def write_jsonl(path: Path, records: list[dict]) -> None:
    lines = [json.dumps(rec, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
             for rec in records]
    atomic_write_bytes(path, ("\n".join(lines) + ("\n" if lines else "")).encode("utf-8"))


def load_shard_samples(path: Path) -> list[Sample]:
    return [Sample.from_dict(rec, shard=path.name) for rec in read_jsonl(path)]


class WorkspaceStore:
    """Manages imported shards, per-sample edits and their content digests."""

    def __init__(self, root: Path, shards_dir: Path, state_dir: Path):
        self.root = root
        self.shards_dir = ensure_dir(shards_dir)
        self.state_dir = ensure_dir(state_dir)
        self.registry_path = state_dir / "input_registry.json"
        self.edits_path = state_dir / "edits.json"

    # ---- registry -------------------------------------------------------
    def load_registry(self) -> dict:
        if self.registry_path.exists():
            return json.loads(self.registry_path.read_text(encoding="utf-8"))
        return {"shards": {}}

    def save_registry(self, registry: dict) -> None:
        atomic_write_json(self.registry_path, registry)

    def import_shard(self, src_path: Path, alias: str | None = None) -> dict:
        alias = alias or src_path.name
        dst = self.shards_dir / alias
        data = src_path.read_bytes()
        atomic_write_bytes(dst, data)
        registry = self.load_registry()
        entry = {
            "file": to_windows(dst.relative_to(self.root)),
            "sha256": sha256_bytes(data),
            "source_hint": alias,
        }
        registry["shards"][alias] = entry
        self.save_registry(registry)
        return entry

    def replace_source_shard(self, source: str, src_path: Path, alias: str) -> dict:
        """Replace the shard file backing a source and update its digest."""
        return self.import_shard(src_path, alias=alias)

    def shard_digest(self, alias: str) -> str:
        return sha256_file(self.shards_dir / alias)

    # ---- edits ----------------------------------------------------------
    def load_edits(self) -> dict[str, dict]:
        if self.edits_path.exists():
            return json.loads(self.edits_path.read_text(encoding="utf-8"))
        return {}

    def save_edits(self, edits: dict[str, dict]) -> None:
        atomic_write_json(self.edits_path, edits)

    def upsert_edit(self, sample: Sample) -> None:
        edits = self.load_edits()
        edits[sample.pair_id] = sample.to_dict()
        self.save_edits(edits)

    # ---- effective input view ------------------------------------------
    def effective_samples(self) -> tuple[list[Sample], dict]:
        """Return samples after applying edits, plus an input content summary."""
        registry = self.load_registry()
        samples: list[Sample] = []
        shard_digests: dict[str, str] = {}
        for alias in sorted(registry["shards"]):
            path = self.root / registry["shards"][alias]["file"].replace("\\", os.sep)
            shard_digests[alias] = sha256_file(path)
            samples.extend(load_shard_samples(path))
        edits = self.load_edits()
        edited_ids = sorted(edits)
        by_id = {sample.pair_id: sample for sample in samples}
        for pair_id in edited_ids:
            by_id[pair_id] = Sample.from_dict(edits[pair_id], shard=by_id.get(pair_id).shard
                                              if pair_id in by_id else "edit")
        samples = [by_id[key] for key in sorted(by_id)]
        summary = {
            "shards": dict(sorted(shard_digests.items())),
            "edits": {pair_id: sha256_bytes(canonical_json(edits[pair_id]).encode("utf-8"))
                      for pair_id in edited_ids},
            "n_samples": len(samples),
        }
        return samples, summary
