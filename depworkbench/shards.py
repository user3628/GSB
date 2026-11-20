"""Shard loading, manifest handling and input fingerprints."""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .errors import E_NOT_FOUND, E_SHARD_MISMATCH, WorkbenchError
from .hashing import canonical_fingerprint
from .tokenizer import count_tokens

MANIFEST_FILENAME = "manifest.json"


@dataclass
class Shard:
    index: int
    filename: str
    sentences: list[str]
    token_count: int
    sentence_token_counts: list[int]
    fingerprint: str

    @property
    def sentence_count(self) -> int:
        return len(self.sentences)

    def to_status(self) -> dict[str, Any]:
        return {
            "index": self.index,
            "filename": self.filename,
            "sentence_count": self.sentence_count,
            "token_count": self.token_count,
            "fingerprint": self.fingerprint,
        }


def shard_fingerprint(index: int, filename: str, sentences: list[str]) -> str:
    payload = json.dumps(
        {"index": index, "filename": filename, "sentences": sentences},
        ensure_ascii=False, sort_keys=True,
    ).encode("utf-8")
    return canonical_fingerprint(payload)


def load_manifest(manifest_path: str | Path) -> list[dict[str, Any]]:
    path = Path(manifest_path)
    if not path.exists():
        raise WorkbenchError(E_NOT_FOUND, f"manifest not found: {path}",
                             {"path": str(path)})
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise WorkbenchError(E_SHARD_MISMATCH, f"invalid manifest JSON: {exc}",
                             {"path": str(path)}) from exc
    entries = data.get("shards") if isinstance(data, dict) else data
    if not isinstance(entries, list) or not entries:
        raise WorkbenchError(E_SHARD_MISMATCH, "manifest must list at least one shard",
                             {"path": str(path)})
    return entries


def load_shards(base_dir: str | Path, manifest_path: str | Path) -> list[Shard]:
    base = Path(base_dir)
    entries = load_manifest(manifest_path)
    shards: list[Shard] = []
    for expected_index, entry in enumerate(entries):
        if not isinstance(entry, dict) or "filename" not in entry:
            raise WorkbenchError(E_SHARD_MISMATCH,
                                 "manifest entry missing 'filename'",
                                 {"entry": entry})
        filename = entry["filename"]
        shard_path = base / filename
        if not shard_path.exists():
            raise WorkbenchError(E_NOT_FOUND, f"shard file not found: {filename}",
                                 {"filename": filename})
        sentences = [
            line.strip() for line in shard_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        if "sentence_count" in entry and entry["sentence_count"] != len(sentences):
            raise WorkbenchError(
                E_SHARD_MISMATCH,
                f"manifest sentence_count mismatch for {filename}",
                {"filename": filename,
                 "manifest_count": entry["sentence_count"],
                 "actual_count": len(sentences)},
            )
        counts = [count_tokens(s) for s in sentences]
        shards.append(Shard(
            index=entry.get("index", expected_index),
            filename=filename, sentences=sentences,
            token_count=sum(counts), sentence_token_counts=counts,
            fingerprint=shard_fingerprint(entry.get("index", expected_index),
                                          filename, sentences),
        ))
    for position, shard in enumerate(shards):
        if shard.index != position:
            raise WorkbenchError(E_SHARD_MISMATCH,
                                 "shard indices must be contiguous starting at 0",
                                 {"position": position, "index": shard.index})
    return shards


def write_manifest(manifest_path: str | Path, shards: list[Shard]) -> None:
    payload = {
        "shards": [
            {"index": s.index, "filename": s.filename,
             "sentence_count": s.sentence_count, "token_count": s.token_count,
             "fingerprint": s.fingerprint}
            for s in shards
        ]
    }
    Path(manifest_path).write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
