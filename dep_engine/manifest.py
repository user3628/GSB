"""Explicit shard manifest loading, validation and atomic updates."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

from .canonical import atomic_write_json, sha256_file, sha256_obj
from .constants import STAGES
from .errors import EngineError
from .tokenize import split_sentences, tokenize


@dataclass(frozen=True)
class ShardEntry:
    index: int
    file: str
    sentence_count: int
    token_count: int
    sha256: str | None = None

    def as_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "index": self.index,
            "file": self.file,
            "sentence_count": self.sentence_count,
            "token_count": self.token_count,
        }
        if self.sha256:
            result["sha256"] = self.sha256
        return result


class Manifest:
    def __init__(self, root: Path, path: Path, raw: dict[str, Any]):
        self.root = root.resolve()
        self.path = path
        self.raw = raw
        self.shards = self._parse_shards(raw)

    @classmethod
    def load(cls, root: Path, relative_path: str = "data/manifest.json") -> "Manifest":
        path = (root / relative_path).resolve()
        try:
            import json

            with path.open("r", encoding="utf-8") as handle:
                raw = json.load(handle)
        except FileNotFoundError as exc:
            raise EngineError(
                "E_INVALID_STAGE",
                f"manifest file not found: {relative_path}",
                {"valid_stages": list(STAGES)},
            ) from exc
        except (OSError, ValueError) as exc:
            raise EngineError(
                "E_INVALID_STAGE",
                "manifest is not valid JSON",
                {"error": str(exc), "valid_stages": list(STAGES)},
            ) from exc
        if not isinstance(raw, dict):
            raise EngineError(
                "E_INVALID_STAGE",
                "manifest must be a JSON object",
                {"valid_stages": list(STAGES)},
            )
        return cls(root, path, raw)

    def _parse_shards(self, raw: dict[str, Any]) -> list[ShardEntry]:
        shards_raw = raw.get("shards")
        if not isinstance(shards_raw, list) or not shards_raw:
            raise EngineError(
                "E_INVALID_STAGE",
                "manifest.shards must be a non-empty list",
                {"valid_stages": list(STAGES)},
            )
        entries: list[ShardEntry] = []
        seen_files: set[str] = set()
        for expected_index, item in enumerate(shards_raw):
            if not isinstance(item, dict):
                raise EngineError(
                    "E_INVALID_STAGE",
                    "each manifest shard entry must be an object",
                    {"valid_stages": list(STAGES)},
                )
            index = item.get("index", expected_index)
            file_value = item.get("file")
            sentence_count = item.get("sentence_count")
            token_count = item.get("token_count")
            if index != expected_index:
                raise EngineError(
                    "E_INVALID_STAGE",
                    "manifest shard indices must be contiguous from zero",
                    {
                        "expected_index": expected_index,
                        "actual_index": index,
                        "valid_stages": list(STAGES),
                    },
                )
            if not isinstance(file_value, str) or not file_value:
                raise EngineError(
                    "E_INVALID_STAGE",
                    "manifest shard file must be a non-empty relative path",
                    {"shard_index": index, "valid_stages": list(STAGES)},
                )
            pure = PurePosixPath(file_value)
            if pure.is_absolute() or ".." in pure.parts:
                raise EngineError(
                    "E_INVALID_STAGE",
                    "manifest shard path must stay inside the workspace",
                    {"shard_index": index, "file": file_value, "valid_stages": list(STAGES)},
                )
            resolved = (self.root / file_value).resolve()
            if self.root not in resolved.parents and resolved != self.root:
                raise EngineError(
                    "E_INVALID_STAGE",
                    "manifest shard path escapes the workspace",
                    {"shard_index": index, "file": file_value, "valid_stages": list(STAGES)},
                )
            if file_value in seen_files:
                raise EngineError(
                    "E_INVALID_STAGE",
                    "manifest contains duplicate shard file paths",
                    {"shard_index": index, "file": file_value, "valid_stages": list(STAGES)},
                )
            seen_files.add(file_value)
            if (
                not isinstance(sentence_count, int)
                or isinstance(sentence_count, bool)
                or sentence_count < 0
            ):
                raise EngineError(
                    "E_INVALID_STAGE",
                    "shard sentence_count must be a non-negative integer",
                    {"shard_index": index, "valid_stages": list(STAGES)},
                )
            if (
                not isinstance(token_count, int)
                or isinstance(token_count, bool)
                or token_count < 0
            ):
                raise EngineError(
                    "E_INVALID_STAGE",
                    "shard token_count must be a non-negative integer",
                    {"shard_index": index, "valid_stages": list(STAGES)},
                )
            digest = item.get("sha256")
            if digest is not None and not isinstance(digest, str):
                raise EngineError(
                    "E_INVALID_STAGE",
                    "shard sha256 must be a string when present",
                    {"shard_index": index, "valid_stages": list(STAGES)},
                )
            entries.append(
                ShardEntry(
                    index=index,
                    file=file_value,
                    sentence_count=sentence_count,
                    token_count=token_count,
                    sha256=digest,
                )
            )
        return entries

    @property
    def shard_count(self) -> int:
        return len(self.shards)

    def entry(self, index: int) -> ShardEntry:
        if not isinstance(index, int) or index < 0 or index >= len(self.shards):
            raise EngineError(
                "E_INVALID_STAGE",
                "shard index is out of range",
                {
                    "shard_index": index,
                    "shard_count": len(self.shards),
                    "valid_stages": list(STAGES),
                },
            )
        return self.shards[index]

    def shard_path(self, index: int) -> Path:
        return (self.root / self.entry(index).file).resolve()

    def shard_text(self, index: int) -> str:
        try:
            return self.shard_path(index).read_text(encoding="utf-8")
        except FileNotFoundError as exc:
            raise EngineError(
                "E_INVALID_STAGE",
                "shard file is missing",
                {
                    "shard_index": index,
                    "file": self.entry(index).file,
                    "valid_stages": list(STAGES),
                },
            ) from exc

    def shard_content_digest(self, index: int) -> str:
        return sha256_file(self.shard_path(index))

    def shard_name(self, index: int) -> str:
        return self.entry(index).file

    def shard_names(self) -> dict[int, str]:
        return {entry.index: entry.file for entry in self.shards}

    def manifest_fingerprint(self) -> str:
        return sha256_obj(self.raw)

    def entry_fingerprint(self, index: int) -> str:
        return sha256_obj(self.entry(index).as_dict())

    def measured_counts(self, index: int) -> tuple[int, int]:
        text = self.shard_text(index)
        lines = split_sentences(text)
        token_count = sum(len(tokenize(line)) for line in lines)
        return len(lines), token_count

    def to_dict(self) -> dict[str, Any]:
        return self.raw

    def write(self, raw: dict[str, Any] | None = None) -> None:
        if raw is not None:
            self.raw = raw
            self.shards = self._parse_shards(raw)
        atomic_write_json(self.path, self.raw)

    def replace_shard_text(self, index: int, text: str) -> dict[str, Any]:
        """Atomically write one shard and its updated manifest.

        The shard file and manifest are each atomically replaced.  The content
        and manifest fingerprints are both part of featureization input, so a
        crash between the two replacements still invalidates the correct stage
        on the next run and is not mistaken for a complete checkpoint.
        """

        entry = self.entry(index)
        shard_path = self.shard_path(index)
        shard_path.parent.mkdir(parents=True, exist_ok=True)
        from .canonical import atomic_write_bytes

        data = text.encode("utf-8")
        atomic_write_bytes(shard_path, data)
        lines = split_sentences(text)
        token_count = sum(len(tokenize(line)) for line in lines)
        digest = sha256_file(shard_path)
        new_raw = dict(self.raw)
        shards = [dict(item) for item in self.raw.get("shards", [])]
        updated = dict(shards[index])
        updated.update(
            {
                "index": entry.index,
                "file": entry.file,
                "sentence_count": len(lines),
                "token_count": token_count,
                "sha256": digest,
            }
        )
        shards[index] = updated
        new_raw["shards"] = shards
        new_raw["total_sentences"] = sum(int(item["sentence_count"]) for item in shards)
        new_raw["total_tokens"] = sum(int(item["token_count"]) for item in shards)
        self.write(new_raw)
        return updated
