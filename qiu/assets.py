"""Asset loading with content-based change detection."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .io import sha256_bytes


@dataclass(frozen=True)
class AssetSnapshot:
    config_bytes: bytes
    training_bytes: bytes
    dictionary_bytes: bytes | None
    config: dict[str, Any]
    training: list[dict[str, Any]]
    dictionary: dict[str, Any] | None
    dictionary_error: str | None

    @property
    def signature(self) -> tuple[str, str, str]:
        return (
            sha256_bytes(self.config_bytes),
            sha256_bytes(self.training_bytes),
            sha256_bytes(self.dictionary_bytes) if self.dictionary_bytes is not None else "missing",
        )

    @property
    def digests(self) -> dict[str, str]:
        config_digest, training_digest, dictionary_digest = self.signature
        return {
            "config_file": config_digest,
            "dictionary_file": dictionary_digest,
            "training_file": training_digest,
        }


class AssetStore:
    def __init__(self, *, config_path: str | Path, training_path: str | Path, dictionary_path: str | Path) -> None:
        self.config_path = Path(config_path)
        self.training_path = Path(training_path)
        self.dictionary_path = Path(dictionary_path)

    def read(self) -> AssetSnapshot:
        config_bytes = self.config_path.read_bytes()
        training_bytes = self.training_path.read_bytes()
        dictionary_bytes = self.dictionary_path.read_bytes() if self.dictionary_path.exists() else None

        config = _parse_json_object(config_bytes, self.config_path)
        training = _parse_jsonl(training_bytes, self.training_path)
        dictionary: dict[str, Any] | None = None
        dictionary_error: str | None = None
        if dictionary_bytes is None:
            dictionary_error = "dictionary_not_found"
        else:
            try:
                value = json.loads(dictionary_bytes.decode("utf-8"))
                if not isinstance(value, dict):
                    raise ValueError("dictionary root is not an object")
                dictionary = value
            except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
                dictionary_error = f"dictionary_invalid_json: {exc}"

        return AssetSnapshot(
            config_bytes=config_bytes,
            training_bytes=training_bytes,
            dictionary_bytes=dictionary_bytes,
            config=config,
            training=training,
            dictionary=dictionary,
            dictionary_error=dictionary_error,
        )


def _parse_json_object(data: bytes, path: Path) -> dict[str, Any]:
    value = json.loads(data.decode("utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return value


def _parse_jsonl(data: bytes, path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for line_number, line in enumerate(data.decode("utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        value = json.loads(line)
        if not isinstance(value, dict):
            raise ValueError(f"{path}:{line_number} must contain a JSON object")
        records.append(value)
    return records
