from __future__ import annotations

import hashlib
import json
import wave
from pathlib import Path
from typing import Any, Protocol

from .errors import WorkbenchError
from .models import ChunkInfo, Word, validate_timeline
from .config import WorkbenchConfig


class TranscriptionAdapter(Protocol):
    """Replaceable boundary between audio and the unified word timeline."""

    name: str

    def transcribe(
        self,
        *,
        chunk_id: str,
        audio_path: Path,
        duration_ms: int,
        reference_text: str,
        language: str,
        source_version: int,
    ) -> list[Word]:
        ...


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def read_wav_info(path: Path) -> dict[str, int]:
    try:
        with wave.open(str(path), "rb") as handle:
            channels = handle.getnchannels()
            sample_width = handle.getsampwidth()
            sample_rate = handle.getframerate()
            frame_count = handle.getnframes()
            compression = handle.getcomptype()
    except (OSError, wave.Error) as exc:
        raise WorkbenchError(
            "E_INVALID_REQUEST",
            "Audio chunk is not a readable WAV file",
            {"path": str(path), "reason": str(exc)},
        ) from exc
    if (channels, sample_width, sample_rate, compression) != (1, 2, 16_000, "NONE"):
        raise WorkbenchError(
            "E_INVALID_REQUEST",
            "WAV chunk must be 16 kHz, mono, 16-bit PCM",
            {
                "path": str(path),
                "channels": channels,
                "sample_width_bytes": sample_width,
                "sample_rate_hz": sample_rate,
                "compression": compression,
            },
        )
    return {
        "channels": channels,
        "sample_width": sample_width,
        "sample_rate": sample_rate,
        "frame_count": frame_count,
        "duration_ms": int(round(frame_count * 1000 / sample_rate)),
    }


def load_manifest(manifest_path: Path) -> tuple[dict[str, Any], dict[str, ChunkInfo]]:
    manifest_path = manifest_path.resolve()
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise WorkbenchError(
            "E_INVALID_REQUEST",
            "Task manifest is not readable JSON",
            {"path": str(manifest_path), "reason": str(exc)},
        ) from exc
    if not isinstance(manifest, dict) or not isinstance(manifest.get("chunks"), list):
        raise WorkbenchError(
            "E_INVALID_REQUEST",
            "Task manifest must contain a chunks array",
            {"path": str(manifest_path)},
        )
    root = manifest_path.parent
    chunks: dict[str, ChunkInfo] = {}
    orders: set[int] = set()
    for index, item in enumerate(manifest["chunks"]):
        if not isinstance(item, dict):
            raise WorkbenchError(
                "E_INVALID_REQUEST",
                "Manifest chunk entries must be objects",
                {"entry_index": index},
            )
        required = ("chunk_id", "file", "reference_text", "language")
        missing = [key for key in required if not item.get(key)]
        if missing:
            raise WorkbenchError(
                "E_INVALID_REQUEST",
                "Manifest chunk is missing required fields",
                {"entry_index": index, "missing": missing},
            )
        chunk_id = str(item["chunk_id"])
        if chunk_id in chunks:
            raise WorkbenchError(
                "E_INVALID_REQUEST",
                "Duplicate chunk_id in manifest",
                {"chunk_id": chunk_id},
            )
        order = int(item.get("order", index + 1))
        if order in orders:
            raise WorkbenchError(
                "E_INVALID_REQUEST",
                "Duplicate chunk order in manifest",
                {"order": order},
            )
        orders.add(order)
        audio_path = (root / str(item["file"])).resolve()
        info = read_wav_info(audio_path)
        declared_duration = item.get("duration_ms")
        if declared_duration is not None and abs(int(declared_duration) - info["duration_ms"]) > 2:
            raise WorkbenchError(
                "E_INVALID_REQUEST",
                "Manifest duration_ms disagrees with the WAV duration",
                {
                    "chunk_id": chunk_id,
                    "declared_duration_ms": int(declared_duration),
                    "actual_duration_ms": info["duration_ms"],
                },
            )
        chunks[chunk_id] = ChunkInfo(
            chunk_id=chunk_id,
            order=order,
            file=str(item["file"]),
            path=str(audio_path),
            duration_ms=info["duration_ms"],
            language=str(item["language"]),
            reference_text=str(item["reference_text"]),
            fingerprint=file_sha256(audio_path),
        )
    if not chunks:
        raise WorkbenchError(
            "E_INVALID_REQUEST", "Task manifest must contain at least one chunk"
        )
    ordered = sorted(chunks.values(), key=lambda c: (c.order, c.chunk_id))
    for expected_order, chunk in enumerate(ordered, start=1):
        if chunk.order != expected_order:
            raise WorkbenchError(
                "E_INVALID_REQUEST",
                "Chunk order must be contiguous and start at 1",
                {"chunk_id": chunk.chunk_id, "order": chunk.order},
            )
    return manifest, chunks


class StubTranscriptionAdapter:
    """Deterministic reference-text adapter; no model or network is used."""

    name = "deterministic-reference-stub-v1"

    def __init__(self, config: WorkbenchConfig = WorkbenchConfig()) -> None:
        self.config = config

    @staticmethod
    def tokenize(text: str) -> list[str]:
        import re

        parts = re.findall(
            r"[A-Za-z0-9]+(?:['’\-][A-Za-z0-9]+)*|[\u3400-\u9fff]|[^\s]",
            text,
        )
        tokens: list[str] = []
        for part in parts:
            if part and not part[0].isalnum() and not ("\u3400" <= part[0] <= "\u9fff"):
                if tokens:
                    tokens[-1] += part
                else:
                    tokens.append(part)
            else:
                tokens.append(part)
        return tokens

    def transcribe(
        self,
        *,
        chunk_id: str,
        audio_path: Path,
        duration_ms: int,
        reference_text: str,
        language: str,
        source_version: int,
    ) -> list[Word]:
        del audio_path
        tokens = self.tokenize(reference_text)
        if not tokens:
            raise WorkbenchError(
                "E_INVALID_REQUEST",
                "Reference text yields no deterministic tokens",
                {"chunk_id": chunk_id},
            )
        margin_ms = 60
        gap_ms = 12
        minimum_word_ms = 24
        available = duration_ms - 2 * margin_ms - gap_ms * (len(tokens) - 1)
        if available < minimum_word_ms * len(tokens):
            raise WorkbenchError(
                "E_INVALID_REQUEST",
                "Chunk is too short for deterministic word timing",
                {"chunk_id": chunk_id, "duration_ms": duration_ms, "tokens": len(tokens)},
            )
        weights = [max(1, len(token)) for token in tokens]
        extra = available - minimum_word_ms * len(tokens)
        total_weight = sum(weights)
        durations = [minimum_word_ms for _ in tokens]
        allocated = 0
        for index, weight in enumerate(weights):
            share = int(extra * weight / total_weight)
            durations[index] += share
            allocated += share
        for index in range(extra - allocated):
            durations[index % len(tokens)] += 1

        words: list[Word] = []
        cursor = margin_ms
        for index, (token, word_duration) in enumerate(zip(tokens, durations)):
            confidence_digest = hashlib.sha256(
                f"{chunk_id}:{index}:{token}".encode("utf-8")
            ).hexdigest()
            confidence = round(
                0.84 + (int(confidence_digest[:8], 16) % 1200) / 10_000.0,
                self.config.confidence_decimals,
            )
            entry_id = (
                f"w-{chunk_id}-{index:04d}-"
                f"{hashlib.sha256(f'{token}:{cursor}:{word_duration}'.encode('utf-8')).hexdigest()[:8]}"
            )
            words.append(
                Word(
                    entry_id=entry_id,
                    chunk_id=chunk_id,
                    start_ms=cursor,
                    end_ms=cursor + word_duration,
                    text=token,
                    confidence=confidence,
                    language=language,
                    source_version=source_version,
                )
            )
            cursor += word_duration + gap_ms
        validate_timeline(
            words,
            {chunk_id: ChunkInfo(chunk_id=chunk_id, order=1, file="", path="", duration_ms=duration_ms, language=language, reference_text=reference_text, fingerprint="")},
            self.config,
        )
        return words
