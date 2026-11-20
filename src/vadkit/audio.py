"""WAV, manifest, and dataset validation."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
from scipy.io import wavfile

from .errors import VadError, input_length, invalid_config
from .util import sha256_canonical


def pcm_fingerprint(samples: np.ndarray) -> str:
    raw = samples.astype("<i2", copy=False).tobytes()
    return hashlib.sha256(raw).hexdigest()


@dataclass(frozen=True)
class ChunkData:
    chunk_id: str
    order: int
    path: Path
    samples: np.ndarray
    sample_count: int
    reference_endpoints_ms: tuple[tuple[float, float], ...]
    conditions: tuple[str, ...]
    input_sha256: str

    def reference_dicts(self) -> list[dict[str, float]]:
        return [
            {"start_ms": float(start), "end_ms": float(end)}
            for start, end in self.reference_endpoints_ms
        ]


@dataclass(frozen=True)
class Dataset:
    manifest_path: Path
    sample_rate: int
    chunks: tuple[ChunkData, ...]
    fingerprint: str
    manifest: dict[str, Any]


def read_wav(
    path: str | Path,
    *,
    chunk_id: str | None = None,
    expected_sample_count: int | None = None,
) -> tuple[np.ndarray, int]:
    source = Path(path)
    try:
        sample_rate, samples = wavfile.read(source)
    except (OSError, ValueError) as exc:
        raise input_length(
            f"unable to read PCM WAV: {exc}",
            chunk_id=chunk_id or source.stem,
            sample_position=0,
            path=str(source),
        ) from exc
    array = np.asarray(samples)
    if array.ndim != 1:
        raise VadError(
            "E_FEATURE_MISMATCH",
            "WAV input must be mono",
            chunk_id=chunk_id or source.stem,
            sample_position=0,
            details={"shape": list(array.shape)},
        )
    if array.dtype != np.int16:
        raise VadError(
            "E_FEATURE_MISMATCH",
            "WAV input must be 16-bit PCM",
            chunk_id=chunk_id or source.stem,
            sample_position=0,
            details={"dtype": str(array.dtype)},
        )
    if expected_sample_count is not None and int(array.shape[0]) != expected_sample_count:
        raise input_length(
            "declared sample count does not match WAV contents",
            chunk_id=chunk_id or source.stem,
            sample_position=min(int(array.shape[0]), expected_sample_count),
            declared_samples=expected_sample_count,
            actual_samples=int(array.shape[0]),
            path=str(source),
        )
    return array, int(sample_rate)


def _load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise input_length(
            f"unable to read manifest JSON: {exc}", chunk_id="__manifest__", sample_position=0
        ) from exc
    if not isinstance(value, dict):
        raise input_length(
            "manifest root must be a JSON object", chunk_id="__manifest__", sample_position=0
        )
    return value


def load_dataset(manifest_path: str | Path) -> Dataset:
    path = Path(manifest_path)
    manifest = _load_json(path)
    if manifest.get("schema_version") != "vadkit.dataset.v1":
        raise invalid_config(
            "unsupported dataset schema", schema_version=manifest.get("schema_version")
        )
    sample_rate = int(manifest.get("sample_rate", 0))
    if sample_rate != 16_000:
        raise invalid_config("dataset sample rate must be 16000", sample_rate=sample_rate)
    raw_chunks = manifest.get("chunks")
    if not isinstance(raw_chunks, list) or not raw_chunks:
        raise input_length(
            "manifest must declare at least one chunk", chunk_id="__manifest__", sample_position=0
        )

    chunks: list[ChunkData] = []
    seen_ids: set[str] = set()
    seen_orders: set[int] = set()
    for entry in raw_chunks:
        if not isinstance(entry, dict):
            raise input_length(
                "each manifest chunk must be an object",
                chunk_id="__manifest__",
                sample_position=0,
            )
        chunk_id = str(entry.get("chunk_id", ""))
        if not chunk_id:
            raise invalid_config("chunk_id is required for every chunk")
        if chunk_id in seen_ids:
            raise invalid_config("duplicate chunk_id in manifest", chunk_id=chunk_id)
        seen_ids.add(chunk_id)
        order = int(entry.get("order", -1))
        if order < 0 or order in seen_orders:
            raise invalid_config("chunk order values must be unique and non-negative", chunk_id=chunk_id)
        seen_orders.add(order)
        declared = int(entry.get("sample_count", -1))
        if declared < 0:
            raise input_length(
                "manifest sample_count must be non-negative",
                chunk_id=chunk_id,
                sample_position=0,
                declared_samples=declared,
            )
        wav_path = path.parent / str(entry.get("path", ""))
        samples, wav_rate = read_wav(
            wav_path,
            chunk_id=chunk_id,
            expected_sample_count=declared,
        )
        if wav_rate != sample_rate:
            raise invalid_config(
                "WAV sample rate differs from manifest",
                chunk_id=chunk_id,
                manifest_sample_rate=sample_rate,
                wav_sample_rate=wav_rate,
            )
        references: list[tuple[float, float]] = []
        for pair in entry.get("reference_endpoints_ms", []):
            if (
                not isinstance(pair, list)
                or len(pair) != 2
                or float(pair[0]) < 0
                or float(pair[1]) <= float(pair[0])
            ):
                raise invalid_config(
                    "reference endpoints must be ordered [start_ms, end_ms]",
                    chunk_id=chunk_id,
                )
            references.append((float(pair[0]), float(pair[1])))
        references.sort()
        conditions = tuple(str(item) for item in entry.get("conditions", []))
        chunks.append(
            ChunkData(
                chunk_id=chunk_id,
                order=order,
                path=wav_path,
                samples=samples,
                sample_count=int(samples.shape[0]),
                reference_endpoints_ms=tuple(references),
                conditions=conditions,
                input_sha256=pcm_fingerprint(samples),
            )
        )

    chunks.sort(key=lambda chunk: chunk.order)
    if [chunk.order for chunk in chunks] != list(range(len(chunks))):
        raise invalid_config("chunk order must be contiguous from zero")
    actual_total = sum(chunk.sample_count for chunk in chunks)
    declared_total = int(manifest.get("total_declared_samples", actual_total))
    if declared_total != actual_total:
        raise input_length(
            "dataset total_declared_samples does not match chunk contents",
            chunk_id=chunks[-1].chunk_id,
            sample_position=actual_total,
            declared_samples=declared_total,
            actual_samples=actual_total,
        )

    fingerprint_data = {
        "schema": "vadkit.dataset-fingerprint.v1",
        "sample_rate": sample_rate,
        "chunks": [
            {
                "chunk_id": chunk.chunk_id,
                "order": chunk.order,
                "sample_count": chunk.sample_count,
                "input_sha256": chunk.input_sha256,
                "reference_endpoints_ms": chunk.reference_endpoints_ms,
            }
            for chunk in chunks
        ],
    }
    return Dataset(
        manifest_path=path,
        sample_rate=sample_rate,
        chunks=tuple(chunks),
        fingerprint=sha256_canonical(fingerprint_data),
        manifest=manifest,
    )

