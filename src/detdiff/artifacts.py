"""Versioned binary stage artifacts with canonical headers."""

from __future__ import annotations

import json
import struct
from dataclasses import dataclass
from typing import Any

import numpy as np

from .util import canonical_json_bytes


MAGIC = b"DETDIFF-ARTIFACT\x01\n"


@dataclass(frozen=True)
class Artifact:
    header: dict[str, Any]
    payload: bytes

    def to_bytes(self) -> bytes:
        header_bytes = canonical_json_bytes(self.header)
        return MAGIC + struct.pack("<Q", len(header_bytes)) + header_bytes + self.payload


def decode_artifact(data: bytes) -> Artifact:
    if not data.startswith(MAGIC) or len(data) < len(MAGIC) + 8:
        raise ValueError("invalid artifact magic or truncated header")
    offset = len(MAGIC)
    (header_length,) = struct.unpack("<Q", data[offset : offset + 8])
    offset += 8
    header_end = offset + header_length
    if header_end > len(data):
        raise ValueError("truncated artifact header")
    header = json.loads(data[offset:header_end].decode("utf-8"))
    if not isinstance(header, dict):
        raise ValueError("artifact header must be an object")
    return Artifact(header=header, payload=data[header_end:])


def embedding_artifact(shard_id: str, normalized_prompt: str, values: np.ndarray) -> bytes:
    payload = np.ascontiguousarray(values, dtype="<f8").tobytes()
    header = {
        "kind": "embedding",
        "schema_version": 1,
        "shard_id": shard_id,
        "normalized_prompt": normalized_prompt,
        "dtype": "float64-le",
        "shape": [int(values.size)],
    }
    return Artifact(header, payload).to_bytes()


def schedule_artifact(shard_id: str, weights: np.ndarray) -> bytes:
    payload = np.ascontiguousarray(weights, dtype="<f8").tobytes()
    header = {
        "kind": "schedule",
        "schema_version": 1,
        "shard_id": shard_id,
        "dtype": "float64-le",
        "shape": [int(weights.size)],
        "length": int(weights.size),
    }
    return Artifact(header, payload).to_bytes()


def sample_artifact(shard_id: str, tensor: np.ndarray, trajectory: list[dict[str, Any]]) -> bytes:
    payload = np.ascontiguousarray(tensor, dtype="<f8").tobytes()
    header = {
        "kind": "sample",
        "schema_version": 1,
        "shard_id": shard_id,
        "dtype": "float64-le",
        "shape": [int(item) for item in tensor.shape],
        "trajectory": trajectory,
        "trajectory_length": len(trajectory),
    }
    return Artifact(header, payload).to_bytes()


def quantize_artifact(
    shard_id: str,
    png_bytes: bytes,
    *,
    shape: tuple[int, ...],
    pre_clamp_count: int,
    post_clamp_count: int,
) -> bytes:
    header = {
        "kind": "quantize-png",
        "schema_version": 1,
        "shard_id": shard_id,
        "media_type": "image/png",
        "shape": [int(item) for item in shape],
        "pre_clamp_count": int(pre_clamp_count),
        "post_clamp_count": int(post_clamp_count),
    }
    return Artifact(header, png_bytes).to_bytes()


def embedding_from_bytes(data: bytes) -> tuple[np.ndarray, dict[str, Any]]:
    artifact = decode_artifact(data)
    if artifact.header.get("kind") != "embedding":
        raise ValueError("not an embedding artifact")
    shape = tuple(int(item) for item in artifact.header["shape"])
    values = np.frombuffer(artifact.payload, dtype="<f8").reshape(shape).copy()
    return values, artifact.header


def schedule_from_bytes(data: bytes) -> tuple[np.ndarray, dict[str, Any]]:
    artifact = decode_artifact(data)
    if artifact.header.get("kind") != "schedule":
        raise ValueError("not a schedule artifact")
    shape = tuple(int(item) for item in artifact.header["shape"])
    values = np.frombuffer(artifact.payload, dtype="<f8").reshape(shape).copy()
    return values, artifact.header


def sample_from_bytes(data: bytes) -> tuple[np.ndarray, dict[str, Any]]:
    artifact = decode_artifact(data)
    if artifact.header.get("kind") != "sample":
        raise ValueError("not a sample artifact")
    shape = tuple(int(item) for item in artifact.header["shape"])
    values = np.frombuffer(artifact.payload, dtype="<f8").reshape(shape).copy()
    return values, artifact.header


def quantize_from_bytes(data: bytes) -> tuple[bytes, dict[str, Any]]:
    artifact = decode_artifact(data)
    if artifact.header.get("kind") != "quantize-png":
        raise ValueError("not a quantize artifact")
    return artifact.payload, artifact.header

