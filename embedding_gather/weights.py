from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import numpy as np

from .digest import array_digest, json_scalar, load_npz, sha256_json
from .errors import (
    E_CONTENT_DIGEST,
    E_DIM_MISMATCH,
    E_MANIFEST_INVALID,
    fail,
)
from .model import ActualShardFacts, IndexProject, ShardSpec, normalize_pad_ranges


def normalize_scale(value: Any) -> float:
    scalar = json_scalar(value)
    if isinstance(scalar, bool):
        raise fail(E_MANIFEST_INVALID, "scale must be numeric", scale=scalar)
    try:
        return float(scalar)
    except (TypeError, ValueError) as exc:
        raise fail(E_MANIFEST_INVALID, "scale must be numeric", scale=scalar) from exc


def weight_segment_digest(
    weights: np.ndarray,
    scale: float,
    *,
    row_start: int,
    row_end: int,
    row_offset: int,
    embedding_dim: int,
    pad_ranges: tuple[tuple[int, int], ...],
    pad_marker: float,
    quantization_bits: int,
) -> str:
    return sha256_json(
        {
            "array": array_digest(weights),
            "scale": float(scale).hex(),
            "row_start": int(row_start),
            "row_end": int(row_end),
            "row_offset": int(row_offset),
            "embedding_dim": int(embedding_dim),
            "pad_ranges": [list(item) for item in pad_ranges],
            "pad_marker": float(pad_marker).hex(),
            "quantization_bits": int(quantization_bits),
        }
    )


def weight_content_digest(weights: np.ndarray, scale: float) -> str:
    return sha256_json({"array": array_digest(weights), "scale": float(scale).hex()})


def read_shard_facts(path: str | Path, shard_id: str) -> ActualShardFacts:
    arrays = load_npz(path)
    required = {"weights_q", "scale", "row_start", "row_end", "row_offset", "embedding_dim"}
    missing = required.difference(arrays)
    if missing:
        raise fail(
            E_MANIFEST_INVALID,
            "weight shard is missing required arrays",
            shard_id=shard_id,
            missing=sorted(missing),
            path=str(path),
        )
    weights = np.asarray(arrays["weights_q"])
    pad_ranges = normalize_pad_ranges(arrays.get("pad_ranges"))
    bits = int(json_scalar(arrays.get("quantization_bits", 12)))
    dim = int(json_scalar(arrays["embedding_dim"]))
    digest = weight_segment_digest(
        weights,
        normalize_scale(arrays["scale"]),
        row_start=int(json_scalar(arrays["row_start"])),
        row_end=int(json_scalar(arrays["row_end"])),
        row_offset=int(json_scalar(arrays["row_offset"])),
        embedding_dim=dim,
        pad_ranges=pad_ranges,
        pad_marker=float(json_scalar(arrays.get("pad_marker", 1.0))),
        quantization_bits=bits,
    )
    return ActualShardFacts(
        shard_id=shard_id,
        embedding_dim=dim,
        row_start=int(json_scalar(arrays["row_start"])),
        row_end=int(json_scalar(arrays["row_end"])),
        row_offset=int(json_scalar(arrays["row_offset"])),
        segment_digest=digest,
        pad_ranges=pad_ranges,
        weight_file=str(path),
    )


def load_manifest_facts(
    manifest: Mapping[str, Any],
    root: str | Path,
) -> dict[str, ActualShardFacts]:
    root_path = Path(root)
    raw_shards = manifest.get("shards")
    if not isinstance(raw_shards, list):
        raise fail(E_MANIFEST_INVALID, "manifest shards must be a list")
    facts: dict[str, ActualShardFacts] = {}
    for raw in raw_shards:
        if not isinstance(raw, dict):
            raise fail(E_MANIFEST_INVALID, "manifest shard must be an object")
        shard_id = str(raw.get("shard_id", ""))
        weight_file = str(raw.get("weight_file", ""))
        if not shard_id or not weight_file:
            raise fail(E_MANIFEST_INVALID, "manifest shard must declare shard_id and weight_file", shard_id=shard_id)
        path = Path(weight_file)
        if not path.is_absolute():
            path = root_path / path
        facts[shard_id] = read_shard_facts(path, shard_id)
    return facts


@dataclass(frozen=True)
class WeightShard:
    spec: ShardSpec
    weights_q: np.ndarray
    scale: float
    quantization_bits: int
    content_digest: str


@dataclass(frozen=True)
class WeightTable:
    project: IndexProject
    shards: Mapping[str, WeightShard]

    def resolve(self, row: int) -> tuple[ShardSpec | None, int | None]:
        shard = self.project.find_shard(row)
        if shard is None:
            return None, None
        return shard, row - shard.row_offset

    def content_digests(self) -> dict[str, str]:
        return {shard_id: shard.content_digest for shard_id, shard in sorted(self.shards.items())}


def _validate_shard_array(shard: ShardSpec, weights: np.ndarray, scale: float) -> None:
    if weights.ndim != 2:
        raise fail(
            E_DIM_MISMATCH,
            "weight shard array must be two-dimensional",
            shard_id=shard.shard_id,
            shape=list(weights.shape),
        )
    if int(weights.shape[1]) != shard.embedding_dim:
        raise fail(
            E_DIM_MISMATCH,
            "weight array embedding dimension differs from manifest",
            shard_id=shard.shard_id,
            index_id=None,
            actual_dim=int(weights.shape[1]),
            declared_dim=shard.embedding_dim,
        )
    required_rows = shard.row_end - shard.row_offset
    if weights.shape[0] < required_rows:
        raise fail(
            E_DIM_MISMATCH,
            "weight array does not contain all rows declared by the shard",
            shard_id=shard.shard_id,
            index_id=None,
            weight_rows=int(weights.shape[0]),
            required_rows=required_rows,
            row_offset=shard.row_offset,
            row_end=shard.row_end,
        )


def load_weight_table(
    project: IndexProject,
    root: str | Path,
    *,
    verify_segment_digest: bool = True,
    verify_metadata: bool = True,
) -> WeightTable:
    project.validate()
    root_path = Path(root)
    shards: dict[str, WeightShard] = {}
    for spec in project.shards:
        path = Path(spec.weight_file)
        if not path.is_absolute():
            path = root_path / path
        arrays = load_npz(path)
        missing = {"weights_q", "scale"}.difference(arrays)
        if missing:
            raise fail(
                E_MANIFEST_INVALID,
                "weight shard is missing required arrays",
                shard_id=spec.shard_id,
                missing=sorted(missing),
                path=str(path),
            )
        weights = np.asarray(arrays["weights_q"])
        scale = normalize_scale(arrays["scale"])
        bits = int(json_scalar(arrays.get("quantization_bits", project.quantization_bits)))
        if verify_metadata and bits != project.quantization_bits:
            raise fail(
                E_MANIFEST_INVALID,
                "weight shard quantization width differs from manifest",
                shard_id=spec.shard_id,
                index_id=None,
                shard_quantization_bits=bits,
                manifest_quantization_bits=project.quantization_bits,
            )
        if verify_metadata:
            file_embedding_dim = int(json_scalar(arrays.get("embedding_dim", spec.embedding_dim)))
            file_row_start = int(json_scalar(arrays.get("row_start", spec.row_start)))
            file_row_end = int(json_scalar(arrays.get("row_end", spec.row_end)))
            if file_embedding_dim != spec.embedding_dim:
                raise fail(
                    E_DIM_MISMATCH,
                    "weight shard embedding_dim metadata differs from manifest",
                    shard_id=spec.shard_id,
                    index_id=None,
                    file_embedding_dim=file_embedding_dim,
                    manifest_embedding_dim=spec.embedding_dim,
                )
            if file_row_start != spec.row_start or file_row_end != spec.row_end:
                raise fail(
                    E_CONTENT_DIGEST,
                    "weight shard row range metadata differs from manifest",
                    shard_id=spec.shard_id,
                    index_id=None,
                    manifest_row_range=[spec.row_start, spec.row_end],
                    file_row_range=[file_row_start, file_row_end],
                )
        _validate_shard_array(spec, weights, scale)
        file_pad = normalize_pad_ranges(arrays.get("pad_ranges"))
        if verify_metadata and file_pad != tuple(spec.pad_ranges):
            raise fail(
                E_CONTENT_DIGEST,
                "weight shard pad metadata differs from manifest",
                shard_id=spec.shard_id,
                manifest_pad_ranges=[list(item) for item in spec.pad_ranges],
                file_pad_ranges=[list(item) for item in file_pad],
            )
        file_offset = int(json_scalar(arrays.get("row_offset", spec.row_offset)))
        if verify_metadata and file_offset != spec.row_offset:
            raise fail(
                E_CONTENT_DIGEST,
                "weight shard row offset differs from manifest",
                shard_id=spec.shard_id,
                manifest_row_offset=spec.row_offset,
                file_row_offset=file_offset,
            )
        segment_digest = weight_segment_digest(
            weights,
            scale,
            row_start=spec.row_start,
            row_end=spec.row_end,
            row_offset=spec.row_offset,
            embedding_dim=spec.embedding_dim,
            pad_ranges=tuple(spec.pad_ranges),
            pad_marker=spec.pad_marker,
            quantization_bits=bits,
        )
        if verify_segment_digest and spec.segment_digest != segment_digest:
            raise fail(
                E_CONTENT_DIGEST,
                "weight shard digest does not match manifest",
                shard_id=spec.shard_id,
                manifest_digest=spec.segment_digest,
                actual_digest=segment_digest,
            )
        content_digest = weight_content_digest(weights, scale)
        shards[spec.shard_id] = WeightShard(
            spec=spec,
            weights_q=weights,
            scale=scale,
            quantization_bits=bits,
            content_digest=content_digest,
        )
    return WeightTable(project=project, shards=shards)
