"""Deterministic normalized feature hashing with explicit missing indicators."""

from __future__ import annotations

import bisect
import hashlib
import json
import math
import re
import unicodedata
from dataclasses import dataclass
from typing import Any, Iterable

from .config import Config
from .data import DataBundle, LoadedShard
from .errors import ErrorCode, PipelineError
from .util import canonical_hash, canonical_json_bytes


FEATURE_FORMAT = "ctr-cvr-hashed-features-v1"
HASH_ALGORITHM = "blake2b-64-mod-v1"
NORMALIZATION = "unicode-nfkc/collapse-whitespace/lowercase"
SECTIONS = ("user_features", "item_features", "context_features")
TOP_LEVEL_FIELDS = ("source", "lang")
_WHITESPACE = re.compile(r"\s+")


@dataclass
class RowFeatures:
    impression_id: str
    shard_id: str
    shard_index: int
    row_number: int
    indices: list[int]
    values: list[int]
    bins: dict[str, int | None]
    missing: list[str]
    tokens: list[str]
    fingerprint: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "impression_id": self.impression_id,
            "row_number": self.row_number,
            "indices": self.indices,
            "values": self.values,
            "bins": self.bins,
            "missing": self.missing,
            "tokens": self.tokens,
            "fingerprint": self.fingerprint,
        }


@dataclass
class ShardFeatures:
    shard_id: str
    shard_index: int
    content_digest: str
    rows: list[RowFeatures]
    fingerprint: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "shard_id": self.shard_id,
            "shard_index": self.shard_index,
            "content_sha256": self.content_digest,
            "fingerprint": self.fingerprint,
            "rows": [row.to_dict() for row in self.rows],
        }


@dataclass
class FeatureBundle:
    schema_fingerprint: str
    data_content_fingerprint: str
    shards: list[ShardFeatures]
    aggregate_fingerprint: str
    artifact: dict[str, Any]

    def by_id(self) -> dict[str, ShardFeatures]:
        return {shard.shard_id: shard for shard in self.shards}

    def artifact_bytes(self) -> bytes:
        return canonical_json_bytes(self.artifact) + b"\n"


def normalize_category(value: Any) -> str:
    text = unicodedata.normalize("NFKC", str(value))
    text = _WHITESPACE.sub(" ", text).strip()
    return text.lower()


def stable_hash_index(token: str, dimension: int) -> int:
    digest = hashlib.blake2b(
        token.encode("utf-8"),
        digest_size=8,
        person=b"ctrcvr01",
    ).digest()
    return int.from_bytes(digest, byteorder="big", signed=False) % dimension


def _split_path(path: str) -> list[str]:
    return [part for part in path.split(".") if part]


def _get_path(sample: dict[str, Any], path: str) -> tuple[bool, Any]:
    current: Any = sample
    for part in _split_path(path):
        if not isinstance(current, dict) or part not in current:
            return False, None
        current = current[part]
    return True, current


def _flatten_unknowns(value: Any, prefix: str) -> Iterable[tuple[str, Any]]:
    if isinstance(value, dict):
        for key in sorted(value):
            yield from _flatten_unknowns(value[key], f"{prefix}.{key}" if prefix else str(key))
    else:
        yield prefix, value


def _stable_scalar(value: Any) -> str:
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return "null"
    return str(value)


def _schema(config: Config) -> dict[str, Any]:
    return {
        "format": FEATURE_FORMAT,
        "hash": HASH_ALGORITHM,
        "normalization": NORMALIZATION,
        "dimension": config.feature_dimension,
        "numeric_bins": {key: config.numeric_bins[key] for key in sorted(config.numeric_bins)},
        "categorical_fields": config.categorical_fields,
        "missing_policy": "explicit_hashed_indicator",
    }


def feature_schema_fingerprint(config: Config) -> str:
    return canonical_hash(_schema(config))


def _assemble_row(
    row: dict[str, Any],
    shard: LoadedShard,
    config: Config,
    schema_fingerprint: str,
) -> RowFeatures:
    dimension = config.feature_dimension
    if dimension <= 0:
        raise PipelineError(
            ErrorCode.E_INVALID_PARAM,
            "feature dimension must be positive",
            stage="feature",
            shard_id=shard.info.shard_id,
            row_number=int(row.get("_row_number", 1)),
            impression_id=row.get("impression_id"),
            details={"feature_dimension": dimension},
        )

    numeric_bins = config.numeric_bins
    expected = set(numeric_bins) | set(config.categorical_fields)
    tokens: list[str] = []
    bins: dict[str, int | None] = {}
    missing: list[str] = []

    for path in sorted(expected):
        present, value = _get_path(row, path)
        if not present or value is None:
            missing.append(path)
            bins[path] = None if path in numeric_bins else None
            tokens.append(f"__MISSING__|{path}")
            continue
        if path in numeric_bins:
            valid_number = True
            try:
                if isinstance(value, bool):
                    raise ValueError("boolean is not numeric")
                number = float(value)
                if not math.isfinite(number):
                    raise ValueError("non-finite")
            except (TypeError, ValueError):
                valid_number = False
            if not valid_number:
                missing.append(path)
                bins[path] = None
                tokens.append(f"__MISSING__|{path}|invalid_numeric")
                continue
            bin_index = bisect.bisect_right(numeric_bins[path], number)
            bins[path] = bin_index
            tokens.append(f"{path}#bin={bin_index}")
        else:
            normalized = normalize_category(value)
            tokens.append(f"{path}={normalized}")

    unknown_leaves: dict[str, Any] = {}
    for section in SECTIONS:
        value = row.get(section)
        if isinstance(value, dict):
            for path, leaf in _flatten_unknowns(value, section):
                if path not in expected:
                    unknown_leaves[path] = leaf
    for path in sorted(unknown_leaves):
        value = unknown_leaves[path]
        if value is None:
            missing.append(path)
            tokens.append(f"__MISSING__|{path}")
        else:
            tokens.append(f"{path}={normalize_category(_stable_scalar(value))}")
    for path in TOP_LEVEL_FIELDS:
        if path in expected:
            continue
        if path not in row or row.get(path) is None:
            missing.append(path)
            tokens.append(f"__MISSING__|{path}")
        else:
            tokens.append(f"{path}={normalize_category(row.get(path))}")

    counts: dict[int, int] = {}
    for token in tokens:
        index = stable_hash_index(token, dimension)
        counts[index] = counts.get(index, 0) + 1
    indices = sorted(counts)
    values = [counts[index] for index in indices]
    row_payload = {
        "schema_fingerprint": schema_fingerprint,
        "feature_dimension": dimension,
        "indices": indices,
        "values": values,
        "bins": bins,
        "missing": sorted(missing),
        "tokens": tokens,
    }
    fingerprint = canonical_hash(row_payload)
    return RowFeatures(
        impression_id=str(row.get("impression_id")),
        shard_id=shard.info.shard_id,
        shard_index=shard.info.index,
        row_number=int(row.get("_row_number", 1)),
        indices=indices,
        values=values,
        bins=bins,
        missing=sorted(missing),
        tokens=tokens,
        fingerprint=fingerprint,
    )


def assemble_features(data: DataBundle, config: Config) -> FeatureBundle:
    schema_payload = _schema(config)
    schema_fingerprint = canonical_hash(schema_payload)
    shard_features: list[ShardFeatures] = []
    for shard in sorted(data.shards, key=lambda item: item.info.index):
        rows = [_assemble_row(row, shard, config, schema_fingerprint) for row in shard.rows]
        shard_fingerprint = canonical_hash(
            {
                "shard_id": shard.info.shard_id,
                "shard_index": shard.info.index,
                "content_sha256": shard.content_digest,
                "schema_fingerprint": schema_fingerprint,
                "rows": [row.fingerprint for row in rows],
            }
        )
        shard_features.append(
            ShardFeatures(
                shard_id=shard.info.shard_id,
                shard_index=shard.info.index,
                content_digest=shard.content_digest,
                rows=rows,
                fingerprint=shard_fingerprint,
            )
        )
    aggregate_fingerprint = canonical_hash(
        {
            "schema_fingerprint": schema_fingerprint,
            "data_content_fingerprint": data.data_content_fingerprint,
            "shards": [shard.fingerprint for shard in shard_features],
        }
    )
    artifact = {
        "format": FEATURE_FORMAT,
        "schema": schema_payload,
        "schema_fingerprint": schema_fingerprint,
        "data_content_fingerprint": data.data_content_fingerprint,
        "aggregate_fingerprint": aggregate_fingerprint,
        "shards": [shard.to_dict() for shard in shard_features],
    }
    return FeatureBundle(
        schema_fingerprint=schema_fingerprint,
        data_content_fingerprint=data.data_content_fingerprint,
        shards=shard_features,
        aggregate_fingerprint=aggregate_fingerprint,
        artifact=artifact,
    )


def dense_matrix(rows: list[RowFeatures], dimension: int) -> Any:
    import numpy as np

    matrix = np.zeros((len(rows), dimension), dtype=np.float64)
    for row_position, row in enumerate(rows):
        for index, value in zip(row.indices, row.values):
            matrix[row_position, index] = float(value)
    return matrix
