from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Iterable, Sequence

from .errors import (
    E_DIM_MISMATCH,
    E_MANIFEST_INVALID,
    E_PAD_OVERLAP,
    fail,
)


SUPPORTED_MANIFEST_VERSIONS = (1, 2)


@dataclass(frozen=True)
class ShardSpec:
    shard_id: str
    row_start: int
    row_end: int
    row_offset: int
    embedding_dim: int
    segment_digest: str
    weight_file: str
    pad_ranges: tuple[tuple[int, int], ...] = ()
    pad_marker: float = 1.0
    notes: tuple[str, ...] = ()

    @property
    def row_count(self) -> int:
        return self.row_end - self.row_start

    @property
    def has_pad(self) -> bool:
        return bool(self.pad_ranges) and self.pad_marker > 0.0

    def contains_row(self, row: int) -> bool:
        return self.row_start <= row < self.row_end

    def is_pad_row(self, row: int) -> bool:
        if not self.contains_row(row) or not self.has_pad:
            return False
        return any(start <= row < end for start, end in self.pad_ranges)

    def pad_rows(self) -> frozenset[int]:
        rows: set[int] = set()
        if self.has_pad:
            for start, end in self.pad_ranges:
                rows.update(range(start, end))
        return frozenset(rows)

    def semantic_dict(self) -> dict[str, object]:
        return {
            "shard_id": self.shard_id,
            "row_start": self.row_start,
            "row_end": self.row_end,
            "row_offset": self.row_offset,
            "embedding_dim": self.embedding_dim,
            "segment_digest": self.segment_digest,
            "weight_file": self.weight_file,
            "pad_ranges": [list(item) for item in self.pad_ranges],
            "pad_marker": self.pad_marker,
        }


@dataclass(frozen=True)
class BatchLink:
    batch_id: str
    table_id: str
    file: str
    batch_number: int
    gather_axis: int | None = None
    notes: tuple[str, ...] = ()

    def semantic_dict(self) -> dict[str, object]:
        return {
            "batch_id": self.batch_id,
            "table_id": self.table_id,
            "file": self.file,
            "batch_number": self.batch_number,
            "gather_axis": self.gather_axis,
        }


@dataclass(frozen=True)
class IndexProject:
    table_id: str
    embedding_dim: int
    structure_version: int
    shards: tuple[ShardSpec, ...]
    index_batches: tuple[BatchLink, ...]
    quantization_bits: int = 12
    provenance: tuple[str, ...] = ()

    @property
    def total_rows(self) -> int:
        if not self.shards:
            return 0
        return max(shard.row_end for shard in self.shards)

    def shard_map(self) -> dict[str, ShardSpec]:
        return {shard.shard_id: shard for shard in self.shards}

    def batch_map(self) -> dict[str, BatchLink]:
        return {batch.batch_id: batch for batch in self.index_batches}

    def find_shard(self, row: int) -> ShardSpec | None:
        for shard in self.shards:
            if shard.contains_row(row):
                return shard
        return None

    def pad_rows(self) -> frozenset[int]:
        rows: set[int] = set()
        for shard in self.shards:
            rows.update(shard.pad_rows())
        return frozenset(rows)

    def validate(self, check_dimensions: bool = True) -> None:
        if not self.table_id:
            raise fail(E_MANIFEST_INVALID, "table_id must not be empty")
        if self.structure_version not in SUPPORTED_MANIFEST_VERSIONS:
            raise fail(
                E_MANIFEST_INVALID,
                "unsupported manifest structure version",
                structure_version=self.structure_version,
            )
        if self.embedding_dim <= 0:
            raise fail(E_MANIFEST_INVALID, "embedding_dim must be positive", embedding_dim=self.embedding_dim)
        if self.quantization_bits <= 0 or self.quantization_bits > 62:
            raise fail(
                E_MANIFEST_INVALID,
                "quantization_bits is outside the supported range",
                quantization_bits=self.quantization_bits,
            )
        if not self.shards:
            raise fail(E_MANIFEST_INVALID, "at least one shard is required", table_id=self.table_id)

        seen_shards: set[str] = set()
        for shard in self.shards:
            if not shard.shard_id:
                raise fail(E_MANIFEST_INVALID, "shard_id must not be empty", table_id=self.table_id)
            if shard.shard_id in seen_shards:
                raise fail(
                    E_MANIFEST_INVALID,
                    "duplicate shard_id",
                    table_id=self.table_id,
                    shard_id=shard.shard_id,
                )
            seen_shards.add(shard.shard_id)
            if not (0 <= shard.row_start < shard.row_end):
                raise fail(
                    E_MANIFEST_INVALID,
                    "row interval must be a non-empty half-open interval",
                    shard_id=shard.shard_id,
                    row_start=shard.row_start,
                    row_end=shard.row_end,
                )
            if shard.row_offset < 0 or shard.row_offset > shard.row_start:
                raise fail(
                    E_MANIFEST_INVALID,
                    "row_offset must be non-negative and no greater than row_start",
                    shard_id=shard.shard_id,
                    row_offset=shard.row_offset,
                    row_start=shard.row_start,
                )
            if shard.row_offset + shard.row_count < shard.row_end:
                raise fail(
                    E_MANIFEST_INVALID,
                    "row_offset and row interval do not cover the shard weights",
                    shard_id=shard.shard_id,
                    row_offset=shard.row_offset,
                    row_start=shard.row_start,
                    row_end=shard.row_end,
                )
            if shard.embedding_dim <= 0:
                raise fail(
                    E_MANIFEST_INVALID,
                    "shard embedding_dim must be positive",
                    shard_id=shard.shard_id,
                    embedding_dim=shard.embedding_dim,
                )
            if check_dimensions and shard.embedding_dim != self.embedding_dim:
                raise fail(
                    E_DIM_MISMATCH,
                    "shard embedding_dim differs from table embedding_dim",
                    shard_id=shard.shard_id,
                    index_id=None,
                    declared_dim=shard.embedding_dim,
                    table_dim=self.embedding_dim,
                )
            if not shard.segment_digest:
                raise fail(
                    E_MANIFEST_INVALID,
                    "segment_digest must not be empty",
                    shard_id=shard.shard_id,
                )
            if not shard.weight_file:
                raise fail(
                    E_MANIFEST_INVALID,
                    "weight_file must not be empty",
                    shard_id=shard.shard_id,
                )
            if shard.pad_marker != shard.pad_marker or shard.pad_marker in (float("inf"), float("-inf")):
                raise fail(
                    E_MANIFEST_INVALID,
                    "pad_marker must be finite",
                    shard_id=shard.shard_id,
                    pad_marker=shard.pad_marker,
                )
            if shard.pad_ranges and shard.pad_marker <= 0.0:
                raise fail(
                    E_PAD_OVERLAP,
                    "pad ranges are declared but the pad marker is not active",
                    shard_id=shard.shard_id,
                    index_id=shard.pad_ranges[0][0],
                    pad_marker=shard.pad_marker,
                )
            if not shard.pad_ranges and shard.pad_marker < 0.0:
                raise fail(
                    E_MANIFEST_INVALID,
                    "pad marker must be non-negative when no pad range is present",
                    shard_id=shard.shard_id,
                    index_id=None,
                    pad_marker=shard.pad_marker,
                )
            previous_end = -1
            for start, end in sorted(shard.pad_ranges):
                if not (shard.row_start <= start < end <= shard.row_end):
                    raise fail(
                        E_PAD_OVERLAP,
                        "pad range is outside the shard row interval",
                        shard_id=shard.shard_id,
                        index_id=start,
                        pad_range=[start, end],
                        row_start=shard.row_start,
                        row_end=shard.row_end,
                    )
                if start < previous_end:
                    raise fail(
                        E_PAD_OVERLAP,
                        "pad ranges overlap",
                        shard_id=shard.shard_id,
                        index_id=start,
                        pad_range=[start, end],
                    )
                previous_end = end

        ordered = sorted(self.shards, key=lambda item: (item.row_start, item.row_end, item.shard_id))
        for left, right in zip(ordered, ordered[1:]):
            if left.row_end <= right.row_start:
                continue
            overlap_start = max(left.row_start, right.row_start)
            overlap_end = min(left.row_end, right.row_end)
            left_pad = any(overlap_start < end and start < overlap_end for start, end in left.pad_ranges)
            right_pad = any(overlap_start < end and start < overlap_end for start, end in right.pad_ranges)
            code = E_PAD_OVERLAP if (left_pad or right_pad) else E_MANIFEST_INVALID
            raise fail(
                code,
                "shard row intervals overlap",
                shard_id=left.shard_id,
                index_id=overlap_start,
                shard_ids=[left.shard_id, right.shard_id],
                overlap=[overlap_start, overlap_end],
            )

        seen_batches: set[str] = set()
        for batch in self.index_batches:
            if not batch.batch_id:
                raise fail(E_MANIFEST_INVALID, "batch_id must not be empty", table_id=self.table_id)
            if batch.batch_id in seen_batches:
                raise fail(
                    E_MANIFEST_INVALID,
                    "duplicate batch_id",
                    batch_id=batch.batch_id,
                )
            seen_batches.add(batch.batch_id)
            if batch.table_id != self.table_id:
                raise fail(
                    E_MANIFEST_INVALID,
                    "index batch is linked to a different embedding table",
                    batch_id=batch.batch_id,
                    batch_table_id=batch.table_id,
                    table_id=self.table_id,
                )
            if not batch.file:
                raise fail(E_MANIFEST_INVALID, "batch file must not be empty", batch_id=batch.batch_id)

    def with_notes(self, notes: Iterable[str]) -> "IndexProject":
        combined = tuple(dict.fromkeys(self.provenance + tuple(notes)))
        return replace(self, provenance=combined)

    def with_shard_notes(self, shard_id: str, notes: Iterable[str]) -> "IndexProject":
        updated: list[ShardSpec] = []
        for shard in self.shards:
            if shard.shard_id == shard_id:
                updated.append(replace(shard, notes=tuple(dict.fromkeys(shard.notes + tuple(notes)))))
            else:
                updated.append(shard)
        return replace(self, shards=tuple(updated))


@dataclass(frozen=True)
class V1UpgradeConfig:
    total_rows: int | None = None
    percentage_rounding: str = "ceil"
    offset_strategy: str = "row_start"
    recover_pad_from: str = "weight_metadata"
    prefer_compat_extension: bool = True

    def __post_init__(self) -> None:
        if self.percentage_rounding not in {"floor", "ceil", "round"}:
            raise fail(
                E_MANIFEST_INVALID,
                "percentage_rounding must be floor, ceil, or round",
                percentage_rounding=self.percentage_rounding,
            )
        if self.offset_strategy not in {"row_start", "contiguous"}:
            raise fail(
                E_MANIFEST_INVALID,
                "offset_strategy must be row_start or contiguous",
                offset_strategy=self.offset_strategy,
            )
        if self.recover_pad_from not in {"weight_metadata", "compat_extension", "none"}:
            raise fail(
                E_MANIFEST_INVALID,
                "recover_pad_from must be weight_metadata, compat_extension, or none",
                recover_pad_from=self.recover_pad_from,
            )
        if self.total_rows is not None and self.total_rows <= 0:
            raise fail(
                E_MANIFEST_INVALID,
                "total_rows must be positive",
                total_rows=self.total_rows,
            )


@dataclass(frozen=True)
class ActualShardFacts:
    shard_id: str
    embedding_dim: int
    row_start: int
    row_end: int
    row_offset: int
    segment_digest: str
    pad_ranges: tuple[tuple[int, int], ...] = ()
    weight_file: str | None = None


def facts_map(items: Sequence[ActualShardFacts]) -> dict[str, ActualShardFacts]:
    return {item.shard_id: item for item in items}


def normalize_pad_ranges(value: Iterable[Iterable[int]] | None) -> tuple[tuple[int, int], ...]:
    if value is None:
        return ()
    pairs = sorted((int(item[0]), int(item[1])) for item in value)
    return tuple(pairs)


def shard_for_row(project: IndexProject, row: int) -> ShardSpec:
    shard = project.find_shard(row)
    if shard is None:
        raise fail(
            "E_INDEX_OOB",
            "embedding row is outside every shard interval",
            index_id=row,
            total_rows=project.total_rows,
        )
    return shard
