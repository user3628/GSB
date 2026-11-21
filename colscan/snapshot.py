from __future__ import annotations

import base64
import hashlib
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .codecs import decode_block, encode_segmented_block
from .errors import BusinessError, error
from .types import DTYPES, ENCODINGS, decode_null_bitmap, encode_null_bitmap, null_at


FORMAT_NAME = "colscan-snapshot/1"
CHECKSUM_ALGORITHM = "sha256"


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def snapshot_checksum(document: dict[str, Any]) -> str:
    payload = dict(document)
    payload.pop("checksum", None)
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class RowBlock:
    block_seq: int
    block_id: str
    start_row: int
    row_count: int


@dataclass(frozen=True)
class BlockDef:
    column: str
    block_seq: int
    block_id: str
    start_row: int
    row_count: int
    encoding: str
    raw_size: int
    compressed_size: int
    compression_ratio: float
    payload_b64: str
    encoding_meta: dict[str, int]


@dataclass(frozen=True)
class ColumnDef:
    name: str
    dtype: str
    nullable: bool
    null_bitmap: bytes
    null_count: int
    blocks: tuple[BlockDef, ...]


@dataclass(frozen=True)
class Snapshot:
    version: int
    generated_at: str
    checksum: str
    row_count: int
    block_size: int
    columns: tuple[ColumnDef, ...]
    row_blocks: tuple[RowBlock, ...]
    document: dict[str, Any]

    def column(self, name: str) -> ColumnDef:
        for column in self.columns:
            if column.name == name:
                return column
        raise error("E_QUERY_INVALID", f"unknown column: {name}", details={"column": name})

    def block(self, column: str, block_seq: int) -> BlockDef:
        return self.column(column).blocks[block_seq]

    def is_null(self, column: str, row: int) -> bool:
        return null_at(self.column(column).null_bitmap, row)


def _invalid(reason: str, **details: Any) -> BusinessError:
    return error("E_SNAPSHOT_INVALID", f"snapshot is invalid: {reason}", details={"reason": reason, **details})


def _require_int(value: Any, field: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise _invalid(f"{field} must be an integer >= {minimum}", field=field, value=value)
    return value


def _require_str(value: Any, field: str) -> str:
    if not isinstance(value, str):
        raise _invalid(f"{field} must be a string", field=field)
    return value


def snapshot_from_document(document: dict[str, Any], *, verify_checksum: bool = True) -> Snapshot:
    if not isinstance(document, dict):
        raise _invalid("document must be an object")
    if document.get("format") != FORMAT_NAME:
        raise _invalid("unsupported snapshot format", format=document.get("format"))
    if document.get("checksum_algorithm") != CHECKSUM_ALGORITHM:
        raise _invalid("unsupported checksum algorithm")
    claimed_checksum = _require_str(document.get("checksum"), "checksum")
    if verify_checksum:
        actual_checksum = snapshot_checksum(document)
        if actual_checksum != claimed_checksum:
            raise _invalid(
                "checksum mismatch",
                expected_checksum=claimed_checksum,
                actual_checksum=actual_checksum,
            )
    version = _require_int(document.get("version"), "version", minimum=1)
    generated_at = _require_str(document.get("generated_at"), "generated_at")
    row_count = _require_int(document.get("row_count"), "row_count")
    block_size = _require_int(document.get("block_size"), "block_size", minimum=1)
    rows_payload = document.get("row_blocks")
    columns_payload = document.get("columns")
    if not isinstance(rows_payload, list) or not rows_payload:
        raise _invalid("row_blocks must be a non-empty array")
    if not isinstance(columns_payload, list) or not columns_payload:
        raise _invalid("columns must be a non-empty array")

    row_blocks: list[RowBlock] = []
    expected_start = 0
    for expected_seq, raw in enumerate(rows_payload):
        if not isinstance(raw, dict):
            raise _invalid("row block must be an object", block_seq=expected_seq)
        seq = _require_int(raw.get("block_seq"), "row_blocks.block_seq")
        if seq != expected_seq:
            raise _invalid("row block sequence is not contiguous", expected=expected_seq, actual=seq)
        block_id = _require_str(raw.get("block_id"), "row_blocks.block_id")
        start_row = _require_int(raw.get("start_row"), "row_blocks.start_row")
        count = _require_int(raw.get("row_count"), "row_blocks.row_count", minimum=1)
        if block_id != f"b{seq:04d}":
            raise _invalid("row block id mismatch", expected=f"b{seq:04d}", actual=block_id)
        if start_row != expected_start:
            raise _invalid("row blocks are not contiguous", expected_start=expected_start, actual_start=start_row)
        if count > block_size:
            raise _invalid("row block exceeds declared block_size", block_seq=seq, row_count=count, block_size=block_size)
        row_blocks.append(RowBlock(seq, block_id, start_row, count))
        expected_start += count
    if expected_start != row_count:
        raise _invalid("row block coverage does not match row_count", expected=row_count, actual=expected_start)
    if len(row_blocks) != math.ceil(row_count / block_size):
        raise _invalid("row block count does not match block_size")

    columns: list[ColumnDef] = []
    seen_names: set[str] = set()
    for raw_column in columns_payload:
        if not isinstance(raw_column, dict):
            raise _invalid("column must be an object")
        name = _require_str(raw_column.get("name"), "columns.name")
        if not name or name in seen_names:
            raise _invalid("column names must be non-empty and unique", column=name)
        seen_names.add(name)
        dtype = _require_str(raw_column.get("dtype"), "columns.dtype")
        if dtype not in DTYPES:
            raise _invalid("unsupported column dtype", column=name, dtype=dtype)
        nullable = raw_column.get("nullable")
        if not isinstance(nullable, bool):
            raise _invalid("column nullable must be boolean", column=name)
        bitmap_b64 = _require_str(raw_column.get("null_bitmap_b64"), "columns.null_bitmap_b64")
        try:
            bitmap = decode_null_bitmap(bitmap_b64, row_count)
        except ValueError as exc:
            raise _invalid(str(exc), column=name) from exc
        null_count = sum(null_at(bitmap, row) for row in range(row_count))
        if not nullable and null_count:
            raise _invalid("non-nullable column contains nulls", column=name, null_count=null_count)
        declared_null_count = _require_int(raw_column.get("null_count"), "columns.null_count")
        if declared_null_count != null_count:
            raise _invalid("column null_count mismatch", column=name, expected=null_count, actual=declared_null_count)
        raw_blocks = raw_column.get("blocks")
        if not isinstance(raw_blocks, list) or len(raw_blocks) != len(row_blocks):
            raise _invalid("column must contain one block per row block", column=name)
        blocks: list[BlockDef] = []
        for expected_seq, (raw_block, row_block) in enumerate(zip(raw_blocks, row_blocks)):
            if not isinstance(raw_block, dict):
                raise _invalid("column block must be an object", column=name, block_seq=expected_seq)
            seq = _require_int(raw_block.get("block_seq"), "columns.blocks.block_seq")
            if seq != expected_seq:
                raise _invalid("column block sequence mismatch", column=name, expected=expected_seq, actual=seq)
            block_id = _require_str(raw_block.get("block_id"), "columns.blocks.block_id")
            expected_block_id = f"{name}:{row_block.block_id}"
            if block_id != expected_block_id:
                raise _invalid("column block id mismatch", expected=expected_block_id, actual=block_id)
            start_row = _require_int(raw_block.get("start_row"), "columns.blocks.start_row")
            count = _require_int(raw_block.get("row_count"), "columns.blocks.row_count")
            if start_row != row_block.start_row or count != row_block.row_count:
                raise _invalid("column block range differs from row block", column=name, block_seq=seq)
            encoding = _require_str(raw_block.get("encoding"), "columns.blocks.encoding")
            if encoding not in ENCODINGS:
                raise _invalid("unsupported block encoding", column=name, block_seq=seq, encoding=encoding)
            raw_size = _require_int(raw_block.get("raw_size"), "columns.blocks.raw_size")
            compressed_size = _require_int(raw_block.get("compressed_size"), "columns.blocks.compressed_size", minimum=1)
            payload_b64 = _require_str(raw_block.get("payload_b64"), "columns.blocks.payload_b64")
            try:
                payload_size = len(base64.b64decode(payload_b64, validate=True))
            except Exception as exc:
                raise _invalid("block payload is not valid base64", column=name, block_seq=seq) from exc
            if payload_size != compressed_size:
                raise _invalid(
                    "compressed_size does not match stored payload",
                    column=name,
                    block_seq=seq,
                    expected=compressed_size,
                    actual=payload_size,
                )
            ratio_value = raw_block.get("compression_ratio")
            if not isinstance(ratio_value, (int, float)) or isinstance(ratio_value, bool) or not math.isfinite(float(ratio_value)):
                raise _invalid("compression_ratio must be finite", column=name, block_seq=seq)
            meta = raw_block.get("encoding_meta", {})
            if not isinstance(meta, dict) or any(isinstance(value, bool) or not isinstance(value, int) for value in meta.values()):
                raise _invalid("encoding_meta must contain integer values", column=name, block_seq=seq)
            blocks.append(
                BlockDef(
                    column=name,
                    block_seq=seq,
                    block_id=block_id,
                    start_row=start_row,
                    row_count=count,
                    encoding=encoding,
                    raw_size=raw_size,
                    compressed_size=compressed_size,
                    compression_ratio=float(ratio_value),
                    payload_b64=payload_b64,
                    encoding_meta=dict(meta),
                )
            )
        columns.append(ColumnDef(name, dtype, nullable, bitmap, null_count, tuple(blocks)))
    return Snapshot(
        version=version,
        generated_at=generated_at,
        checksum=claimed_checksum,
        row_count=row_count,
        block_size=block_size,
        columns=tuple(columns),
        row_blocks=tuple(row_blocks),
        document=document,
    )


def load_snapshot_file(path: str | Path) -> Snapshot:
    snapshot_path = Path(path)
    try:
        raw = snapshot_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise _invalid("cannot read snapshot file", path=str(snapshot_path), error=str(exc)) from exc
    try:
        document = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise _invalid("snapshot file is not valid JSON", path=str(snapshot_path), error=str(exc)) from exc
    return snapshot_from_document(document, verify_checksum=True)


def build_snapshot_document(
    *,
    version: int,
    generated_at: str,
    block_size: int,
    columns: list[dict[str, Any]],
) -> dict[str, Any]:
    if not columns:
        raise ValueError("at least one column is required")
    names: set[str] = set()
    lengths: set[int] = set()
    for spec in columns:
        name = str(spec["name"])
        if name in names:
            raise ValueError(f"duplicate column name: {name}")
        names.add(name)
        if spec["dtype"] not in DTYPES:
            raise ValueError(f"unsupported dtype: {spec['dtype']}")
        if spec["encoding"] not in ENCODINGS:
            raise ValueError(f"unsupported encoding: {spec['encoding']}")
        lengths.add(len(spec["values"]))
    if len(lengths) != 1:
        raise ValueError("all columns must have equal row count")
    row_count = lengths.pop()
    block_count = math.ceil(row_count / block_size) if row_count else 0
    row_blocks: list[dict[str, Any]] = []
    for seq in range(block_count):
        start = seq * block_size
        count = min(block_size, row_count - start)
        row_blocks.append(
            {
                "block_seq": seq,
                "block_id": f"b{seq:04d}",
                "start_row": start,
                "row_count": count,
            }
        )
    column_documents: list[dict[str, Any]] = []
    for spec in columns:
        name = str(spec["name"])
        dtype = str(spec["dtype"])
        values = list(spec["values"])
        nulls = [value is None for value in values]
        nullable = bool(spec.get("nullable", any(nulls)))
        if not nullable and any(nulls):
            raise ValueError(f"non-nullable column {name} contains nulls")
        blocks: list[dict[str, Any]] = []
        for row_block in row_blocks:
            start = row_block["start_row"]
            count = row_block["row_count"]
            block_values = []
            for value in values[start : start + count]:
                if value is None:
                    block_values.append(0 if dtype in ("int64", "float64", "bool") else "")
                else:
                    block_values.append(value)
            encoded = encode_segmented_block(
                dtype,
                str(spec["encoding"]),
                block_values,
                segment_rows=int(spec.get("segment_rows", 4)),
            )
            blocks.append(
                {
                    "block_seq": row_block["block_seq"],
                    "block_id": f"{name}:{row_block['block_id']}",
                    "start_row": start,
                    "row_count": count,
                    "encoding": spec["encoding"],
                    "raw_size": encoded.raw_size,
                    "compressed_size": encoded.compressed_size,
                    "compression_ratio": encoded.compression_ratio,
                    "encoding_meta": encoded.encoding_meta,
                    "payload_b64": encoded.payload_b64,
                }
            )
        column_documents.append(
            {
                "name": name,
                "dtype": dtype,
                "nullable": nullable,
                "null_count": sum(nulls),
                "null_bitmap_b64": encode_null_bitmap(nulls),
                "encoding": spec["encoding"],
                "blocks": blocks,
            }
        )
    document: dict[str, Any] = {
        "format": FORMAT_NAME,
        "checksum_algorithm": CHECKSUM_ALGORITHM,
        "version": version,
        "generated_at": generated_at,
        "row_count": row_count,
        "block_size": block_size,
        "row_blocks": row_blocks,
        "columns": column_documents,
    }
    document["checksum"] = snapshot_checksum(document)
    return document


def write_snapshot_file(path: str | Path, document: dict[str, Any]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(document, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def decode_column_block(
    snapshot: Snapshot,
    column: str,
    block_seq: int,
    row_range: tuple[int, int] | None = None,
) -> tuple[Any, Any]:
    column_def = snapshot.column(column)
    block = column_def.blocks[block_seq]
    values = decode_block(
        block.payload_b64,
        block_id=block.block_id,
        dtype=column_def.dtype,
        encoding=block.encoding,
        raw_size=block.raw_size,
        row_count=block.row_count,
        row_range=row_range,
    )
    start, end = row_range if row_range is not None else (0, block.row_count)
    nulls = [
        null_at(column_def.null_bitmap, row)
        for row in range(block.start_row + start, block.start_row + end)
    ]
    import numpy as np

    return values, np.asarray(nulls, dtype=bool)

