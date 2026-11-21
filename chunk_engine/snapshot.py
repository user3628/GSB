from __future__ import annotations

import json
import re
import threading
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path, PurePosixPath
from types import MappingProxyType
from typing import Any, Mapping

from . import codec
from .errors import BusinessError, E_SNAPSHOT_INVALID
from .utils import canonical_json_bytes, sha256_hex, strict_json_loads


SNAPSHOT_FORMAT = "chunk-column-snapshot-v1"
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def _snapshot_error(message: str, **context: Any) -> BusinessError:
    return BusinessError(E_SNAPSHOT_INVALID, message, context, 400)


@dataclass(frozen=True)
class BlockMeta:
    block_no: int
    column: str
    dtype: str
    encoding: str
    row_offset: int
    row_count: int
    original_rows: int
    key_min: int
    key_max: int
    file: str
    decompressed_bytes: int
    encoded_bytes: int
    null_bitmap_len: int
    null_count: int
    body_len: int
    checksum: str


@dataclass(frozen=True)
class ColumnMeta:
    name: str
    dtype: str
    nullable: bool
    encoding: str
    blocks: tuple[BlockMeta, ...]


@dataclass(frozen=True)
class Snapshot:
    root: Path
    version: int
    generated_at: str
    row_count: int
    primary_key: str
    block_size: int
    columns: Mapping[str, ColumnMeta]
    digest: str
    manifest: Mapping[str, Any]

    @property
    def primary_column(self) -> ColumnMeta:
        return self.columns[self.primary_key]

    @property
    def block_count(self) -> int:
        return len(self.primary_column.blocks)

    def block_path(self, block: BlockMeta) -> Path:
        return self.root / block.file

    def column_block(self, column: str, block_no: int) -> BlockMeta:
        blocks = self.columns[column].blocks
        if block_no < 0 or block_no >= len(blocks):
            raise _snapshot_error(
                "block number is outside the column index",
                column=column,
                block_no=block_no,
            )
        return blocks[block_no]

    def block_for_key(self, key: int) -> BlockMeta | None:
        blocks = self.primary_column.blocks
        low, high = 0, len(blocks)
        while low < high:
            middle = (low + high) // 2
            if blocks[middle].key_max < key:
                low = middle + 1
            else:
                high = middle
        if low >= len(blocks):
            return None
        block = blocks[low]
        if key < block.key_min or key > block.key_max:
            return None
        return block

    def info(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "generated_at": self.generated_at,
            "row_count": self.row_count,
            "primary_key": self.primary_key,
            "block_size": self.block_size,
            "block_count": self.block_count,
            "columns": [
                {
                    "name": column.name,
                    "dtype": column.dtype,
                    "encoding": column.encoding,
                    "nullable": column.nullable,
                    "block_count": len(column.blocks),
                }
                for column in self.columns.values()
            ],
            "digest": self.digest,
            "root": str(self.root),
        }


def _require_bool(value: Any, field: str) -> bool:
    if not isinstance(value, bool):
        raise _snapshot_error(f"{field} must be boolean", field=field)
    return value


def _require_nonnegative_int(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise _snapshot_error(f"{field} must be a non-negative integer", field=field)
    return value


def _require_positive_int(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise _snapshot_error(f"{field} must be a positive integer", field=field)
    return value


def _safe_relative_file(value: Any) -> str:
    if not isinstance(value, str) or not value:
        raise _snapshot_error("block file must be a non-empty relative path", field="file")
    pure = PurePosixPath(value)
    if pure.is_absolute() or any(part in ("", ".", "..") for part in pure.parts):
        raise _snapshot_error("block file must stay inside the snapshot directory", file=value)
    return value


def _validate_block(raw: Any, column_name: str, column_dtype: str, expected_block_no: int) -> BlockMeta:
    if not isinstance(raw, dict):
        raise _snapshot_error("block index entry must be an object", column=column_name)
    block_no = _require_nonnegative_int(raw.get("block_no"), "block_no")
    if block_no != expected_block_no:
        raise _snapshot_error(
            "block numbers must be contiguous and start at zero",
            column=column_name,
            expected=expected_block_no,
            actual=block_no,
        )
    if raw.get("column") != column_name:
        raise _snapshot_error("column name in block index does not match its column", column=column_name)
    if raw.get("dtype") != column_dtype:
        raise _snapshot_error("dtype in block index does not match its column", column=column_name)
    encoding = raw.get("encoding")
    if encoding not in codec.SUPPORTED_ENCODINGS:
        raise _snapshot_error("unsupported block encoding", column=column_name, encoding=encoding)
    row_offset = _require_nonnegative_int(raw.get("row_offset"), "row_offset")
    row_count = _require_positive_int(raw.get("row_count"), "row_count")
    original_rows = _require_positive_int(raw.get("original_rows"), "original_rows")
    if row_count != original_rows:
        raise _snapshot_error("original_rows must equal row_count", column=column_name, block_no=block_no)
    key_min = raw.get("key_min")
    key_max = raw.get("key_max")
    if isinstance(key_min, bool) or not isinstance(key_min, int):
        raise _snapshot_error("key_min must be an integer", column=column_name, block_no=block_no)
    if isinstance(key_max, bool) or not isinstance(key_max, int):
        raise _snapshot_error("key_max must be an integer", column=column_name, block_no=block_no)
    if key_min > key_max:
        raise _snapshot_error("key_min must not exceed key_max", column=column_name, block_no=block_no)
    file = _safe_relative_file(raw.get("file"))
    decompressed_bytes = _require_positive_int(raw.get("decompressed_bytes"), "decompressed_bytes")
    encoded_bytes = _require_positive_int(raw.get("encoded_bytes"), "encoded_bytes")
    body_len = _require_positive_int(raw.get("body_len"), "body_len")
    null_bitmap_len = _require_nonnegative_int(raw.get("null_bitmap_len"), "null_bitmap_len")
    if null_bitmap_len != (row_count + 7) // 8:
        raise _snapshot_error("null bitmap length does not match row count", column=column_name, block_no=block_no)
    null_count = _require_nonnegative_int(raw.get("null_count"), "null_count")
    if null_count > row_count:
        raise _snapshot_error("null count exceeds row count", column=column_name, block_no=block_no)
    checksum = raw.get("checksum")
    if not isinstance(checksum, str) or not _SHA256_RE.fullmatch(checksum):
        raise _snapshot_error("block checksum must be a SHA-256 hex string", column=column_name, block_no=block_no)
    return BlockMeta(
        block_no=block_no,
        column=column_name,
        dtype=column_dtype,
        encoding=encoding,
        row_offset=row_offset,
        row_count=row_count,
        original_rows=original_rows,
        key_min=key_min,
        key_max=key_max,
        file=file,
        decompressed_bytes=decompressed_bytes,
        encoded_bytes=encoded_bytes,
        null_bitmap_len=null_bitmap_len,
        null_count=null_count,
        body_len=body_len,
        checksum=checksum,
    )


def _validate_column(raw: Any) -> ColumnMeta:
    if not isinstance(raw, dict):
        raise _snapshot_error("column definition must be an object")
    name = raw.get("name")
    if not isinstance(name, str) or not name or any(ord(char) < 32 for char in name):
        raise _snapshot_error("column name must be a non-empty printable string")
    if name in {"__proto__", "constructor", "prototype"}:
        raise _snapshot_error("column name is reserved", column=name)
    dtype = raw.get("dtype")
    if dtype not in codec.SUPPORTED_DTYPES:
        raise _snapshot_error("unsupported column dtype", column=name, dtype=dtype)
    nullable = _require_bool(raw.get("nullable"), "nullable")
    encoding = raw.get("encoding")
    if encoding not in codec.SUPPORTED_ENCODINGS:
        raise _snapshot_error("unsupported column encoding", column=name, encoding=encoding)
    if encoding == "fixed" and dtype not in ("int64", "float64"):
        raise _snapshot_error("fixed encoding requires int64 or float64", column=name)
    null_bitmap = raw.get("null_bitmap")
    if not isinstance(null_bitmap, dict) or null_bitmap.get("encoding") != "bitmap":
        raise _snapshot_error("column must declare a bitmap null mask", column=name)
    if null_bitmap.get("bit_order") not in ("lsb0", "lsb-first"):
        raise _snapshot_error("unsupported null bitmap bit order", column=name)
    if null_bitmap.get("null_is_one") is not True:
        raise _snapshot_error("null bitmap must use null_is_one=true", column=name)
    raw_blocks = raw.get("blocks")
    if not isinstance(raw_blocks, list):
        raise _snapshot_error("column blocks must be an array", column=name)
    blocks = tuple(_validate_block(item, name, dtype, index) for index, item in enumerate(raw_blocks))
    if not nullable and any(block.null_count for block in blocks):
        raise _snapshot_error("non-nullable column contains null values", column=name)
    return ColumnMeta(name=name, dtype=dtype, nullable=nullable, encoding=encoding, blocks=blocks)


def _validate_index_shape(columns: tuple[ColumnMeta, ...], row_count: int) -> None:
    if not columns:
        raise _snapshot_error("snapshot must define at least one column")
    if row_count == 0:
        if any(column.blocks for column in columns):
            raise _snapshot_error("empty snapshot must not contain row blocks")
        return
    reference = columns[0]
    if not reference.blocks:
        raise _snapshot_error("non-empty snapshot must contain row blocks")
    # Every column must use exactly the same primary-key/row partition.
    row_offset = 0
    previous_key_max: int | None = None
    for index, block in enumerate(reference.blocks):
        if block.row_offset != row_offset:
            raise _snapshot_error(
                "row offsets must be contiguous",
                block_no=index,
                expected=row_offset,
                actual=block.row_offset,
            )
        if previous_key_max is not None and block.key_min <= previous_key_max:
            raise _snapshot_error(
                "primary-key block ranges must be strictly increasing and non-overlapping",
                block_no=index,
            )
        row_offset += block.row_count
        previous_key_max = block.key_max
    if row_offset != row_count:
        raise _snapshot_error(
            "row block counts do not sum to snapshot row_count",
            expected=row_count,
            actual=row_offset,
        )
    for column in columns[1:]:
        if len(column.blocks) != len(reference.blocks):
            raise _snapshot_error(
                "all columns must share the same block partition",
                column=column.name,
                expected=len(reference.blocks),
                actual=len(column.blocks),
            )
        for index, (left, right) in enumerate(zip(reference.blocks, column.blocks)):
            if (
                left.block_no != right.block_no
                or left.row_offset != right.row_offset
                or left.row_count != right.row_count
                or left.key_min != right.key_min
                or left.key_max != right.key_max
            ):
                raise _snapshot_error(
                    "all columns must share the same primary-key/row partition",
                    column=column.name,
                    block_no=index,
                )


def _validate_files(root: Path, columns: tuple[ColumnMeta, ...]) -> None:
    seen: set[str] = set()
    for column in columns:
        for block in column.blocks:
            if block.file in seen:
                raise _snapshot_error("block file is referenced more than once", file=block.file)
            seen.add(block.file)
            path = root / block.file
            if not path.is_file():
                raise _snapshot_error("block file is missing", file=block.file, column=column.name, block_no=block.block_no)
            try:
                header = codec.read_header(path, block)
            except BusinessError as exc:
                raise _snapshot_error(
                    "block header is invalid",
                    file=block.file,
                    block_no=block.block_no,
                    reason=exc.context.get("reason", exc.message),
                ) from exc
            for field in (
                "block_no",
                "column",
                "dtype",
                "encoding",
                "row_offset",
                "row_count",
                "key_min",
                "key_max",
                "null_bitmap_len",
                "body_len",
                "checksum",
            ):
                if header.get(field) != getattr(block, field):
                    raise _snapshot_error(
                        "block header does not match snapshot index",
                        file=block.file,
                        block_no=block.block_no,
                        field=field,
                    )


def load_snapshot(path: str | Path) -> Snapshot:
    root = Path(path).resolve()
    manifest_path = root / "manifest.json"
    try:
        raw_bytes = manifest_path.read_bytes()
        raw = strict_json_loads(raw_bytes)
    except FileNotFoundError as exc:
        raise _snapshot_error("snapshot manifest is missing", path=str(root), reason="manifest_missing") from exc
    except (OSError, UnicodeError, json.JSONDecodeError, ValueError) as exc:
        raise _snapshot_error("snapshot manifest cannot be parsed", path=str(root), reason=str(exc)) from exc
    if not isinstance(raw, dict):
        raise _snapshot_error("snapshot manifest must be an object", path=str(root))
    digest = raw.get("digest")
    if not isinstance(digest, str) or not _SHA256_RE.fullmatch(digest):
        raise _snapshot_error("snapshot digest must be a SHA-256 hex string", path=str(root))
    payload = dict(raw)
    payload.pop("digest", None)
    actual_digest = sha256_hex(canonical_json_bytes(payload))
    if actual_digest != digest:
        raise _snapshot_error(
            "snapshot manifest digest mismatch",
            path=str(root),
            expected=digest,
            actual=actual_digest,
        )
    if raw.get("format") != SNAPSHOT_FORMAT:
        raise _snapshot_error("unsupported snapshot format", format=raw.get("format"))
    version = _require_positive_int(raw.get("version"), "version")
    row_count = _require_nonnegative_int(raw.get("row_count"), "row_count")
    block_size = _require_positive_int(raw.get("block_size"), "block_size")
    generated_at = raw.get("generated_at")
    if not isinstance(generated_at, str):
        raise _snapshot_error("generated_at must be an ISO-8601 string")
    try:
        datetime.fromisoformat(generated_at.replace("Z", "+00:00"))
    except ValueError as exc:
        raise _snapshot_error("generated_at is not a valid ISO-8601 timestamp") from exc
    primary_key = raw.get("primary_key")
    if not isinstance(primary_key, str) or not primary_key:
        raise _snapshot_error("primary_key must name a column")
    raw_columns = raw.get("columns")
    if not isinstance(raw_columns, list):
        raise _snapshot_error("columns must be an array")
    columns = tuple(_validate_column(item) for item in raw_columns)
    names = [column.name for column in columns]
    if len(names) != len(set(names)):
        raise _snapshot_error("column names must be unique")
    by_name = {column.name: column for column in columns}
    if primary_key not in by_name:
        raise _snapshot_error("primary_key column is not defined", primary_key=primary_key)
    primary = by_name[primary_key]
    if primary.dtype != "int64" or primary.encoding != "fixed" or primary.nullable:
        raise _snapshot_error("primary key must be a non-nullable fixed-encoded int64 column", primary_key=primary_key)
    _validate_index_shape(columns, row_count)
    _validate_files(root, columns)
    return Snapshot(
        root=root,
        version=version,
        generated_at=generated_at,
        row_count=row_count,
        primary_key=primary_key,
        block_size=block_size,
        columns=MappingProxyType(by_name),
        digest=digest,
        manifest=MappingProxyType(payload),
    )


class SnapshotManager:
    """Version-monotonic snapshot holder with atomic publication."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._active: Snapshot | None = None

    def load(self, path: str | Path) -> Snapshot:
        candidate = load_snapshot(path)
        with self._lock:
            if self._active is not None and candidate.version <= self._active.version:
                raise _snapshot_error(
                    "snapshot version must be strictly greater than the active version",
                    active_version=self._active.version,
                    candidate_version=candidate.version,
                    path=str(candidate.root),
                )
            # Publication is one reference assignment after all validation succeeds.
            self._active = candidate
            return candidate

    def active(self) -> Snapshot:
        with self._lock:
            if self._active is None:
                raise _snapshot_error("no snapshot has been loaded", reason="no_active_snapshot")
            return self._active

    def try_active(self) -> Snapshot | None:
        with self._lock:
            return self._active
