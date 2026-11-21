from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np

from .predicate import evaluate_predicate, filter_indexes, predicate_columns
from .snapshot import Snapshot, decode_column_block, load_snapshot_file
from .types import json_scalar


def reference_scan_snapshot(
    snapshot: Snapshot,
    *,
    columns: list[str],
    predicate: dict[str, Any] | None = None,
    block_start: int = 0,
    block_end: int | None = None,
    offset: int = 0,
    limit: int | None = None,
) -> dict[str, Any]:
    if block_end is None:
        block_end = len(snapshot.row_blocks)
    needed = list(columns)
    for name in sorted(predicate_columns(predicate)):
        if name not in needed:
            needed.append(name)
    output_rows: list[list[Any]] = []
    for block_seq in range(block_start, block_end):
        arrays: dict[str, np.ndarray] = {}
        null_masks: dict[str, np.ndarray] = {}
        dtype_map: dict[str, str] = {}
        for column_name in needed:
            values, nulls = decode_column_block(snapshot, column_name, block_seq)
            arrays[column_name] = values
            null_masks[column_name] = nulls
            dtype_map[column_name] = snapshot.column(column_name).dtype
        mask = evaluate_predicate(predicate, arrays, null_masks, dtype_map)
        for index in filter_indexes(mask):
            row = []
            for column_name in columns:
                if bool(null_masks[column_name][index]):
                    row.append(None)
                else:
                    row.append(json_scalar(dtype_map[column_name], arrays[column_name][index]))
            output_rows.append(row)
    total_matched = len(output_rows)
    if limit is None:
        window_rows = output_rows[offset:]
    else:
        window_rows = output_rows[offset : offset + limit]
    return {
        "snapshot_version": snapshot.version,
        "columns": [{"name": name, "dtype": snapshot.column(name).dtype} for name in columns],
        "rows": window_rows,
        "all_rows": output_rows,
        "total_matched": total_matched,
        "window": {"offset": offset, "limit": limit, "row_count": len(window_rows)},
    }


def reference_scan_file(
    path: str | Path,
    *,
    columns: list[str],
    predicate: dict[str, Any] | None = None,
    block_start: int = 0,
    block_end: int | None = None,
    offset: int = 0,
    limit: int | None = None,
) -> dict[str, Any]:
    return reference_scan_snapshot(
        load_snapshot_file(path),
        columns=columns,
        predicate=predicate,
        block_start=block_start,
        block_end=block_end,
        offset=offset,
        limit=limit,
    )

