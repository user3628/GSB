"""Block tile movement for head/sequence swap and tile-grid reorder modes."""

from __future__ import annotations

import itertools
from typing import Any

import numpy as np

from .activation import block_count, matrix_layout
from .errors import E_SHAPE_MISMATCH, BusinessError


def natural_target_shape(shape: tuple[int, ...], mode: str) -> tuple[int, ...]:
    if mode == "swap_head_seq":
        if len(shape) == 4:
            batch, heads, sequence, head_dim = shape
            return (batch, sequence, heads, head_dim)
        if len(shape) == 2:
            return (shape[1], shape[0])
    elif mode == "tile":
        return tuple(shape)
    raise ValueError(f"unsupported shape/mode: {shape}/{mode}")


def _region(region: tuple[slice, ...]) -> list[list[int]]:
    return [[int(item.start), int(item.stop)] for item in region]


def _span_for_region(shape: tuple[int, ...], region: tuple[slice, ...]) -> tuple[int, int]:
    rows, _cols = matrix_layout(shape)
    leading = shape[:-1]
    stride = 1
    strides: list[int] = []
    for dim in reversed(leading):
        strides.append(stride)
        stride *= dim
    strides.reverse()
    low = 0
    high = 0
    for index, (dim, rng) in enumerate(zip(leading, region[:-1])):
        low += int(rng.start) * strides[index]
        high += int(rng.stop - 1) * strides[index]
    return low, min(rows, high + 1)


def _base_descriptor(
    schedule: list[dict[str, Any]],
    *,
    key: str,
    source_region: tuple[slice, ...],
    target_region: tuple[slice, ...],
    target_shape: tuple[int, ...],
    coords: dict[str, Any],
    transpose: bool,
) -> dict[str, Any]:
    row_range = _span_for_region(target_shape, target_region)
    col_range = [int(target_region[-1].start), int(target_region[-1].stop)]
    index = len(schedule)
    descriptor = {
        "index": index,
        "key": key,
        "source_region": _region(source_region),
        "target_region": _region(target_region),
        "row_range": [int(row_range[0]), int(row_range[1])],
        "col_range": col_range,
        "coords": coords,
        "transpose": transpose,
    }
    schedule.append(descriptor)
    return descriptor


def _swap_schedule(
    shape: tuple[int, ...], row_block: int, col_block: int, target_shape: tuple[int, ...]
) -> list[dict[str, Any]]:
    schedule: list[dict[str, Any]] = []
    if len(shape) == 4:
        batch, heads, sequence, head_dim = shape
        for batch_index in range(batch):
            for sequence_tile in range(block_count(sequence, row_block)):
                sequence_start = sequence_tile * row_block
                sequence_end = min(sequence, sequence_start + row_block)
                for head_tile in range(block_count(heads, col_block)):
                    head_start = head_tile * col_block
                    head_end = min(heads, head_start + col_block)
                    source = (
                        slice(batch_index, batch_index + 1),
                        slice(head_start, head_end),
                        slice(sequence_start, sequence_end),
                        slice(0, head_dim),
                    )
                    target = (
                        slice(batch_index, batch_index + 1),
                        slice(sequence_start, sequence_end),
                        slice(head_start, head_end),
                        slice(0, head_dim),
                    )
                    _base_descriptor(
                        schedule,
                        key=f"s-b{batch_index:04d}-s{sequence_tile:04d}-h{head_tile:04d}",
                        source_region=source,
                        target_region=target,
                        target_shape=target_shape,
                        coords={
                            "batch": batch_index,
                            "sequence_block": sequence_tile,
                            "head_block": head_tile,
                            "sequence_start": sequence_start,
                            "sequence_end": sequence_end,
                            "head_start": head_start,
                            "head_end": head_end,
                        },
                        transpose=True,
                    )
    else:
        rows, cols = shape
        for target_row_tile in range(block_count(cols, row_block)):
            target_row_start = target_row_tile * row_block
            target_row_end = min(cols, target_row_start + row_block)
            for target_col_tile in range(block_count(rows, col_block)):
                target_col_start = target_col_tile * col_block
                target_col_end = min(rows, target_col_start + col_block)
                source = (
                    slice(target_col_start, target_col_end),
                    slice(target_row_start, target_row_end),
                )
                target = (
                    slice(target_row_start, target_row_end),
                    slice(target_col_start, target_col_end),
                )
                _base_descriptor(
                    schedule,
                    key=f"s-r{target_row_tile:04d}-c{target_col_tile:04d}",
                    source_region=source,
                    target_region=target,
                    target_shape=target_shape,
                    coords={
                        "target_row_block": target_row_tile,
                        "target_col_block": target_col_tile,
                        "target_row_start": target_row_start,
                        "target_row_end": target_row_end,
                        "target_col_start": target_col_start,
                        "target_col_end": target_col_end,
                    },
                    transpose=True,
                )
    return schedule


def _plane_tile_schedule(
    schedule: list[dict[str, Any]],
    *,
    plane_prefix: tuple[int, ...],
    plane_shape: tuple[int, int],
    full_target_shape: tuple[int, ...],
    row_block: int,
    col_block: int,
    key_prefix: str,
) -> None:
    rows, cols = plane_shape
    row_tiles = block_count(rows, row_block)
    col_tiles = block_count(cols, col_block)
    full_coords = [
        (row_tile, col_tile)
        for row_tile in range(max(0, row_tiles - 1))
        for col_tile in range(max(0, col_tiles - 1))
    ]
    destination_coords = sorted(full_coords, key=lambda item: (item[1], item[0]))
    mapping = dict(zip(full_coords, destination_coords))
    for row_tile in range(row_tiles):
        row_start = row_tile * row_block
        row_end = min(rows, row_start + row_block)
        for col_tile in range(col_tiles):
            col_start = col_tile * col_block
            col_end = min(cols, col_start + col_block)
            destination = mapping.get((row_tile, col_tile), (row_tile, col_tile))
            dest_row_tile, dest_col_tile = destination
            dest_row_start = dest_row_tile * row_block
            dest_row_end = min(rows, dest_row_start + row_block)
            dest_col_start = dest_col_tile * col_block
            dest_col_end = min(cols, dest_col_start + col_block)
            source = tuple(slice(dim, dim + 1) for dim in plane_prefix) + (
                slice(row_start, row_end),
                slice(col_start, col_end),
            )
            target = tuple(slice(dim, dim + 1) for dim in plane_prefix) + (
                slice(dest_row_start, dest_row_end),
                slice(dest_col_start, dest_col_end),
            )
            _base_descriptor(
                schedule,
                key=(
                    f"{key_prefix}-r{row_tile:04d}-c{col_tile:04d}"
                    f"-tr{dest_row_tile:04d}-tc{dest_col_tile:04d}"
                ),
                source_region=source,
                target_region=target,
                target_shape=full_target_shape,
                coords={
                    "source_row_block": row_tile,
                    "source_col_block": col_tile,
                    "target_row_block": dest_row_tile,
                    "target_col_block": dest_col_tile,
                    "source_row_start": row_start,
                    "source_row_end": row_end,
                    "source_col_start": col_start,
                    "source_col_end": col_end,
                    "target_row_start": dest_row_start,
                    "target_row_end": dest_row_end,
                    "target_col_start": dest_col_start,
                    "target_col_end": dest_col_end,
                },
                transpose=False,
            )


def _tile_schedule(
    shape: tuple[int, ...], row_block: int, col_block: int, target_shape: tuple[int, ...]
) -> list[dict[str, Any]]:
    schedule: list[dict[str, Any]] = []
    if len(shape) == 4:
        batch, heads, sequence, head_dim = shape
        for batch_index in range(batch):
            for head_index in range(heads):
                _plane_tile_schedule(
                    schedule,
                    plane_prefix=(batch_index, head_index),
                    plane_shape=(sequence, head_dim),
                    full_target_shape=target_shape,
                    row_block=row_block,
                    col_block=col_block,
                    key_prefix=f"t-b{batch_index:04d}-h{head_index:04d}",
                )
    else:
        _plane_tile_schedule(
            schedule,
            plane_prefix=(),
            plane_shape=(shape[0], shape[1]),
            full_target_shape=target_shape,
            row_block=row_block,
            col_block=col_block,
            key_prefix="t",
        )
    return schedule


def reorder_schedule(
    shape: tuple[int, ...], config: Any, target_shape: tuple[int, ...]
) -> list[dict[str, Any]]:
    if config.reorder_mode == "swap_head_seq":
        return _swap_schedule(shape, config.row_block, config.col_block, target_shape)
    return _tile_schedule(shape, config.row_block, config.col_block, target_shape)


def compute_reorder_block(
    source: np.ndarray,
    schedule_item: dict[str, Any],
    *,
    block_context: dict[str, Any],
) -> tuple[np.ndarray, dict[str, Any]]:
    source_region = tuple(slice(a, b) for a, b in schedule_item["source_region"])
    target_region = tuple(slice(a, b) for a, b in schedule_item["target_region"])
    block = np.ascontiguousarray(source[source_region], dtype=np.float64)
    if schedule_item["transpose"]:
        if block.ndim == 4:
            block = np.ascontiguousarray(block.transpose(0, 2, 1, 3))
        elif block.ndim == 2:
            block = np.ascontiguousarray(block.T)
        else:
            raise ValueError("unexpected reorder rank")
    expected_shape = tuple(int(rng.stop - rng.start) for rng in target_region)
    if block.shape != expected_shape:
        raise BusinessError(
            E_SHAPE_MISMATCH,
            "reorder block shape does not match target block",
            declared_shape=list(expected_shape),
            actual_shape=list(block.shape),
            block=schedule_item["key"],
        )
    metadata = {
        "block_key": schedule_item["key"],
        "block_index": int(schedule_item["index"]),
        "shape": [int(dim) for dim in block.shape],
        "source_region": schedule_item["source_region"],
        "target_region": schedule_item["target_region"],
        "source_coords": schedule_item["coords"],
        "target_coords": schedule_item["coords"],
        "status": "ok",
        "diagnostics": None,
    }
    return block, metadata


def validate_declared_target(
    *,
    source_shape: tuple[int, ...],
    mode: str,
    declared_shape: tuple[int, ...] | None,
    config_shape: tuple[int, ...] | None,
) -> tuple[int, ...]:
    actual = natural_target_shape(source_shape, mode)
    if declared_shape is not None and tuple(declared_shape) != actual:
        raise BusinessError(
            E_SHAPE_MISMATCH,
            "reordered shape does not match the shape manifest",
            declared_shape=list(declared_shape),
            actual_shape=list(actual),
            source_shape=list(source_shape),
            reorder_mode=mode,
        )
    if config_shape is not None and tuple(config_shape) != actual:
        raise BusinessError(
            E_SHAPE_MISMATCH,
            "reordered shape does not match the YAML target_shape",
            declared_shape=list(config_shape),
            actual_shape=list(actual),
            source_shape=list(source_shape),
            reorder_mode=mode,
        )
    return actual

