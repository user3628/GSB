from __future__ import annotations

import numpy as np

from .errors import EngineError


MAX_PARTITIONS = 4096
_C1 = np.uint64(0x9E3779B97F4A7C15)
_C2 = np.uint64(0xBF58476D1CE4E5B9)
_C3 = np.uint64(0x94D049BB133111EB)


def deterministic_partition_ids(keys: np.ndarray, partitions: int) -> np.ndarray:
    """SplitMix64 over the signed 64-bit key, then modulo the partition count."""

    if isinstance(partitions, bool) or not isinstance(partitions, int):
        raise EngineError("E_INVALID_REQUEST", "partitions must be an integer")
    if partitions < 1 or partitions > MAX_PARTITIONS:
        raise EngineError(
            "E_INVALID_REQUEST",
            f"partitions must be between 1 and {MAX_PARTITIONS}",
            {"partitions": partitions},
        )
    contiguous = np.ascontiguousarray(keys, dtype="<i8")
    z = contiguous.view(np.uint64).copy()
    with np.errstate(over="ignore", invalid="ignore"):
        z = z + _C1
        z = (z ^ (z >> np.uint64(30))) * _C2
        z = (z ^ (z >> np.uint64(27))) * _C3
        z = z ^ (z >> np.uint64(31))
        return (z % np.uint64(partitions)).astype(np.int64)


def deterministic_partition(key: int, partitions: int) -> int:
    return int(
        deterministic_partition_ids(np.asarray([key], dtype="<i8"), partitions)[0]
    )


def estimate_partition_bytes(left_count: int, right_count: int) -> int:
    # 8-byte key, 8-byte measure, 1-byte null mask and 8-byte row locator per row,
    # plus a fixed 128-byte partition envelope for deterministic padding/headers.
    return 128 + int(left_count + right_count) * 25
