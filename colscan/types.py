from __future__ import annotations

import base64
import math
from dataclasses import dataclass
from typing import Any, Iterable

import numpy as np


DTYPES: tuple[str, ...] = ("int64", "float64", "bool", "string")
ENCODINGS: tuple[str, ...] = ("fixed", "rle", "dict")


def normalize_value(dtype: str, value: Any) -> Any:
    if value is None:
        return None
    if dtype == "int64":
        if isinstance(value, bool):
            return int(value)
        return int(value)
    if dtype == "float64":
        return float(value)
    if dtype == "bool":
        return bool(value)
    if dtype == "string":
        return str(value)
    raise ValueError(f"unsupported dtype: {dtype}")


def values_to_array(dtype: str, values: Iterable[Any], nulls: Iterable[bool]) -> np.ndarray:
    vals = list(values)
    nuls = list(nulls)
    if len(vals) != len(nuls):
        raise ValueError("values and nulls must have equal length")
    if dtype == "int64":
        out = np.zeros(len(vals), dtype=np.int64)
        for index, (value, is_null) in enumerate(zip(vals, nuls)):
            if not is_null:
                out[index] = int(value)
        return out
    if dtype == "float64":
        out = np.zeros(len(vals), dtype=np.float64)
        for index, (value, is_null) in enumerate(zip(vals, nuls)):
            if not is_null:
                out[index] = float(value)
        return out
    if dtype == "bool":
        out = np.zeros(len(vals), dtype=np.uint8)
        for index, (value, is_null) in enumerate(zip(vals, nuls)):
            if not is_null:
                out[index] = int(bool(value))
        return out
    if dtype == "string":
        out = np.empty(len(vals), dtype=object)
        for index, (value, is_null) in enumerate(zip(vals, nuls)):
            out[index] = "" if is_null else str(value)
        return out
    raise ValueError(f"unsupported dtype: {dtype}")


def array_to_values(dtype: str, array: np.ndarray) -> list[Any]:
    if dtype == "int64":
        return [int(value) for value in array.tolist()]
    if dtype == "float64":
        return [float(value) for value in array.tolist()]
    if dtype == "bool":
        return [bool(value) for value in array.tolist()]
    if dtype == "string":
        return [str(value) for value in array.tolist()]
    raise ValueError(f"unsupported dtype: {dtype}")


def encode_null_bitmap(nulls: Iterable[bool]) -> str:
    flags = [bool(value) for value in nulls]
    payload = bytearray((len(flags) + 7) // 8)
    for index, flag in enumerate(flags):
        if flag:
            payload[index // 8] |= 1 << (index % 8)
    return base64.b64encode(bytes(payload)).decode("ascii")


def decode_null_bitmap(encoded: str, row_count: int) -> bytes:
    try:
        payload = base64.b64decode(encoded, validate=True)
    except Exception as exc:
        raise ValueError("null bitmap is not valid base64") from exc
    expected = (row_count + 7) // 8
    if len(payload) != expected:
        raise ValueError(f"null bitmap length {len(payload)} != {expected}")
    return payload


def null_at(bitmap: bytes, row: int) -> bool:
    return bool(bitmap[row // 8] & (1 << (row % 8)))


def json_scalar(dtype: str, value: Any) -> Any:
    if value is None:
        return None
    if dtype == "float64":
        number = float(value)
        if not math.isfinite(number):
            return number if math.isfinite(number) else str(number)
        return number
    if dtype == "int64":
        return int(value)
    if dtype == "bool":
        return bool(value)
    return str(value)


@dataclass(frozen=True)
class EncodedBlock:
    payload_b64: str
    raw_size: int
    compressed_size: int
    compression_ratio: float
    encoding_meta: dict[str, int]

