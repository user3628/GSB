from __future__ import annotations

from typing import Any, Iterable

import numpy as np

from .errors import error


TRUE = 1
FALSE = 0
UNKNOWN = -1


def predicate_columns(predicate: dict[str, Any] | None) -> set[str]:
    if not predicate:
        return set()
    op = predicate.get("op")
    if op in ("and", "or"):
        columns: set[str] = set()
        for child in predicate.get("predicates", []):
            columns.update(predicate_columns(child))
        return columns
    if op == "not":
        return predicate_columns(predicate.get("predicate"))
    column = predicate.get("column")
    return {column} if isinstance(column, str) else set()


def validate_predicate(predicate: dict[str, Any] | None, column_types: dict[str, str]) -> None:
    if predicate is None:
        return
    if not isinstance(predicate, dict):
        raise error("E_QUERY_INVALID", "predicate must be an object")
    op = predicate.get("op")
    if op in ("and", "or"):
        children = predicate.get("predicates")
        if not isinstance(children, list) or not children:
            raise error("E_QUERY_INVALID", f"{op} predicate requires a non-empty predicates array")
        for child in children:
            validate_predicate(child, column_types)
        return
    if op == "not":
        if not isinstance(predicate.get("predicate"), dict):
            raise error("E_QUERY_INVALID", "not predicate requires a predicate object")
        validate_predicate(predicate["predicate"], column_types)
        return
    column = predicate.get("column")
    if not isinstance(column, str):
        raise error("E_QUERY_INVALID", "predicate column is required")
    if column not in column_types:
        raise error("E_QUERY_INVALID", f"unknown predicate column: {column}", details={"column": column})
    supported = {"eq", "ne", "lt", "le", "gt", "ge", "in", "is_null", "is_not_null"}
    if op not in supported:
        raise error("E_QUERY_INVALID", f"unsupported predicate operator: {op}", details={"op": op})
    if op not in ("is_null", "is_not_null") and not (op == "in" and isinstance(predicate.get("values"), list)) and "value" not in predicate:
        raise error("E_QUERY_INVALID", f"predicate operator {op} requires value")


def _compare_array(values: np.ndarray, op: str, target: Any) -> np.ndarray:
    if op == "eq":
        return np.asarray(values == target)
    if op == "ne":
        return np.asarray(values != target)
    if op == "lt":
        return np.asarray(values < target)
    if op == "le":
        return np.asarray(values <= target)
    if op == "gt":
        return np.asarray(values > target)
    if op == "ge":
        return np.asarray(values >= target)
    raise ValueError(f"unsupported comparison: {op}")


def evaluate_predicate(
    predicate: dict[str, Any] | None,
    arrays: dict[str, np.ndarray],
    null_masks: dict[str, np.ndarray],
    dtype_map: dict[str, str],
) -> np.ndarray:
    size = len(next(iter(arrays.values()))) if arrays else 0
    if predicate is None:
        return np.full(size, TRUE, dtype=np.int8)
    op = predicate["op"]
    if op == "and":
        masks = [evaluate_predicate(child, arrays, null_masks, dtype_map) for child in predicate["predicates"]]
        result = np.full(size, TRUE, dtype=np.int8)
        for mask in masks:
            result = np.where((result == FALSE) | (mask == FALSE), FALSE, np.where((result == UNKNOWN) | (mask == UNKNOWN), UNKNOWN, TRUE))
        return result.astype(np.int8)
    if op == "or":
        masks = [evaluate_predicate(child, arrays, null_masks, dtype_map) for child in predicate["predicates"]]
        result = np.full(size, FALSE, dtype=np.int8)
        for mask in masks:
            result = np.where((result == TRUE) | (mask == TRUE), TRUE, np.where((result == UNKNOWN) | (mask == UNKNOWN), UNKNOWN, FALSE))
        return result.astype(np.int8)
    if op == "not":
        mask = evaluate_predicate(predicate["predicate"], arrays, null_masks, dtype_map)
        return np.where(mask == UNKNOWN, UNKNOWN, np.where(mask == TRUE, FALSE, TRUE)).astype(np.int8)

    column = predicate["column"]
    values = arrays[column]
    null_mask = null_masks[column]
    if op == "is_null":
        return np.where(null_mask, TRUE, FALSE).astype(np.int8)
    if op == "is_not_null":
        return np.where(null_mask, FALSE, TRUE).astype(np.int8)
    if op == "in":
        target = predicate.get("values", [])
        raw = np.isin(values, target)
    else:
        raw = _compare_array(values, op, predicate.get("value"))
    if dtype_map.get(column) == "float64":
        finite = np.isfinite(values)
        raw = np.asarray(raw) & finite
    return np.where(null_mask, UNKNOWN, np.where(raw, TRUE, FALSE)).astype(np.int8)


def filter_indexes(mask: np.ndarray) -> Iterable[int]:
    return np.flatnonzero(mask == TRUE).tolist()

