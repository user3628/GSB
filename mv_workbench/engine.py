from __future__ import annotations

from collections import defaultdict
from typing import Any, Callable

import numpy as np

from .errors import fail
from .util import canonical_json


INCREMENTAL_AGGREGATES = {"sum", "count"}
NON_INCREMENTAL_AGGREGATES = {"min", "max", "avg", "quantile", "median"}
ALL_GROUP_KEY = canonical_json({"$all": True})


def _truth_in(value: Any, candidates: list[Any]) -> bool:
    return value in candidates


def _compare(left: Any, operator: str, right: Any = None) -> bool:
    if operator == "is_null":
        return left is None
    if operator == "not_null":
        return left is not None
    if left is None:
        return False
    if operator == "eq":
        return left == right
    if operator == "ne":
        return left != right
    if operator == "gt":
        return left > right
    if operator == "gte":
        return left >= right
    if operator == "lt":
        return left < right
    if operator == "lte":
        return left <= right
    if operator == "in":
        return isinstance(right, list) and _truth_in(left, right)
    if operator == "not_in":
        return isinstance(right, list) and not _truth_in(left, right)
    fail(
        "E_INVALID_REQUEST",
        "Unsupported filter operator at execution time.",
        context={"operator": operator},
    )
    return False


def _aggregate(function: str, values: list[Any], *, row_count: int, q: float | None) -> Any:
    if function == "count":
        if values and values[0] == "__COUNT_ALL__":
            return row_count
        return sum(1 for value in values if value is not None)
    clean = [value for value in values if value is not None]
    if function == "sum":
        return sum(clean) if clean else None
    if function == "min":
        return min(clean) if clean else None
    if function == "max":
        return max(clean) if clean else None
    if function == "avg":
        return (sum(clean) / len(clean)) if clean else None
    if function in {"quantile", "median"}:
        if not clean:
            return None
        quantile = 0.5 if function == "median" else float(q)
        return float(np.quantile(np.asarray(clean, dtype=float), quantile, method="linear"))
    fail(
        "E_INVALID_REQUEST",
        "Unsupported aggregate at execution time.",
        context={"function": function},
    )
    return None


def _apply_join(source: dict[str, Any], resolve_ref: Callable[[str, str], list[dict[str, Any]]]) -> list[dict[str, Any]]:
    left_ref = source["left"]
    right_ref = source["right"]
    left_rows = resolve_ref(left_ref["kind"], left_ref["name"])
    right_rows = resolve_ref(right_ref["kind"], right_ref["name"])
    join_type = source.get("type", "inner")
    predicates = source["on"]
    select = source["select"]
    output: list[dict[str, Any]] = []
    matched_right: set[int] = set()
    for left in left_rows:
        matched = False
        for right_index, right in enumerate(right_rows):
            ok = True
            for predicate in predicates:
                left_expr = predicate["left"]
                right_expr = predicate["right"]
                left_value = _qualified_value(left, "left", left_expr)
                right_value = _qualified_value(right, "right", right_expr)
                if left_value is None or right_value is None or left_value != right_value:
                    ok = False
                    break
            if not ok:
                continue
            matched = True
            matched_right.add(right_index)
            row: dict[str, Any] = {}
            for key, expression in select.items():
                row[key] = _join_expression_value(left, right, expression)
            output.append(row)
        if join_type == "left" and not matched:
            row = {}
            for key, expression in select.items():
                row[key] = _join_expression_value(left, {}, expression)
            output.append(row)
    return output


def _qualified_value(row: dict[str, Any], alias: str, expression: str) -> Any:
    prefix = alias + "."
    if expression.startswith(prefix):
        return row.get(expression[len(prefix) :])
    return row.get(expression)


def _join_expression_value(left: dict[str, Any], right: dict[str, Any], expression: str) -> Any:
    if expression.startswith("left."):
        return left.get(expression[5:])
    if expression.startswith("right."):
        return right.get(expression[6:])
    return left.get(expression, right.get(expression))


def execute_view(
    definition: dict[str, Any],
    resolve_ref: Callable[[str, str], list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    source = definition["source"]
    if source["kind"] == "join":
        rows = _apply_join(source, resolve_ref)
    else:
        rows = [dict(row) for row in resolve_ref(source["kind"], source["name"])]

    for operator in definition["operators"]:
        op = operator["op"]
        if op == "project":
            rows = [
                {output: row.get(expression) for output, expression in operator["columns"].items()}
                for row in rows
            ]
        elif op == "filter":
            kept: list[dict[str, Any]] = []
            for row in rows:
                if all(
                    _compare(
                        row.get(predicate["column"]),
                        predicate["operator"],
                        predicate.get("value"),
                    )
                    for predicate in operator["where"]
                ):
                    kept.append(row)
            rows = kept
        elif op == "group_by":
            rows = _group_rows(rows, operator)
        else:  # validation normally prevents this branch
            fail(
                "E_INVALID_REQUEST",
                "Unsupported operator at execution time.",
                context={"operator": op},
            )

    return sorted(rows, key=canonical_json)


def _group_rows(rows: list[dict[str, Any]], operator: dict[str, Any]) -> list[dict[str, Any]]:
    key_columns = list(operator.get("keys", []))
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    key_values: dict[str, tuple[Any, ...]] = {}
    for row in rows:
        values = tuple(row.get(column) for column in key_columns)
        key = canonical_json(values)
        grouped[key].append(row)
        key_values[key] = values

    # SQL-like grouping of an empty input yields no groups.
    output: list[dict[str, Any]] = []
    for key, group in grouped.items():
        result: dict[str, Any] = dict(zip(key_columns, key_values[key]))
        for aggregate in operator["aggregates"]:
            function = aggregate["func"]
            column = aggregate.get("column")
            if function == "count" and column is None:
                values = ["__COUNT_ALL__"]
            else:
                values = [row.get(column) for row in group]
            result[aggregate["name"]] = _aggregate(
                function,
                values,
                row_count=len(group),
                q=aggregate.get("q"),
            )
        output.append(result)
    return sorted(output, key=canonical_json)


def split_groups(
    definition: dict[str, Any], rows: list[dict[str, Any]]
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, Any], dict[str, int]]:
    """Return group-key -> rows, key -> display key, key -> ordinal."""

    group_operator = next(
        (item for item in definition["operators"] if item["op"] == "group_by"), None
    )
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    display: dict[str, Any] = {}
    ordinals: dict[str, int] = {}
    if group_operator is None:
        groups[ALL_GROUP_KEY] = sorted(rows, key=canonical_json)
        display[ALL_GROUP_KEY] = {"$all": True}
        ordinals[ALL_GROUP_KEY] = 0
        return dict(groups), display, ordinals

    key_columns = group_operator.get("keys", [])
    for row in rows:
        key_object = {column: row.get(column) for column in key_columns}
        key = canonical_json(key_object)
        groups[key].append(row)
        display[key] = key_object
    for ordinal, key in enumerate(sorted(groups)):
        ordinals[key] = ordinal
        groups[key].sort(key=canonical_json)
    return dict(groups), display, ordinals


def aggregate_functions(definition: dict[str, Any]) -> list[str]:
    for operator in definition["operators"]:
        if operator["op"] == "group_by":
            return [aggregate["func"] for aggregate in operator["aggregates"]]
    return []


def plan_materialization(
    definition: dict[str, Any],
    rows: list[dict[str, Any]],
    *,
    old_rows: list[dict[str, Any]] | None,
    old_graph_version: int | None,
    graph_version: int,
) -> dict[str, Any]:
    new_groups, display, ordinals = split_groups(definition, rows)
    old_groups, _, _ = split_groups(definition, old_rows or [])
    affected_keys = sorted(
        key
        for key in set(new_groups) | set(old_groups)
        if new_groups.get(key, []) != old_groups.get(key, [])
    )
    functions = aggregate_functions(definition)
    non_incremental = [function for function in functions if function not in INCREMENTAL_AGGREGATES]

    if old_rows is None or old_graph_version is None:
        mode = "initial_build"
        recomputed_groups = len(new_groups)
        reason = "No reusable materialization exists; the view was built in full."
    elif old_graph_version != graph_version:
        mode = "full_recompute"
        recomputed_groups = len(new_groups)
        reason = "The view definition changed; a new graph version requires a full rebuild."
    elif not functions:
        mode = "full_recompute"
        recomputed_groups = len(new_groups)
        reason = "The view has no mergeable aggregate; all output groups were replaced."
    elif non_incremental:
        mode = "full_recompute"
        recomputed_groups = len(new_groups)
        reason = (
            "Aggregate functions "
            + ", ".join(sorted(set(non_incremental)))
            + " are not closed under correction/deletion; the whole view was recomputed."
        )
    else:
        mode = "incremental_groups"
        recomputed_groups = len(affected_keys)
        reason = (
            "All aggregates are distributive (sum/count); only groups whose input "
            "rows changed were rewritten and the remaining groups were preserved."
        )

    materialized_groups: dict[str, list[dict[str, Any]]] = {}
    for key, group_rows in new_groups.items():
        if mode == "incremental_groups" and key not in affected_keys and key in old_groups:
            materialized_groups[key] = old_groups[key]
        else:
            materialized_groups[key] = group_rows

    preserved_groups = (
        len(new_groups) - recomputed_groups if mode == "incremental_groups" else 0
    )
    return {
        "rows": rows,
        "groups": materialized_groups,
        "group_display": display,
        "group_ordinals": ordinals,
        "mode": mode,
        "aggregate_functions": functions,
        "non_incremental_functions": sorted(set(non_incremental)),
        "affected_groups": [display[key] for key in affected_keys if key in display],
        "recomputed_groups": recomputed_groups,
        "preserved_groups": preserved_groups,
        "reason": reason,
    }
