"""Tiny reference executor used to prove rewrite equivalence on local samples."""

from __future__ import annotations

import functools
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .business import SemanticError
from .expressions import Expr
from .plan import PlanNode


@dataclass(frozen=True)
class QueryResult:
    columns: tuple[str, ...]
    rows: tuple[tuple[Any, ...], ...]

    def as_dict(self) -> dict[str, Any]:
        return {"columns": list(self.columns), "rows": [list(row) for row in self.rows]}


Row = dict[str, Any]


class PlanExecutor:
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self.connection = sqlite3.connect(
            self.db_path.resolve().as_uri() + "?mode=ro", uri=True
        )
        self.connection.row_factory = sqlite3.Row

    def close(self) -> None:
        self.connection.close()

    def execute(self, plan: PlanNode) -> QueryResult:
        rows, columns = self._execute_node(plan)
        display_columns = _display_columns(plan, columns)
        return QueryResult(columns=tuple(display_columns), rows=tuple(tuple(row) for row in rows))

    def _execute_node(self, plan: PlanNode) -> tuple[list[list[Any]], list[str]]:
        node_type = plan.node_type
        attrs = plan.attributes
        if node_type == "Scan":
            return self._scan(plan)
        if node_type == "Filter":
            rows, columns = self._execute_node(plan.children[0])
            predicate = attrs["predicate"]
            kept = [
                row
                for row in rows
                if _as_where_truth(_eval(predicate, _row_dict(columns, row)))
            ]
            return kept, columns
        if node_type == "Project":
            rows, child_columns = self._execute_node(plan.children[0])
            output_names = [str(item["name"]) for item in attrs["items"]]
            projected: list[list[Any]] = []
            for row in rows:
                row_map = _row_dict(child_columns, row)
                projected.append([_eval(item["expr"], row_map) for item in attrs["items"]])
            return projected, output_names
        if node_type == "Join":
            return self._join(plan)
        if node_type == "Aggregate":
            return self._aggregate(plan)
        if node_type == "Sort":
            rows, columns = self._execute_node(plan.children[0])
            keys = attrs.get("keys", [])
            if not keys:
                return rows, columns

            def compare(left: list[Any], right: list[Any]) -> int:
                left_map = _row_dict(columns, left)
                right_map = _row_dict(columns, right)
                for key in keys:
                    left_value = _eval(key["expr"], left_map)
                    right_value = _eval(key["expr"], right_map)
                    result = _compare_sort_values(
                        left_value,
                        right_value,
                        str(key.get("direction", "ASC")),
                        str(key.get("nulls", "FIRST")),
                    )
                    if result:
                        return result
                return 0

            sorted_rows = sorted(rows, key=functools.cmp_to_key(compare))
            return sorted_rows, columns
        if node_type == "Limit":
            limit = int(attrs["limit"])
            rows, columns = self._execute_node(plan.children[0])
            return rows[:limit], columns
        raise SemanticError("executor does not support node type", node_type=node_type)

    def _scan(self, plan: PlanNode) -> tuple[list[list[Any]], list[str]]:
        attrs = plan.attributes
        table = str(attrs["table"])
        qualified = list(attrs["columns"])
        if not qualified:
            query = f'SELECT 1 FROM "{_quote_identifier(table)}"'
            rows = [[] for _ in self.connection.execute(query)]
            return rows, []
        source_columns = [name.split(".", 1)[1] for name in qualified]
        select_sql = ", ".join(f'"{_quote_identifier(name)}"' for name in source_columns)
        query = f'SELECT {select_sql} FROM "{_quote_identifier(table)}"'
        result: list[list[Any]] = []
        for sqlite_row in self.connection.execute(query):
            result.append([sqlite_row[index] for index in range(len(source_columns))])
        return result, qualified

    def _join(self, plan: PlanNode) -> tuple[list[list[Any]], list[str]]:
        left_rows, left_columns = self._execute_node(plan.children[0])
        right_rows, right_columns = self._execute_node(plan.children[1])
        join_type = str(plan.attributes.get("join_type", "INNER")).upper()
        condition = plan.attributes.get("condition")
        columns = left_columns + right_columns
        output: list[list[Any]] = []
        matched_right: set[int] = set()
        for left in left_rows:
            left_map = _row_dict(left_columns, left)
            matched = False
            for right_index, right in enumerate(right_rows):
                right_map = _row_dict(right_columns, right)
                combined = {**left_map, **right_map}
                if condition is None or _as_where_truth(_eval(condition, combined)):
                    output.append(left + list(right))
                    matched = True
                    matched_right.add(right_index)
            if not matched and join_type in {"LEFT", "FULL"}:
                output.append(left + [None] * len(right_columns))
        if join_type in {"RIGHT", "FULL"}:
            for right_index, right in enumerate(right_rows):
                if right_index not in matched_right:
                    output.append([None] * len(left_columns) + list(right))
        return output, columns

    def _aggregate(self, plan: PlanNode) -> tuple[list[list[Any]], list[str]]:
        rows, child_columns = self._execute_node(plan.children[0])
        group_entries = plan.attributes.get("group_by", [])
        aggregate_entries = plan.attributes.get("aggregates", [])
        columns = [entry["name"] for entry in group_entries] + [
            entry["name"] for entry in aggregate_entries
        ]
        groups: dict[tuple[Any, ...], list[Row]] = {}
        for row in rows:
            row_map = _row_dict(child_columns, row)
            key = tuple(_normalize_group_value(_eval(entry["expr"], row_map)) for entry in group_entries)
            groups.setdefault(key, []).append(row_map)
        if not group_entries and not groups:
            groups[()] = []
        output: list[list[Any]] = []
        for group_key, group_rows in groups.items():
            values: list[Any] = list(group_key)
            for entry in aggregate_entries:
                values.append(_eval_aggregate(entry["expr"], group_rows))
            output.append(values)
        return output, columns


def _quote_identifier(identifier: str) -> str:
    return identifier.replace('"', '""')


def _display_columns(plan: PlanNode, internal_columns: list[str]) -> list[str]:
    if plan.node_type == "Project":
        return [
            str(item.get("output_name", item["name"]))
            for item in plan.attributes.get("items", [])
            if item.get("visible", True)
        ]
    if plan.node_type in {"Sort", "Limit", "Filter"} and plan.children:
        return _display_columns(plan.children[0], internal_columns)
    return internal_columns


def _row_dict(columns: list[str], row: list[Any]) -> Row:
    return {column: row[index] for index, column in enumerate(columns)}


def _as_where_truth(value: Any) -> bool:
    if value is None:
        return False
    return bool(value)


def _sql_and(left: Any, right: Any) -> Any:
    if left is False or right is False:
        return False
    if left is None or right is None:
        return None
    return True


def _sql_or(left: Any, right: Any) -> Any:
    if left is True or right is True:
        return True
    if left is None or right is None:
        return None
    return False


def _eval(expr: Expr, row: Row) -> Any:
    kind = expr.get("kind")
    if kind == "literal":
        return expr.get("value")
    if kind == "column":
        if expr["name"] not in row:
            raise SemanticError("executor could not resolve column", column=expr["name"])
        return row[expr["name"]]
    if kind == "unary":
        value = _eval(expr["operand"], row)
        if expr["op"] == "NOT":
            if value is None:
                return None
            return not bool(value)
        if value is None:
            return None
        if expr["op"] == "+":
            return +value
        if expr["op"] == "-":
            return -value
    if kind == "binary":
        op = expr["op"]
        left = _eval(expr["left"], row)
        right = _eval(expr["right"], row)
        if op == "AND":
            return _sql_and(None if left is None else bool(left), None if right is None else bool(right))
        if op == "OR":
            return _sql_or(None if left is None else bool(left), None if right is None else bool(right))
        if left is None or right is None:
            return None
        if op == "=":
            return left == right
        if op in {"!=", "<>"}:
            return left != right
        if op == "<":
            return left < right
        if op == "<=":
            return left <= right
        if op == ">":
            return left > right
        if op == ">=":
            return left >= right
        if op == "+":
            return left + right
        if op == "-":
            return left - right
        if op == "*":
            return left * right
        if op == "/":
            if right == 0:
                return None
            return left / right
        if op == "%":
            if right == 0:
                return None
            return left % right
        if op == "||":
            return str(left) + str(right)
    if kind == "is_null":
        value = _eval(expr["operand"], row)
        result = value is None
        return not result if expr.get("negated") else result
    if kind == "in":
        operand = _eval(expr["operand"], row)
        items = [_eval(item, row) for item in expr["items"]]
        if operand is None:
            return None
        if operand in items:
            result: bool | None = True
        elif None in items:
            result = None
        else:
            result = False
        if expr.get("negated") and result is not None:
            result = not result
        return result
    if kind == "func":
        raise SemanticError("aggregate function evaluated outside Aggregate", function=expr.get("name"))
    raise SemanticError("executor does not support expression kind", kind=kind)


def _eval_aggregate(expr: Expr, rows: list[Row]) -> Any:
    name = str(expr["name"]).upper()
    star = bool(expr.get("star"))
    distinct = bool(expr.get("distinct"))
    if name == "COUNT" and star:
        return len(rows)
    argument = expr["args"][0]
    values = [_eval(argument, row) for row in rows]
    values = [value for value in values if value is not None]
    if distinct:
        unique: list[Any] = []
        for value in values:
            if value not in unique:
                unique.append(value)
        values = unique
    if name == "COUNT":
        return len(values)
    if not values:
        return None
    if name == "SUM":
        return sum(values)
    if name == "MIN":
        return min(values)
    if name == "MAX":
        return max(values)
    raise SemanticError("unsupported aggregate function in executor", function=name)


def _normalize_group_value(value: Any) -> Any:
    return value


def _compare_sort_values(left: Any, right: Any, direction: str, nulls: str) -> int:
    if left is None or right is None:
        if left is None and right is None:
            return 0
        if left is None:
            return -1 if nulls.upper() == "FIRST" else 1
        return 1 if nulls.upper() == "FIRST" else -1
    try:
        result = (left > right) - (left < right)
    except TypeError:
        left_text, right_text = str(left), str(right)
        result = (left_text > right_text) - (left_text < right_text)
    return -result if direction.upper() == "DESC" else result


def execute_plan(plan: PlanNode, db_path: str | Path) -> QueryResult:
    executor = PlanExecutor(db_path)
    try:
        return executor.execute(plan)
    finally:
        executor.close()


def execute_sql(sql: str, db_path: str | Path) -> QueryResult:
    connection = sqlite3.connect(
        Path(db_path).resolve().as_uri() + "?mode=ro", uri=True
    )
    try:
        cursor = connection.execute(sql)
        columns = tuple(description[0] for description in cursor.description or ())
        rows = tuple(tuple(row) for row in cursor.fetchall())
        return QueryResult(columns=columns, rows=rows)
    finally:
        connection.close()

