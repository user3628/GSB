"""Canonical logical expressions, traversal, and conservative folding."""

from __future__ import annotations

import json
import math
from copy import deepcopy
from typing import Any, Iterator


Expr = dict[str, Any]


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def expr_key(expr: Expr) -> str:
    return canonical_json(expr)


def column(name: str) -> Expr:
    return {"kind": "column", "name": name}


def literal(value: Any, value_type: str | None = None) -> Expr:
    if value_type is None:
        if value is None:
            value_type = "null"
        elif isinstance(value, bool):
            value_type = "boolean"
        elif isinstance(value, (int, float)):
            value_type = "number"
        elif isinstance(value, str):
            value_type = "string"
        else:
            raise TypeError(f"unsupported literal value: {type(value).__name__}")
    return {"kind": "literal", "value_type": value_type, "value": value}


def boolean(value: bool) -> Expr:
    return literal(value, "boolean")


def binary(op: str, left: Expr, right: Expr) -> Expr:
    return {"kind": "binary", "op": op.upper(), "left": left, "right": right}


def unary(op: str, operand: Expr) -> Expr:
    return {"kind": "unary", "op": op.upper(), "operand": operand}


def is_literal(expr: Expr) -> bool:
    return expr.get("kind") == "literal"


def is_bool_literal(expr: Expr, value: bool | None = None) -> bool:
    if expr.get("kind") != "literal" or expr.get("value_type") != "boolean":
        return False
    return value is None or expr.get("value") is value


def is_null_literal(expr: Expr) -> bool:
    return expr.get("kind") == "literal" and expr.get("value_type") == "null"


def clone_expr(expr: Expr) -> Expr:
    return deepcopy(expr)


def walk(expr: Expr) -> Iterator[Expr]:
    yield expr
    kind = expr.get("kind")
    if kind == "unary":
        yield from walk(expr["operand"])
    elif kind == "binary":
        yield from walk(expr["left"])
        yield from walk(expr["right"])
    elif kind == "is_null":
        yield from walk(expr["operand"])
    elif kind == "in":
        yield from walk(expr["operand"])
        for item in expr["items"]:
            yield from walk(item)
    elif kind == "func":
        for arg in expr["args"]:
            yield from walk(arg)


def columns_in(expr: Expr) -> set[str]:
    return {node["name"] for node in walk(expr) if node.get("kind") == "column"}


def aggregate_functions(expr: Expr) -> list[Expr]:
    return [
        node
        for node in walk(expr)
        if node.get("kind") == "func" and node.get("name") in {"COUNT", "SUM", "MIN", "MAX"}
    ]


def contains_aggregate(expr: Expr) -> bool:
    return bool(aggregate_functions(expr))


def substitute(expr: Expr, replacements: dict[str, Expr]) -> Expr:
    """Replace whole expressions by canonical key. The outermost match wins."""

    key = expr_key(expr)
    if key in replacements:
        return deepcopy(replacements[key])
    kind = expr.get("kind")
    if kind == "unary":
        return {**expr, "operand": substitute(expr["operand"], replacements)}
    if kind == "binary":
        return {
            **expr,
            "left": substitute(expr["left"], replacements),
            "right": substitute(expr["right"], replacements),
        }
    if kind == "is_null":
        return {**expr, "operand": substitute(expr["operand"], replacements)}
    if kind == "in":
        return {
            **expr,
            "operand": substitute(expr["operand"], replacements),
            "items": [substitute(item, replacements) for item in expr["items"]],
        }
    if kind == "func":
        return {**expr, "args": [substitute(arg, replacements) for arg in expr["args"]]}
    return deepcopy(expr)


def _sql_truth(value: Any) -> bool | None:
    if value is None:
        return None
    return bool(value)


def _literal_value(expr: Expr) -> Any:
    return expr.get("value")


def _fold_comparison(op: str, left: Any, right: Any) -> Expr | None:
    if left is None or right is None:
        return None
    try:
        if op == "=":
            result = left == right
        elif op in {"!=", "<>"}:
            result = left != right
        elif op == "<":
            result = left < right
        elif op == "<=":
            result = left <= right
        elif op == ">":
            result = left > right
        elif op == ">=":
            result = left >= right
        else:
            return None
    except TypeError:
        return None
    return boolean(result)


def fold_expr(expr: Expr) -> tuple[Expr, bool]:
    """Conservatively fold expressions. Returns (expression, changed)."""

    kind = expr.get("kind")
    if kind == "unary":
        operand, child_changed = fold_expr(expr["operand"])
        changed = child_changed
        if operand.get("kind") == "literal" and expr["op"] in {"NOT", "+", "-"}:
            value = _literal_value(operand)
            if expr["op"] == "NOT":
                truth = _sql_truth(value)
                return (literal(None) if truth is None else boolean(not truth)), True
            if expr["op"] == "+" and isinstance(value, (int, float)) and not isinstance(value, bool):
                return literal(value), True
            if expr["op"] == "-" and isinstance(value, (int, float)) and not isinstance(value, bool):
                return literal(-value), True
        return {**expr, "operand": operand}, changed

    if kind == "binary":
        left, left_changed = fold_expr(expr["left"])
        right, right_changed = fold_expr(expr["right"])
        changed = left_changed or right_changed
        op = expr["op"]
        if op == "AND":
            if is_bool_literal(left, False) or is_bool_literal(right, False):
                return boolean(False), True
            if is_bool_literal(left, True):
                return right, True
            if is_bool_literal(right, True):
                return left, True
        elif op == "OR":
            if is_bool_literal(left, True) or is_bool_literal(right, True):
                return boolean(True), True
            if is_bool_literal(left, False):
                return right, True
            if is_bool_literal(right, False):
                return left, True
        elif is_literal(left) and is_literal(right):
            left_value = _literal_value(left)
            right_value = _literal_value(right)
            if op in {"+", "-", "*", "/", "%"}:
                if (
                    isinstance(left_value, (int, float))
                    and not isinstance(left_value, bool)
                    and isinstance(right_value, (int, float))
                    and not isinstance(right_value, bool)
                ):
                    try:
                        if op == "+":
                            value = left_value + right_value
                        elif op == "-":
                            value = left_value - right_value
                        elif op == "*":
                            value = left_value * right_value
                        elif op == "/":
                            value = left_value / right_value
                        else:
                            value = left_value % right_value
                        if isinstance(value, float) and not math.isfinite(value):
                            return {**expr, "left": left, "right": right}, changed
                        return literal(value), True
                    except (ZeroDivisionError, OverflowError):
                        pass
            if op == "||" and isinstance(left_value, str) and isinstance(right_value, str):
                return literal(left_value + right_value), True
            comparison = _fold_comparison(op, left_value, right_value)
            if comparison is not None:
                return comparison, True
        return {**expr, "left": left, "right": right}, changed

    if kind == "is_null":
        operand, child_changed = fold_expr(expr["operand"])
        changed = child_changed
        if operand.get("kind") == "literal":
            result = operand.get("value") is None
            if expr.get("negated"):
                result = not result
            return boolean(result), True
        return {**expr, "operand": operand}, changed

    if kind == "in":
        operand, child_changed = fold_expr(expr["operand"])
        items = []
        items_changed = False
        for item in expr["items"]:
            folded, folded_changed = fold_expr(item)
            items.append(folded)
            items_changed = items_changed or folded_changed
        changed = child_changed or items_changed
        if operand.get("kind") == "literal" and all(is_literal(item) for item in items):
            operand_value = operand.get("value")
            item_values = [item.get("value") for item in items]
            if operand_value is None:
                result: bool | None = None
            elif operand_value in item_values:
                result = True
            elif None in item_values:
                result = None
            else:
                result = False
            if result is not None:
                if expr.get("negated"):
                    result = not result
                return boolean(result), True
        return {**expr, "operand": operand, "items": items}, changed

    if kind == "func":
        args = []
        changed = False
        for arg in expr["args"]:
            folded, folded_changed = fold_expr(arg)
            args.append(folded)
            changed = changed or folded_changed
        return {**expr, "args": args}, changed

    return expr, False

