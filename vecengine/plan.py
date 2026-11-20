from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass
from typing import Any, Union

from .errors import PlanError
from .model import BOOL, FLOAT64, INT64, INT64_MAX, INT64_MIN, DatasetSpec


_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,63}$")
_MAX_EXPR_DEPTH = 128
_MAX_EXPR_NODES = 1024


@dataclass(frozen=True)
class ColumnExpr:
    name: str
    dtype: str


@dataclass(frozen=True)
class LiteralExpr:
    dtype: str
    value: Any


@dataclass(frozen=True)
class UnaryExpr:
    fn: str
    dtype: str
    arg: "Expr"


@dataclass(frozen=True)
class BinaryExpr:
    fn: str
    dtype: str
    left: "Expr"
    right: "Expr"


@dataclass(frozen=True)
class CompareExpr:
    fn: str
    left: "Expr"
    right: "Expr"
    dtype: str = BOOL


@dataclass(frozen=True)
class LogicalExpr:
    fn: str
    left: "Expr"
    right: "Expr"
    dtype: str = BOOL


@dataclass(frozen=True)
class ConditionalExpr:
    condition: "Expr"
    then_expr: "Expr"
    else_expr: "Expr"
    dtype: str


Expr = Union[
    ColumnExpr,
    LiteralExpr,
    UnaryExpr,
    BinaryExpr,
    CompareExpr,
    LogicalExpr,
    ConditionalExpr,
]


@dataclass(frozen=True)
class Projection:
    name: str
    expr: Expr

    @property
    def dtype(self) -> str:
        return self.expr.dtype


@dataclass(frozen=True)
class Aggregate:
    name: str
    fn: str
    dtype: str
    expr: Expr | None


@dataclass(frozen=True)
class ParsedPlan:
    dataset: str
    spec: DatasetSpec
    filter_expr: Expr | None
    projections: tuple[Projection, ...]
    aggregates: tuple[Aggregate, ...]
    node_count: int
    complexity: int
    source_columns: tuple[str, ...]

    @property
    def output_columns(self) -> tuple[tuple[str, str], ...]:
        if self.aggregates:
            return tuple((agg.name, agg.dtype) for agg in self.aggregates)
        return tuple((projection.name, projection.dtype) for projection in self.projections)

    @property
    def is_aggregate(self) -> bool:
        return bool(self.aggregates)


def parse_plan(raw: Any, spec_resolver: Any) -> ParsedPlan:
    """Validate a complete plan without touching or executing a batch."""

    if not isinstance(raw, dict):
        raise PlanError("plan must be a JSON object")
    allowed = {"version", "dataset", "operators"}
    unknown = set(raw) - allowed
    if unknown:
        raise PlanError("plan contains unknown fields", details={"fields": sorted(unknown)})
    version = raw.get("version", 1)
    if version != 1:
        raise PlanError("only plan version 1 is supported", details={"version": version})
    dataset_name = raw.get("dataset")
    if not isinstance(dataset_name, str) or not dataset_name:
        raise PlanError("plan.dataset must be a non-empty string")
    spec = spec_resolver(dataset_name)
    operators = raw.get("operators")
    if not isinstance(operators, list) or not operators:
        raise PlanError("plan.operators must be a non-empty array")
    if len(operators) > 512:
        raise PlanError("plan contains too many operators")

    schema = spec.schema
    state = _ParseState(schema)
    filter_expr: Expr | None = None
    projections: list[Projection] = []
    aggregates: list[Aggregate] = []
    saw_output = False
    output_mode: str | None = None

    for index, operator in enumerate(operators):
        if not isinstance(operator, dict):
            raise PlanError("operator must be a JSON object", details={"operator_index": index})
        op_name = operator.get("op")
        if op_name == "filter":
            if index != 0:
                raise PlanError("Filter must be the first operator", details={"operator_index": index})
            if filter_expr is not None:
                raise PlanError("plan may contain at most one Filter")
            if set(operator) != {"op", "predicate"}:
                raise PlanError("Filter fields must be exactly op and predicate")
            filter_expr = parse_expr(operator["predicate"], schema, state)
            if filter_expr.dtype != BOOL:
                raise PlanError(
                    "Filter predicate must have dtype bool",
                    details={"actual_dtype": filter_expr.dtype},
                )
            continue

        saw_output = True
        if op_name == "project":
            if output_mode == "aggregate":
                raise PlanError("project and aggregate operators cannot be mixed")
            output_mode = "project"
            if set(operator) != {"op", "name", "expr"}:
                raise PlanError("Project fields must be exactly op, name and expr")
            name = _validate_output_name(operator.get("name"), "projection", index)
            expr = parse_expr(operator["expr"], schema, state)
            projections.append(Projection(name, expr))
        elif op_name == "aggregate":
            if output_mode == "project":
                raise PlanError("project and aggregate operators cannot be mixed")
            output_mode = "aggregate"
            aggregates.append(_parse_aggregate(operator, schema, state, index))
        else:
            raise PlanError(
                "unknown operator",
                details={"operator_index": index, "op": op_name},
            )

    if not saw_output:
        raise PlanError("plan must contain at least one project or aggregate operator")

    names = [projection.name for projection in projections] + [aggregate.name for aggregate in aggregates]
    if len(names) != len(set(names)):
        raise PlanError("output column names must be unique", details={"names": names})

    expression_nodes = 0
    for projection in projections:
        expression_nodes += expr_node_count(projection.expr)
    for aggregate in aggregates:
        if aggregate.expr is not None:
            expression_nodes += expr_node_count(aggregate.expr)
    if filter_expr is not None:
        expression_nodes += expr_node_count(filter_expr)
    node_count = expression_nodes
    if node_count > _MAX_EXPR_NODES:
        raise PlanError(
            "plan expression node count exceeds the configured limit",
            details={"node_count": node_count, "max_nodes": _MAX_EXPR_NODES},
        )
    complexity = node_count + (4 if filter_expr is not None else 0) + 2 * len(aggregates)
    source_columns = tuple(sorted(state.source_columns))
    return ParsedPlan(
        dataset=dataset_name,
        spec=spec,
        filter_expr=filter_expr,
        projections=tuple(projections),
        aggregates=tuple(aggregates),
        node_count=node_count,
        complexity=complexity,
        source_columns=source_columns,
    )


@dataclass
class _ParseState:
    schema: dict[str, str]
    source_columns: set[str]

    def __init__(self, schema: dict[str, str]) -> None:
        self.schema = schema
        self.source_columns = set()


def parse_expr(raw: Any, schema: dict[str, str], state: _ParseState, depth: int = 0) -> Expr:
    if depth > _MAX_EXPR_DEPTH:
        raise PlanError("expression nesting exceeds the configured limit")
    if not isinstance(raw, dict):
        raise PlanError("expression node must be a JSON object")
    op_name = raw.get("op")
    if op_name == "column":
        if set(raw) != {"op", "name"}:
            raise PlanError("column expression fields must be exactly op and name")
        name = raw.get("name")
        if not isinstance(name, str) or name not in schema:
            raise PlanError("column does not exist", details={"column": name})
        state.source_columns.add(name)
        return ColumnExpr(name, schema[name])
    if op_name == "literal":
        if set(raw) != {"op", "dtype", "value"}:
            raise PlanError("literal expression fields must be exactly op, dtype and value")
        dtype = raw.get("dtype")
        value = raw.get("value")
        return LiteralExpr(dtype, _validate_literal(dtype, value))
    if op_name == "unary":
        if set(raw) != {"op", "fn", "arg"}:
            raise PlanError("unary expression fields must be exactly op, fn and arg")
        fn = raw.get("fn")
        arg = parse_expr(raw.get("arg"), schema, state, depth + 1)
        if fn == "neg":
            if arg.dtype not in (INT64, FLOAT64):
                raise PlanError("unary neg requires int64 or float64", details={"actual_dtype": arg.dtype})
            return UnaryExpr(fn, arg.dtype, arg)
        if fn == "not":
            if arg.dtype != BOOL:
                raise PlanError("unary not requires bool", details={"actual_dtype": arg.dtype})
            return UnaryExpr(fn, BOOL, arg)
        raise PlanError("unknown unary function", details={"fn": fn})
    if op_name in ("binary", "compare", "logical"):
        if set(raw) != {"op", "fn", "left", "right"}:
            raise PlanError(f"{op_name} expression fields must be exactly op, fn, left and right")
        fn = raw.get("fn")
        left = parse_expr(raw.get("left"), schema, state, depth + 1)
        right = parse_expr(raw.get("right"), schema, state, depth + 1)
        if op_name == "binary":
            if fn not in ("add", "sub", "mul", "div"):
                raise PlanError("unknown arithmetic function", details={"fn": fn})
            if left.dtype not in (INT64, FLOAT64) or right.dtype not in (INT64, FLOAT64):
                raise PlanError("arithmetic requires int64 or float64 operands")
            if left.dtype != right.dtype:
                raise PlanError(
                    "arithmetic operands must have the same dtype; implicit promotion is disabled",
                    details={"left_dtype": left.dtype, "right_dtype": right.dtype},
                )
            return BinaryExpr(fn, left.dtype, left, right)
        if op_name == "compare":
            if fn not in ("eq", "ne", "lt", "le", "gt", "ge"):
                raise PlanError("unknown comparison function", details={"fn": fn})
            if left.dtype != right.dtype:
                raise PlanError(
                    "comparison operands must have the same dtype",
                    details={"left_dtype": left.dtype, "right_dtype": right.dtype},
                )
            if left.dtype == BOOL and fn not in ("eq", "ne"):
                raise PlanError("ordered comparison is not defined for bool")
            return CompareExpr(fn, left, right)
        if fn not in ("and", "or"):
            raise PlanError("unknown logical function", details={"fn": fn})
        if left.dtype != BOOL or right.dtype != BOOL:
            raise PlanError(
                "logical operands must both be bool",
                details={"left_dtype": left.dtype, "right_dtype": right.dtype},
            )
        return LogicalExpr(fn, left, right)
    if op_name == "conditional":
        if set(raw) != {"op", "condition", "then", "else"}:
            raise PlanError("conditional fields must be exactly op, condition, then and else")
        condition = parse_expr(raw.get("condition"), schema, state, depth + 1)
        then_expr = parse_expr(raw.get("then"), schema, state, depth + 1)
        else_expr = parse_expr(raw.get("else"), schema, state, depth + 1)
        if condition.dtype != BOOL:
            raise PlanError("conditional condition must have dtype bool")
        if then_expr.dtype != else_expr.dtype:
            raise PlanError(
                "conditional branches must have the same dtype",
                details={"then_dtype": then_expr.dtype, "else_dtype": else_expr.dtype},
            )
        return ConditionalExpr(condition, then_expr, else_expr, then_expr.dtype)
    raise PlanError("unknown expression operator", details={"op": op_name})


def _parse_aggregate(operator: dict[str, Any], schema: dict[str, str], state: _ParseState, index: int) -> Aggregate:
    fn = operator.get("fn")
    if fn not in ("sum", "count", "count_star", "min", "max", "avg"):
        raise PlanError("unknown aggregate function", details={"fn": fn, "operator_index": index})
    expected = {"op", "name", "fn"} if fn == "count_star" else {"op", "name", "fn", "expr"}
    if set(operator) != expected:
        raise PlanError(
            f"aggregate {fn!r} fields must be {sorted(expected)}",
            details={"operator_index": index},
        )
    name = _validate_output_name(operator.get("name"), "aggregate", index)
    if fn == "count_star":
        return Aggregate(name, fn, INT64, None)
    expr = parse_expr(operator["expr"], schema, state)
    if fn in ("sum", "avg") and expr.dtype not in (INT64, FLOAT64):
        raise PlanError(f"aggregate {fn!r} requires int64 or float64", details={"actual_dtype": expr.dtype})
    if fn == "count":
        return Aggregate(name, fn, INT64, expr)
    if fn == "avg":
        return Aggregate(name, fn, FLOAT64, expr)
    return Aggregate(name, fn, expr.dtype, expr)


def _validate_output_name(value: Any, kind: str, index: int) -> str:
    if not isinstance(value, str) or not _NAME_RE.fullmatch(value):
        raise PlanError(
            f"{kind} name must match [A-Za-z_][A-Za-z0-9_]{{0,63}}",
            details={"operator_index": index, "name": value},
        )
    return value


def _validate_literal(dtype: Any, value: Any) -> Any:
    if dtype not in (INT64, FLOAT64, BOOL):
        raise PlanError("literal dtype must be int64, float64 or bool", details={"dtype": dtype})
    if value is None:
        return None
    if dtype == INT64:
        if isinstance(value, bool) or not isinstance(value, int):
            raise PlanError("int64 literal must be an integer or null")
        if value < INT64_MIN or value > INT64_MAX:
            raise PlanError("int64 literal is outside the signed 64-bit range")
        return value
    if dtype == FLOAT64:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise PlanError("float64 literal must be numeric or null")
        result = float(value)
        if not math.isfinite(result):
            raise PlanError("float64 literal must be finite or null")
        return result
    if not isinstance(value, bool):
        raise PlanError("bool literal must be true, false or null")
    return value


def expr_node_count(expr: Expr) -> int:
    if isinstance(expr, (ColumnExpr, LiteralExpr)):
        return 1
    if isinstance(expr, UnaryExpr):
        return 1 + expr_node_count(expr.arg)
    if isinstance(expr, (BinaryExpr, CompareExpr, LogicalExpr)):
        return 1 + expr_node_count(expr.left) + expr_node_count(expr.right)
    if isinstance(expr, ConditionalExpr):
        return 1 + expr_node_count(expr.condition) + expr_node_count(expr.then_expr) + expr_node_count(expr.else_expr)
    raise TypeError(f"unhandled expression type {type(expr)!r}")


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def payload_hash(plan: Any, execution: Any) -> str:
    return hashlib.sha256(canonical_json({"plan": plan, "execution": execution}).encode("utf-8")).hexdigest()


def plan_summary(plan: ParsedPlan) -> dict[str, Any]:
    return {
        "dataset": plan.dataset,
        "row_count": plan.spec.row_count,
        "filter": plan.filter_expr is not None,
        "mode": "aggregate" if plan.is_aggregate else "project",
        "output_columns": [{"name": name, "dtype": dtype} for name, dtype in plan.output_columns],
        "source_columns": list(plan.source_columns),
        "node_count": plan.node_count,
        "complexity": plan.complexity,
    }


def canonical_plan_for_hash(raw_plan: Any, execution: dict[str, Any]) -> str:
    return hashlib.sha256(
        canonical_json({"plan": raw_plan, "execution": execution}).encode("utf-8")
    ).hexdigest()
