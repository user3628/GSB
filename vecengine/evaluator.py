from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Callable

import numpy as np

from .model import (
    BOOL,
    FLOAT64,
    INT64,
    INT64_MAX,
    INT64_MIN,
    Column,
    ColumnBatch,
)
from .plan import (
    BinaryExpr,
    ColumnExpr,
    CompareExpr,
    ConditionalExpr,
    Expr,
    LiteralExpr,
    LogicalExpr,
    UnaryExpr,
)


_ZERO_U64 = np.uint64(0)
_ONE_U64 = np.uint64(1)
_SIGN_BIT_U64 = np.uint64(1 << 63)
_SIGNED_MAX_U64 = np.uint64(INT64_MAX)


def wrap_int64(value: int) -> int:
    value &= (1 << 64) - 1
    if value >= (1 << 63):
        value -= 1 << 64
    return value


def saturate_int64(value: int) -> int:
    if value > INT64_MAX:
        return INT64_MAX
    if value < INT64_MIN:
        return INT64_MIN
    return value


def apply_integer_policy(value: int, policy: str) -> int:
    return wrap_int64(value) if policy == "wrap" else saturate_int64(value)


class ScalarEvaluator:
    """Exact row-at-a-time evaluator. It intentionally mirrors vector kernels."""

    def __init__(self, integer_overflow: str) -> None:
        self.integer_overflow = integer_overflow

    def evaluate(self, expr: Expr, batch: ColumnBatch, row: int) -> Any:
        if isinstance(expr, ColumnExpr):
            return batch.columns[expr.name].value_at(row)
        if isinstance(expr, LiteralExpr):
            return expr.value
        if isinstance(expr, UnaryExpr):
            arg = self.evaluate(expr.arg, batch, row)
            if arg is None:
                return None
            if expr.fn == "not":
                return not arg
            if expr.dtype == INT64:
                return apply_integer_policy(-arg, self.integer_overflow)
            return -arg
        if isinstance(expr, BinaryExpr):
            left = self.evaluate(expr.left, batch, row)
            right = self.evaluate(expr.right, batch, row)
            if left is None or right is None:
                return None
            if expr.fn == "div" and right == 0:
                return None
            if expr.dtype == INT64:
                return self._integer_binary(expr.fn, left, right)
            if expr.fn == "add":
                return left + right
            if expr.fn == "sub":
                return left - right
            if expr.fn == "mul":
                return left * right
            return left / right
        if isinstance(expr, CompareExpr):
            left = self.evaluate(expr.left, batch, row)
            right = self.evaluate(expr.right, batch, row)
            if left is None or right is None:
                return None
            if expr.fn == "eq":
                return left == right
            if expr.fn == "ne":
                return left != right
            if expr.fn == "lt":
                return left < right
            if expr.fn == "le":
                return left <= right
            if expr.fn == "gt":
                return left > right
            return left >= right
        if isinstance(expr, LogicalExpr):
            left = self.evaluate(expr.left, batch, row)
            right = self.evaluate(expr.right, batch, row)
            if left is None or right is None:
                return None
            return bool(left and right) if expr.fn == "and" else bool(left or right)
        if isinstance(expr, ConditionalExpr):
            condition = self.evaluate(expr.condition, batch, row)
            # Explicit contract: a null condition is not true and selects ELSE.
            branch = expr.then_expr if condition is True else expr.else_expr
            return self.evaluate(branch, batch, row)
        raise TypeError(f"unhandled expression type {type(expr)!r}")

    def _integer_binary(self, fn: str, left: int, right: int) -> int:
        if fn == "add":
            return apply_integer_policy(left + right, self.integer_overflow)
        if fn == "sub":
            return apply_integer_policy(left - right, self.integer_overflow)
        if fn == "mul":
            return apply_integer_policy(left * right, self.integer_overflow)
        if left == INT64_MIN and right == -1:
            return INT64_MIN if self.integer_overflow == "wrap" else INT64_MAX
        negative = (left < 0) != (right < 0)
        quotient = abs(left) // abs(right)
        return -quotient if negative else quotient


class VectorEvaluator:
    """Whole-array evaluator with explicit validity bitmaps."""

    def __init__(self, integer_overflow: str) -> None:
        self.integer_overflow = integer_overflow

    def evaluate(self, expr: Expr, batch: ColumnBatch) -> Column:
        if isinstance(expr, ColumnExpr):
            return batch.columns[expr.name]
        if isinstance(expr, LiteralExpr):
            size = batch.row_count
            values = np.zeros(size, dtype=_numpy_dtype(expr.dtype))
            valid = np.ones(size, dtype=np.bool_)
            if expr.value is None:
                valid.fill(False)
            else:
                values.fill(expr.value)
            return _finish_column("<literal>", expr.dtype, values, valid)
        if isinstance(expr, UnaryExpr):
            arg = self.evaluate(expr.arg, batch)
            if expr.fn == "not":
                return _finish_column("<not>", BOOL, np.logical_not(arg.values), arg.valid.copy())
            if expr.dtype == INT64:
                values = np.negative(arg.values)
                if self.integer_overflow == "saturate":
                    overflow = arg.values == INT64_MIN
                    if np.any(overflow):
                        values = values.copy()
                        values[overflow] = INT64_MAX
                return _finish_column("<neg>", INT64, values, arg.valid.copy())
            return _finish_column("<neg>", FLOAT64, np.negative(arg.values), arg.valid.copy())
        if isinstance(expr, BinaryExpr):
            left = self.evaluate(expr.left, batch)
            right = self.evaluate(expr.right, batch)
            valid = np.logical_and(left.valid, right.valid)
            if expr.dtype == INT64:
                values = self._integer_binary(expr.fn, left.values, right.values, valid)
            else:
                values = self._float_binary(expr.fn, left.values, right.values, right.valid, valid)
            return _finish_column("<binary>", expr.dtype, values, valid)
        if isinstance(expr, CompareExpr):
            left = self.evaluate(expr.left, batch)
            right = self.evaluate(expr.right, batch)
            valid = np.logical_and(left.valid, right.valid)
            values = _compare(expr.fn, left.values, right.values, expr.left.dtype)
            return _finish_column("<compare>", BOOL, values, valid)
        if isinstance(expr, LogicalExpr):
            left = self.evaluate(expr.left, batch)
            right = self.evaluate(expr.right, batch)
            valid = np.logical_and(left.valid, right.valid)
            kernel = np.logical_and if expr.fn == "and" else np.logical_or
            return _finish_column("<logical>", BOOL, kernel(left.values, right.values), valid)
        if isinstance(expr, ConditionalExpr):
            condition = self.evaluate(expr.condition, batch)
            then_col = self.evaluate(expr.then_expr, batch)
            else_col = self.evaluate(expr.else_expr, batch)
            take_then = np.logical_and(condition.valid, condition.values)
            # Null condition is not true, exactly like the scalar path.
            values = np.where(take_then, then_col.values, else_col.values)
            valid = np.where(take_then, then_col.valid, else_col.valid)
            return _finish_column(
                "<conditional>",
                expr.dtype,
                values.astype(_numpy_dtype(expr.dtype), copy=False),
                valid,
            )
        raise TypeError(f"unhandled expression type {type(expr)!r}")

    def _integer_binary(
        self, fn: str, left: np.ndarray, right: np.ndarray, valid: np.ndarray
    ) -> np.ndarray:
        if fn == "add":
            values = np.add(left, right)
            if self.integer_overflow == "saturate":
                overflow = np.logical_and(left < 0, right < 0, out=np.empty_like(valid))
                pos_overflow = np.logical_and(left >= 0, right >= 0, out=np.empty_like(valid))
                overflow = np.logical_or(
                    np.logical_and(pos_overflow, values < 0),
                    np.logical_and(overflow, values >= 0),
                )
                values = np.where(overflow, np.where(left < 0, INT64_MIN, INT64_MAX), values)
            return values
        if fn == "sub":
            values = np.subtract(left, right)
            if self.integer_overflow == "saturate":
                left_neg = left < 0
                right_neg = right < 0
                overflow = np.logical_and(
                    np.logical_xor(left_neg, right_neg),
                    np.logical_xor(left_neg, values < 0),
                )
                values = np.where(overflow, np.where(left_neg, INT64_MIN, INT64_MAX), values)
            return values
        if fn == "mul":
            if self.integer_overflow == "wrap":
                return np.multiply(left, right)
            return self._saturating_multiply(left, right)
        with np.errstate(divide="ignore", invalid="ignore", over="ignore"):
            safe_left = np.where(np.logical_and(left == INT64_MIN, right == -1), 0, left)
            safe_right = np.where(right == 0, 1, right)
            quotient = np.floor_divide(safe_left, safe_right)
            remainder = np.remainder(safe_left, safe_right)
            adjust = np.logical_and(
                remainder != 0,
                np.logical_xor(left < 0, right < 0),
            )
            quotient = np.add(quotient, adjust.astype(np.int64))
            special = np.logical_and(left == INT64_MIN, right == -1)
            special_value = INT64_MIN if self.integer_overflow == "wrap" else INT64_MAX
            quotient = np.where(special, special_value, quotient)
            # Division by zero is represented by the null bitmap, never as an exception.
            valid[:] = np.logical_and(valid, right != 0, out=valid)
            return quotient

    def _saturating_multiply(self, left: np.ndarray, right: np.ndarray) -> np.ndarray:
        left_mag = _unsigned_magnitude(left)
        right_mag = _unsigned_magnitude(right)
        result_negative = np.logical_xor(left < 0, right < 0)
        limit = np.where(result_negative, _SIGN_BIT_U64, _SIGNED_MAX_U64)
        safe_right = np.where(right_mag == 0, _ONE_U64, right_mag)
        overflow = np.logical_and(right_mag != 0, left_mag > np.floor_divide(limit, safe_right))
        product = np.multiply(left_mag, right_mag)
        negative_product = np.add(np.bitwise_not(product), _ONE_U64)
        wrapped = np.where(result_negative, negative_product, product).view(np.int64)
        saturated = np.where(result_negative, INT64_MIN, INT64_MAX)
        return np.where(overflow, saturated, wrapped)

    def _float_binary(
        self,
        fn: str,
        left: np.ndarray,
        right: np.ndarray,
        right_valid: np.ndarray,
        valid: np.ndarray,
    ) -> np.ndarray:
        if fn == "add":
            return np.add(left, right)
        if fn == "sub":
            return np.subtract(left, right)
        if fn == "mul":
            return np.multiply(left, right)
        # Division by zero is null. Invalid/null positions are also left as zero.
        valid[:] = np.logical_and(valid, np.logical_and(right_valid, right != 0), out=valid)
        safe_right = np.where(valid, right, 1.0)
        with np.errstate(divide="ignore", invalid="ignore", over="ignore"):
            return np.divide(left, safe_right, out=np.zeros_like(left), where=valid)


@dataclass
class ResultTable:
    names: tuple[str, ...]
    dtypes: tuple[str, ...]
    values: dict[str, np.ndarray]
    valid: dict[str, np.ndarray]
    row_count: int


class CompensatedSum:
    """Order-stable Neumaier compensated sum used by both execution paths."""

    def __init__(self) -> None:
        self.total = 0.0
        self.correction = 0.0
        self.nonfinite: float | None = None

    def add(self, value: float) -> None:
        value = float(value)
        if not math.isfinite(value):
            if math.isnan(value):
                self.nonfinite = math.nan
            elif self.nonfinite is None:
                self.nonfinite = value
            elif math.isinf(self.nonfinite) and self.nonfinite != value:
                self.nonfinite = math.nan
            return
        if self.nonfinite is not None:
            return
        updated = self.total + value
        if not math.isfinite(updated):
            self.nonfinite = updated
            return
        if abs(self.total) >= abs(value):
            self.correction += (self.total - updated) + value
        else:
            self.correction += (value - updated) + self.total
        self.total = updated

    def value(self) -> float:
        if self.nonfinite is not None:
            return self.nonfinite
        return self.total + self.correction


class AggregateState:
    def __init__(self, aggregate: Any, integer_overflow: str) -> None:
        self.aggregate = aggregate
        self.integer_overflow = integer_overflow
        self.count = 0
        self.integer_total = 0
        self.float_total = CompensatedSum()
        self.extreme: Any = None

    def add_row(self) -> None:
        if self.aggregate.fn == "count_star":
            self.count += 1

    def add_rows(self, count: int) -> None:
        if self.aggregate.fn == "count_star":
            self.count += count

    def add(self, value: Any) -> None:
        fn = self.aggregate.fn
        if value is None:
            return
        if fn == "count":
            self.count += 1
        elif fn in ("sum", "avg"):
            self.count += 1
            if self.aggregate.expr is not None and self.aggregate.expr.dtype == INT64:
                self.integer_total += int(value)
            else:
                self.float_total.add(float(value))
        elif fn == "min":
            self.count += 1
            self.extreme = value if self.extreme is None or value < self.extreme else self.extreme
        elif fn == "max":
            self.count += 1
            self.extreme = value if self.extreme is None or value > self.extreme else self.extreme

    def finalize(self) -> tuple[Any, bool]:
        fn = self.aggregate.fn
        if fn in ("count", "count_star"):
            return self.count, True
        if self.count == 0:
            return None, False
        if fn == "avg":
            if self.aggregate.expr is not None and self.aggregate.expr.dtype == INT64:
                return self.integer_total / self.count, True
            return self.float_total.value() / self.count, True
        if fn == "sum":
            if self.aggregate.expr is not None and self.aggregate.expr.dtype == INT64:
                return apply_integer_policy(self.integer_total, self.integer_overflow), True
            return self.float_total.value(), True
        return self.extreme, True


BatchBeforeCallback = Callable[[int, int, int], None]
BatchAfterCallback = Callable[[dict[str, Any]], None]


def execute_plan(
    plan: Any,
    dataset: Any,
    options: Any,
    selection: Any,
    *,
    before_batch: BatchBeforeCallback | None = None,
    after_batch: BatchAfterCallback | None = None,
) -> ResultTable:
    if plan.is_aggregate:
        return _execute_aggregate(plan, dataset, options, selection, before_batch, after_batch)
    return _execute_projection(plan, dataset, options, selection, before_batch, after_batch)


def _execute_projection(
    plan: Any,
    dataset: Any,
    options: Any,
    selection: Any,
    before_batch: BatchBeforeCallback | None,
    after_batch: BatchAfterCallback | None,
) -> ResultTable:
    rows = dataset.row_count
    names = tuple(projection.name for projection in plan.projections)
    dtypes = tuple(projection.dtype for projection in plan.projections)
    values = {name: np.zeros(rows, dtype=_numpy_dtype(dtype)) for name, dtype in zip(names, dtypes)}
    valid = {name: np.ones(rows, dtype=np.bool_) for name in names}
    scalar = ScalarEvaluator(options.integer_overflow)
    vector = VectorEvaluator(options.integer_overflow)
    written = 0
    batch_index = 0
    for start in range(0, rows, selection.batch_size):
        end = min(rows, start + selection.batch_size)
        if before_batch is not None:
            before_batch(batch_index, start, end)
        if options.test_batch_delay_ms:
            import time

            time.sleep(options.test_batch_delay_ms / 1000.0)
        batch = dataset.batch(start, end - start)
        rows_out = 0
        if selection.path == "scalar":
            for row in range(batch.row_count):
                if plan.filter_expr is not None:
                    predicate = scalar.evaluate(plan.filter_expr, batch, row)
                    if predicate is not True:
                        continue
                for projection in plan.projections:
                    value = scalar.evaluate(projection.expr, batch, row)
                    if value is None:
                        valid[projection.name][written + rows_out] = False
                    else:
                        values[projection.name][written + rows_out] = value
                rows_out += 1
        else:
            filtered = _filter_batch_vector(plan.filter_expr, batch, vector)
            rows_out = filtered.row_count
            for projection in plan.projections:
                column = vector.evaluate(projection.expr, filtered)
                values[projection.name][written : written + rows_out] = column.values
                values[projection.name][written : written + rows_out][~column.valid] = 0
                valid[projection.name][written : written + rows_out] = column.valid
        written += rows_out
        if after_batch is not None:
            after_batch(
                _batch_stat(
                    batch_index,
                    start,
                    end,
                    rows_out,
                    selection,
                    plan.node_count,
                )
            )
        batch_index += 1
    trimmed_values = {name: values[name][:written] for name in names}
    trimmed_valid = {name: valid[name][:written] for name in names}
    return ResultTable(names, dtypes, trimmed_values, trimmed_valid, written)


def _execute_aggregate(
    plan: Any,
    dataset: Any,
    options: Any,
    selection: Any,
    before_batch: BatchBeforeCallback | None,
    after_batch: BatchAfterCallback | None,
) -> ResultTable:
    states = [AggregateState(aggregate, options.integer_overflow) for aggregate in plan.aggregates]
    scalar = ScalarEvaluator(options.integer_overflow)
    vector = VectorEvaluator(options.integer_overflow)
    batch_index = 0
    rows = dataset.row_count
    for start in range(0, rows, selection.batch_size):
        end = min(rows, start + selection.batch_size)
        if before_batch is not None:
            before_batch(batch_index, start, end)
        if options.test_batch_delay_ms:
            import time

            time.sleep(options.test_batch_delay_ms / 1000.0)
        batch = dataset.batch(start, end - start)
        if selection.path == "scalar":
            rows_out = 0
            for row in range(batch.row_count):
                if plan.filter_expr is not None:
                    predicate = scalar.evaluate(plan.filter_expr, batch, row)
                    if predicate is not True:
                        continue
                rows_out += 1
                for aggregate, state in zip(plan.aggregates, states):
                    if aggregate.fn == "count_star":
                        state.add_row()
                    else:
                        state.add(scalar.evaluate(aggregate.expr, batch, row))
        else:
            filtered = _filter_batch_vector(plan.filter_expr, batch, vector)
            rows_out = filtered.row_count
            for aggregate, state in zip(plan.aggregates, states):
                if aggregate.fn == "count_star":
                    state.add_rows(rows_out)
                else:
                    column = vector.evaluate(aggregate.expr, filtered)
                    for value in _python_values(column):
                        state.add(value)
        if after_batch is not None:
            after_batch(
                _batch_stat(
                    batch_index,
                    start,
                    end,
                    rows_out,
                    selection,
                    plan.node_count,
                )
            )
        batch_index += 1

    names = tuple(aggregate.name for aggregate in plan.aggregates)
    dtypes = tuple(aggregate.dtype for aggregate in plan.aggregates)
    values: dict[str, np.ndarray] = {}
    valid: dict[str, np.ndarray] = {}
    for aggregate, state in zip(plan.aggregates, states):
        value, is_valid = state.finalize()
        array = np.zeros(1, dtype=_numpy_dtype(aggregate.dtype))
        validity = np.ones(1, dtype=np.bool_)
        if is_valid:
            if value is not None:
                array[0] = value
        else:
            validity[0] = False
        values[aggregate.name] = array
        valid[aggregate.name] = validity
    return ResultTable(names, dtypes, values, valid, 1)


def _filter_batch_vector(expr: Expr | None, batch: ColumnBatch, evaluator: VectorEvaluator) -> ColumnBatch:
    if expr is None:
        return batch
    predicate = evaluator.evaluate(expr, batch)
    mask = np.logical_and(predicate.valid, predicate.values)
    return batch.take(mask)


def _python_values(column: Column) -> list[Any]:
    if column.dtype == INT64:
        return [int(value) for value in column.values[column.valid]]
    if column.dtype == FLOAT64:
        return [float(value) for value in column.values[column.valid]]
    return [bool(value) for value in column.values[column.valid]]


def _batch_stat(
    batch_index: int,
    row_start: int,
    row_end: int,
    rows_out: int,
    selection: Any,
    expression_nodes: int,
) -> dict[str, Any]:
    return {
        "batch_index": batch_index,
        "row_start": row_start,
        "row_end": row_end,
        "rows_in": row_end - row_start,
        "rows_out": rows_out,
        "path": selection.path,
        "batch_size": selection.batch_size,
        "memory_estimate_bytes": selection.batch_working_bytes,
        "expression_nodes": expression_nodes,
    }


def _compare(fn: str, left: np.ndarray, right: np.ndarray, dtype: str) -> np.ndarray:
    if dtype == BOOL:
        return np.equal(left, right) if fn == "eq" else np.not_equal(left, right)
    if fn == "eq":
        return np.equal(left, right)
    if fn == "ne":
        return np.not_equal(left, right)
    if fn == "lt":
        return np.less(left, right)
    if fn == "le":
        return np.less_equal(left, right)
    if fn == "gt":
        return np.greater(left, right)
    return np.greater_equal(left, right)


def _unsigned_magnitude(values: np.ndarray) -> np.ndarray:
    raw = values.view(np.uint64)
    negative = values < 0
    return np.where(negative, np.add(np.bitwise_not(raw), _ONE_U64), raw).astype(np.uint64, copy=False)


def _numpy_dtype(dtype: str) -> np.dtype:
    if dtype == INT64:
        return np.dtype("int64")
    if dtype == FLOAT64:
        return np.dtype("float64")
    if dtype == BOOL:
        return np.dtype("bool")
    raise ValueError(f"unsupported dtype {dtype!r}")


def _finish_column(name: str, dtype: str, values: np.ndarray, valid: np.ndarray) -> Column:
    if not bool(np.all(valid)):
        values = values.copy()
        values[~valid] = 0
    return Column(name, dtype, values, valid)
