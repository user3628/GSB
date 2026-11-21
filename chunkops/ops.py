"""Blocked Softmax/LayerNorm kernels and static execution scheduling."""

from __future__ import annotations

import math
import time
from dataclasses import dataclass, field
from typing import Any, Callable

import numpy as np

from .errors import (
    ChunkOpsError,
    E_INVALID_AXIS,
    E_INVALID_SHAPE,
    E_MEMORY_BUDGET,
    E_NONFINITE,
    E_ZERO_ROW,
)
from .fingerprint import sha256_array
from .strategies import (
    COMPENSATED,
    PAIRWISE,
    SEQ,
    ChunkPlan,
    estimate_workspace_bytes,
    gamma_for_strategy,
)


class ExecutionInterrupted(RuntimeError):
    """Raised when a cooperative graceful-stop request is observed."""


def _ceil_div(numerator: int, denominator: int) -> int:
    return (numerator + denominator - 1) // denominator


def _ranges(length: int, step: int) -> list[tuple[int, int]]:
    return [(start, min(length, start + step)) for start in range(0, length, step)]


def validate_matrix(x: np.ndarray, *, fragment_id: str | None = None) -> None:
    if not isinstance(x, np.ndarray) or x.ndim != 2 or x.shape[0] <= 0 or x.shape[1] <= 0:
        shape = list(getattr(x, "shape", ()))
        raise ChunkOpsError(
            E_INVALID_SHAPE,
            "input must be a non-empty 2-D tensor",
            {"fragment_id": fragment_id, "shape": shape, "position": "tensor"},
        )
    if not np.issubdtype(x.dtype, np.floating):
        raise ChunkOpsError(
            E_INVALID_SHAPE,
            "input tensor must have a floating dtype",
            {"fragment_id": fragment_id, "dtype": str(x.dtype), "position": "tensor"},
        )


def validate_axis(x: np.ndarray, axis: int, *, fragment_id: str | None = None) -> None:
    if int(axis) not in (0, 1) or int(axis) >= x.ndim:
        raise ChunkOpsError(
            E_INVALID_AXIS,
            "reduction axis does not match tensor dimensions",
            {
                "fragment_id": fragment_id,
                "shape": [int(v) for v in x.shape],
                "axis": int(axis),
                "position": "axis",
            },
        )


def scan_nonfinite(x: np.ndarray, *, fragment_id: str | None = None) -> dict[str, Any]:
    """Locate the first NaN/+Inf/-Inf positions without modifying the tensor."""

    arr = np.asarray(x)
    nan_mask = np.isnan(arr)
    pos_mask = np.isposinf(arr)
    neg_mask = np.isneginf(arr)
    any_mask = ~np.isfinite(arr)

    def first(mask: np.ndarray) -> dict[str, Any] | None:
        if not bool(mask.any()):
            return None
        index = int(np.argmax(mask))
        row, col = np.unravel_index(index, arr.shape)
        return {
            "linear_index": index,
            "row": int(row),
            "col": int(col),
            "position": [int(row), int(col)],
        }

    result = {
        "fragment_id": fragment_id,
        "shape": [int(v) for v in arr.shape],
        "nan_count": int(np.count_nonzero(nan_mask)),
        "pos_inf_count": int(np.count_nonzero(pos_mask)),
        "neg_inf_count": int(np.count_nonzero(neg_mask)),
        "anomaly_ratio": float(np.count_nonzero(any_mask) / max(1, arr.size)),
        "first_any": first(any_mask),
        "first_nan": first(nan_mask),
        "first_pos_inf": first(pos_mask),
        "first_neg_inf": first(neg_mask),
        "ieee_policy": "propagate; no zero substitution",
    }
    return result


def find_zero_rows(x: np.ndarray) -> list[int]:
    if x.ndim != 2:
        return []
    return [int(index) for index in np.flatnonzero(np.all(x == 0, axis=1))]


def reject_nonfinite_or_zero_rows(
    x: np.ndarray,
    *,
    fragment_id: str | None = None,
    reject_nonfinite: bool = False,
    reject_zero_rows: bool = False,
) -> None:
    if reject_nonfinite:
        scan = scan_nonfinite(x, fragment_id=fragment_id)
        if scan["first_any"] is not None:
            raise ChunkOpsError(
                E_NONFINITE,
                "input contains NaN or Inf values",
                {
                    "fragment_id": fragment_id,
                    "shape": scan["shape"],
                    **{key: scan[key] for key in ("first_any", "first_nan", "first_pos_inf", "first_neg_inf")},
                },
            )
    if reject_zero_rows:
        rows = find_zero_rows(x)
        if rows:
            raise ChunkOpsError(
                E_ZERO_ROW,
                "input contains all-zero rows",
                {
                    "fragment_id": fragment_id,
                    "shape": [int(v) for v in x.shape],
                    "zero_rows": rows,
                    "count": len(rows),
                    "position": {"row": rows[0], "col": 0},
                },
            )


def reduce_sum(values: np.ndarray, axis: int, strategy: str) -> np.ndarray:
    """Explicit sequential, pairwise, or compensated reduction along one axis."""

    array = np.asarray(values, dtype=np.float64)
    if array.ndim != 2:
        raise ValueError("reduce_sum expects a 2-D array")
    if axis not in (0, 1):
        raise ValueError("reduce_sum axis must be 0 or 1")
    n = array.shape[axis]
    if n == 0:
        raise ValueError("cannot reduce an empty axis")
    output_shape = (array.shape[1],) if axis == 0 else (array.shape[0],)

    if strategy == SEQ:
        accumulator = np.zeros(output_shape, dtype=np.float64)
        with np.errstate(over="ignore", invalid="ignore", under="ignore"):
            for index in range(n):
                if axis == 0:
                    np.add(accumulator, array[index, :], out=accumulator)
                else:
                    np.add(accumulator, array[:, index], out=accumulator)
        return accumulator

    if strategy == PAIRWISE:
        work = np.moveaxis(array, axis, 0)
        carry: np.ndarray | None = None
        with np.errstate(over="ignore", invalid="ignore", under="ignore"):
            while work.shape[0] > 1:
                count = work.shape[0]
                if count % 2:
                    last = work[-1, :]
                    carry = last.copy() if carry is None else carry + last
                    work = work[:-1, :]
                    count -= 1
                work = work[0:count:2, :] + work[1:count:2, :]
            result = work[0, :].copy()
            if carry is not None:
                result = result + carry
        return result

    if strategy == COMPENSATED:
        # Neumaier compensated summation tracks the lost low-order term in
        # both magnitude branches, including the common large+small case.
        accumulator = np.zeros(output_shape, dtype=np.float64)
        compensation = np.zeros(output_shape, dtype=np.float64)
        with np.errstate(over="ignore", invalid="ignore", under="ignore"):
            for index in range(n):
                value = array[index, :] if axis == 0 else array[:, index]
                updated = accumulator + value
                magnitude_acc = np.abs(accumulator)
                magnitude_value = np.abs(value)
                high = np.where(magnitude_acc >= magnitude_value, accumulator, value)
                low = np.where(magnitude_acc >= magnitude_value, value, accumulator)
                compensation = compensation + ((high - updated) + low)
                accumulator = updated
        return accumulator + compensation

    raise ValueError(f"unknown sum strategy: {strategy}")


def reference_softmax(x: np.ndarray, axis: int) -> np.ndarray:
    """Vectorized float64 reference used for broad smoke checks."""

    values = np.asarray(x, dtype=np.float64)
    maximum = np.max(values, axis=axis, keepdims=True)
    with np.errstate(all="ignore"):
        exponentials = np.exp(values - maximum)
        return exponentials / np.sum(exponentials, axis=axis, keepdims=True)


def reference_layernorm(
    x: np.ndarray,
    axis: int,
    *,
    gamma: np.ndarray | None = None,
    beta: np.ndarray | None = None,
    eps: float = 1e-5,
) -> np.ndarray:
    values = np.asarray(x, dtype=np.float64)
    mean = np.mean(values, axis=axis, keepdims=True)
    variance = np.mean((values - mean) ** 2, axis=axis, keepdims=True)
    with np.errstate(all="ignore"):
        normalized = (values - mean) / np.sqrt(variance + eps)
    if gamma is not None:
        shape = [1, 1]
        shape[axis] = values.shape[axis]
        normalized = normalized * np.asarray(gamma, dtype=np.float64).reshape(shape)
    if beta is not None:
        shape = [1, 1]
        shape[axis] = values.shape[axis]
        normalized = normalized + np.asarray(beta, dtype=np.float64).reshape(shape)
    return normalized


@dataclass
class ScheduleStep:
    index: int
    row_range: list[int]
    col_range: list[int]
    reduction_chunk: list[int]
    estimated_peak_bytes: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "index": self.index,
            "row_range": self.row_range,
            "col_range": self.col_range,
            "reduction_chunk": self.reduction_chunk,
            "estimated_peak_bytes": self.estimated_peak_bytes,
        }


@dataclass
class ExecutionSchedule:
    requested_row_block: int
    requested_col_block: int
    effective_row_block: int
    effective_col_block: int
    input_bytes: int
    output_bytes: int
    workspace_bytes: int
    base_bytes: int
    peak_bytes: int
    budget_bytes: int
    split_decisions: list[dict[str, Any]] = field(default_factory=list)
    steps: list[ScheduleStep] = field(default_factory=list)

    def to_dict(self, include_steps: bool = True) -> dict[str, Any]:
        value = {
            "requested_row_block": self.requested_row_block,
            "requested_col_block": self.requested_col_block,
            "effective_row_block": self.effective_row_block,
            "effective_col_block": self.effective_col_block,
            "input_bytes": self.input_bytes,
            "output_bytes": self.output_bytes,
            "workspace_bytes": self.workspace_bytes,
            "base_bytes": self.base_bytes,
            "peak_bytes": self.peak_bytes,
            "budget_bytes": self.budget_bytes,
            "split_decisions": self.split_decisions,
            "step_count": len(self.steps),
        }
        if include_steps:
            value["steps"] = [step.to_dict() for step in self.steps]
        return value


def build_schedule(
    plan: ChunkPlan,
    *,
    input_itemsize: int,
    output_itemsize: int,
    memory_budget_bytes: int,
) -> ExecutionSchedule:
    """Statically fit full buffers plus worst-case block workspace into budget."""

    if memory_budget_bytes <= 0:
        raise ChunkErrorBudget(
            "memory_budget_bytes must be positive",
            {
                "budget_bytes": memory_budget_bytes,
                "reason": "non_positive_budget",
            },
        )
    input_bytes = plan.rows * plan.cols * input_itemsize
    output_bytes = plan.rows * plan.cols * output_itemsize
    overhead = 1024 * 1024
    base = input_bytes + output_bytes + overhead
    row_block = plan.row_block
    col_block = plan.col_block
    decisions: list[dict[str, Any]] = []

    def workspace(rb: int, cb: int) -> int:
        return estimate_workspace_bytes(
            plan.op,
            plan.axis,
            plan.rows,
            plan.cols,
            rb,
            cb,
            8,
            output_itemsize,
        )

    work = workspace(row_block, col_block)
    while base + work > memory_budget_bytes and (row_block > 1 or col_block > 1):
        old = (row_block, col_block, work)
        if plan.axis == 1:
            if row_block > 1:
                row_block = max(1, row_block // 2)
            elif col_block > 1:
                col_block = max(1, col_block // 2)
        else:
            if col_block > 1:
                col_block = max(1, col_block // 2)
            elif row_block > 1:
                row_block = max(1, row_block // 2)
        # If halving the preferred output dimension did not shrink the tile
        # enough, also halve the other dimension before repeating.
        new_work = workspace(row_block, col_block)
        if new_work == old[2] and (row_block > 1 or col_block > 1):
            if row_block > 1 and row_block >= col_block:
                row_block = max(1, row_block // 2)
            elif col_block > 1:
                col_block = max(1, col_block // 2)
            new_work = workspace(row_block, col_block)
        work = new_work
        decisions.append(
            {
                "reason": "workspace_reduced_to_fit_peak_budget",
                "before": {"row_block": old[0], "col_block": old[1], "workspace_bytes": old[2]},
                "after": {"row_block": row_block, "col_block": col_block, "workspace_bytes": work},
                "budget_bytes": memory_budget_bytes,
                "base_bytes": base,
            }
        )

    required = base + work
    if required > memory_budget_bytes:
        raise ChunkErrorBudget(
            "peak memory budget cannot hold input, output, and minimum workspace",
            {
                "budget_bytes": memory_budget_bytes,
                "required_bytes": required,
                "base_bytes": base,
                "input_bytes": input_bytes,
                "output_bytes": output_bytes,
                "minimum_workspace_bytes": work,
                "renderable_output_bytes": output_bytes,
                "reason": "irreducible_buffers_exceed_budget",
                "suggested_minimum_budget_bytes": int(required * 1.10 + 65536),
            },
        )

    steps: list[ScheduleStep] = []
    step_index = 0
    if plan.axis == 1:
        for row_start, row_end in _ranges(plan.rows, row_block):
            for col_start, col_end in _ranges(plan.cols, col_block):
                steps.append(
                    ScheduleStep(
                        index=step_index,
                        row_range=[row_start, row_end],
                        col_range=[0, plan.cols],
                        reduction_chunk=[col_start, col_end],
                        estimated_peak_bytes=required,
                    )
                )
                step_index += 1
    else:
        for col_start, col_end in _ranges(plan.cols, col_block):
            for row_start, row_end in _ranges(plan.rows, row_block):
                steps.append(
                    ScheduleStep(
                        index=step_index,
                        row_range=[0, plan.rows],
                        col_range=[col_start, col_end],
                        reduction_chunk=[row_start, row_end],
                        estimated_peak_bytes=required,
                    )
                )
                step_index += 1

    return ExecutionSchedule(
        requested_row_block=plan.row_block,
        requested_col_block=plan.col_block,
        effective_row_block=row_block,
        effective_col_block=col_block,
        input_bytes=input_bytes,
        output_bytes=output_bytes,
        workspace_bytes=work,
        base_bytes=base,
        peak_bytes=required,
        budget_bytes=memory_budget_bytes,
        split_decisions=decisions,
        steps=steps,
    )


class ChunkErrorBudget(ChunkOpsError):
    """Specialized E_MEMORY_BUDGET error with a stable constructor."""

    def __init__(self, message: str, details: dict[str, Any]):
        super().__init__(E_MEMORY_BUDGET, message, details)


@dataclass
class OpResult:
    output: np.ndarray
    plan: ChunkPlan
    schedule: ExecutionSchedule
    output_digest: str
    nonfinite: dict[str, Any]
    zero_rows: list[int]
    elapsed_seconds: float
    execution_steps: int

    def metadata(self, *, include_steps: bool = False) -> dict[str, Any]:
        return {
            "output_digest": self.output_digest,
            "output_shape": [int(v) for v in self.output.shape],
            "output_dtype": str(self.output.dtype),
            "elapsed_seconds": self.elapsed_seconds,
            "execution_steps": self.execution_steps,
            "nonfinite": self.nonfinite,
            "zero_rows": self.zero_rows,
            "schedule": self.schedule.to_dict(include_steps=include_steps),
        }


def _softmax_effective(
    x: np.ndarray,
    output: np.ndarray,
    *,
    output_row_block: int,
    reduction_col_block: int,
    strategy: str,
    output_dtype: np.dtype,
    stop_check: Callable[[], bool] | None,
) -> int:
    rows, cols = x.shape
    steps = 0
    for row_start, row_end in _ranges(rows, output_row_block):
        if stop_check is not None and stop_check():
            raise ExecutionInterrupted("stop requested during softmax reduction")
        row_count = row_end - row_start
        running_max = np.full(row_count, -np.inf, dtype=np.float64)
        running_sum = np.zeros(row_count, dtype=np.float64)
        for col_start, col_end in _ranges(cols, reduction_col_block):
            block = np.asarray(x[row_start:row_end, col_start:col_end], dtype=np.float64)
            with np.errstate(over="ignore", invalid="ignore", divide="ignore"):
                block_max = np.max(block, axis=1)
                new_max = np.maximum(running_max, block_max)
                exponentials = np.exp(block - new_max[:, None])
            block_sum = reduce_sum(exponentials, 1, strategy)
            with np.errstate(over="ignore", invalid="ignore", divide="ignore"):
                running_sum = (
                    running_sum * np.exp(running_max - new_max) + block_sum
                )
            running_max = new_max
            steps += 1
        for col_start, col_end in _ranges(cols, reduction_col_block):
            block = np.asarray(x[row_start:row_end, col_start:col_end], dtype=np.float64)
            with np.errstate(over="ignore", invalid="ignore", divide="ignore"):
                normalized = np.exp(block - running_max[:, None]) / running_sum[:, None]
            output[row_start:row_end, col_start:col_end] = normalized.astype(
                output_dtype, copy=False
            )
            steps += 1
    return steps


def _layernorm_effective(
    x: np.ndarray,
    output: np.ndarray,
    *,
    output_row_block: int,
    reduction_col_block: int,
    strategy: str,
    gamma: np.ndarray | None,
    beta: np.ndarray | None,
    eps: float,
    output_dtype: np.dtype,
    stop_check: Callable[[], bool] | None,
) -> int:
    rows, cols = x.shape
    gamma64 = None if gamma is None else np.asarray(gamma, dtype=np.float64)
    beta64 = None if beta is None else np.asarray(beta, dtype=np.float64)
    steps = 0
    for row_start, row_end in _ranges(rows, output_row_block):
        if stop_check is not None and stop_check():
            raise ExecutionInterrupted("stop requested during layernorm reduction")
        row_count = row_end - row_start
        count = 0
        mean = np.zeros(row_count, dtype=np.float64)
        m2 = np.zeros(row_count, dtype=np.float64)
        for col_start, col_end in _ranges(cols, reduction_col_block):
            block = np.asarray(x[row_start:row_end, col_start:col_end], dtype=np.float64)
            block_count = col_end - col_start
            if block_count:
                block_mean = reduce_sum(block, 1, strategy) / block_count
                delta = block - block_mean[:, None]
                with np.errstate(over="ignore", invalid="ignore"):
                    block_m2 = reduce_sum(delta * delta, 1, strategy)
                new_count = count + block_count
                delta_mean = block_mean - mean
                with np.errstate(over="ignore", invalid="ignore", divide="ignore"):
                    mean = mean + delta_mean * (block_count / new_count)
                    m2 = (
                        m2
                        + block_m2
                        + delta_mean * delta_mean * (count * block_count / new_count)
                    )
                count = new_count
            steps += 1
        if count == 0:
            continue
        with np.errstate(over="ignore", invalid="ignore", divide="ignore"):
            variance = m2 / count
            inverse_std = 1.0 / np.sqrt(variance + eps)
        for col_start, col_end in _ranges(cols, reduction_col_block):
            block = np.asarray(x[row_start:row_end, col_start:col_end], dtype=np.float64)
            with np.errstate(over="ignore", invalid="ignore", divide="ignore"):
                normalized = (block - mean[:, None]) * inverse_std[:, None]
            if gamma64 is not None:
                normalized = normalized * gamma64[col_start:col_end][None, :]
            if beta64 is not None:
                normalized = normalized + beta64[col_start:col_end][None, :]
            output[row_start:row_end, col_start:col_end] = normalized.astype(
                output_dtype, copy=False
            )
            steps += 1
    return steps


def execute_blocked_op(
    x: np.ndarray,
    plan: ChunkPlan,
    *,
    gamma: np.ndarray | None = None,
    beta: np.ndarray | None = None,
    eps: float = 1e-5,
    output_dtype: str | np.dtype | None = None,
    memory_budget_bytes: int = 512 * 1024 * 1024,
    fragment_id: str | None = None,
    stop_check: Callable[[], bool] | None = None,
) -> OpResult:
    """Execute a blocked Softmax or LayerNorm with deterministic ordering."""

    validate_matrix(x, fragment_id=fragment_id)
    validate_axis(x, plan.axis, fragment_id=fragment_id)
    if (plan.rows, plan.cols) != (int(x.shape[0]), int(x.shape[1])):
        raise ChunkOpsError(
            E_INVALID_SHAPE,
            "plan dimensions do not match the input tensor",
            {
                "fragment_id": fragment_id,
                "plan_shape": [plan.rows, plan.cols],
                "input_shape": [int(v) for v in x.shape],
                "position": "plan.shape",
            },
        )
    if plan.op == "softmax":
        if gamma is not None or beta is not None:
            raise ChunkOpsError(
                "E_INVALID_CONFIG",
                "Softmax does not accept gamma or beta",
                {"fragment_id": fragment_id},
            )
    elif plan.op == "layernorm":
        reduction_length = plan.reduction_length
        if gamma is not None and np.asarray(gamma).shape != (reduction_length,):
            raise ChunkOpsError(
                E_INVALID_SHAPE,
                "gamma shape does not match normalized axis",
                {
                    "fragment_id": fragment_id,
                    "gamma_shape": list(np.asarray(gamma).shape),
                    "expected": [reduction_length],
                    "position": "gamma",
                },
            )
        if beta is not None and np.asarray(beta).shape != (reduction_length,):
            raise ChunkOpsError(
                E_INVALID_SHAPE,
                "beta shape does not match normalized axis",
                {
                    "fragment_id": fragment_id,
                    "beta_shape": list(np.asarray(beta).shape),
                    "expected": [reduction_length],
                    "position": "beta",
                },
            )
    else:
        raise ChunkOpsError(
            "E_INVALID_CONFIG", f"unsupported op: {plan.op}", {"op": plan.op}
        )

    dtype = np.dtype(output_dtype if output_dtype is not None else x.dtype)
    schedule = build_schedule(
        plan,
        input_itemsize=x.dtype.itemsize,
        output_itemsize=dtype.itemsize,
        memory_budget_bytes=memory_budget_bytes,
    )
    nonfinite = scan_nonfinite(x, fragment_id=fragment_id)
    zero_rows = find_zero_rows(x)
    output = np.empty_like(x, dtype=dtype)
    started = time.perf_counter()

    if plan.axis == 1:
        x_effective = x
        output_effective = output
        output_block = schedule.effective_row_block
        reduction_block = schedule.effective_col_block
    else:
        x_effective = x.T
        output_effective = output.T
        output_block = schedule.effective_col_block
        reduction_block = schedule.effective_row_block

    if plan.op == "softmax":
        steps = _softmax_effective(
            x_effective,
            output_effective,
            output_row_block=output_block,
            reduction_col_block=reduction_block,
            strategy=plan.sum_strategy,
            output_dtype=dtype,
            stop_check=stop_check,
        )
    else:
        if plan.axis == 1:
            gamma_effective = gamma
            beta_effective = beta
        else:
            gamma_effective = gamma
            beta_effective = beta
        steps = _layernorm_effective(
            x_effective,
            output_effective,
            output_row_block=output_block,
            reduction_col_block=reduction_block,
            strategy=plan.sum_strategy,
            gamma=gamma_effective,
            beta=beta_effective,
            eps=float(eps),
            output_dtype=dtype,
            stop_check=stop_check,
        )

    elapsed = time.perf_counter() - started
    return OpResult(
        output=output,
        plan=plan,
        schedule=schedule,
        output_digest=sha256_array(output),
        nonfinite=nonfinite,
        zero_rows=zero_rows,
        elapsed_seconds=elapsed,
        execution_steps=steps,
    )


def softmax(
    x: np.ndarray,
    axis: int = 1,
    *,
    plan: ChunkPlan | None = None,
    cache_budget_bytes: int = 4 * 1024 * 1024,
    memory_budget_bytes: int = 512 * 1024 * 1024,
    output_dtype: str | np.dtype | None = None,
    fragment_id: str | None = None,
) -> np.ndarray:
    """Convenience Softmax entry point returning the output tensor only."""

    from .strategies import build_auto_plan

    validate_matrix(x, fragment_id=fragment_id)
    validate_axis(x, axis, fragment_id=fragment_id)
    if plan is None:
        plan = build_auto_plan(
            op="softmax",
            axis=int(axis),
            rows=int(x.shape[0]),
            cols=int(x.shape[1]),
            cache_budget_bytes=cache_budget_bytes,
            anomaly_ratio=float(scan_nonfinite(x)["anomaly_ratio"]),
            input_itemsize=x.dtype.itemsize,
            output_itemsize=(
                np.dtype(output_dtype).itemsize if output_dtype else x.dtype.itemsize
            ),
        )
    return execute_blocked_op(
        x,
        plan,
        memory_budget_bytes=memory_budget_bytes,
        output_dtype=output_dtype,
        fragment_id=fragment_id,
    ).output


def layernorm(
    x: np.ndarray,
    axis: int = 0,
    *,
    gamma: np.ndarray | None = None,
    beta: np.ndarray | None = None,
    eps: float = 1e-5,
    plan: ChunkPlan | None = None,
    cache_budget_bytes: int = 4 * 1024 * 1024,
    memory_budget_bytes: int = 512 * 1024 * 1024,
    output_dtype: str | np.dtype | None = None,
    fragment_id: str | None = None,
) -> np.ndarray:
    """Convenience LayerNorm entry point returning the output tensor only."""

    from .strategies import build_auto_plan

    validate_matrix(x, fragment_id=fragment_id)
    validate_axis(x, axis, fragment_id=fragment_id)
    if plan is None:
        plan = build_auto_plan(
            op="layernorm",
            axis=int(axis),
            rows=int(x.shape[0]),
            cols=int(x.shape[1]),
            cache_budget_bytes=cache_budget_bytes,
            anomaly_ratio=float(scan_nonfinite(x)["anomaly_ratio"]),
            input_itemsize=x.dtype.itemsize,
            output_itemsize=(
                np.dtype(output_dtype).itemsize if output_dtype else x.dtype.itemsize
            ),
        )
    return execute_blocked_op(
        x,
        plan,
        gamma=gamma,
        beta=beta,
        eps=eps,
        memory_budget_bytes=memory_budget_bytes,
        output_dtype=output_dtype,
        fragment_id=fragment_id,
    ).output


def block_error_estimate(
    *,
    op: str,
    reduction_length: int,
    strategy: str,
    sampled_abs_sum_scaled: float,
    sampled_max_weight: float,
    output_itemsize: int,
) -> float:
    """Analytic envelope tied to reduction order and sampled magnitudes."""

    gamma = gamma_for_strategy(reduction_length, strategy)
    quantization = 1.1920928955078125e-7 if output_itemsize == 4 else 2.220446049250313e-16
    if op == "softmax":
        return float(gamma * (1.0 + max(0.0, sampled_max_weight)) + 2.0 * quantization)
    return float(
        gamma * (1.0 + math.log1p(max(0.0, sampled_abs_sum_scaled)))
        + 4.0 * quantization
    )
