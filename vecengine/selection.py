from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

from .errors import MemoryBudgetError, PlanError
from .model import DTYPE_BYTES
from .plan import ParsedPlan


DEFAULT_MEMORY_BUDGET_BYTES = 512 * 1024 * 1024


@dataclass(frozen=True)
class EngineConfig:
    scalar_max_rows: int = 50_000
    scalar_max_cells: int = 400_000
    scalar_max_nodes: int = 48
    batch_min: int = 512
    batch_max: int = 65_536
    default_memory_budget_bytes: int = DEFAULT_MEMORY_BUDGET_BYTES
    test_hooks_enabled: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "scalar_max_rows": self.scalar_max_rows,
            "scalar_max_cells": self.scalar_max_cells,
            "scalar_max_nodes": self.scalar_max_nodes,
            "batch_bounds": [self.batch_min, self.batch_max],
        }


@dataclass(frozen=True)
class ExecutionOptions:
    path: str
    batch_size: int | None
    integer_overflow: str
    memory_budget_bytes: int
    float_compare: str
    float_decimal_places: int
    test_batch_delay_ms: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "batch_size": self.batch_size,
            "integer_overflow": self.integer_overflow,
            "memory_budget_bytes": self.memory_budget_bytes,
            "float_compare": self.float_compare,
            "float_decimal_places": self.float_decimal_places,
            "test_batch_delay_ms": self.test_batch_delay_ms,
        }


@dataclass(frozen=True)
class SelectionDecision:
    path: str
    batch_size: int
    reasons: tuple[str, ...]
    rows: int
    column_count: int
    cells: int
    node_count: int
    complexity: int
    working_bytes_per_row: int
    output_bytes: int
    batch_working_bytes: int
    fixed_overhead_bytes: int
    estimated_peak_bytes: int
    memory_budget_bytes: int
    thresholds: dict[str, int]

    def to_dict(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "batch_size": self.batch_size,
            "reasons": list(self.reasons),
            "basis": {
                "rows": self.rows,
                "dataset_columns": self.column_count,
                "cells": self.cells,
                "expression_nodes": self.node_count,
                "complexity": self.complexity,
            },
            "memory": {
                "working_bytes_per_row": self.working_bytes_per_row,
                "output_bytes": self.output_bytes,
                "batch_working_bytes": self.batch_working_bytes,
                "fixed_overhead_bytes": self.fixed_overhead_bytes,
                "estimated_peak_bytes": self.estimated_peak_bytes,
                "budget_bytes": self.memory_budget_bytes,
            },
            "thresholds": self.thresholds,
        }


def normalize_execution(raw: Any, config: EngineConfig) -> ExecutionOptions:
    if raw is None:
        raw = {}
    if not isinstance(raw, dict):
        raise PlanError("execution must be a JSON object")
    allowed = {
        "path",
        "batch_size",
        "integer_overflow",
        "memory_budget_bytes",
        "float_compare",
        "float_decimal_places",
        "test_batch_delay_ms",
    }
    unknown = set(raw) - allowed
    if unknown:
        raise PlanError("execution contains unknown fields", details={"fields": sorted(unknown)})

    path = raw.get("path", "auto")
    if path not in ("auto", "scalar", "vector"):
        raise PlanError("execution.path must be auto, scalar or vector")
    batch_size = raw.get("batch_size")
    if batch_size is not None:
        if isinstance(batch_size, bool) or not isinstance(batch_size, int):
            raise PlanError("execution.batch_size must be an integer or null")
        if batch_size < config.batch_min or batch_size > config.batch_max:
            raise PlanError(
                "execution.batch_size is outside the configured bounds",
                details={"batch_size": batch_size, "bounds": [config.batch_min, config.batch_max]},
            )
    integer_overflow = raw.get("integer_overflow", "wrap")
    if integer_overflow not in ("wrap", "saturate"):
        raise PlanError("execution.integer_overflow must be wrap or saturate")
    memory_budget = raw.get("memory_budget_bytes", config.default_memory_budget_bytes)
    if isinstance(memory_budget, bool) or not isinstance(memory_budget, int) or memory_budget <= 0:
        raise PlanError("execution.memory_budget_bytes must be a positive integer")
    float_compare = raw.get("float_compare", "ieee754_exact")
    if float_compare != "ieee754_exact":
        raise PlanError("only execution.float_compare=ieee754_exact is supported")
    decimal_places = raw.get("float_decimal_places", 6)
    if isinstance(decimal_places, bool) or not isinstance(decimal_places, int) or not 0 <= decimal_places <= 12:
        raise PlanError("execution.float_decimal_places must be an integer from 0 to 12")
    test_delay = raw.get("test_batch_delay_ms", 0)
    if isinstance(test_delay, bool) or not isinstance(test_delay, int) or test_delay < 0:
        raise PlanError("execution.test_batch_delay_ms must be a non-negative integer")
    if test_delay and not config.test_hooks_enabled:
        raise PlanError("execution.test_batch_delay_ms requires VECENGINE_TEST_HOOKS=1")
    return ExecutionOptions(
        path=path,
        batch_size=batch_size,
        integer_overflow=integer_overflow,
        memory_budget_bytes=memory_budget,
        float_compare=float_compare,
        float_decimal_places=decimal_places,
        test_batch_delay_ms=test_delay,
    )


def choose_execution(plan: ParsedPlan, options: ExecutionOptions, config: EngineConfig) -> SelectionDecision:
    rows = plan.spec.row_count
    column_count = max(1, len(plan.spec.columns))
    cells = rows * column_count
    thresholds = {
        "scalar_max_rows": config.scalar_max_rows,
        "scalar_max_cells": config.scalar_max_cells,
        "scalar_max_nodes": config.scalar_max_nodes,
        "batch_min": config.batch_min,
        "batch_max": config.batch_max,
    }

    if options.path == "auto":
        passes_rows = rows <= config.scalar_max_rows
        passes_cells = cells <= config.scalar_max_cells
        passes_nodes = plan.complexity <= config.scalar_max_nodes
        if passes_rows and passes_cells and passes_nodes:
            path = "scalar"
            reasons = (
                "auto: rows <= scalar_max_rows",
                "auto: rows*dataset_columns <= scalar_max_cells",
                "auto: plan complexity <= scalar_max_nodes",
            )
        else:
            path = "vector"
            failed = []
            if not passes_rows:
                failed.append("rows > scalar_max_rows")
            if not passes_cells:
                failed.append("rows*dataset_columns > scalar_max_cells")
            if not passes_nodes:
                failed.append("plan complexity > scalar_max_nodes")
            reasons = ("auto: vector chosen because " + ", ".join(failed),)
    else:
        path = options.path
        reasons = (f"caller explicitly requested {path}",)

    output_columns = len(plan.output_columns)
    # Expression evaluation creates temporary value and validity arrays. This
    # deliberately overestimates concurrent temporaries so batch size is
    # conservative rather than silently exceeding the declared budget.
    temporary_slots = column_count + plan.node_count + output_columns
    working_bytes_per_row = max(32, temporary_slots * 16 + 2)
    output_row_bytes = sum(DTYPE_BYTES[dtype] + 1 for _, dtype in plan.output_columns)
    output_rows = 1 if plan.is_aggregate else rows
    output_bytes = max(0, output_rows * output_row_bytes)
    fixed_overhead = 1024 * 1024

    if options.batch_size is not None:
        batch_size = options.batch_size
        batch_reason = "caller supplied a batch_size within configured bounds"
    else:
        working_budget = max(64 * 1024, int(options.memory_budget_bytes * 0.35))
        raw_batch = max(1, working_budget // working_bytes_per_row)
        power = 1 << max(0, int(raw_batch).bit_length() - 1)
        batch_size = max(config.batch_min, min(config.batch_max, power))
        batch_reason = (
            f"adaptive: floor_pow2(35% * budget / bytes_per_row) clamped to "
            f"[{config.batch_min}, {config.batch_max}]"
        )
    if rows > 0:
        batch_size = min(batch_size, rows)
    batch_size = max(1, batch_size)
    reasons = reasons + (batch_reason,)

    batch_working_bytes = batch_size * working_bytes_per_row
    estimated_peak = output_bytes + batch_working_bytes + fixed_overhead
    if estimated_peak > options.memory_budget_bytes:
        raise MemoryBudgetError(
            "estimated execution peak exceeds memory_budget_bytes",
            details={
                "estimated_peak_bytes": estimated_peak,
                "budget_bytes": options.memory_budget_bytes,
                "output_bytes": output_bytes,
                "batch_working_bytes": batch_working_bytes,
                "fixed_overhead_bytes": fixed_overhead,
                "batch_size": batch_size,
                "rows": rows,
                "output_columns": output_columns,
            },
        )
    return SelectionDecision(
        path=path,
        batch_size=batch_size,
        reasons=reasons,
        rows=rows,
        column_count=column_count,
        cells=cells,
        node_count=plan.node_count,
        complexity=plan.complexity,
        working_bytes_per_row=working_bytes_per_row,
        output_bytes=output_bytes,
        batch_working_bytes=batch_working_bytes,
        fixed_overhead_bytes=fixed_overhead,
        estimated_peak_bytes=estimated_peak,
        memory_budget_bytes=options.memory_budget_bytes,
        thresholds=thresholds,
    )
