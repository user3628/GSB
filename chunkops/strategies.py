"""Automatic block planning, cost scoring, and sum strategy metadata."""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any

from .errors import ChunkOpsError, E_INVALID_CONFIG
from .fingerprint import digest_object

SEQ = "sequential"
PAIRWISE = "pairwise"
COMPENSATED = "compensated"
SUM_STRATEGIES = (SEQ, PAIRWISE, COMPENSATED)
OPS = ("softmax", "layernorm")

_EPS64 = 2.220446049250313e-16
_EPS32 = 1.1920928955078125e-7


def _ceil_div(numerator: int, denominator: int) -> int:
    return (int(numerator) + int(denominator) - 1) // int(denominator)


def _ranges(length: int, step: int) -> list[tuple[int, int]]:
    return [(start, min(length, start + step)) for start in range(0, length, step)]


def _base_chunk(length: int) -> int:
    if length <= 1:
        return 1
    raw = 2 ** int(math.ceil(math.log2(math.sqrt(length))))
    return max(1, min(length, raw))


def _candidate_sizes(length: int) -> list[int]:
    if length <= 0:
        return []
    base = _base_chunk(length)
    raw = [max(1, base // 4), max(1, base // 2), base, base * 2, base * 4]
    values = {min(length, int(value)) for value in raw}
    if length <= 8:
        values.add(length)
    return sorted(values)


def _scale_class(rows: int, cols: int) -> str:
    elements = rows * cols
    if elements <= 250_000:
        return "small"
    if elements <= 10_000_000:
        return "medium"
    return "large"


def _tier_caps(rows: int, cols: int) -> tuple[int, int]:
    """Return conservative row/column block caps for the observed scale."""

    scale = _scale_class(rows, cols)
    if scale == "small":
        return min(rows, 32), min(cols, 64)
    if scale == "medium":
        return min(rows, 128), min(cols, 256)
    return min(rows, 512), min(cols, 1024)


def gamma_for_strategy(n: int, strategy: str) -> float:
    """First-order floating-point error growth for a reduction order."""

    n = max(1, int(n))
    if strategy == SEQ:
        return n * _EPS64 / max(1.0 - n * _EPS64, _EPS64)
    if strategy == PAIRWISE:
        levels = int(math.ceil(math.log2(max(2, n)))) + 1
        return levels * _EPS64 / max(1.0 - levels * _EPS64, _EPS64)
    if strategy == COMPENSATED:
        return (2.0 + n * _EPS64) * _EPS64
    raise ChunkOpsError(E_INVALID_CONFIG, f"unknown sum strategy: {strategy}")


def estimate_workspace_bytes(
    op: str,
    axis: int,
    rows: int,
    cols: int,
    row_block: int,
    col_block: int,
    work_itemsize: int = 8,
    output_itemsize: int = 4,
) -> int:
    """Conservative transient workspace estimate for one reduction step."""

    tile = int(row_block) * int(col_block)
    acc_elems = int(col_block) if axis == 0 else int(row_block)
    fields = 3 if op == "layernorm" else 2
    tile_copies = 4 if op == "layernorm" else 3
    # Input tile, exponent/delta tile, output tile, and online accumulators.
    output_tile = tile * output_itemsize
    estimate = tile * work_itemsize * tile_copies
    estimate += acc_elems * fields * work_itemsize
    estimate += output_tile
    estimate += 16 * 1024  # block metadata, temporaries, and allocator slack
    return int(estimate)


def _traffic_estimate(
    op: str,
    axis: int,
    rows: int,
    cols: int,
    row_block: int,
    col_block: int,
    input_itemsize: int,
    output_itemsize: int,
) -> int:
    total = rows * cols
    n_row_blocks = _ceil_div(rows, row_block)
    n_col_blocks = _ceil_div(cols, col_block)
    # Both kernels make a stable online pass and a normalized output pass.
    traffic = 2 * total * input_itemsize + total * output_itemsize
    acc_fields = 3 if op == "layernorm" else 2
    chunk_count = n_row_blocks * n_col_blocks
    if axis == 1:
        traffic += n_col_blocks * rows * acc_fields * 8 * 2
    else:
        traffic += n_row_blocks * cols * acc_fields * 8 * 2
    traffic += chunk_count * 96
    return int(traffic)


def _error_bound(
    op: str,
    axis: int,
    rows: int,
    cols: int,
    row_block: int,
    col_block: int,
    strategy: str,
    anomaly_ratio: float,
    output_itemsize: int,
) -> float:
    reduction_length = cols if axis == 1 else rows
    gamma = gamma_for_strategy(reduction_length, strategy)
    output_units = rows if axis == 1 else cols
    # Magnitude term is deliberately conservative and includes the
    # normalization pass. Non-finite observations make the analytic envelope
    # unbounded; for ranking we retain a large finite penalty and record the
    # non-finite condition in score_details.
    anomaly_factor = 1.0 + min(1.0e6, anomaly_ratio * 1.0e6)
    op_factor = 2.0 if op == "layernorm" else 1.0
    quantization = _EPS32 if output_itemsize == 4 else _EPS64
    magnitude = 1.0 + math.log1p(reduction_length) + math.log1p(output_units)
    return float(
        quantization * 2.0
        + op_factor * gamma * magnitude * anomaly_factor
    )


def _strategy_compute_coefficient(strategy: str) -> float:
    # Sequential additions use explicit scalar-order loops; pairwise vectorizes
    # the tree; compensated carries two additional vectors per element.
    return {SEQ: 1.50, PAIRWISE: 0.80, COMPENSATED: 2.50}[strategy]


def _strategy_fixed_penalty(strategy: str) -> float:
    return {SEQ: 0.0, PAIRWISE: 0.20, COMPENSATED: 0.50}[strategy]


def _anomaly_strategy_penalty(strategy: str, anomaly_ratio: float) -> float:
    pressure = min(200.0, max(0.0, anomaly_ratio) * 1.0e6)
    return pressure * {SEQ: 1.0, PAIRWISE: 0.30, COMPENSATED: 0.0}[strategy]


@dataclass
class ChunkPlan:
    op: str
    axis: int
    rows: int
    cols: int
    row_block: int
    col_block: int
    sum_strategy: str
    cache_budget_bytes: int
    workspace_bytes: int
    estimated_traffic_bytes: int
    predicted_error_bound: float
    score: float
    selection_reason: str
    score_details: dict[str, Any] = field(default_factory=dict)
    candidates: list[dict[str, Any]] = field(default_factory=list)
    auto_selected: bool = True
    schema_version: int = 1

    @property
    def effective_blocks(self) -> tuple[int, int]:
        """(output-row block, reduction-column block) on the effective matrix."""

        if self.axis == 1:
            return self.row_block, self.col_block
        return self.col_block, self.row_block

    @property
    def reduction_length(self) -> int:
        return self.cols if self.axis == 1 else self.rows

    @property
    def output_units(self) -> int:
        return self.rows if self.axis == 1 else self.cols

    def cache_key(
        self,
        *,
        shape_fp: str,
        value_fp: str,
        input_dtype: str,
        output_dtype: str,
        eps: float = 1e-5,
        gamma_fp: str | None = None,
        beta_fp: str | None = None,
        memory_budget_bytes: int | None = None,
        effective_row_block: int | None = None,
        effective_col_block: int | None = None,
    ) -> str:
        return digest_object(
            {
                "kind": "result-cache-v1",
                "op": self.op,
                "axis": self.axis,
                "shape_fingerprint": shape_fp,
                "value_fingerprint": value_fp,
                "sum_strategy": self.sum_strategy,
                "row_block": self.row_block,
                "col_block": self.col_block,
                "cache_budget_bytes": self.cache_budget_bytes,
                "input_dtype": input_dtype,
                "output_dtype": output_dtype,
                "eps": float(eps),
                "gamma_fingerprint": gamma_fp,
                "beta_fingerprint": beta_fp,
                "memory_budget_bytes": memory_budget_bytes,
                "effective_row_block": effective_row_block,
                "effective_col_block": effective_col_block,
            }
        )

    def to_dict(self, include_candidates: bool = True) -> dict[str, Any]:
        result: dict[str, Any] = {
            "schema_version": self.schema_version,
            "op": self.op,
            "axis": self.axis,
            "rows": self.rows,
            "cols": self.cols,
            "row_block": self.row_block,
            "col_block": self.col_block,
            "sum_strategy": self.sum_strategy,
            "cache_budget_bytes": self.cache_budget_bytes,
            "workspace_bytes": self.workspace_bytes,
            "estimated_traffic_bytes": self.estimated_traffic_bytes,
            "predicted_error_bound": self.predicted_error_bound,
            "score": self.score,
            "selection_reason": self.selection_reason,
            "score_details": self.score_details,
            "auto_selected": self.auto_selected,
        }
        if include_candidates:
            result["candidates"] = self.candidates
        return result

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "ChunkPlan":
        return cls(
            op=value["op"],
            axis=int(value["axis"]),
            rows=int(value["rows"]),
            cols=int(value["cols"]),
            row_block=int(value["row_block"]),
            col_block=int(value["col_block"]),
            sum_strategy=value["sum_strategy"],
            cache_budget_bytes=int(value["cache_budget_bytes"]),
            workspace_bytes=int(value.get("workspace_bytes", 0)),
            estimated_traffic_bytes=int(value.get("estimated_traffic_bytes", 0)),
            predicted_error_bound=float(value.get("predicted_error_bound", 0.0)),
            score=float(value.get("score", 0.0)),
            selection_reason=value.get("selection_reason", ""),
            score_details=dict(value.get("score_details", {})),
            candidates=list(value.get("candidates", [])),
            auto_selected=bool(value.get("auto_selected", True)),
            schema_version=int(value.get("schema_version", 1)),
        )


def _validate_problem(op: str, axis: int, rows: int, cols: int) -> None:
    if op not in OPS:
        raise ChunkOpsError(E_INVALID_CONFIG, f"unsupported op: {op}", {"op": op})
    if axis not in (0, 1):
        raise ChunkOpsError(E_INVALID_CONFIG, f"unsupported axis: {axis}", {"axis": axis})
    if rows <= 0 or cols <= 0:
        raise ChunkOpsError(
            E_INVALID_CONFIG,
            "rows and cols must be positive",
            {"rows": rows, "cols": cols},
        )


def _candidate_score(
    *,
    op: str,
    axis: int,
    rows: int,
    cols: int,
    row_block: int,
    col_block: int,
    strategy: str,
    cache_budget_bytes: int,
    input_itemsize: int,
    output_itemsize: int,
    anomaly_ratio: float,
) -> tuple[float, dict[str, Any]]:
    workspace = estimate_workspace_bytes(
        op, axis, rows, cols, row_block, col_block, 8, output_itemsize
    )
    traffic = _traffic_estimate(
        op,
        axis,
        rows,
        cols,
        row_block,
        col_block,
        input_itemsize,
        output_itemsize,
    )
    error_bound = _error_bound(
        op,
        axis,
        rows,
        cols,
        row_block,
        col_block,
        strategy,
        anomaly_ratio,
        output_itemsize,
    )
    reduction_length = cols if axis == 1 else rows
    output_units = rows if axis == 1 else cols
    work_units = reduction_length * output_units

    traffic_score = traffic / float(2**20) * 0.05
    compute_score = (
        work_units / float(2**20) * _strategy_compute_coefficient(strategy)
    )
    error_score = error_bound / (_EPS32 if output_itemsize == 4 else _EPS64) * 0.02
    fixed_strategy_penalty = _strategy_fixed_penalty(strategy)
    anomaly_penalty = _anomaly_strategy_penalty(strategy, anomaly_ratio)

    cap_row, cap_col = _tier_caps(rows, cols)
    row_ratio = row_block / max(1, cap_row)
    col_ratio = col_block / max(1, cap_col)
    tier_penalty = 0.0
    tier_reasons: list[str] = []
    if row_ratio > 1.0:
        tier_penalty += 50.0 * (row_ratio - 1.0)
        tier_reasons.append("row_block_exceeds_scale_cap")
    if col_ratio > 1.0:
        tier_penalty += 50.0 * (col_ratio - 1.0)
        tier_reasons.append("col_block_exceeds_scale_cap")

    cache_penalty = 0.0
    if workspace > cache_budget_bytes:
        cache_penalty = 100.0 * (workspace / max(1, cache_budget_bytes) - 1.0)
        cache_reason = "workspace_exceeds_cache_budget"
    elif workspace > cache_budget_bytes * 0.75:
        cache_penalty = 2.0
        cache_reason = "workspace_near_cache_budget"
    else:
        cache_reason = None

    total = (
        traffic_score
        + compute_score
        + error_score
        + fixed_strategy_penalty
        + anomaly_penalty
        + tier_penalty
        + cache_penalty
    )
    details = {
        "traffic_score": round(traffic_score, 10),
        "compute_score": round(compute_score, 10),
        "error_score": round(error_score, 10),
        "fixed_strategy_penalty": round(fixed_strategy_penalty, 10),
        "anomaly_strategy_penalty": round(anomaly_penalty, 10),
        "tier_penalty": round(tier_penalty, 10),
        "cache_penalty": round(cache_penalty, 10),
        "tier_reasons": tier_reasons,
        "cache_reason": cache_reason,
        "workspace_bytes": workspace,
        "traffic_bytes": traffic,
        "predicted_error_bound": error_bound,
        "score": round(total, 10),
    }
    return float(round(total, 10)), details


def build_auto_plan(
    *,
    op: str,
    axis: int,
    rows: int,
    cols: int,
    cache_budget_bytes: int = 4 * 1024 * 1024,
    anomaly_ratio: float = 0.0,
    input_itemsize: int = 4,
    output_itemsize: int = 4,
) -> ChunkPlan:
    _validate_problem(op, axis, rows, cols)
    if cache_budget_bytes <= 0:
        raise ChunkOpsError(
            E_INVALID_CONFIG,
            "cache_budget_bytes must be positive",
            {"cache_budget_bytes": cache_budget_bytes},
        )
    if anomaly_ratio < 0 or not math.isfinite(anomaly_ratio):
        raise ChunkOpsError(
            E_INVALID_CONFIG,
            "anomaly_ratio must be finite and non-negative",
            {"anomaly_ratio": anomaly_ratio},
        )

    row_candidates = _candidate_sizes(rows)
    col_candidates = _candidate_sizes(cols)
    cap_row, cap_col = _tier_caps(rows, cols)
    # Keep oversized candidates for an explicit rejection trail, but constrain
    # the search space so planning itself remains inexpensive.
    row_candidates = sorted(set(row_candidates + [cap_row]))
    col_candidates = sorted(set(col_candidates + [cap_col]))

    scored: list[dict[str, Any]] = []
    best: tuple[float, dict[str, Any]] | None = None
    for row_block in row_candidates:
        for col_block in col_candidates:
            for strategy in SUM_STRATEGIES:
                score, details = _candidate_score(
                    op=op,
                    axis=axis,
                    rows=rows,
                    cols=cols,
                    row_block=row_block,
                    col_block=col_block,
                    strategy=strategy,
                    cache_budget_bytes=cache_budget_bytes,
                    input_itemsize=input_itemsize,
                    output_itemsize=output_itemsize,
                    anomaly_ratio=anomaly_ratio,
                )
                item = {
                    "row_block": row_block,
                    "col_block": col_block,
                    "sum_strategy": strategy,
                    "score": score,
                    **details,
                }
                scored.append(item)
                if best is None or score < best[0]:
                    best = (score, item)

    assert best is not None
    selected = best[1]
    selected_key = (
        selected["row_block"],
        selected["col_block"],
        selected["sum_strategy"],
    )
    rejected = []
    for item in scored:
        if (
            item["row_block"],
            item["col_block"],
            item["sum_strategy"],
        ) == selected_key:
            continue
        reasons: list[str] = []
        if item["tier_reasons"]:
            reasons.extend(item["tier_reasons"])
        if item["cache_reason"]:
            reasons.append(item["cache_reason"])
        if not reasons:
            reasons.append("higher_weighted_score")
        rejected.append(
            {
                "row_block": item["row_block"],
                "col_block": item["col_block"],
                "sum_strategy": item["sum_strategy"],
                "score": item["score"],
                "reasons": reasons,
            }
        )
    rejected.sort(key=lambda item: (item["score"], item["row_block"], item["col_block"], item["sum_strategy"]))

    scale = _scale_class(rows, cols)
    reason = (
        f"{scale} tensor {rows}x{cols}: selected row_block={selected['row_block']}, "
        f"col_block={selected['col_block']}, sum_strategy={selected['sum_strategy']} "
        f"after scoring {len(scored)} candidates. Predicted traffic "
        f"{selected['traffic_bytes']} bytes, analytic error envelope "
        f"{selected['predicted_error_bound']:.3e}, workspace {selected['workspace_bytes']} bytes, "
        f"scale caps row<={cap_row}/col<={cap_col}, anomaly_ratio={anomaly_ratio:.6g}."
    )
    score_details = {
        key: value
        for key, value in selected.items()
        if key
        not in {
            "row_block",
            "col_block",
            "sum_strategy",
            "score",
        }
    }
    score_details.update(
        {
            "scale_class": scale,
            "scale_caps": {"row_block": cap_row, "col_block": cap_col},
            "candidate_count": len(scored),
            "rejected": rejected,
            "selection_rule": (
                "minimize traffic + compute + analytic-error + cache-fit + "
                "scale-cap + anomaly-stability weighted score"
            ),
        }
    )
    return ChunkPlan(
        op=op,
        axis=axis,
        rows=rows,
        cols=cols,
        row_block=int(selected["row_block"]),
        col_block=int(selected["col_block"]),
        sum_strategy=str(selected["sum_strategy"]),
        cache_budget_bytes=int(cache_budget_bytes),
        workspace_bytes=int(selected["workspace_bytes"]),
        estimated_traffic_bytes=int(selected["traffic_bytes"]),
        predicted_error_bound=float(selected["predicted_error_bound"]),
        score=float(selected["score"]),
        selection_reason=reason,
        score_details=score_details,
        candidates=rejected,
        auto_selected=True,
    )


def build_requested_plan(
    *,
    op: str,
    axis: int,
    rows: int,
    cols: int,
    row_block: int,
    col_block: int,
    sum_strategy: str,
    cache_budget_bytes: int = 4 * 1024 * 1024,
    anomaly_ratio: float = 0.0,
    input_itemsize: int = 4,
    output_itemsize: int = 4,
) -> ChunkPlan:
    _validate_problem(op, axis, rows, cols)
    for name, value in (("row_block", row_block), ("col_block", col_block)):
        if int(value) <= 0:
            raise ChunkOpsError(
                E_INVALID_CONFIG,
                f"{name} must be positive",
                {name: value},
            )
    if sum_strategy not in SUM_STRATEGIES:
        raise ChunkOpsError(
            E_INVALID_CONFIG,
            f"unknown sum strategy: {sum_strategy}",
            {"sum_strategy": sum_strategy, "allowed": list(SUM_STRATEGIES)},
        )
    score, details = _candidate_score(
        op=op,
        axis=axis,
        rows=rows,
        cols=cols,
        row_block=int(row_block),
        col_block=int(col_block),
        strategy=sum_strategy,
        cache_budget_bytes=cache_budget_bytes,
        input_itemsize=input_itemsize,
        output_itemsize=output_itemsize,
        anomaly_ratio=anomaly_ratio,
    )
    cap_row, cap_col = _tier_caps(rows, cols)
    reason = (
        f"user-specified row_block={row_block}, col_block={col_block}, "
        f"sum_strategy={sum_strategy}; the same scorer was retained for audit "
        f"(score={score:.6f}, scale caps row<={cap_row}/col<={cap_col})."
    )
    return ChunkPlan(
        op=op,
        axis=axis,
        rows=rows,
        cols=cols,
        row_block=int(row_block),
        col_block=int(col_block),
        sum_strategy=sum_strategy,
        cache_budget_bytes=int(cache_budget_bytes),
        workspace_bytes=int(details["workspace_bytes"]),
        estimated_traffic_bytes=int(details["traffic_bytes"]),
        predicted_error_bound=float(details["predicted_error_bound"]),
        score=score,
        selection_reason=reason,
        score_details={
            **details,
            "scale_class": _scale_class(rows, cols),
            "scale_caps": {"row_block": cap_row, "col_block": cap_col},
            "candidate_count": 1,
            "rejected": [],
            "selection_rule": "explicit user request; audited by scorer",
        },
        candidates=[],
        auto_selected=False,
    )


def softmax_plan_is_reasonable(plan: ChunkPlan) -> bool:
    """Small-size guard used by verification and tests."""

    cap_row, cap_col = _tier_caps(plan.rows, plan.cols)
    return plan.row_block <= cap_row and plan.col_block <= cap_col
