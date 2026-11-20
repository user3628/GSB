from __future__ import annotations

import dataclasses
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np

from .blocking import (
    DEFAULT_L1_BYTES,
    DEFAULT_L2_BYTES,
    BlockPlan,
    PlanSelection,
    explicit_selection,
    select_block_plan,
)
from .cache import CacheLookup, ResultCache, make_cache_key
from .config_store import ConfigRecord, VersionedConfigStore
from .errors import BusinessCode, GemmError, fail
from .kernel import (
    KernelInfo,
    SumStrategy,
    bmm_into,
    choose_accumulation_mode,
    gemm_2d_into,
    normalize_strategy,
)
from .memory import MemorySchedule, plan_memory
from .shapes import (
    MatrixShape,
    array_digest,
    logical_views,
    validate_operands,
)
from .storage import digest_value, json_safe
from .verify import nonfinite_summary, verify_result


@dataclass
class ComputeResult:
    output: np.ndarray
    shape: MatrixShape
    plan_selection: PlanSelection
    kernel: KernelInfo
    error_report: dict[str, Any]
    nonfinite: dict[str, Any]
    memory: MemorySchedule
    config: dict[str, Any]
    cache: dict[str, Any]
    execution: dict[str, Any] = field(default_factory=dict)

    def summary(self) -> dict[str, Any]:
        payload = {
            "ok": True,
            "code": BusinessCode.OK.value,
            "shape": self.shape.to_dict(),
            "shape_fingerprint": self.shape.fingerprint,
            "plan_selection": self.plan_selection.to_dict(),
            "kernel": self.kernel.to_dict(),
            "error_report": self.error_report,
            "nonfinite": self.nonfinite,
            "memory": self.memory.to_dict(),
            "config": self.config,
            "cache": self.cache,
            "execution": self.execution,
            "output_digest": array_digest([self.output]),
            "output_stats": {
                "nan": int(np.count_nonzero(np.isnan(self.output))),
                "pos_inf": int(np.count_nonzero(np.isposinf(self.output))),
                "neg_inf": int(np.count_nonzero(np.isneginf(self.output))),
                "finite": int(np.count_nonzero(np.isfinite(self.output))),
            },
        }
        deterministic = {
            key: value
            for key, value in payload.items()
            if key not in {"execution", "cache"}
        }
        deterministic["execution"] = {
            "source": self.execution.get("source"),
            "accumulation_mode": self.execution.get("accumulation_mode"),
        }
        deterministic["config"] = {
            "enabled": self.config.get("enabled"),
            "seq_before": self.config.get("seq_before"),
            "seq_after": self.config.get("seq_after"),
            "adopted_stored_plan": self.config.get("adopted_stored_plan"),
        }
        payload["deterministic_digest"] = digest_value(deterministic)
        return json_safe(payload)


class OperatorWorkspace:
    """A local workspace containing versioned configuration and result cache."""

    def __init__(
        self,
        root: str | Path,
        *,
        config_dir: str | Path | None = None,
        cache_dir: str | Path | None = None,
        lock_timeout_seconds: float = 10.0,
    ) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.locks_dir = self.root / "locks"
        self.locks_dir.mkdir(parents=True, exist_ok=True)
        self.config_store = VersionedConfigStore(
            Path(config_dir) if config_dir is not None else self.root / "config_store",
            lock_timeout_seconds=lock_timeout_seconds,
            lock_path=self.locks_dir / "config.lock",
        )
        self.result_cache = ResultCache(
            Path(cache_dir) if cache_dir is not None else self.root / "cache",
            lock_timeout_seconds=lock_timeout_seconds,
            lock_path=self.locks_dir / "cache.lock",
        )

    def config_state(self) -> dict[str, Any]:
        return self.config_store.verify_state().to_dict()

    def latest_config(self) -> ConfigRecord | None:
        return self.config_store.load_latest()

    def cache_status(self) -> dict[str, Any]:
        return self.result_cache.status()

    def clear_cache(self, *, expected_seq: int | None = None) -> dict[str, int]:
        return self.result_cache.clear(expected_seq=expected_seq)


def _coerce_plan(plan: BlockPlan | dict[str, Any] | None) -> BlockPlan | None:
    if plan is None or isinstance(plan, BlockPlan):
        return plan
    if isinstance(plan, dict):
        return BlockPlan.from_dict(plan)
    fail(
        BusinessCode.E_INVALID_SHAPE,
        "plan must be a BlockPlan or mapping",
        value_type=type(plan).__name__,
    )


def _validate_dimensions(m: int, n: int, k: int, batch: int) -> None:
    if any(int(value) <= 0 for value in (m, n, k, batch)):
        fail(
            BusinessCode.E_INVALID_SHAPE,
            "m, n, k, and batch must be positive",
            m=m,
            n=n,
            k=k,
            batch=batch,
        )


def select_plan(
    m: int,
    n: int,
    k: int,
    *,
    batch: int = 1,
    dtype: str | np.dtype = "float64",
    plan: BlockPlan | dict[str, Any] | None = None,
    l1_bytes: int = DEFAULT_L1_BYTES,
    l2_bytes: int = DEFAULT_L2_BYTES,
    parallel_workers: int | None = None,
) -> PlanSelection:
    _validate_dimensions(int(m), int(n), int(k), int(batch))
    numpy_dtype = np.dtype(dtype)
    if numpy_dtype.name not in {"float32", "float64"}:
        fail(
            BusinessCode.E_INVALID_SHAPE,
            "dtype must be float32 or float64",
            dtype=numpy_dtype.name,
        )
    if l1_bytes <= 0 or l2_bytes < l1_bytes:
        fail(
            BusinessCode.E_INVALID_SHAPE,
            "cache budgets must satisfy 0 < L1 <= L2",
            reason="invalid_cache_budget",
            l1_bytes=l1_bytes,
            l2_bytes=l2_bytes,
        )
    explicit = _coerce_plan(plan)
    if explicit is not None:
        return explicit_selection(
            explicit,
            m=int(m),
            n=int(n),
            k=int(k),
            itemsize=numpy_dtype.itemsize,
            l1_bytes=l1_bytes,
            l2_bytes=l2_bytes,
        )
    workers = parallel_workers if parallel_workers is not None else (os.cpu_count() or 1)
    return select_block_plan(
        m=int(m),
        n=int(n),
        k=int(k),
        batch=int(batch),
        itemsize=numpy_dtype.itemsize,
        l1_bytes=l1_bytes,
        l2_bytes=l2_bytes,
        parallel_workers=workers,
    )


def _default_tolerances(dtype: np.dtype, rtol: float | None, atol: float | None) -> tuple[float, float]:
    if dtype == np.dtype("float64"):
        default_rtol, default_atol = 1e-10, 1e-12
    else:
        default_rtol, default_atol = 3e-5, 1e-5
    resolved_rtol = default_rtol if rtol is None else float(rtol)
    resolved_atol = default_atol if atol is None else float(atol)
    if resolved_rtol < 0 or resolved_atol < 0:
        fail(
            BusinessCode.E_INVALID_SHAPE,
            "tolerances must be non-negative",
            rtol=resolved_rtol,
            atol=resolved_atol,
        )
    return resolved_rtol, resolved_atol


def _merge_kernel_infos(infos: list[KernelInfo], plan: BlockPlan, mode: str, batch: int) -> KernelInfo:
    if not infos:
        return KernelInfo(
            sum_strategy=SumStrategy.SEQUENTIAL.value,
            accumulation_mode=mode,
            plan=plan,
            batches_executed=batch,
            register_tiles=0,
            cache_tiles=0,
            microkernel_calls=0,
        )
    return KernelInfo(
        sum_strategy=infos[0].sum_strategy,
        accumulation_mode=infos[0].accumulation_mode,
        plan=plan,
        batches_executed=sum(info.batches_executed for info in infos),
        register_tiles=sum(info.register_tiles for info in infos),
        cache_tiles=sum(info.cache_tiles for info in infos),
        microkernel_calls=sum(info.microkernel_calls for info in infos),
    )


def _tensor_nonfinite(array: np.ndarray, label: str) -> dict[str, Any]:
    if array.ndim == 2:
        return nonfinite_summary(array, label=label)
    matrices = [
        nonfinite_summary(array[index], label=f"{label}[{index}]")
        for index in range(array.shape[0])
    ]
    total = sum(int(matrix["total_nonfinite"]) for matrix in matrices)
    return {
        "label": label,
        "shape": list(array.shape),
        "total_nonfinite": total,
        "nan_count": sum(int(matrix["nan_count"]) for matrix in matrices),
        "pos_inf_count": sum(int(matrix["pos_inf_count"]) for matrix in matrices),
        "neg_inf_count": sum(int(matrix["neg_inf_count"]) for matrix in matrices),
        "matrices": matrices,
    }


def _kernel_info_from_dict(value: dict[str, Any]) -> KernelInfo:
    return KernelInfo(
        sum_strategy=str(value["sum_strategy"]),
        accumulation_mode=str(value["accumulation_mode"]),
        plan=BlockPlan.from_dict(value["plan"]),
        batches_executed=int(value["batches_executed"]),
        register_tiles=int(value["register_tiles"]),
        cache_tiles=int(value["cache_tiles"]),
        microkernel_calls=int(value["microkernel_calls"]),
    )


def _stored_config_report(
    store: VersionedConfigStore | None,
    *,
    seq_before: int | None,
    adopted: bool,
    record: ConfigRecord | None = None,
) -> dict[str, Any]:
    if store is None:
        return {"enabled": False, "seq_before": None, "seq_after": None, "adopted_stored_plan": False}
    return {
        "enabled": True,
        "path": str(store.root),
        "seq_before": seq_before,
        "seq_after": record.seq if record is not None else seq_before,
        "adopted_stored_plan": adopted,
        "published": record.to_public_dict() if record is not None else None,
    }


def _execute(
    a: np.ndarray,
    b: np.ndarray,
    *,
    require_batched: bool,
    trans_a: str | bool,
    trans_b: str | bool,
    plan: BlockPlan | dict[str, Any] | None,
    sum_strategy: SumStrategy | str,
    l1_bytes: int,
    l2_bytes: int,
    accumulation_mode: str,
    verify: bool,
    rtol: float | None,
    atol: float | None,
    memory_budget_bytes: int | None,
    workspace: OperatorWorkspace | None,
    config_store: VersionedConfigStore | None,
    cache_dir: str | Path | None,
    use_cache: bool,
    publish_config: bool,
    expected_config_seq: int | None,
    expected_cache_seq: int | None,
    adopt_stored_plan: bool,
    selection_metadata: dict[str, Any] | None,
) -> ComputeResult:
    started = time.perf_counter()
    shape = validate_operands(a, b, trans_a=trans_a, trans_b=trans_b)
    if require_batched and not shape.is_batched:
        fail(
            BusinessCode.E_BATCH_MISMATCH,
            "BMM requires 3-D A and B with a common batch dimension",
            a_shape=list(a.shape),
            b_shape=list(b.shape),
        )
    if not require_batched and shape.is_batched:
        fail(
            BusinessCode.E_BATCH_MISMATCH,
            "GEMM requires 2-D A and B; use bmm for 3-D batched operands",
            a_shape=list(a.shape),
            b_shape=list(b.shape),
        )
    strategy = normalize_strategy(sum_strategy)
    numpy_dtype = np.dtype(shape.dtype)
    resolved_rtol, resolved_atol = _default_tolerances(numpy_dtype, rtol, atol)
    resolved_accumulation_mode = choose_accumulation_mode(
        shape.m, shape.n, shape.k, accumulation_mode
    )

    if workspace is not None:
        if config_store is None:
            config_store = workspace.config_store
        if cache_dir is None:
            cache_dir = workspace.result_cache.root

    cache_store = ResultCache(cache_dir) if cache_dir is not None else None
    current_config_seq = 0
    config_seq_before: int | None = None
    adopted_stored_plan = False
    if config_store is not None:
        state = config_store.verify_state()
        current_config_seq = state.seq
        config_seq_before = state.seq
        if (
            adopt_stored_plan
            and plan is None
            and state.record is not None
            and state.record.fingerprint == shape.fingerprint
        ):
            explicit_candidate = state.record.plan
            plan = explicit_candidate
            adopted_stored_plan = True

    selection = select_plan(
        shape.m,
        shape.n,
        shape.k,
        batch=shape.batch,
        dtype=numpy_dtype,
        plan=plan,
        l1_bytes=l1_bytes,
        l2_bytes=l2_bytes,
    )
    if adopted_stored_plan:
        selection = dataclasses.replace(
            selection,
            reason=(
                f"adopted verified configuration sequence {config_seq_before} for matching "
                f"shape fingerprint; {selection.reason}"
            ),
        )
    schedule = plan_memory(
        batch=shape.batch,
        m=shape.m,
        n=shape.n,
        k=shape.k,
        itemsize=numpy_dtype.itemsize,
        plan=selection.plan,
        budget_bytes=memory_budget_bytes,
    )

    input_digest = array_digest([a, b]) if cache_store is not None else None
    cache_key = (
        make_cache_key(
            shape_fingerprint=shape.fingerprint,
            plan=selection.plan,
            sum_strategy=strategy.value,
            input_digest=str(input_digest),
            accumulation_mode=resolved_accumulation_mode,
        )
        if cache_store is not None
        else None
    )
    cache_report: dict[str, Any] = {
        "enabled": cache_store is not None,
        "key": cache_key,
        "status": "disabled",
    }
    cache_lookup: CacheLookup | None = None
    if cache_store is not None and use_cache and cache_key is not None:
        cache_lookup = cache_store.lookup(cache_key, current_config_seq=current_config_seq)
        cache_report = {"enabled": True, "key": cache_key, **cache_lookup.to_dict()}
        if cache_lookup.status == "hit" and cache_lookup.array is not None:
            cached_array = cache_lookup.array
            expected_shape = shape.output_shape
            if tuple(cached_array.shape) != expected_shape:
                fail(
                    BusinessCode.E_INVALID_SHAPE,
                    "cached result shape does not match requested output",
                    expected=list(expected_shape),
                    actual=list(cached_array.shape),
                )
            metadata = cache_lookup.metadata or {}
            computation_metadata = metadata.get("computation_metadata", {})
            kernel_info = _kernel_info_from_dict(computation_metadata["kernel"])
            stored_report = computation_metadata.get(
                "error_report", {"passed": True, "source": "cached"}
            )
            if verify:
                a_logical, b_logical = logical_views(a, b, shape)
                stored_report = verify_result(
                    cached_array,
                    a_logical,
                    b_logical,
                    strategy=strategy,
                    dtype=numpy_dtype,
                    atol=resolved_atol,
                    rtol=resolved_rtol,
                    seed=int(shape.fingerprint[:16], 16),
                )
            nonfinite = {
                "A": _tensor_nonfinite(a, "A"),
                "B": _tensor_nonfinite(b, "B"),
                "output": _tensor_nonfinite(cached_array, "output"),
            }
            return ComputeResult(
                output=cached_array,
                shape=shape,
                plan_selection=selection,
                kernel=kernel_info,
                error_report=stored_report,
                nonfinite=nonfinite,
                memory=schedule,
                config=_stored_config_report(
                    config_store,
                    seq_before=config_seq_before,
                    adopted=adopted_stored_plan,
                ),
                cache=cache_report,
                execution={
                    "source": "cache",
                    "accumulation_mode": kernel_info.accumulation_mode,
                    "elapsed_seconds": time.perf_counter() - started,
                },
            )

    a_logical, b_logical = logical_views(a, b, shape)
    output = np.empty(shape.output_shape, dtype=numpy_dtype)
    infos: list[KernelInfo] = []
    if shape.is_batched:
        for shard in schedule.shards:
            _, info = bmm_into(
                a_logical,
                b_logical,
                output,
                plan=selection.plan,
                strategy=strategy,
                accumulation_mode=resolved_accumulation_mode,
                batch_start=shard.batch_start,
                batch_stop=shard.batch_stop,
            )
            infos.append(info)
        kernel_info = _merge_kernel_infos(
            infos,
            selection.plan,
            infos[0].accumulation_mode if infos else accumulation_mode,
            shape.batch,
        )
    else:
        _, kernel_info = gemm_2d_into(
            a_logical,
            b_logical,
            output,
            plan=selection.plan,
            strategy=strategy,
            accumulation_mode=resolved_accumulation_mode,
        )

    if verify:
        error_report = verify_result(
            output,
            a_logical,
            b_logical,
            strategy=strategy,
            dtype=numpy_dtype,
            atol=resolved_atol,
            rtol=resolved_rtol,
            seed=int(shape.fingerprint[:16], 16),
        )
    else:
        error_report = {
            "passed": None,
            "skipped": True,
            "reason": "verification disabled by caller",
            "tolerance": {"atol": resolved_atol, "rtol": resolved_rtol},
        }
    nonfinite = {
        "A": _tensor_nonfinite(a, "A"),
        "B": _tensor_nonfinite(b, "B"),
        "output": _tensor_nonfinite(output, "output"),
    }

    published_config: ConfigRecord | None = None
    if publish_config and config_store is not None:
        published_config = config_store.publish(
            fingerprint=shape.fingerprint,
            plan=selection.plan,
            sum_strategy=strategy.value,
            cache_budget={"l1_bytes": l1_bytes, "l2_bytes": l2_bytes},
            selection_source="config" if adopted_stored_plan else selection.workload_class,
            metadata={**(selection_metadata or {}), "shape": shape.to_dict()},
            expected_seq=expected_config_seq,
        )
        current_config_seq = published_config.seq

    if cache_store is not None and cache_key is not None:
        computation_metadata = {
            "shape": shape.to_dict(),
            "shape_fingerprint": shape.fingerprint,
            "plan_selection": selection.to_dict(),
            "kernel": kernel_info.to_dict(),
            "error_report": error_report,
            "nonfinite": nonfinite,
            "memory": schedule.to_dict(),
        }
        published_cache = cache_store.publish(
            key=cache_key,
            result=output,
            key_components={
                "shape_fingerprint": shape.fingerprint,
                "plan": selection.plan.to_dict(),
                "sum_strategy": strategy.value,
                "accumulation_mode": resolved_accumulation_mode,
                "input_digest": input_digest,
            },
            config_seq=current_config_seq,
            computation_metadata=computation_metadata,
            expected_seq=expected_cache_seq,
        )
        cache_report = {
            "enabled": True,
            "key": cache_key,
            "status": "published",
            "cache_seq": published_cache.cache_seq,
            "config_seq": current_config_seq,
            "lookup_status": cache_lookup.status if cache_lookup is not None else "disabled",
            "lookup_reason": cache_lookup.reason if cache_lookup is not None else None,
            "rebuild": bool(cache_lookup is not None),
        }
    elif cache_store is not None and cache_lookup is not None:
        cache_report = {
            "enabled": True,
            "key": cache_key,
            **cache_lookup.to_dict(),
            "status": "published",
            "lookup_status": cache_lookup.status,
            "lookup_reason": cache_lookup.reason,
            "rebuild": True,
        }

    return ComputeResult(
        output=output,
        shape=shape,
        plan_selection=selection,
        kernel=kernel_info,
        error_report=error_report,
        nonfinite=nonfinite,
        memory=schedule,
        config=_stored_config_report(
            config_store,
            seq_before=config_seq_before,
            adopted=adopted_stored_plan,
            record=published_config,
        ),
        cache=cache_report,
        execution={
            "source": "computed",
            "accumulation_mode": kernel_info.accumulation_mode,
            "elapsed_seconds": time.perf_counter() - started,
        },
    )


def gemm(
    a: np.ndarray,
    b: np.ndarray,
    *,
    trans_a: str | bool = "N",
    trans_b: str | bool = "N",
    plan: BlockPlan | dict[str, Any] | None = None,
    sum_strategy: SumStrategy | str = SumStrategy.SEQUENTIAL,
    l1_bytes: int = DEFAULT_L1_BYTES,
    l2_bytes: int = DEFAULT_L2_BYTES,
    accumulation_mode: str = "auto",
    verify: bool = True,
    rtol: float | None = None,
    atol: float | None = None,
    memory_budget_bytes: int | None = None,
    workspace: OperatorWorkspace | None = None,
    config_store: VersionedConfigStore | None = None,
    cache_dir: str | Path | None = None,
    use_cache: bool = False,
    publish_config: bool = False,
    expected_config_seq: int | None = None,
    expected_cache_seq: int | None = None,
    adopt_stored_plan: bool = True,
    selection_metadata: dict[str, Any] | None = None,
) -> ComputeResult:
    return _execute(
        a,
        b,
        require_batched=False,
        trans_a=trans_a,
        trans_b=trans_b,
        plan=plan,
        sum_strategy=sum_strategy,
        l1_bytes=l1_bytes,
        l2_bytes=l2_bytes,
        accumulation_mode=accumulation_mode,
        verify=verify,
        rtol=rtol,
        atol=atol,
        memory_budget_bytes=memory_budget_bytes,
        workspace=workspace,
        config_store=config_store,
        cache_dir=cache_dir,
        use_cache=use_cache,
        publish_config=publish_config,
        expected_config_seq=expected_config_seq,
        expected_cache_seq=expected_cache_seq,
        adopt_stored_plan=adopt_stored_plan,
        selection_metadata=selection_metadata,
    )


def bmm(
    a: np.ndarray,
    b: np.ndarray,
    *,
    trans_a: str | bool = "N",
    trans_b: str | bool = "N",
    plan: BlockPlan | dict[str, Any] | None = None,
    sum_strategy: SumStrategy | str = SumStrategy.SEQUENTIAL,
    l1_bytes: int = DEFAULT_L1_BYTES,
    l2_bytes: int = DEFAULT_L2_BYTES,
    accumulation_mode: str = "auto",
    verify: bool = True,
    rtol: float | None = None,
    atol: float | None = None,
    memory_budget_bytes: int | None = None,
    workspace: OperatorWorkspace | None = None,
    config_store: VersionedConfigStore | None = None,
    cache_dir: str | Path | None = None,
    use_cache: bool = False,
    publish_config: bool = False,
    expected_config_seq: int | None = None,
    expected_cache_seq: int | None = None,
    adopt_stored_plan: bool = True,
    selection_metadata: dict[str, Any] | None = None,
) -> ComputeResult:
    return _execute(
        a,
        b,
        require_batched=True,
        trans_a=trans_a,
        trans_b=trans_b,
        plan=plan,
        sum_strategy=sum_strategy,
        l1_bytes=l1_bytes,
        l2_bytes=l2_bytes,
        accumulation_mode=accumulation_mode,
        verify=verify,
        rtol=rtol,
        atol=atol,
        memory_budget_bytes=memory_budget_bytes,
        workspace=workspace,
        config_store=config_store,
        cache_dir=cache_dir,
        use_cache=use_cache,
        publish_config=publish_config,
        expected_config_seq=expected_config_seq,
        expected_cache_seq=expected_cache_seq,
        adopt_stored_plan=adopt_stored_plan,
        selection_metadata=selection_metadata,
    )

