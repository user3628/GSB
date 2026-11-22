"""Checkpointed GEMM/BMM execution, recovery, and invalidation."""

from __future__ import annotations

import math
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import numpy as np

from .checkpoint import CheckpointStore, ScanResult
from .errors import BlockGemmError, E_INVALID_CONFIG
from .fingerprint import sha256_json, version_fingerprint
from .kernel import (
    ACCUMULATION_STRATEGY_ALIASES,
    KERNEL_VERSION,
    BlockDescriptor,
    Problem,
    ShapeExpectation,
    build_block_descriptors,
    compute_block,
    content_digests,
    normalize_problem,
)
from .reference import DEFAULT_ATOL, DEFAULT_RTOL, sample_sequences, verify_block_against_reference
from .tiling import (
    DEFAULT_L1_CACHE_KIB,
    DEFAULT_L2_CACHE_KIB,
    PlanSelection,
    select_tiling,
)
from .fingerprint import static_version as make_static_version


@dataclass
class ResumeDecision:
    start_sequence: int
    reason: str
    details: dict[str, Any]
    reused_blocks: int
    scan: ScanResult

    def to_dict(self) -> dict[str, Any]:
        return {
            "start_sequence": self.start_sequence,
            "reused_blocks": self.reused_blocks,
            "reason": self.reason,
            "details": self.details,
        }


@dataclass
class ExecutionResult:
    output: np.ndarray | None
    output_digest: str | None
    output_path: Path | None
    summary: dict[str, Any] | None
    summary_path: Path | None
    completed: bool
    stopped: bool
    computed_blocks: int
    reused_blocks: int
    next_sequence: int
    workspace: Path
    resume_decision: ResumeDecision


@dataclass
class ExecutionContext:
    store: CheckpointStore
    problem: Problem
    plan_selection: PlanSelection
    descriptors: list[BlockDescriptor]
    static_components: dict[str, Any]
    static_version: str
    a_content_digest: str
    b_content_digest: str
    version_fingerprint: str
    verification: dict[str, Any]
    verification_sample_set: set[int]
    accumulation_strategy: str

    @property
    def block_count(self) -> int:
        return len(self.descriptors)

    @property
    def plan(self):
        return self.plan_selection.plan

    @property
    def cache_budget(self) -> dict[str, int]:
        return {
            "l1_cache_kib": self.plan_selection.l1_cache_kib,
            "l2_cache_kib": self.plan_selection.l2_cache_kib,
            "l1_cache_bytes": self.plan_selection.l1_cache_kib * 1024,
            "l2_cache_bytes": self.plan_selection.l2_cache_kib * 1024,
        }


def default_tolerances(dtype: np.dtype) -> tuple[float, float]:
    if np.dtype(dtype) == np.dtype("float32"):
        return 1e-5, 1e-4
    return DEFAULT_ATOL, DEFAULT_RTOL


def _build_context(
    *,
    workspace: str | os.PathLike[str],
    a: np.ndarray,
    b: np.ndarray,
    a_transpose: bool,
    b_transpose: bool,
    expected: ShapeExpectation | None,
    l1_cache_kib: int,
    l2_cache_kib: int,
    verify_blocks: int,
    atol: float | None,
    rtol: float | None,
    accumulation_strategy: str = "k-ascending",
) -> ExecutionContext:
    problem = normalize_problem(
        a,
        b,
        a_transpose=a_transpose,
        b_transpose=b_transpose,
        expected=expected,
    )
    default_atol, default_rtol = default_tolerances(problem.dtype)
    resolved_atol = float(default_atol if atol is None else atol)
    resolved_rtol = float(default_rtol if rtol is None else rtol)
    if verify_blocks < 0:
        raise BlockGemmError(
            E_INVALID_CONFIG,
            "verify_blocks must be non-negative",
            reason="invalid_verification_count",
            details={"verify_blocks": verify_blocks},
        )
    if resolved_atol < 0 or resolved_rtol < 0:
        raise BlockGemmError(
            E_INVALID_CONFIG,
            "numeric tolerances must be non-negative",
            reason="invalid_numeric_tolerance",
            details={"atol": resolved_atol, "rtol": resolved_rtol},
        )
    resolved_accumulation = ACCUMULATION_STRATEGY_ALIASES.get(str(accumulation_strategy))
    if resolved_accumulation is None:
        raise BlockGemmError(
            E_INVALID_CONFIG,
            "unsupported accumulation strategy",
            reason="invalid_accumulation_strategy",
            details={
                "accumulation_strategy": accumulation_strategy,
                "supported": sorted(ACCUMULATION_STRATEGY_ALIASES),
            },
        )
    plan_selection = select_tiling(
        m=problem.m,
        n=problem.n,
        k=problem.k,
        batch=problem.batch_count,
        dtype=problem.dtype,
        l1_cache_kib=l1_cache_kib,
        l2_cache_kib=l2_cache_kib,
    )
    verification = {
        "verify_blocks": int(verify_blocks),
        "atol": resolved_atol,
        "rtol": resolved_rtol,
        "reference": "longdouble-power-of-two-leaf8-v1",
        "skip_nonfinite": True,
    }
    components, static_fp = make_static_version(
        shape_fp=problem.shape_fingerprint,
        plan_fp=plan_selection.plan.fingerprint(),
        accumulation_strategy=resolved_accumulation,
        l1_cache_kib=plan_selection.l1_cache_kib,
        l2_cache_kib=plan_selection.l2_cache_kib,
        verification=verification,
        kernel_version=KERNEL_VERSION,
    )
    a_content_digest, b_content_digest = content_digests(problem)
    version_fp = version_fingerprint(
        components,
        a_content_digest=a_content_digest,
        b_content_digest=b_content_digest,
    )
    descriptors = build_block_descriptors(
        problem,
        plan_selection,
        static_version=static_fp,
        version_fp=version_fp,
        accumulation_strategy=resolved_accumulation,
    )
    sample_set = set(
        sample_sequences(
            len(descriptors),
            int(verification["verify_blocks"]),
        )
    )
    return ExecutionContext(
        store=CheckpointStore(workspace),
        problem=problem,
        plan_selection=plan_selection,
        descriptors=descriptors,
        static_components=components,
        static_version=static_fp,
        a_content_digest=a_content_digest,
        b_content_digest=b_content_digest,
        version_fingerprint=version_fp,
        verification=verification,
        verification_sample_set=sample_set,
        accumulation_strategy=resolved_accumulation,
    )


def _changed_static_keys(
    old_components: Any,
    new_components: dict[str, Any],
) -> list[str]:
    if not isinstance(old_components, dict):
        return ["static_version"]
    keys = sorted(set(old_components) | set(new_components))
    return [key for key in keys if old_components.get(key) != new_components.get(key)]


def _block_reference(descriptor: BlockDescriptor) -> dict[str, Any]:
    return {
        "sequence": descriptor.sequence,
        "shard_id": descriptor.shard_id,
        "block_coord": descriptor.block_coord,
        "row_range": descriptor.row_range,
        "col_range": descriptor.col_range,
        "tail_flags": descriptor.tail_flags,
    }


def analyze_resume(context: ExecutionContext) -> ResumeDecision:
    """Read checkpoints and identify the earliest affected downstream interval."""

    scan = context.store.scan(verify_artifacts=True)
    total = context.block_count
    if scan.max_observed_sequence < 0 and not scan.issues:
        return ResumeDecision(
            start_sequence=0,
            reason="initial_build",
            details={"block_count": total},
            reused_blocks=0,
            scan=scan,
        )

    # A corrupt/incomplete record at the edge of the contiguous prefix or later
    # records after a gap all make that sequence the earliest reconstruct point.
    if scan.valid_prefix < total and (
        scan.valid_prefix in scan.issues or scan.max_observed_sequence > scan.valid_prefix
    ):
        sequence = scan.valid_prefix
        descriptor = context.descriptors[sequence]
        return ResumeDecision(
            start_sequence=sequence,
            reason="checkpoint_corrupt_or_missing",
            details={
                "issues": scan.issue_summary(),
                "earliest_affected_block": _block_reference(descriptor),
                "missing_or_invalid_sequence": sequence,
            },
            reused_blocks=sequence,
            scan=scan,
        )

    earliest: int | None = None
    reason = ""
    details: dict[str, Any] = {}

    for sequence in range(min(scan.valid_prefix, total)):
        record = scan.records[sequence]
        descriptor = context.descriptors[sequence]
        old_components = record.get("static_components")
        if record.get("static_version") != context.static_version:
            changed = _changed_static_keys(old_components, context.static_components)
            earliest = sequence
            reason = "static_version_changed"
            details = {
                "changed_components": changed,
                "old_static_version": record.get("static_version"),
                "new_static_version": context.static_version,
                "earliest_affected_block": _block_reference(descriptor),
            }
            break
        layout_expected = {
            "shard_id": descriptor.shard_id,
            "batch_index": descriptor.batch_index,
            "block_coord": descriptor.block_coord,
            "row_range": descriptor.row_range,
            "col_range": descriptor.col_range,
            "input_shape_fingerprint": descriptor.shape_fingerprint,
            "plan_fingerprint": descriptor.plan_fingerprint,
            "accumulation_strategy": descriptor.accumulation_strategy,
            "cache_budget": context.cache_budget,
        }
        mismatched_layout = [
            field
            for field, expected_value in layout_expected.items()
            if record.get(field) != expected_value
        ]
        if mismatched_layout:
            earliest = sequence
            reason = "checkpoint_layout_mismatch"
            details = {
                "mismatched_fields": mismatched_layout,
                "earliest_affected_block": _block_reference(descriptor),
            }
            break
        if record.get("block_input_digest") != descriptor.block_input_digest:
            earliest = sequence
            reason = "input_content_changed"
            details = {
                "earliest_affected_block": _block_reference(descriptor),
                "a_stage_changed": record.get("a_stage_digest") != descriptor.a_stage_digest,
                "b_stage_changed": record.get("b_stage_digest") != descriptor.b_stage_digest,
                "old_block_input_digest": record.get("block_input_digest"),
                "new_block_input_digest": descriptor.block_input_digest,
            }
            break
        if sequence in context.verification_sample_set and not isinstance(
            record.get("numeric_report"), dict
        ):
            earliest = sequence
            reason = "missing_numeric_diagnostic"
            details = {"earliest_affected_block": _block_reference(descriptor)}
            break

    if earliest is not None:
        return ResumeDecision(
            start_sequence=earliest,
            reason=reason,
            details=details,
            reused_blocks=earliest,
            scan=scan,
        )

    if scan.valid_prefix < total:
        sequence = scan.valid_prefix
        descriptor = context.descriptors[sequence]
        return ResumeDecision(
            start_sequence=sequence,
            reason="resume_incomplete",
            details={
                "earliest_affected_block": _block_reference(descriptor),
                "valid_prefix": scan.valid_prefix,
            },
            reused_blocks=sequence,
            scan=scan,
        )

    # If global content shows a change that is not represented by any reusable
    # block digest, force a full rebuild rather than assembling mixed old state.
    if scan.valid_prefix and context.problem.kind:
        first_record = scan.records[0]
        if (
            first_record.get("version_fingerprint") != context.version_fingerprint
            and not any(
                scan.records[seq].get("block_input_digest")
                != context.descriptors[seq].block_input_digest
                for seq in range(total)
            )
        ):
            return ResumeDecision(
                start_sequence=0,
                reason="content_digest_changed_without_block_mapping",
                details={"global_old_version": first_record.get("version_fingerprint")},
                reused_blocks=0,
                scan=scan,
            )

    return ResumeDecision(
        start_sequence=total,
        reason="all_blocks_reusable",
        details={"block_count": total},
        reused_blocks=total,
        scan=scan,
    )


def describe_execution(
    *,
    workspace: str | os.PathLike[str],
    a: np.ndarray,
    b: np.ndarray,
    a_transpose: bool = False,
    b_transpose: bool = False,
    expected: ShapeExpectation | None = None,
    l1_cache_kib: int = DEFAULT_L1_CACHE_KIB,
    l2_cache_kib: int = DEFAULT_L2_CACHE_KIB,
    verify_blocks: int = 3,
    atol: float | None = None,
    rtol: float | None = None,
    accumulation_strategy: str = "k-ascending",
) -> tuple[ExecutionContext, ResumeDecision]:
    context = _build_context(
        workspace=workspace,
        a=a,
        b=b,
        a_transpose=a_transpose,
        b_transpose=b_transpose,
        expected=expected,
        l1_cache_kib=l1_cache_kib,
        l2_cache_kib=l2_cache_kib,
        verify_blocks=verify_blocks,
        atol=atol,
        rtol=rtol,
        accumulation_strategy=accumulation_strategy,
    )
    decision = analyze_resume(context)
    return context, decision


def _record_base(
    context: ExecutionContext,
    descriptor: BlockDescriptor,
    numeric_report: dict[str, Any] | None,
) -> dict[str, Any]:
    plan = descriptor.plan
    return {
        "schema_version": 1,
        "sequence": descriptor.sequence,
        "shard_id": descriptor.shard_id,
        "batch_index": descriptor.batch_index,
        "block_coord": descriptor.block_coord,
        "row_range": descriptor.row_range,
        "col_range": descriptor.col_range,
        "k_range": [0, descriptor.k],
        "k_block_size": plan.kc,
        "k_steps": math.ceil(descriptor.k / plan.kc),
        "input_shape": {
            "M": descriptor.m,
            "N": descriptor.n,
            "K": descriptor.k,
            "batch": descriptor.batch,
        },
        "input_shape_fingerprint": descriptor.shape_fingerprint,
        "plan": plan.to_dict(),
        "plan_fingerprint": descriptor.plan_fingerprint,
        "accumulation_strategy": descriptor.accumulation_strategy,
        "cache_budget": context.cache_budget,
        "static_version": descriptor.static_version,
        "static_components": context.static_components,
        "version_fingerprint": descriptor.version_fingerprint,
        "block_input_digest": descriptor.block_input_digest,
        "a_stage_digest": descriptor.a_stage_digest,
        "b_stage_digest": descriptor.b_stage_digest,
        "block_fingerprint": descriptor.block_fingerprint(),
        "tail_flags": descriptor.tail_flags,
        "nonfinite": {
            "a_count": descriptor.a_nonfinite_count,
            "b_count": descriptor.b_nonfinite_count,
            "affected": descriptor.nonfinite_affected,
        },
        "numeric_report": numeric_report,
        "output_dtype": context.problem.dtype.str,
    }


def _layout_fingerprint(descriptors: list[BlockDescriptor]) -> str:
    return sha256_json(
        {
            "schema": "blockgemm.block-layout.v1",
            "blocks": [
                {
                    "sequence": descriptor.sequence,
                    "shard_id": descriptor.shard_id,
                    "block_coord": descriptor.block_coord,
                    "row_range": descriptor.row_range,
                    "col_range": descriptor.col_range,
                    "block_fingerprint": descriptor.block_fingerprint(),
                }
                for descriptor in descriptors
            ],
        }
    )


def _run_spec(
    context: ExecutionContext,
    *,
    case_id: str | None,
    source: dict[str, Any] | None,
) -> dict[str, Any]:
    problem = context.problem
    return {
        "schema_version": 1,
        "case_id": case_id,
        "kind": problem.kind,
        "input": {
            "M": problem.m,
            "N": problem.n,
            "K": problem.k,
            "batch": problem.batch,
            "dtype": problem.dtype.str,
            "a_transpose": problem.a_transpose,
            "b_transpose": problem.b_transpose,
            "a_stored_shape": list(problem.a_raw.shape),
            "b_stored_shape": list(problem.b_raw.shape),
            "effective_a_shape": list(problem.a.shape),
            "effective_b_shape": list(problem.b.shape),
            "input_shape_fingerprint": problem.shape_fingerprint,
            "a_content_digest": context.a_content_digest,
            "b_content_digest": context.b_content_digest,
            "source": source or {},
        },
        "plan_selection": context.plan_selection.to_dict(),
        "plan": context.plan.to_dict(),
        "plan_fingerprint": context.plan.fingerprint(),
        "accumulation_strategy": context.accumulation_strategy,
        "cache_budget": context.cache_budget,
        "verification": context.verification,
        "static_version": context.static_version,
        "static_components": context.static_components,
        "version_fingerprint": context.version_fingerprint,
        "block_count": context.block_count,
        "layout_fingerprint": _layout_fingerprint(context.descriptors),
    }


def _summary(
    context: ExecutionContext,
    *,
    records: dict[int, dict[str, Any]],
    output: np.ndarray,
    output_file_digest: str,
    output_digest: str,
    case_id: str | None,
) -> dict[str, Any]:
    problem = context.problem
    block_reports = []
    numeric_reports = []
    nonfinite_blocks = []
    for descriptor in context.descriptors:
        record = records[descriptor.sequence]
        numeric_report = record.get("numeric_report")
        if isinstance(numeric_report, dict):
            numeric_reports.append(numeric_report)
        entry = {
            "sequence": descriptor.sequence,
            "shard_id": descriptor.shard_id,
            "batch_index": descriptor.batch_index,
            "block_coord": descriptor.block_coord,
            "row_range": descriptor.row_range,
            "col_range": descriptor.col_range,
            "tail_flags": descriptor.tail_flags,
            "block_input_digest": descriptor.block_input_digest,
            "block_fingerprint": descriptor.block_fingerprint(),
            "product_digest": record.get("product_digest"),
            "nonfinite": {
                "a_count": descriptor.a_nonfinite_count,
                "b_count": descriptor.b_nonfinite_count,
                "affected": descriptor.nonfinite_affected,
            },
            "numeric_status": numeric_report.get("status") if isinstance(numeric_report, dict) else "not_sampled",
        }
        block_reports.append(entry)
        if descriptor.nonfinite_affected:
            nonfinite_blocks.append(
                {
                    "sequence": descriptor.sequence,
                    "shard_id": descriptor.shard_id,
                    "block_coord": descriptor.block_coord,
                    "row_range": descriptor.row_range,
                    "col_range": descriptor.col_range,
                    "a_nonfinite_count": descriptor.a_nonfinite_count,
                    "b_nonfinite_count": descriptor.b_nonfinite_count,
                }
            )
    return {
        "schema_version": 1,
        "status": "completed",
        "case_id": case_id,
        "kind": problem.kind,
        "input_shape": {
            "M": problem.m,
            "N": problem.n,
            "K": problem.k,
            "batch": problem.batch,
        },
        "effective_shapes": {
            "A": list(problem.a.shape),
            "B": list(problem.b.shape),
            "C": list(problem.output_shape),
        },
        "dtype": problem.dtype.str,
        "a_transpose": problem.a_transpose,
        "b_transpose": problem.b_transpose,
        "input_shape_fingerprint": problem.shape_fingerprint,
        "content_digests": {
            "A": context.a_content_digest,
            "B": context.b_content_digest,
            "combined": context.version_fingerprint,
        },
        "plan": context.plan.to_dict(),
        "plan_fingerprint": context.plan.fingerprint(),
        "plan_selection": context.plan_selection.to_dict(),
        "accumulation_strategy": context.accumulation_strategy,
        "cache_budget": context.cache_budget,
        "version_fingerprint": context.version_fingerprint,
        "block_count": context.block_count,
        "sequence_range": [0, context.block_count - 1],
        "checkpoint_contract": {
            "sequence": "global-batch-major-row-major",
            "atomic_publish": "artifact_then_checksummed_record",
            "partial_accumulators_persisted": False,
            "tail_markers": "explicit-per-block",
        },
        "output": {
            "file": "output/result.npy",
            "shape": list(output.shape),
            "dtype": output.dtype.str,
            "array_payload_digest": output_digest,
            "file_sha256": output_file_digest,
        },
        "blocks": block_reports,
        "numeric_verification": {
            "method": "longdouble-power-of-two-leaf8-v1",
            "sampled_sequences": [report.get("sequence") for report in numeric_reports],
            "reports": numeric_reports,
        },
        "nonfinite_blocks": nonfinite_blocks,
    }


def execute_problem(
    *,
    workspace: str | os.PathLike[str],
    a: np.ndarray,
    b: np.ndarray,
    a_transpose: bool = False,
    b_transpose: bool = False,
    expected: ShapeExpectation | None = None,
    l1_cache_kib: int = DEFAULT_L1_CACHE_KIB,
    l2_cache_kib: int = DEFAULT_L2_CACHE_KIB,
    verify_blocks: int = 3,
    atol: float | None = None,
    rtol: float | None = None,
    case_id: str | None = None,
    source: dict[str, Any] | None = None,
    consume_stop: bool = True,
    stop_after_blocks: int | None = None,
    external_stop: Callable[[], bool] | None = None,
    on_block_published: Callable[[int, dict[str, Any]], None] | None = None,
    accumulation_strategy: str = "k-ascending",
) -> ExecutionResult:
    if stop_after_blocks is not None and stop_after_blocks < 0:
        raise BlockGemmError(
            E_INVALID_CONFIG,
            "stop_after_blocks must be non-negative",
            reason="invalid_stop_after_blocks",
            details={"stop_after_blocks": stop_after_blocks},
        )
    context = _build_context(
        workspace=workspace,
        a=a,
        b=b,
        a_transpose=a_transpose,
        b_transpose=b_transpose,
        expected=expected,
        l1_cache_kib=l1_cache_kib,
        l2_cache_kib=l2_cache_kib,
        verify_blocks=verify_blocks,
        atol=atol,
        rtol=rtol,
        accumulation_strategy=accumulation_strategy,
    )
    store = context.store
    if consume_stop:
        store.clear_stop()
    decision = analyze_resume(context)
    if decision.start_sequence < context.block_count or decision.reason == "initial_build":
        store.invalidate_from(
            decision.start_sequence,
            block_count=context.block_count,
            reason=decision.reason,
            details=decision.details,
        )
    store.write_run_spec(_run_spec(context, case_id=case_id, source=source))

    computed = 0
    stopped = False
    next_sequence = decision.start_sequence
    for descriptor in context.descriptors[decision.start_sequence :]:
        if (
            store.stop_requested()
            or (external_stop is not None and external_stop())
        ):
            stopped = True
            break
        product = compute_block(context.problem, descriptor)
        numeric_report: dict[str, Any] | None = None
        if descriptor.sequence in context.verification_sample_set:
            numeric_report = verify_block_against_reference(
                context.problem,
                descriptor,
                product,
                atol=float(context.verification["atol"]),
                rtol=float(context.verification["rtol"]),
            )
        record_base = _record_base(context, descriptor, numeric_report)
        published = store.publish_block(
            record_base,
            product,
            expected_plan_fingerprint=context.plan.fingerprint(),
        )
        computed += 1
        next_sequence = descriptor.sequence + 1
        if on_block_published is not None:
            on_block_published(published["sequence"], published)
        if stop_after_blocks is not None and computed >= stop_after_blocks:
            store.request_stop("stop_after_blocks")
            stopped = True
            break
        if external_stop is not None and external_stop():
            stopped = True
            break

    if stopped:
        return ExecutionResult(
            output=None,
            output_digest=None,
            output_path=None,
            summary=None,
            summary_path=None,
            completed=False,
            stopped=True,
            computed_blocks=computed,
            reused_blocks=decision.reused_blocks,
            next_sequence=next_sequence,
            workspace=store.paths.root,
            resume_decision=decision,
        )

    output, output_digest, records = store.assemble_from_checkpoints(
        output_shape=context.problem.output_shape,
        dtype=context.problem.dtype,
        block_count=context.block_count,
        descriptors=context.descriptors,
    )
    output_file_digest = store.save_output(output)
    summary = _summary(
        context,
        records=records,
        output=output,
        output_file_digest=output_file_digest,
        output_digest=output_digest,
        case_id=case_id,
    )
    store.write_summary(summary)
    return ExecutionResult(
        output=output,
        output_digest=output_digest,
        output_path=store.paths.output_file,
        summary=summary,
        summary_path=store.paths.summary_file,
        completed=True,
        stopped=False,
        computed_blocks=computed,
        reused_blocks=decision.reused_blocks,
        next_sequence=context.block_count,
        workspace=store.paths.root,
        resume_decision=decision,
    )


def run_gemm_job(
    workspace: str | os.PathLike[str],
    a: np.ndarray,
    b: np.ndarray,
    *,
    a_transpose: bool = False,
    b_transpose: bool = False,
    expected: ShapeExpectation | None = None,
    l1_cache_kib: int = DEFAULT_L1_CACHE_KIB,
    l2_cache_kib: int = DEFAULT_L2_CACHE_KIB,
    verify_blocks: int = 3,
    atol: float | None = None,
    rtol: float | None = None,
    case_id: str | None = None,
    source: dict[str, Any] | None = None,
    consume_stop: bool = True,
    stop_after_blocks: int | None = None,
    external_stop: Callable[[], bool] | None = None,
    on_block_published: Callable[[int, dict[str, Any]], None] | None = None,
    accumulation_strategy: str = "k-ascending",
) -> ExecutionResult:
    return execute_problem(
        workspace=workspace,
        a=a,
        b=b,
        a_transpose=a_transpose,
        b_transpose=b_transpose,
        expected=expected,
        l1_cache_kib=l1_cache_kib,
        l2_cache_kib=l2_cache_kib,
        verify_blocks=verify_blocks,
        atol=atol,
        rtol=rtol,
        case_id=case_id,
        source=source,
        consume_stop=consume_stop,
        stop_after_blocks=stop_after_blocks,
        external_stop=external_stop,
        on_block_published=on_block_published,
        accumulation_strategy=accumulation_strategy,
    )


def run_bmm_job(*args: Any, **kwargs: Any) -> ExecutionResult:
    return run_gemm_job(*args, **kwargs)


def gemm(
    a: np.ndarray,
    b: np.ndarray,
    *,
    a_transpose: bool = False,
    b_transpose: bool = False,
    workspace: str | os.PathLike[str] | None = None,
    **kwargs: Any,
) -> np.ndarray:
    """Convenience GEMM API returning the output ndarray.

    Passing ``workspace`` enables durable checkpoints; otherwise a temporary
    workspace is used and removed after assembly.
    """

    if workspace is not None:
        result = run_gemm_job(
            workspace,
            a,
            b,
            a_transpose=a_transpose,
            b_transpose=b_transpose,
            **kwargs,
        )
        assert result.output is not None
        return result.output
    with tempfile.TemporaryDirectory(prefix="blockgemm-") as temporary:
        result = run_gemm_job(
            temporary,
            a,
            b,
            a_transpose=a_transpose,
            b_transpose=b_transpose,
            **kwargs,
        )
        assert result.output is not None
        return result.output.copy()


def bmm(
    a: np.ndarray,
    b: np.ndarray,
    *,
    a_transpose: bool = False,
    b_transpose: bool = False,
    workspace: str | os.PathLike[str] | None = None,
    **kwargs: Any,
) -> np.ndarray:
    return gemm(
        a,
        b,
        a_transpose=a_transpose,
        b_transpose=b_transpose,
        workspace=workspace,
        **kwargs,
    )


def query_status(workspace: str | os.PathLike[str]) -> dict[str, Any]:
    return CheckpointStore(workspace).status()


def query_report(workspace: str | os.PathLike[str]) -> dict[str, Any] | None:
    return CheckpointStore(workspace).read_summary()


def query_diagnostics(workspace: str | os.PathLike[str]) -> dict[str, Any]:
    summary = CheckpointStore(workspace).read_summary()
    if summary is None:
        return {
            "available": False,
            "numeric_verification": {},
            "nonfinite_blocks": [],
            "not_ok_blocks": [],
        }
    blocks = summary.get("blocks", [])
    return {
        "available": True,
        "numeric_verification": summary.get("numeric_verification", {}),
        "nonfinite_blocks": summary.get("nonfinite_blocks", []),
        "not_ok_blocks": [
            block
            for block in blocks
            if block.get("numeric_status") not in {"ok", "skipped_nonfinite", "not_sampled"}
            or (block.get("nonfinite") or {}).get("affected")
        ],
    }


def query_invalidation(workspace: str | os.PathLike[str]) -> dict[str, Any] | None:
    return CheckpointStore(workspace).read_invalidation()


def request_stop(workspace: str | os.PathLike[str], reason: str = "external_request") -> dict[str, Any]:
    return CheckpointStore(workspace).request_stop(reason)


def clear_stop(workspace: str | os.PathLike[str]) -> None:
    CheckpointStore(workspace).clear_stop()


def query_plan(
    *,
    m: int,
    n: int,
    k: int,
    batch: int = 1,
    dtype: str = "float64",
    l1_cache_kib: int = DEFAULT_L1_CACHE_KIB,
    l2_cache_kib: int = DEFAULT_L2_CACHE_KIB,
) -> dict[str, Any]:
    return select_tiling(
        m=m,
        n=n,
        k=k,
        batch=batch,
        dtype=dtype,
        l1_cache_kib=l1_cache_kib,
        l2_cache_kib=l2_cache_kib,
    ).to_dict()
