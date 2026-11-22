from __future__ import annotations

import numpy as np

from blockgemm import describe_execution, run_gemm_job
from blockgemm.checkpoint import CheckpointStore
from blockgemm.manifest import load_case


def _load(manifest_path, case_id="small_gemm"):
    return load_case(manifest_path, case_id)


def test_graceful_stop_then_resume_is_byte_identical(manifest_path, tmp_path) -> None:
    loaded = _load(manifest_path)
    baseline_workspace = tmp_path / "baseline"
    resumed_workspace = tmp_path / "resumed"
    baseline = run_gemm_job(
        baseline_workspace,
        loaded.a,
        loaded.b,
        expected=loaded.expected,
        l2_cache_kib=32,
        verify_blocks=3,
    )
    stopped = run_gemm_job(
        resumed_workspace,
        loaded.a,
        loaded.b,
        expected=loaded.expected,
        l2_cache_kib=32,
        verify_blocks=3,
        stop_after_blocks=2,
    )
    assert stopped.stopped and not stopped.completed
    assert stopped.next_sequence == 2
    assert not (resumed_workspace / "output" / "result.npy").exists()
    status = CheckpointStore(resumed_workspace).status()
    assert status["valid_prefix"] == 2
    assert status["stop_requested"] is True

    resumed = run_gemm_job(
        resumed_workspace,
        loaded.a,
        loaded.b,
        expected=loaded.expected,
        l2_cache_kib=32,
        verify_blocks=3,
    )
    assert resumed.completed
    assert resumed.reused_blocks == 2
    assert resumed.computed_blocks == 2
    assert baseline.output_path.read_bytes() == resumed.output_path.read_bytes()
    assert baseline.summary_path.read_bytes() == resumed.summary_path.read_bytes()
    records = [
        CheckpointStore(resumed_workspace).scan(verify_artifacts=True).records[index]
        for index in range(4)
    ]
    assert [record["sequence"] for record in records] == [0, 1, 2, 3]


def test_content_change_invalidates_earliest_block_and_downstream(manifest_path, tmp_path) -> None:
    loaded = _load(manifest_path)
    workspace = tmp_path / "reuse"
    fresh_workspace = tmp_path / "fresh"
    run_gemm_job(workspace, loaded.a, loaded.b, expected=loaded.expected, l2_cache_kib=32)
    context, _ = describe_execution(
        workspace=workspace,
        a=loaded.a,
        b=loaded.b,
        expected=loaded.expected,
        l2_cache_kib=32,
    )
    assert len(context.descriptors) >= 2
    first = context.descriptors[0]
    target = next(descriptor for descriptor in context.descriptors[1:] if descriptor.col_range != first.col_range)
    b_modified = loaded.b.copy()
    b_modified[0, target.col_start] += 0.125

    changed = run_gemm_job(
        workspace,
        loaded.a,
        b_modified,
        expected=loaded.expected,
        l2_cache_kib=32,
    )
    fresh = run_gemm_job(
        fresh_workspace,
        loaded.a,
        b_modified,
        expected=loaded.expected,
        l2_cache_kib=32,
    )
    assert changed.resume_decision.reason == "input_content_changed"
    assert changed.resume_decision.start_sequence == target.sequence
    assert changed.resume_decision.reused_blocks == target.sequence
    assert changed.resume_decision.details["b_stage_changed"] is True
    assert changed.output_path.read_bytes() == fresh.output_path.read_bytes()
    assert changed.summary_path.read_bytes() == fresh.summary_path.read_bytes()
    invalidation = CheckpointStore(workspace).read_invalidation()
    assert invalidation["reason"] == "input_content_changed"
    assert invalidation["affected_start_sequence"] == target.sequence
    assert invalidation["affected_end_sequence"] == len(context.descriptors) - 1


def test_accumulation_strategy_change_invalidates_all(manifest_path, tmp_path) -> None:
    loaded = _load(manifest_path)
    workspace = tmp_path / "strategy"
    first = run_gemm_job(
        workspace,
        loaded.a,
        loaded.b,
        expected=loaded.expected,
        l2_cache_kib=32,
        accumulation_strategy="k-ascending",
    )
    second = run_gemm_job(
        workspace,
        loaded.a,
        loaded.b,
        expected=loaded.expected,
        l2_cache_kib=32,
        accumulation_strategy="k-descending",
    )
    assert first.completed and second.completed
    assert second.resume_decision.reason == "static_version_changed"
    assert second.resume_decision.start_sequence == 0
    assert "accumulation_strategy" in second.resume_decision.details["changed_components"]
    assert second.reused_blocks == 0


def test_cache_budget_change_invalidates_all_even_if_output_matches(manifest_path, tmp_path) -> None:
    loaded = _load(manifest_path)
    workspace = tmp_path / "budget"
    run_gemm_job(workspace, loaded.a, loaded.b, expected=loaded.expected, l2_cache_kib=32)
    changed = run_gemm_job(workspace, loaded.a, loaded.b, expected=loaded.expected, l2_cache_kib=64)
    assert changed.resume_decision.reason == "static_version_changed"
    assert changed.resume_decision.start_sequence == 0
    assert "l2_cache_kib" in changed.resume_decision.details["changed_components"]


def test_transpose_flag_change_invalidates_all(tmp_path) -> None:
    rng = np.random.default_rng(71)
    a = rng.standard_normal((64, 64))
    b = rng.standard_normal((64, 64))
    workspace = tmp_path / "transpose"
    run_gemm_job(workspace, a, b, l2_cache_kib=32)
    changed = run_gemm_job(workspace, a, b, a_transpose=True, l2_cache_kib=32)
    assert changed.resume_decision.reason == "static_version_changed"
    assert changed.resume_decision.start_sequence == 0
    assert "shape_fingerprint" in changed.resume_decision.details["changed_components"]
