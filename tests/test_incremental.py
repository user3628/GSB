from __future__ import annotations

from pathlib import Path

import pytest

from xrel_workbench.pipeline import compare_outputs, run_pipeline, verify_against_full
from xrel_workbench.util import read_json
from xrel_workbench.workspace import (
    WorkspacePaths,
    clear_stop,
    edit_pair,
    initialize_workspace,
    update_bridge_entry,
    update_config,
)


def _change_bridge(workspace) -> None:
    update_bridge_entry(workspace, "br-003", weight=0.71)


def _change_bits(workspace) -> None:
    update_config(workspace, "mapping.quantization_bits", 11)


def _change_smooth(workspace) -> None:
    update_config(workspace, "mapping.smooth_strength", 0.42)


def _change_clip(workspace) -> None:
    update_config(workspace, "scoring.score_clip", 5.5)


def _change_thresholds(workspace) -> None:
    update_config(workspace, "grading.thresholds", [0.5, 1.1, 2.0])


def _change_text(workspace) -> None:
    edit_pair(workspace, "part-001", "p0001", query="跨语言检索与排序")


@pytest.mark.parametrize(
    ("mutation", "expected_stage"),
    [
        (_change_bridge, "stage2"),
        (_change_bits, "stage2"),
        (_change_smooth, "stage2"),
        (_change_clip, "stage3"),
        (_change_thresholds, "stage4"),
        (_change_text, "stage1"),
    ],
)
def test_earliest_invalidation_stage_and_shadow_equality(
    workspace,
    mutation,
    expected_stage: str,
) -> None:
    old_build = read_json(workspace.reports / "summary.json")["build_id"]
    mutation(workspace)
    result = run_pipeline(workspace, full=False, verify_incremental=True)
    plan = result["invalidation_plan"]["summary"]
    assert any(item["earliest_affected_stage"] == expected_stage for item in plan)
    assert result["verification"]["match"] is True
    assert result["verification"]["difference_count"] == 0
    assert result["rebuilt_units"] > 0
    assert result["reused_units"] > 0 or expected_stage == "stage1"
    new_build = read_json(workspace.reports / "summary.json")["build_id"]
    assert new_build != old_build


def test_from_scratch_builds_are_byte_identical(generated_data: Path, tmp_path: Path) -> None:
    left = initialize_workspace(generated_data, tmp_path / "left", force=True)
    right = initialize_workspace(generated_data, tmp_path / "right", force=True)
    left_result = run_pipeline(left, full=True, verify_incremental=False)
    right_result = run_pipeline(right, full=True, verify_incremental=False)
    assert left_result["build_id"] == right_result["build_id"]
    assert compare_outputs(left, right) == []


def test_stop_resume_matches_full_and_sequence_is_contiguous(
    generated_data: Path,
    workspace,
    tmp_path: Path,
) -> None:
    baseline = read_json(workspace.reports / "summary.json")
    resumed = initialize_workspace(generated_data, tmp_path / "stop-resume", force=True)
    stopped = run_pipeline(resumed, full=True, verify_incremental=False, stop_after_units=4)
    assert stopped["stopped"] is True
    assert stopped["rebuilt_units"] == 4
    assert read_json(resumed.run_status)["status"] == "stopped"
    clear_stop(resumed)
    result = run_pipeline(resumed, verify_incremental=True)
    assert result["stopped"] is False
    assert result["summary"]["build_id"] == baseline["build_id"]
    assert result["verification"]["match"] is True
    records = sorted(
        (int(read_json(path)["seq"]), path) for path in resumed.checkpoints.glob("*.json")
    )
    assert [seq for seq, _ in records] == list(range(1, 14))


def test_incremental_mismatch_returns_business_error(workspace, monkeypatch) -> None:
    import xrel_workbench.pipeline as pipeline

    monkeypatch.setattr(
        pipeline,
        "compare_outputs",
        lambda left, right: [{"path": "reports/summary.json", "kind": "byte_mismatch"}],
    )
    with pytest.raises(Exception) as raised:
        verify_against_full(workspace)
    error = raised.value
    assert getattr(error, "code", None) == "E_INCREMENTAL_MISMATCH"
    assert error.details["difference_count"] == 1


def test_unused_bridge_edit_reuses_scoring_and_refreshes_summary(workspace) -> None:
    # Add a bridge entry whose terms do not occur in any query or document.
    from xrel_workbench.bridge import load_bridge, write_bridge

    bridge = load_bridge(workspace.bridge)
    bridge["entries"].append(
        {"id": "br-unused", "zh": "无关词", "en": ["unrelatedterm"], "weight": 0.5}
    )
    write_bridge(workspace.bridge, bridge)
    result = run_pipeline(workspace, full=False, verify_incremental=True)
    assert result["reused_units"] == 12
    assert result["rebuilt_units"] == 1
    assert result["invalidation_plan"]["summary"]
    assert result["verification"]["match"] is True
    summary = read_json(workspace.reports / "summary.json")
    assert "input_digests" in summary
