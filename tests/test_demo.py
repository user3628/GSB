from __future__ import annotations

import json
from pathlib import Path

from transcript_workbench.demo import run_demo


def test_complete_demo_is_deterministic_and_covers_required_findings(tmp_path, demo_root):
    first = run_demo(demo_root)
    first_summary = json.loads(
        (demo_root / "artifacts" / "demo_summary.json").read_text(encoding="utf-8")
    )
    second_root = tmp_path / "second"
    second_root.mkdir()
    second = run_demo(second_root)
    second_summary = json.loads(
        (second_root / "artifacts" / "demo_summary.json").read_text(encoding="utf-8")
    )
    first.pop("summary_path")
    second.pop("summary_path")
    assert first == second
    assert first_summary == second_summary
    assert first["dataset"]["chunks"] == 3
    assert first["dataset"]["mixed_language"] is True
    assert first["format_checks"]["checks"] == 6
    assert first["format_checks"]["all_exact"] is True
    assert first["format_checks"]["loss_entries"] == 0
    assert first["merge"]["conflict_count"] == 1
    assert first["migration"]["status"] == "migrated"
    assert first["error_codes"]["E_TIMELINE_ERROR"] == 1
    assert first["error_codes"]["E_STALE_SNAPSHOT"] == 3
    assert first["error_codes"]["E_STOPPING"] == 1
    assert sorted(first["versions"]["word_counts"]) == ["1", "2", "3", "4", "5"]
    workspace = demo_root / "artifacts" / "demo_workspace"
    assert list(workspace.rglob("*.tmp-*")) == []
    assert (demo_root / "demo_data" / "replacement_snapshot.json").exists()
    assert (demo_root / "demo_data" / "corrupt_snapshot.json").exists()
