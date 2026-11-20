from __future__ import annotations

from pathlib import Path

from image_workbench.demo import (
    EXPECTED_PIPELINE_SUMMARY_DIGEST,
    EXPECTED_SAMPLE_DIGEST,
    run_demo,
)


def test_complete_demo_has_fixed_digests_and_all_checks_pass(tmp_path: Path) -> None:
    report = run_demo(tmp_path, runtime_dir=tmp_path / "runtime")
    assert report["ok"] is True, report.get("exception")
    assert report["passed_count"] == report["check_count"] == 45
    assert report["sample_digest"] == EXPECTED_SAMPLE_DIGEST
    assert report["pipeline_summary_digest"] == EXPECTED_PIPELINE_SUMMARY_DIGEST
    assert {item["actual"] for item in report["error_codes"]} == {
        "E_FORMAT_LOSS",
        "E_NUMERIC",
        "E_STALE_SNAPSHOT",
        "E_SNAPSHOT_DIGEST",
    }
