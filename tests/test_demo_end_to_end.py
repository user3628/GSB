from __future__ import annotations

from grad_fault_lab.demo import run_demo

from .conftest import make_test_config, write_config_file


def test_full_demo_including_forced_kill_recovery(tmp_path) -> None:
    config = make_test_config(tmp_path)
    config_path = write_config_file(config, tmp_path / "demo-config.json")
    report = run_demo(config_path)
    assert report["scenario_count"] == 6
    assert len(report["scenarios"]) == 6
    names = {scenario["name"] for scenario in report["scenarios"]}
    assert {
        "timeout_then_retry_success",
        "content_truncated_fatal_abort",
        "duplicate_submission_conflict",
        "order_mismatch_fatal",
        "connection_refused_then_retry_success",
        "retry_exhausted_shortened_set",
    } == names
    recovery = report["forced_kill_recovery"]
    assert recovery["recovered_state"] == "ABORTED"
    assert recovery["recovered_final_code"] == "E_RECOVERED_AFTER_RESTART"
    assert recovery["recovery"]["diff"]["exit_report_complete"] is False

