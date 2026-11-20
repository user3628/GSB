from __future__ import annotations

from pathlib import Path

from quant_audit.demo import run_demo


def test_complete_demo_is_deterministic_and_exercises_all_codes(tmp_path: Path) -> None:
    first = run_demo(
        workspace_path=tmp_path / "run1",
        data_dir=tmp_path / "data1",
        report_path=tmp_path / "report1.json",
        force=True,
    )
    second = run_demo(
        workspace_path=tmp_path / "run2",
        data_dir=tmp_path / "data2",
        report_path=tmp_path / "report2.json",
        force=True,
    )

    assert first["deterministic_digest"] == second["deterministic_digest"]
    assert first["versions"]["v1_full"]["usable"] is True
    assert first["versions"]["v2_incremental"]["usable"] is True
    assert first["versions"]["v3_over_budget"]["usable"] is False
    assert first["incremental"]["reused_layers"] == ["linear_00", "linear_01", "linear_02"]
    assert first["rollback"]["active_version_after_rollback"] == 2
    assert first["rollback"]["result_matches_historical"] is True
    assert first["rollback"]["budget_matches_historical"] is True

    replacement_codes = {
        value["code"] for value in first["version_replacement_rejected"].values()
    }
    assert {"E_VERSION_ROLLBACK", "E_AUDIT_TAMPERED", "E_INVALID_STAGE"} <= replacement_codes
    anomaly_codes = {value["code"] for value in first["anomaly_codes"].values()}
    assert {
        "E_QUANT_INVALID",
        "E_ATTRIBUTION_GAP",
        "E_INCREMENTAL_MISMATCH",
        "E_STALE_VERSION",
        "E_INVALID_ROLLBACK",
    } <= anomaly_codes

