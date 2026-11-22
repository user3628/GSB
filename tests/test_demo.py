from __future__ import annotations

from audioq.demo import run_demo


def test_demo_acceptance(workspace):
    root = workspace["root"]
    # Use a temporary asset root so this test does not race the session demo assets.
    summary = run_demo(root / "acceptance_demo", force=True)
    assert summary["acceptance_ok"] is True
    assert summary["stop_resume"]["stopped_status"] == "stopped"
    assert summary["stop_resume"]["recovered"] is True
    assert summary["stop_resume"]["byte_identical"] is True
    assert summary["config_roundtrip"]["ok"] is True
    assert summary["error_checks"]["invalid_weight"]["ok"] is True
    assert summary["error_checks"]["length_mismatch"]["ok"] is True
    assert "E_CLIPPED_AUDIO" in summary["error_checks"]["observed_report_codes"]
    assert "E_EMPTY_AUDIO" in summary["error_checks"]["observed_report_codes"]
