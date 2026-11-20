from __future__ import annotations

import json
import subprocess
from pathlib import Path

from linreg_workbench.demo import EXPECTED_DIR, verify_demo


def test_fixed_demo_and_error_matrix() -> None:
    report = verify_demo(data_dir=Path(__file__).resolve().parent.parent / "data")
    assert report["ok"] is True
    assert report["determinism"]["ok"] is True
    assert report["stop_resume"]["ok"] is True
    assert report["stop_resume"]["matches_full"] is True
    assert report["invalidation"]["earliest_stage"] == "solve"
    assert report["invalidation"]["reuse_map"]["preprocess"] is True
    assert report["invalidation"]["reuse_map"]["folds"] is True
    assert report["errors"]["invalid_l1_ratio"] == "E_INVALID_HYPERPARAM"
    assert report["errors"]["single_fold_cv"] == "E_INVALID_CV"
    assert report["errors"]["numeric"]["code"] == "E_NUMERIC"
    assert report["errors"]["empty_feature"]["ok"] is True
    assert report["errors"]["checkpoint_lab"]["invalid_stage_code"] == "E_INVALID_STAGE"
    assert report["errors"]["checkpoint_lab"]["concurrent_one_success_one_stale"] is True
    assert report["expected_summary_comparison"]["small"]["matches"] is True
    assert report["expected_summary_comparison"]["medium"]["matches"] is True
    for name in ("small.summary.json", "medium.summary.json"):
        payload = json.loads((EXPECTED_DIR / name).read_text("utf-8"))
        assert payload["result_digest"]


def test_ui_javascript_syntax() -> None:
    root = Path(__file__).resolve().parent.parent
    completed = subprocess.run(
        ["node", "--check", str(root / "linreg_workbench" / "static" / "app.js")],
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
