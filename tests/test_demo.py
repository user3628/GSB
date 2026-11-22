from __future__ import annotations

import json
from pathlib import Path

from density_trace.demo import run_demo


def test_full_demo_fixed_summary(tmp_path: Path) -> None:
    output = tmp_path / "demo"
    summary = run_demo(output, overwrite=True)
    assert summary["recovery_identical"] is True
    assert summary["continuous"]["record_count"] == 4
    assert summary["continuous"]["record_chain_sha256"] == (
        "sha256:d709582ac194c34f50ea15cf0378764475f57a85e62b7e586ea1f931df5f4799"
    )
    assert summary["continuous"]["final_curve_sha256"] == (
        "sha256:04b89ab4eeb67338a0e31855585befa2bde77f5f5ac04164d01c33b3378b41e3"
    )
    assert summary["continuous"]["verdicts"] == ["warning", "drift", "drift", "drift"]
    assert summary["resume"]["first_resume_status"] == "stopped"
    assert summary["resume"]["first_resume_published_batches"] == []
    assert summary["resume"]["second_resume_published_batches"] == [2, 3]
    assert summary["invalid_reference_error"]["code"] == "E_REFERENCE_INVALID"
    assert summary["invalid_reference_error"]["batch_id"] == 0
    assert summary["invalid_reference_error"]["quantile_index"] == 7
    assert summary["invalidation"]["plan"]["earliest_affected_batch"] == 2
    assert summary["invalidation"]["reused_prefix_unchanged"] == {"0": True, "1": True}
    assert summary["invalidation"]["final"]["generation"] == 2
    on_disk = json.loads((output / "summary.json").read_text(encoding="utf-8"))
    assert on_disk == summary
