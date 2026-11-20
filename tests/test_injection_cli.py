from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

from vadkit.errors import VadError
from vadkit.injection import execute_injection


@pytest.mark.parametrize(
    ("kind", "expected_code"),
    [
        ("silent_input", "E_SILENT_INPUT"),
        ("clipped_peak", "E_FEATURE_MISMATCH"),
        ("length_mismatch", "E_INPUT_LENGTH"),
        ("invalid_threshold", "E_INVALID_CONFIG"),
        ("feature_dimension", "E_FEATURE_MISMATCH"),
    ],
)
def test_library_injection_returns_stable_codes(
    dataset_dir: Path, kind: str, expected_code: str
) -> None:
    with pytest.raises(VadError) as caught:
        execute_injection(kind, dataset_dir)
    error = caught.value
    assert error.code == expected_code
    if kind in {"silent_input", "clipped_peak", "length_mismatch", "feature_dimension"}:
        assert error.chunk_id
        assert error.sample_position is not None
        assert error.sample_position >= 0


def test_cli_injection_exit_codes(dataset_dir: Path) -> None:
    expected = {
        "silent_input": 4,
        "clipped_peak": 5,
        "length_mismatch": 3,
        "invalid_threshold": 2,
        "feature_dimension": 5,
    }
    for kind, exit_code in expected.items():
        command = [
            sys.executable,
            "-m",
            "vadkit",
            "inject",
            "--data",
            str(dataset_dir),
            "--kind",
            kind,
        ]
        completed = subprocess.run(command, cwd=Path.cwd(), capture_output=True, text=True, timeout=30)
        assert completed.returncode == exit_code, completed.stderr
        assert '"handled": true' in completed.stdout


def test_manifest_sample_count_mismatch_reports_chunk(
    dataset_copy: Path, tmp_path: Path
) -> None:
    import json

    from vadkit.audio import load_dataset

    manifest_path = dataset_copy / "manifest.json"
    body = json.loads(manifest_path.read_text(encoding="utf-8"))
    body["chunks"][1]["sample_count"] += 1
    # The WAV paths are relative to the manifest, so keep the manifest beside the data.
    local_manifest = dataset_copy / "broken-manifest.json"
    local_manifest.write_text(json.dumps(body), encoding="utf-8")
    with pytest.raises(VadError) as caught:
        load_dataset(local_manifest)
    assert caught.value.code == "E_INPUT_LENGTH"
    assert caught.value.chunk_id == "low_snr_02"
    assert caught.value.details["declared_samples"] == body["chunks"][1]["sample_count"]

