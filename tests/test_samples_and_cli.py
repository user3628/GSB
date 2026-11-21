from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path


def _env(root: Path) -> dict[str, str]:
    env = dict(os.environ)
    env["PYTHONPATH"] = str(root / "src")
    return env


def test_generator_is_reproducible(root, tmp_path):
    out = tmp_path / "samples"
    subprocess.run(
        [
            sys.executable,
            str(root / "scripts" / "generate_samples.py"),
            "--seed",
            "20261122",
            "--out",
            str(out),
        ],
        check=True,
        cwd=root,
        env=_env(root),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    for relative in (
        "corpus/shard-01.jsonl",
        "corpus/shard-02.jsonl",
        "corpus/shard-03.jsonl",
        "expected.json",
        "legacy_evidence_v1.jsonl",
        "legacy_evidence_v1_invalid.jsonl",
        "edge_cases.jsonl",
    ):
        assert (out / relative).read_bytes() == (root / "samples" / relative).read_bytes(), relative


def test_cli_verify_batch_query_and_evidence_validate(root, tmp_path):
    state = tmp_path / "state"
    out = tmp_path / "artifacts"
    common = [
        sys.executable,
        "-m",
        "factmerge",
        "--config",
        str(root / "configs" / "default.json"),
        "--corpus",
        str(root / "samples" / "corpus"),
        "--state-dir",
        str(state),
        "--expected",
        str(root / "samples" / "expected.json"),
    ]
    batch = subprocess.run(
        common + ["batch", "--out-dir", str(out)],
        check=True,
        cwd=root,
        env=_env(root),
        text=True,
        stdout=subprocess.PIPE,
    )
    payload = json.loads(batch.stdout)
    assert payload["status"] == "complete"
    assert payload["summary"]["decision_accuracy"] == "1.000000"
    manifest_path = out / "evidence_manifest.json"
    summary_path = out / "batch_summary.json"
    assert manifest_path.exists() and summary_path.exists()

    validated = subprocess.run(
        common + ["evidence", "validate", str(manifest_path)],
        check=True,
        cwd=root,
        env=_env(root),
        text=True,
        stdout=subprocess.PIPE,
    )
    assert json.loads(validated.stdout)["ok"] is True

    query = subprocess.run(
        [sys.executable, "-m", "factmerge", "query", "--manifest", str(manifest_path), "--claim-id", "C-TIE-B"],
        check=True,
        cwd=root,
        env=_env(root),
        text=True,
        stdout=subprocess.PIPE,
    )
    claim = json.loads(query.stdout)
    assert claim["stage3"]["reason_code"] == "CONFLICT_LOWER_CONFIDENCE"
    assert claim["stage3"]["winner_claim_id"] == "C-TIE-A"


def test_cli_returns_structured_error_codes(root, tmp_path):
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "factmerge",
            "--config",
            str(root / "configs" / "default.json"),
            "migrate-v1",
            str(root / "samples" / "legacy_evidence_v1_invalid.jsonl"),
            "--out-manifest",
            str(tmp_path / "manifest.json"),
            "--out-v1",
            str(tmp_path / "v1.jsonl"),
        ],
        cwd=root,
        env=_env(root),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert result.returncode == 4
    error = json.loads(result.stderr)
    assert error["code"] == "E_INVALID_FORMAT"
    assert error["claim_id"] == "L900"
