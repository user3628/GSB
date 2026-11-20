from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _run_cli(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "qworkbench", *args],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )


def test_cli_run_manifest_and_export(tmp_path) -> None:
    workdir = tmp_path / "cli"
    run = _run_cli(
        "run",
        "--workdir",
        str(workdir),
        "--data-dir",
        str(ROOT / "data/demo"),
        "--config",
        str(ROOT / "configs/demo.json"),
    )
    assert run.returncode == 0, run.stderr
    run_payload = json.loads(run.stdout)
    assert run_payload["status"] == "COMPLETE"
    manifest = _run_cli(
        "manifest",
        "--workdir",
        str(workdir),
        "--data-dir",
        str(ROOT / "data/demo"),
        "--config",
        str(ROOT / "configs/demo.json"),
    )
    assert manifest.returncode == 0, manifest.stderr
    manifest_payload = json.loads(manifest.stdout)["manifest"]
    output = tmp_path / "mix.jsonl"
    exported = _run_cli(
        "export",
        "--workdir",
        str(workdir),
        "--data-dir",
        str(ROOT / "data/demo"),
        "--config",
        str(ROOT / "configs/demo.json"),
        "--output",
        str(output),
    )
    assert exported.returncode == 0, exported.stderr
    assert json.loads(exported.stdout)["sha256"] == manifest_payload["mixture_sha256"]


def test_cli_business_error_codes(tmp_path) -> None:
    missing = _run_cli(
        "diagnostics",
        "--workdir",
        str(tmp_path / "empty"),
        "--data-dir",
        str(ROOT / "data/demo"),
    )
    assert missing.returncode == 2
    assert json.loads(missing.stderr)["code"] == "E_INVALID_STAGE"

    raw = json.loads((ROOT / "configs/demo.json").read_text(encoding="utf-8"))
    raw["quantization"]["fraction_bits"] = 0
    bad_config = tmp_path / "bad.json"
    bad_config.write_text(json.dumps(raw), encoding="utf-8")
    config_error = _run_cli(
        "status",
        "--workdir",
        str(tmp_path / "bad"),
        "--data-dir",
        str(ROOT / "data/demo"),
        "--config",
        str(bad_config),
    )
    assert config_error.returncode == 2
    assert json.loads(config_error.stderr)["code"] == "E_CONFIG"

    raw = json.loads((ROOT / "configs/demo.json").read_text(encoding="utf-8"))
    raw["allocation"]["source_weights"]["curated-sft"] = 2.0
    bad_weight = tmp_path / "bad-weight.json"
    bad_weight.write_text(json.dumps(raw), encoding="utf-8")
    weight_error = _run_cli(
        "status",
        "--workdir",
        str(tmp_path / "bad-weight"),
        "--data-dir",
        str(ROOT / "data/demo"),
        "--config",
        str(bad_weight),
    )
    assert weight_error.returncode == 2
    assert json.loads(weight_error.stderr)["code"] == "E_CONFIG"
