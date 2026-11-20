from __future__ import annotations

import json
from pathlib import Path

from qoplib.cli import main


def test_cli_full_lifecycle(demo_data, tmp_path, capsys):
    run_dir = tmp_path / "run"
    manifest = str(demo_data / "manifest.yaml")

    rc = main(["schedule", "--manifest", manifest])
    assert rc == 0
    schedule = json.loads(capsys.readouterr().out)
    assert len(schedule["accepted"]) == 4

    assert main(["run", "--manifest", manifest, "--data-dir",
                 str(demo_data), "--run-dir", str(run_dir),
                 "--serial"]) == 0
    run_payload = json.loads(capsys.readouterr().out)
    assert run_payload["summary"]["totals"]["shards_completed"] == 3

    assert main(["status", "--run-dir", str(run_dir)]) == 0
    status = json.loads(capsys.readouterr().out)
    assert status["latest_sequence"] == 12

    assert main(["fingerprints", "--run-dir", str(run_dir)]) == 0
    fingerprints = json.loads(capsys.readouterr().out)
    assert len(fingerprints["fingerprints"]) == 13

    assert main(["audit", "--run-dir", str(run_dir)]) == 0
    audit = json.loads(capsys.readouterr().out)
    assert Path(audit["path"]).exists()

    assert main(["manifest-fingerprint", "--manifest", manifest]) == 0
    fp_payload = json.loads(capsys.readouterr().out)
    assert len(fp_payload["manifest_fingerprint"]) == 64


def test_cli_stop_and_resume(demo_data, tmp_path, capsys):
    run_dir = tmp_path / "run"
    manifest = str(demo_data / "manifest.yaml")
    assert main(["stop", "--run-dir", str(run_dir)]) == 0
    stopped = json.loads(capsys.readouterr().out)
    assert stopped["status"] == "stop_requested"

    outcome = main(["run", "--manifest", manifest, "--data-dir",
                    str(demo_data), "--run-dir", str(run_dir), "--serial"])
    assert outcome == 0
    capsys.readouterr()  # discard the stopped-run output
    (run_dir / "STOP").unlink()
    assert main(["resume", "--manifest", manifest, "--data-dir",
                 str(demo_data), "--run-dir", str(run_dir),
                 "--serial"]) == 0
    resumed = json.loads(capsys.readouterr().out)
    assert resumed["summary"]["totals"]["shards_completed"] == 3


def test_cli_business_error_exit_code(demo_data, tmp_path, capsys):
    import yaml
    bad_manifest = tmp_path / "bad.yaml"
    manifest = yaml.safe_load((demo_data / "manifest.yaml").read_text())
    manifest["shards"][0]["layers"][0]["bitwidth"] = 3
    bad_manifest.write_text(yaml.safe_dump(manifest))
    rc = main(["schedule", "--manifest", str(bad_manifest)])
    assert rc == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload["code"] == "E_INVALID_BITWIDTH"
