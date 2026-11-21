from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from conftest import SAMPLES


def run_cli(*arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "matgen", *arguments],
        cwd=SAMPLES.parent,
        text=True,
        capture_output=True,
        timeout=120,
    )


def test_cli_generate_status_report_and_versions(tmp_path: Path) -> None:
    generate = run_cli(
        "generate",
        "--workspace",
        str(tmp_path),
        "--recipes",
        str(SAMPLES / "recipes.json"),
        "--cards",
        str(SAMPLES / "color_cards.json"),
        "--config",
        str(SAMPLES / "config.json"),
        "--output",
        str(tmp_path / "out"),
    )
    assert generate.returncode == 0, generate.stderr
    generated = json.loads(generate.stdout)
    assert generated["status"] == "complete"
    assert generated["run_digest"]

    status = run_cli("status", "--workspace", str(tmp_path))
    assert status.returncode == 0, status.stderr
    status_payload = json.loads(status.stdout)
    assert len(status_payload["materials"]) == 4

    report = run_cli("report", "--workspace", str(tmp_path), "--material-id", "terra_cotta")
    assert report.returncode == 0, report.stderr
    report_payload = json.loads(report.stdout)
    assert report_payload["material_id"] == "terra_cotta"
    assert report_payload["octaves"]

    versions = run_cli("versions", "--workspace", str(tmp_path), "--config", str(SAMPLES / "config.json"))
    assert versions.returncode == 0, versions.stderr
    assert json.loads(versions.stdout)["algorithm_version"] == "matgen-core-1.0.0"


def test_cli_returns_structured_business_error(tmp_path: Path) -> None:
    bad_recipe = tmp_path / "bad.json"
    bad_recipe.write_text(
        json.dumps(
            [
                {
                    "material_id": "bad_prompt",
                    "description": "！！！",
                    "base_color": [0.5, 0.5, 0.5],
                    "roughness": 0.5,
                    "metalness": 0.1,
                    "resolution": [8, 6],
                    "noise": {"seed": 1, "octaves": [{"frequency": 1.0, "amplitude": 1.0}]},
                }
            ],
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    result = run_cli("generate", "--workspace", str(tmp_path), "--recipes", str(bad_recipe))
    assert result.returncode == 2
    error_payload = json.loads(result.stderr)
    assert error_payload["error"]["code"] == "E_EMPTY_PROMPT"
    assert error_payload["error"]["material_id"] == "bad_prompt"


def test_cli_stop_and_resume(tmp_path: Path) -> None:
    recipes = json.loads((SAMPLES / "recipes.json").read_text(encoding="utf-8"))[:2]
    recipe_path = tmp_path / "recipes.json"
    recipe_path.write_text(json.dumps(recipes, ensure_ascii=False), encoding="utf-8")
    generation_arguments = [
        "--workspace",
        str(tmp_path),
        "--recipes",
        str(recipe_path),
        "--cards",
        str(SAMPLES / "color_cards.json"),
        "--config",
        str(SAMPLES / "config.json"),
        "--output",
        str(tmp_path / "out"),
    ]
    stopped = run_cli("generate", *generation_arguments, "--stop-after", "1")
    assert stopped.returncode == 0, stopped.stderr
    assert json.loads(stopped.stdout)["status"] == "stopped"
    status = run_cli("status", "--workspace", str(tmp_path))
    assert json.loads(status.stdout)["stop_requested"] is True

    resumed = run_cli("resume", *generation_arguments)
    assert resumed.returncode == 0, resumed.stderr
    assert json.loads(resumed.stdout)["status"] == "complete"
    assert (tmp_path / "out" / "report.json").exists()

