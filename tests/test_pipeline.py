from __future__ import annotations

import json
import shutil
from pathlib import Path

from matgen.config import load_config, normalize_config
from matgen.pipeline import clear_stop, generate_all, get_status

from conftest import SAMPLES, tree_digest, write_recipes


def run_sample(workspace: Path, config, *, stop_after: int | None = None):
    return generate_all(
        workspace=workspace,
        recipes_path=SAMPLES / "recipes.json",
        cards_path=SAMPLES / "color_cards.json",
        config=config,
        output_root="out",
        stop_after_materials=stop_after,
    )


def test_complete_sample_covers_required_material_variation(tmp_path: Path) -> None:
    config = load_config(SAMPLES / "config.json")
    result = run_sample(tmp_path, config)
    assert result["status"] == "complete"
    assert len(result["completed_material_ids"]) == 4
    assert result["run_digest"]
    status = get_status(tmp_path)
    assert status["stop_requested"] is False
    assert list(status["materials"]) == sorted(status["materials"])
    for material_id, material_state in status["materials"].items():
        assert [stage["stage"] for stage in material_state["stages"]] == [
            "control",
            "noise",
            "palette",
            "diagnostics",
        ]
        assert [stage["sequence"] for stage in material_state["stages"]] == [1, 2, 3, 4]
        assert [stage["stage_revision"] for stage in material_state["stages"]] == [1, 1, 1, 1]
        assert (tmp_path / "out" / material_id / "texture.png").exists()
        assert (tmp_path / "out" / material_id / "texture.rgba8").exists()
    recipes = json.loads((SAMPLES / "recipes.json").read_text(encoding="utf-8"))
    assert len({len(recipe["noise"]["octaves"]) for recipe in recipes}) >= 3
    assert len({tuple(recipe["resolution"]) for recipe in recipes}) == 4
    assert any("palette_card" in recipe for recipe in recipes)
    assert any("palette_card" not in recipe for recipe in recipes)
    report = json.loads((tmp_path / "out" / "report.json").read_text(encoding="utf-8"))
    assert [item["material_id"] for item in report["error_summary"]] == sorted(
        item["material_id"] for item in report["error_summary"]
    )


def test_rerun_and_resume_are_byte_identical(tmp_path: Path) -> None:
    config = load_config(SAMPLES / "config.json")
    full = run_sample(tmp_path / "full", config)
    full_digest = tree_digest(tmp_path / "full" / "out")
    rerun = run_sample(tmp_path / "full", config)
    assert rerun["run_digest"] == full["run_digest"]
    assert tree_digest(tmp_path / "full" / "out") == full_digest

    stopped = run_sample(tmp_path / "resumed", config, stop_after=2)
    assert stopped["status"] == "stopped"
    assert stopped["completed_material_ids"]
    clear_stop(tmp_path / "resumed")
    resumed = run_sample(tmp_path / "resumed", config)
    assert resumed["status"] == "complete"
    assert resumed["run_digest"] == full["run_digest"]
    assert tree_digest(tmp_path / "resumed" / "out") == full_digest


def test_single_and_multithreaded_runs_have_same_digest(tmp_path: Path) -> None:
    single = normalize_config({"sample_count": 6, "max_threads": 1})
    multi = normalize_config({"sample_count": 6, "max_threads": 4})
    single_result = run_sample(tmp_path / "single", single)
    multi_result = run_sample(tmp_path / "multi", multi)
    assert single_result["run_digest"] == multi_result["run_digest"]
    for material_id in single_result["completed_material_ids"]:
        assert (
            tmp_path / "single" / "out" / material_id / "texture.rgba8"
        ).read_bytes() == (tmp_path / "multi" / "out" / material_id / "texture.rgba8").read_bytes()


def test_incremental_invalidation_starts_at_earliest_affected_stage(tmp_path: Path) -> None:
    base = normalize_config({"sample_count": 6})
    recipes = json.loads((SAMPLES / "recipes.json").read_text(encoding="utf-8"))
    recipe_path = write_recipes(tmp_path / "recipes.json", [recipes[0]])
    first = generate_all(
        workspace=tmp_path,
        recipes_path=recipe_path,
        cards_path=SAMPLES / "color_cards.json",
        config=base,
        output_root="out",
    )
    assert first["status"] == "complete"
    material_id = recipes[0]["material_id"]
    before = {
        stage["stage"]: stage["global_sequence"]
        for stage in get_status(tmp_path)["materials"][material_id]["stages"]
    }
    budget_changed = normalize_config({"sample_count": 6, "octave_error_budget": 1e-4})
    generate_all(
        workspace=tmp_path,
        recipes_path=recipe_path,
        cards_path=SAMPLES / "color_cards.json",
        config=budget_changed,
        output_root="out",
    )
    after_budget = {
        stage["stage"]: stage["global_sequence"]
        for stage in get_status(tmp_path)["materials"][material_id]["stages"]
    }
    assert after_budget["control"] == before["control"]
    assert after_budget["noise"] == before["noise"]
    assert after_budget["palette"] == before["palette"]
    assert after_budget["diagnostics"] > before["diagnostics"]
    assert get_status(tmp_path)["materials"][material_id]["stages"][3]["stage_revision"] == 2

    recipes[0]["base_color"] = [0.21, 0.32, 0.43]
    write_recipes(recipe_path, [recipes[0]])
    generate_all(
        workspace=tmp_path,
        recipes_path=recipe_path,
        cards_path=SAMPLES / "color_cards.json",
        config=budget_changed,
        output_root="out",
    )
    after_color = {
        stage["stage"]: stage["global_sequence"]
        for stage in get_status(tmp_path)["materials"][material_id]["stages"]
    }
    assert after_color["control"] == after_budget["control"]
    assert after_color["noise"] == after_budget["noise"]
    assert after_color["palette"] > after_budget["palette"]
    assert after_color["diagnostics"] > after_budget["diagnostics"]
    assert get_status(tmp_path)["materials"][material_id]["stages"][2]["stage_revision"] == 2


def test_workspace_relocation_does_not_change_run_digest(tmp_path: Path) -> None:
    recipes = json.loads((SAMPLES / "recipes.json").read_text(encoding="utf-8"))[:1]
    config = normalize_config({"sample_count": 5})
    results = []
    for name in ("first", "second"):
        workspace = tmp_path / name
        local_samples = workspace / "samples"
        local_samples.mkdir(parents=True)
        write_recipes(local_samples / "recipes.json", recipes)
        shutil.copy2(SAMPLES / "color_cards.json", local_samples / "color_cards.json")
        shutil.copy2(SAMPLES / "color_cards.png", local_samples / "color_cards.png")
        result = generate_all(
            workspace=workspace,
            recipes_path=local_samples / "recipes.json",
            cards_path=local_samples / "color_cards.json",
            config=config,
            output_root="out",
        )
        results.append(result)
    assert results[0]["run_digest"] == results[1]["run_digest"]
    assert tree_digest(tmp_path / "first" / "out") == tree_digest(tmp_path / "second" / "out")

