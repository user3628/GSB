from __future__ import annotations

import copy
from pathlib import Path

import pytest
import yaml

from headworkbench.canonical import canonical_bytes
from headworkbench.config import current_config_path, install_config, load_config
from headworkbench.data import load_dataset
from headworkbench.demo import create_mismatch_fixture
from headworkbench.errors import WorkbenchError
from headworkbench.pipeline import Pipeline, compare_runs


def configured_pipeline(root: Path) -> Pipeline:
    return Pipeline(root, load_config(current_config_path(root)))


def write_modified_config(root: Path, name: str, **changes: object) -> Path:
    config_path = current_config_path(root)
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    payload.update(changes)
    target = root / "config" / name
    target.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    install_config(root, target)
    return target


def test_generated_shapes_heads_and_windows(workspace: Path) -> None:
    dataset = load_dataset(workspace)
    config = load_config(current_config_path(workspace))
    assert len(dataset.manifest["shards"]) == 3
    assert len(dataset.heads) == 6
    assert [head.head_id for head in dataset.heads] == list(range(6))
    assert config.sampling_windows["compact"] == 12
    assert config.sampling_windows["full"] == 64
    for head in dataset.heads:
        assert head.activation.shape == (64, 16, 8)
        assert head.query_proj.shape == (8, 8)
        assert head.key_proj.shape == (8, 8)


def test_window_expansion_incremental_equals_full_from_scratch(workspace: Path) -> None:
    pipeline = configured_pipeline(workspace)
    compact = pipeline.run("compact", mode="incremental")
    assert compact["recomputed_stages"] == ["reference", "probe", "adjudication", "allocation"]
    assert compact["cache_hits"] == []

    compact_full = pipeline.run("compact", mode="full")
    assert compare_runs(compact, compact_full)["match"] is True

    expanded = pipeline.run("full", mode="incremental")
    assert expanded["recomputed_stages"] == ["probe", "adjudication", "allocation"]
    assert expanded["cache_hits"] == ["reference"]
    fresh_full = pipeline.run("full", mode="full")
    comparison = compare_runs(expanded, fresh_full)
    assert comparison["match"] is True
    assert canonical_bytes(expanded["payload"]) == canonical_bytes(fresh_full["payload"])

    # Deterministic repeat yields identical bytes and digests.
    repeat = configured_pipeline(workspace).run("full", mode="incremental")
    assert repeat["result_digest"] == expanded["result_digest"]
    assert canonical_bytes(repeat["payload"]) == canonical_bytes(expanded["payload"])


def test_adjudication_bounds_intervals_and_ranking(workspace: Path) -> None:
    result = configured_pipeline(workspace).run("compact", mode="incremental")
    adjudication = result["payload"]["adjudication"]
    probe = result["payload"]["probe"]
    assert len(adjudication["heads_by_id"]) == 6
    assert [item["rank"] for item in adjudication["ranking"]] == [1, 2, 3, 4, 5, 6]
    scores = [item["score"] for item in adjudication["ranking"]]
    assert scores == sorted(scores, reverse=True)
    assert len(adjudication["sampling_insufficient_head_ids"]) == 0
    for head in probe["heads"]:
        assert head["sample_interval"] == [0, 11]
        for width in head["widths"].values():
            assert len(width["sample_errors"]) == 12
            assert width["upper_bound"] == max(item["relative_error"] for item in width["sample_errors"])
    allocation = result["payload"]["allocation"]
    assert allocation["total_error_estimate"] <= allocation["global_error_budget"]
    assert allocation["budget_within_limit"] is True
    assert len(allocation["heads"]) == 6
    assert allocation["downgraded_head_ids"]
    assert 0 in allocation["budget_limited_head_ids"]
    assert all(item["decision"] for item in allocation["diagnostics"])


def test_threshold_change_starts_at_adjudication(workspace: Path) -> None:
    base_pipeline = configured_pipeline(workspace)
    base = base_pipeline.run("compact", mode="incremental")
    write_modified_config(
        workspace,
        "threshold-v2.yaml",
        version="threshold-v2",
        sensitivity_threshold=0.35,
    )
    changed_config = load_config(current_config_path(workspace))
    changed_pipeline = Pipeline(workspace, changed_config)
    changed = changed_pipeline.run("compact", mode="incremental")
    assert changed["cache_hits"] == ["reference", "probe"]
    assert changed["recomputed_stages"] == ["adjudication", "allocation"]
    fresh = changed_pipeline.run("compact", mode="full")
    assert compare_runs(changed, fresh)["match"] is True
    assert changed["result_digest"] != base["result_digest"]


def test_bitwidth_change_starts_at_probe(workspace: Path) -> None:
    pipeline = configured_pipeline(workspace)
    pipeline.run("compact", mode="incremental")
    write_modified_config(
        workspace,
        "widths-v2.yaml",
        version="widths-v2",
        candidate_bitwidths=[6, 8, 10, 12, 14, 16],
    )
    changed_pipeline = Pipeline(workspace, load_config(current_config_path(workspace)))
    changed = changed_pipeline.run("compact", mode="incremental")
    assert changed["cache_hits"] == ["reference"]
    assert changed["recomputed_stages"] == ["probe", "adjudication", "allocation"]
    assert compare_runs(changed, changed_pipeline.run("compact", mode="full"))["match"] is True


def test_insufficient_samples_keep_placeholders_and_force_widest(workspace: Path) -> None:
    pipeline = configured_pipeline(workspace)
    pipeline.run("compact", mode="incremental")
    write_modified_config(
        workspace,
        "insufficient-v2.yaml",
        version="insufficient-v2",
        min_samples_per_head=20,
    )
    changed_pipeline = Pipeline(workspace, load_config(current_config_path(workspace)))
    result = changed_pipeline.run("compact", mode="incremental")
    adjudication = result["payload"]["adjudication"]
    assert adjudication["sampling_insufficient_head_ids"] == [0, 1, 2, 3, 4, 5]
    assert len(adjudication["heads_by_id"]) == 6
    assert all(not item["sampling_sufficient"] for item in adjudication["heads_by_id"])
    allocation = result["payload"]["allocation"]
    assert allocation["suggested_head_widths"] == {str(index): 14 for index in range(6)}
    assert all(item["decision"] == "insufficient_samples_forced_widest" for item in allocation["heads"])
    assert allocation["total_error_estimate"] <= allocation["global_error_budget"]


def test_head_dimension_mismatch_injection(workspace: Path) -> None:
    manifest, error = create_mismatch_fixture(workspace)
    assert manifest.is_file()
    assert error["error"]["code"] == "E_HEAD_MISMATCH"
    assert error["error"]["head_id"] == 0
    assert error["error"]["activation_head_dim"] == 8
    assert error["error"]["projection_shape"] == [2, 7, 8]
    with pytest.raises(WorkbenchError) as raised:
        load_dataset(workspace, manifest)
    assert raised.value.code == "E_HEAD_MISMATCH"


def test_incremental_mismatch_error_reports_heads_and_first_position(workspace: Path) -> None:
    result = configured_pipeline(workspace).run("compact", mode="incremental")
    mutated = copy.deepcopy(result)
    mutated["payload"]["adjudication"]["heads_by_id"][2]["score"] += 0.125
    mutated["payload"]["adjudication"]["ranking"][2]["score"] += 0.125
    with pytest.raises(WorkbenchError) as raised:
        compare_runs(mutated, result)
    error = raised.value
    assert error.code == "E_INCREMENTAL_MISMATCH"
    assert 2 in error.details["diff_head_ids"]
    assert error.details["first_divergence"].startswith("$/adjudication")

