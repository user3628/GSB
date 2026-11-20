import copy
import json
from pathlib import Path

import yaml

from prefalign.core.config import Config
from prefalign.core.workspace import (
    build_workspace,
    edit_sample,
    full_rebuild_digests,
    import_demo_shards,
)


def _fresh_pipeline(workspace: Path):
    cfg, store, pipe = build_workspace(workspace)
    import_demo_shards(store, workspace)
    pipe.run(reason="baseline")
    return cfg, store, pipe


def test_sample_edit_invalidates_from_normalize(workspace):
    cfg, store, pipe = _fresh_pipeline(workspace)
    samples, _ = store.effective_samples()
    target = next(s for s in samples if s.prompt)
    payload = target.to_dict()
    payload.pop("shard")
    payload["chosen"] = target.chosen + " additional chosen detail"
    before = pipe.stage_digests()

    edit_sample(store, payload)
    result = pipe.run(reason="edit")
    assert result["rebuilt"] == ["normalize", "quality", "features", "sampling"]
    status = pipe.status()
    assert status["earliest_invalid_stage"] is None
    log = status["invalidation_log"][-1]
    assert log["earliest_stage"] == "normalize"
    assert log["invalidated_chain"] == ["normalize", "quality", "features", "sampling"]
    assert pipe.stage_digests()["normalize"] != before["normalize"]


def test_quality_weight_change_reuses_normalize(workspace):
    cfg, store, pipe = _fresh_pipeline(workspace)
    before = pipe.stage_digests()
    config_path = workspace / "config" / "default.yaml"
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    raw["quality"]["weights"]["length_penalty"] = 0.55
    config_path.write_text(yaml.safe_dump(raw, sort_keys=True), encoding="utf-8")

    cfg2, store2, pipe2 = build_workspace(workspace, config_path)
    result = pipe2.run(reason="config_quality")
    assert result["reused"] == ["normalize"]
    assert result["rebuilt"] == ["quality", "features", "sampling"]
    assert pipe2.stage_digests()["normalize"] == before["normalize"]
    assert pipe2.stage_digests()["quality"] != before["quality"]


def test_fold_seed_change_invalidates_features_only(workspace):
    cfg, store, pipe = _fresh_pipeline(workspace)
    before = pipe.stage_digests()
    config_path = workspace / "config" / "default.yaml"
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    raw["folds"]["seed"] += 1
    config_path.write_text(yaml.safe_dump(raw, sort_keys=True), encoding="utf-8")

    _, _, pipe2 = build_workspace(workspace, config_path)
    result = pipe2.run(reason="config_folds")
    assert result["reused"] == ["normalize", "quality"]
    assert result["rebuilt"][0] == "features"
    assert pipe2.stage_digests()["quality"] == before["quality"]


def test_sampling_weight_change_only_rebuilds_sampling(workspace):
    cfg, store, pipe = _fresh_pipeline(workspace)
    before = pipe.stage_digests()
    config_path = workspace / "config" / "default.yaml"
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    raw["sampling"]["target_weights"]["web::en"] = 0.15
    config_path.write_text(yaml.safe_dump(raw, sort_keys=True), encoding="utf-8")

    _, _, pipe2 = build_workspace(workspace, config_path)
    result = pipe2.run(reason="config_sampling")
    assert result["reused"] == ["normalize", "quality", "features"]
    assert result["rebuilt"] == ["sampling"]
    assert pipe2.stage_digests()["features"] == before["features"]


def test_full_rebuild_matches_incremental_after_edit(workspace):
    cfg, store, pipe = _fresh_pipeline(workspace)
    samples, _ = store.effective_samples()
    target = next(s for s in samples if s.source == "rlhf" and s.prompt)
    payload = target.to_dict()
    payload.pop("shard")
    payload["rejected"] = "edited rejected response with more detail"
    edit_sample(store, payload)
    pipe.run(reason="edit")

    digests, count = full_rebuild_digests(
        workspace, workspace / "workspace" / "verify_full",
        workspace / "config" / "default.yaml")
    report = pipe.verify_incremental(digests, count)
    assert report["status"] == "matched"
    assert report["stages_covered"] == ["normalize", "quality", "features", "sampling"]
    assert report["sample_count"] == count


def test_source_shard_replacement_matches_full_rebuild(workspace):
    cfg, store, pipe = _fresh_pipeline(workspace)
    shard = workspace / "data" / "shards" / "shard_03_web.jsonl"
    records = [json.loads(line) for line in shard.read_text(
        encoding="utf-8").splitlines() if line.strip()]
    records[0]["chosen"] = "replacement upstream chosen text"
    replacement = workspace / "data" / "shards" / "shard_03_web_v2.jsonl"
    replacement.write_text(
        "\n".join(json.dumps(r, ensure_ascii=False, sort_keys=True,
                             separators=(",", ":")) for r in records) + "\n",
        encoding="utf-8")

    store.replace_source_shard("web", replacement, alias="shard_03_web_v2.jsonl")
    result = pipe.run(reason="replace_source:web")
    assert "normalize" in result["rebuilt"]

    digests, count = full_rebuild_digests(
        workspace, workspace / "workspace" / "verify_full2",
        workspace / "config" / "default.yaml")
    report = pipe.verify_incremental(digests, count)
    assert report["status"] == "matched"


def test_mismatch_detection(workspace, monkeypatch):
    cfg, store, pipe = _fresh_pipeline(workspace)
    fake_baseline = {stage: "0" * 16 for stage in
                     ["normalize", "quality", "features", "sampling"]}
    try:
        pipe.verify_incremental(fake_baseline, 10)
        assert False
    except Exception as exc:
        assert exc.code == "E_INCREMENTAL_MISMATCH"
        assert exc.details["first_diff_stage"] == "normalize"
        assert "position" in exc.details
