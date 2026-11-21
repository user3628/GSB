from __future__ import annotations

import copy

from factmerge import FactMergeEngine


def _run(config, rows, expected, state_dir):
    return FactMergeEngine(config, rows, state_dir=state_dir, expected=expected).run()


def test_stage3_config_change_invalidates_only_stage3_and_downstream(config, rows, expected, tmp_path):
    base = _run(config, rows, expected, tmp_path / "state")
    changed = copy.deepcopy(config)
    changed["language_weights"]["en"] = "0.51"
    result = _run(changed, rows, expected, tmp_path / "state")
    status = result["version_status"]
    assert status["earliest_affected_stage"] == 3
    assert status["invalidations"]["invalidated_stages"] == [3, 4]
    assert status["last_run"]["reuse_counts"]["stage1"] == 7
    assert status["last_run"]["reuse_counts"]["stage2"] == 7
    assert status["last_run"]["reuse_counts"]["stage3"] == 0
    assert status["last_run"]["reuse_counts"]["stage4"] == 0
    assert status["stage_fingerprints"]["stage1"] == base["version_status"]["stage_fingerprints"]["stage1"]
    assert status["stage_fingerprints"]["stage2"] == base["version_status"]["stage_fingerprints"]["stage2"]
    assert status["stage_fingerprints"]["stage3"] != base["version_status"]["stage_fingerprints"]["stage3"]
    assert set(status["stage_content_digests"]) == {"stage1", "stage2", "stage3", "stage4"}


def test_stage2_unit_change_reuses_stage1(config, rows, expected, tmp_path):
    _run(config, rows, expected, tmp_path / "state")
    changed = copy.deepcopy(config)
    changed["unit_dictionary"]["population"]["units"][0]["aliases"].append("居民数")
    result = _run(changed, rows, expected, tmp_path / "state")
    status = result["version_status"]
    assert status["earliest_affected_stage"] == 2
    assert status["last_run"]["reuse_counts"]["stage1"] == 7
    assert status["last_run"]["reuse_counts"]["stage2"] == 0
    assert "UNIT_OR_QUANTIZATION_CONFIG_CHANGED" in status["invalidations"]["reason_codes"]


def test_stage4_schema_change_reuses_first_three_stages(config, rows, expected, tmp_path):
    _run(config, rows, expected, tmp_path / "state")
    changed = copy.deepcopy(config)
    changed["evidence"]["schema_version"] = "2.1"
    result = _run(changed, rows, expected, tmp_path / "state")
    status = result["version_status"]
    assert status["earliest_affected_stage"] == 4
    assert status["last_run"]["reuse_counts"]["stage1"] == 7
    assert status["last_run"]["reuse_counts"]["stage2"] == 7
    assert status["last_run"]["reuse_counts"]["stage3"] == 7
    assert status["last_run"]["reuse_counts"]["stage4"] == 0


def test_corpus_change_starts_from_stage1(config, rows, expected, tmp_path):
    _run(config, rows, expected, tmp_path / "state")
    changed_rows = copy.deepcopy(rows)
    for row in changed_rows:
        if row["claim_id"] == "C-POP-ATLAS-ZH":
            row["sentence"] = row["sentence"] + "（更新）"
            break
    result = _run(config, changed_rows, expected, tmp_path / "state")
    status = result["version_status"]
    assert status["earliest_affected_stage"] == 1
    assert status["invalidations"]["invalidated_stages"] == [1, 2, 3, 4]
    assert status["last_run"]["reuse_counts"]["stage1"] == 0
    assert "CORPUS_CONTENT_CHANGED" in status["invalidations"]["reason_codes"]
