from __future__ import annotations

import json

import numpy as np
import pytest

from prep_pipeline.errors import PipelineError
from prep_pipeline.service import PipelineService

from tests.helpers import batch_request


def test_same_version_is_bitwise_reproducible(bootstrapped_service: PipelineService) -> None:
    first = bootstrapped_service.submit_batch(batch_request("repro-a"))
    second = bootstrapped_service.submit_batch(batch_request("repro-b"))

    first_stages = {stage["stage"]: stage for stage in first["stages"]}
    second_stages = {stage["stage"]: stage for stage in second["stages"]}
    assert {name: stage["input_digest"] for name, stage in first_stages.items()} == {
        name: stage["input_digest"] for name, stage in second_stages.items()
    }
    assert {name: stage["output_digest"] for name, stage in first_stages.items()} == {
        name: stage["output_digest"] for name, stage in second_stages.items()
    }
    assert [shard["manifest_sha256"] for shard in first["shards"]] == [
        shard["manifest_sha256"] for shard in second["shards"]
    ]

    settings = bootstrapped_service.settings
    for shard in first["shards"]:
        for other in second["shards"]:
            if shard["shard_id"] != other["shard_id"]:
                continue
            for filename in (
                "features.npy",
                "timestamps.npy",
                "text_lengths.npy",
                "label_ids.npy",
                "shard_ids.npy",
                "null_mask.npy",
                "manifest.json",
            ):
                left = settings.artifact_dir / shard["relative_path"] / filename
                right = settings.artifact_dir / other["relative_path"] / filename
                assert left.read_bytes() == right.read_bytes()

    parse_stats = first_stages["parse_clean"]["stats"]
    assert parse_stats["raw_lines"] == 21
    assert parse_stats["records_cleaned"] == 20
    assert parse_stats["duplicate_records_removed"] == 1
    assert first_stages["filter"]["stats"]["output_records"] == 15
    assert first["summary"]["samples_total"] == 12
    assert sum(shard["sample_count"] for shard in first["shards"]) == 12
    shard_sample_ids = [
        sample_id
        for shard_id, sample_ids in first_stages["shard"]["details"]["shard_sample_ids"].items()
        for sample_id in sample_ids
    ]
    assert len(shard_sample_ids) == len(set(shard_sample_ids)) == 15


def test_tensor_manifest_dtypes_mask_and_sample_count(
    bootstrapped_service: PipelineService,
) -> None:
    batch = bootstrapped_service.submit_batch(batch_request("tensor-contract"))
    settings = bootstrapped_service.settings
    for shard in batch["shards"]:
        root = settings.artifact_dir / shard["relative_path"]
        manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
        assert manifest["sample_count"] == shard["sample_count"]
        assert manifest["dtypes"] == {
            "features": "float32",
            "timestamps": "int64",
            "text_lengths": "int64",
            "label_ids": "int64",
            "shard_ids": "int64",
            "null_mask": "uint8",
        }
        features = np.load(root / "features.npy", allow_pickle=False)
        mask = np.load(root / "null_mask.npy", allow_pickle=False)
        timestamps = np.load(root / "timestamps.npy", allow_pickle=False)
        assert features.dtype == np.float32
        assert features.shape == (shard["sample_count"], 2)
        assert mask.dtype == np.uint8
        assert mask.shape == (shard["sample_count"], 2)
        assert int(mask.sum()) == manifest["null_mask"]["null_count"] == 0
        assert timestamps.dtype == np.int64
        assert timestamps.shape == (shard["sample_count"],)


def test_invalid_line_rejects_whole_batch(bootstrapped_service: PipelineService) -> None:
    with pytest.raises(PipelineError) as exc:
        bootstrapped_service.submit_batch(
            batch_request("mixed-invalid", input_path="records_mixed.tsv")
        )
    assert exc.value.code == "E_RECORD_INVALID"
    row = bootstrapped_service.db.get_batch("mixed-invalid")
    assert row["status"] == "failed"
    assert row["error_code"] == "E_RECORD_INVALID"
    assert bootstrapped_service.db.get_shard_rows("mixed-invalid") == []
    with pytest.raises(PipelineError) as hidden:
        bootstrapped_service.get_batch("mixed-invalid")
    assert hidden.value.code == "E_BATCH_NOT_COMMITTED"


def test_conflicting_duplicate_rejects_batch(bootstrapped_service: PipelineService) -> None:
    with pytest.raises(PipelineError) as exc:
        bootstrapped_service.submit_batch(
            batch_request("raw-conflict", input_path="records_conflict.tsv")
        )
    assert exc.value.code == "E_CONFLICTING_RECORD"
    assert exc.value.context["sample_id"] == "dup001"


def test_skew_warning_does_not_change_shard_membership(
    bootstrapped_service: PipelineService,
) -> None:
    batch = bootstrapped_service.submit_batch(
        batch_request("skew-warning", policy_version=2)
    )
    shard_stage = next(stage for stage in batch["stages"] if stage["stage"] == "shard")
    warning_codes = {warning["code"] for warning in shard_stage["stats"]["warnings"]}
    assert warning_codes == {"W_SHARD_SAMPLE_SKEW", "W_SHARD_BYTE_SKEW"}
    assert all(
        warning["effect"] == "warning_only; assignments unchanged"
        for warning in batch["summary"]["warnings"]
    )
    ids = [
        sample_id
        for sample_ids in shard_stage["details"]["shard_sample_ids"].values()
        for sample_id in sample_ids
    ]
    assert len(ids) == len(set(ids)) == 15
    assert sum(
        shard["sample_count"] for shard in batch["shards"]
    ) == batch["summary"]["samples_total"]
