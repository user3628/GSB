from __future__ import annotations

from qworkbench.allocation import allocate_and_sample
from qworkbench.config import AllocationConfig, ConsistencyConfig
from qworkbench.consistency import spearman_rank_correlation, validate_consistency
from qworkbench.dedup import deduplicate
from qworkbench.scoring import score_records
from qworkbench.util import canonical_json


def _scores(config, dataset):
    normalized = deduplicate(dataset.samples, config.dedup)
    return score_records(normalized["records"], config)


def _allocation(config, dataset):
    scored = _scores(config, dataset)
    return allocate_and_sample(scored["records"], config.allocation)


def test_allocation_max_remainder_and_actual_counts(config, dataset) -> None:
    result = _allocation(config, dataset)
    assert result["summary"]["actual_sampled"] == min(
        config.allocation.target_size, result["summary"]["retained_samples"]
    )
    assert sum(layer["allocated"] for layer in result["allocation"]["layers"]) == result[
        "summary"
    ]["effective_target"]
    assert all(layer["actual_sampled"] == layer["allocated"] for layer in result["allocation"]["layers"])
    assert len({record["id"] for record in result["records"]}) == len(result["records"])


def test_allocation_tie_break_is_lexicographic() -> None:
    records = [
        {
            "id": "b-1",
            "source": "b",
            "lang": "en",
            "quality_score": 0.5,
            "whitespace_output_tokens": 10,
        },
        {
            "id": "a-1",
            "source": "a",
            "lang": "en",
            "quality_score": 0.5,
            "whitespace_output_tokens": 10,
        },
    ]
    config = AllocationConfig(
        target_size=1,
        source_weights={"a": 0.5, "b": 0.5},
        lang_weights={"en": 1.0},
        default_weight=0.0,
    )
    result = allocate_and_sample(records, config)
    assert result["summary"]["actual_sampled"] == 1
    assert [record["source"] for record in result["records"]] == ["a"]


def test_allocation_is_byte_deterministic(config, dataset) -> None:
    first = _allocation(config, dataset)
    second = _allocation(config, dataset)
    assert canonical_json(first) == canonical_json(second)


def test_spearman_known_values() -> None:
    assert spearman_rank_correlation([1, 2, 3, 4], [10, 20, 30, 40])["value"] == 1.0
    assert spearman_rank_correlation([1, 2, 3, 4], [4, 3, 2, 1])["value"] == -1.0
    tied = spearman_rank_correlation([1, 1, 2, 3], [1, 1, 3, 2])
    assert tied["value"] is not None


def test_consistency_flags_source_without_dropping(config, dataset) -> None:
    allocation = _allocation(config, dataset)
    result = validate_consistency(
        allocation["records"],
        dataset.annotations,
        config.consistency,
        config.allocation.source_weights,
    )
    assert len(result["records"]) == len(allocation["records"])
    assert result["summary"]["annotation_coverage"] == 1.0
    assert result["summary"]["review_required"] is True
    assert "legacy-import" in result["summary"]["review_sources"]
    review = result["summary"]["source_review"]["legacy-import"]
    assert review["weight_status"] == "PENDING_REVIEW"
    assert result["report"]["quality_buckets"]
    assert result["report"]["outliers"] or review["mean_abs_delta"] > config.consistency.source_bias_threshold


def test_out_of_range_annotation_is_reported_not_dropped(config, dataset) -> None:
    allocation = _allocation(config, dataset)
    annotations = dict(dataset.annotations)
    first_id = allocation["records"][0]["id"]
    annotations[first_id] = 99.0
    result = validate_consistency(
        allocation["records"],
        annotations,
        ConsistencyConfig(**{**config.consistency.__dict__, "source_bias_threshold": 1.0}),
        config.allocation.source_weights,
    )
    assert len(result["records"]) == len(allocation["records"])
    assert result["summary"]["invalid_annotations"] == 1
    assert any(item["id"] == first_id for item in result["report"]["invalid_annotations"])
