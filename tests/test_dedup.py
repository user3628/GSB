from __future__ import annotations

from pathlib import Path

from qworkbench.config import DedupConfig
from qworkbench.dedup import deduplicate, hamming_distance, normalize_record, simhash64
from qworkbench.samples import SampleDataset
from qworkbench.util import canonical_json, normalize_text


def test_nfkc_whitespace_case_normalization() -> None:
    assert normalize_text("  Ａｐｐｌｙ\n\tＮＦＫＣ   " + "Text  ") == "apply nfkc text"


def test_simhash_bucket_and_near_duplicate(dataset: SampleDataset, config) -> None:
    by_id = {sample["id"]: sample for sample in dataset.samples}
    left = normalize_record(by_id["near-dup-a"], config.dedup.separator)
    right = normalize_record(by_id["near-dup-b"], config.dedup.separator)
    assert 0 < hamming_distance(left["simhash_uint"], right["simhash_uint"]) <= config.dedup.hamming_threshold
    assert len(left["buckets"]) == 4
    assert all(0 <= bucket < (1 << 16) for bucket in left["buckets"])
    result = deduplicate(dataset.samples, config.dedup)
    groups = {group["retained_id"]: group for group in result["groups"]}
    assert "near-dup-b" in groups["near-dup-a"]["member_ids"]
    assert "exact-dup-b" in groups["exact-dup-a"]["member_ids"]
    assert groups["exact-dup-a"]["kind"] == "exact"
    assert "near-dup-b" not in {record["id"] for record in result["records"]}


def test_dedup_is_byte_deterministic_under_input_reordering(dataset: SampleDataset, config) -> None:
    first = deduplicate(dataset.samples, config.dedup)
    second = deduplicate(list(reversed(dataset.samples)), config.dedup)
    assert canonical_json(first) == canonical_json(second)
    assert [record["id"] for record in first["records"]] == [record["id"] for record in second["records"]]


def test_simhash_empty_tokens_is_zero() -> None:
    assert simhash64([]) == 0
