"""Dedup, quality scoring and stratified sampling tests."""
import json

from sft_workbench.codec import decode_sample
from sft_workbench.dedup import DedupConfig, dedup_samples
from sft_workbench.errors import E_EMPTY_SAMPLE
from sft_workbench.quality import ScoringConfig, score_samples
from sft_workbench.sampling import (
    SamplingConfig,
    largest_remainder_allocate,
    stratified_mixture,
)
from sft_workbench.textnorm import normalize_text, simhash_features
from sft_workbench.simhash import simhash64, hamming_distance


def make_sample(sample_id, instruction, output, source="s", lang="en", quality=0.5):
    return decode_sample(
        {
            "id": sample_id,
            "version": 2,
            "instruction": instruction,
            "input": "",
            "output": {"format": {"type": "text"}, "text": output},
            "source": source,
            "lang": lang,
            "quality": {"score": quality, "lang_consistent": True},
        }
    )


def test_normalization_nfkc_case_whitespace():
    a = normalize_text("Ｈｅｌｌｏ   World\n\tFOO")
    b = normalize_text("hello world foo")
    assert a == b == "hello world foo"


def test_simhash_normalization_equivalence():
    h1 = simhash64(simhash_features(normalize_text("Ｔｒａｎｓｌａｔｅ  this")))
    h2 = simhash64(simhash_features(normalize_text("translate this")))
    assert hamming_distance(h1, h2) == 0


def test_dedup_keeps_first_and_records_distance():
    samples = [
        make_sample("a", "Translate the following text into English", "hello world output"),
        make_sample("b", "translate the following text into english", "hello world output"),
        make_sample("c", "Completely different instruction here", "another output entirely"),
    ]
    result = dedup_samples(samples, DedupConfig(hamming_threshold=3))
    assert [s.sample_id for s in result.kept] == ["a", "c"]
    assert result.removed == [{"sample_id": "b", "duplicate_of": "a", "distance": 0}]


def test_dedup_threshold_respected():
    base_out = " ".join(f"word{i}" for i in range(30))
    samples = [
        make_sample("a", "Write a Python function that sorts a list", base_out),
        make_sample("b", "Write a Python function that sorts an array", base_out),
        make_sample("c", "请用三句话总结以下文章", "完全不同的输出内容。"),
    ]
    loose = dedup_samples(samples, DedupConfig(hamming_threshold=4))
    assert [s.sample_id for s in loose.kept] == ["a", "c"]
    assert loose.removed[0]["distance"] > 0
    strict = dedup_samples(samples, DedupConfig(hamming_threshold=3))
    assert [s.sample_id for s in strict.kept] == ["a", "b", "c"]


def test_empty_sample_error():
    samples = [make_sample("empty", "   ", "  \n  ")]
    result = dedup_samples(samples, DedupConfig())
    assert result.kept == []
    assert len(result.errors) == 1
    assert result.errors[0]["code"] == E_EMPTY_SAMPLE
    assert result.errors[0]["details"]["sample_id"] == "empty"


def test_dedup_determinism_byte_identical():
    samples = [
        make_sample(f"s{i}", f"instruction number {i % 5}", f"output {i % 7}")
        for i in range(40)
    ]
    first = dedup_samples(samples, DedupConfig()).summary()
    second = dedup_samples(samples, DedupConfig()).summary()
    assert json.dumps(first, sort_keys=True) == json.dumps(second, sort_keys=True)


def test_scoring_determinism_and_range():
    samples = [
        make_sample(f"s{i}", f"explain concept number {i % 4}", f"concept explanation output {i}")
        for i in range(20)
    ]
    config = ScoringConfig(source_priors={"s": 0.9})
    first = score_samples(samples, config)
    second = score_samples(samples, config)
    assert first == second
    for record in first:
        assert 0.0 < record["score"] <= 1.0
        assert record["fallback_reason"] == ""
        for value in record["factors"].values():
            assert 0.0 < value <= 1.0


def test_scoring_zero_factor_fallback():
    samples = [make_sample("x", "some instruction", "some output")]
    config = ScoringConfig(source_priors={"s": 0.0}, floor=0.001)
    record = score_samples(samples, config)[0]
    assert record["score"] == 0.001
    assert "source" in record["fallback_reason"]


def test_scoring_all_zero_weights_fallback():
    samples = [make_sample("x", "some instruction", "some output")]
    config = ScoringConfig(weights={"length": 0.0, "overlap": 0.0, "template": 0.0, "source": 0.0})
    record = score_samples(samples, config)[0]
    assert record["score"] == config.floor
    assert "weights" in record["fallback_reason"]


def test_largest_remainder_allocation():
    weights = {"a|en": 1.0, "b|zh": 1.0, "c|en": 1.0}
    caps = {"a|en": 10, "b|zh": 10, "c|en": 10}
    alloc = largest_remainder_allocate(weights, 10, caps)
    assert sum(alloc.values()) == 10
    # 10/3 -> floors of 3 each, one leftover goes to the first key (tie-break)
    assert alloc == {"a|en": 4, "b|zh": 3, "c|en": 3}


def test_largest_remainder_caps_and_redistribution():
    weights = {"a": 3.0, "b": 1.0, "c": 1.0}
    caps = {"a": 2, "b": 10, "c": 10}
    alloc = largest_remainder_allocate(weights, 9, caps)
    assert alloc["a"] == 2  # capped
    assert sum(alloc.values()) == 9


def test_largest_remainder_tie_break_deterministic():
    weights = {"b": 1.0, "a": 1.0}
    caps = {"a": 5, "b": 5}
    first = largest_remainder_allocate(weights, 3, caps)
    second = largest_remainder_allocate(dict(reversed(list(weights.items()))), 3, caps)
    assert first == second == {"a": 2, "b": 1}


def test_stratified_mixture_determinism():
    samples = []
    for i in range(30):
        source = ["alpha", "beta"][i % 2]
        lang = ["en", "zh"][i % 3 == 0]
        samples.append(make_sample(f"s{i:02d}", f"unique instruction {i}", f"output text {i}", source, lang))
    scores = score_samples(samples, ScoringConfig())
    config = SamplingConfig(total_budget=10)
    first = stratified_mixture(samples, scores, config)
    second = stratified_mixture(samples, scores, config)
    assert json.dumps(first, sort_keys=True) == json.dumps(second, sort_keys=True)
    assert len(first["mixture"]) == 10
    assert sum(first["allocation"].values()) == 10
    for key, stat in first["strata"].items():
        assert stat["count"] > 0 and stat["token_estimate"] > 0
