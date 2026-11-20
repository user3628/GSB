from prefalign.core.model import Sample
from prefalign.core.stage_normalize import run, normalize_sample


def test_nfkc_whitespace_and_lowercase():
    sample = Sample("x1", "  Hello\u3000World  ", "Chosen\u00a0Text", "Reject", "web", "en")
    record = normalize_sample(sample)
    assert record["norm_prompt"] == "hello world"
    assert record["norm_chosen"] == "chosen text"
    assert len(record["prompt_fingerprint"]) == 16


def test_cross_shard_same_prompt_same_group(pipeline):
    pipeline.run(reason="init")
    result = pipeline.load_artifact("normalize")
    groups = {}
    for record in result["records"]:
        groups.setdefault(record["group_id"], set()).add(record["pair_id"])
    multi = {gid: ids for gid, ids in groups.items() if len(ids) >= 2}
    assert multi, "shared-prompt pairs must be grouped together"
    # Pair ordinals are deterministic and contiguous within each group.
    for group in result["groups"]:
        if group["size"] >= 2:
            members = [r for r in result["records"] if r["group_id"] == group["group_id"]]
            assert [r["pair_ordinal"] for r in members] == list(range(group["size"]))


def test_invalid_samples_keep_pair_id_in_diagnostics(pipeline):
    result = pipeline.run(reason="init")
    normalized = pipeline.load_artifact("normalize")
    codes = {d["code"] for d in normalized["diagnostics"]}
    assert "E_INVALID_SAMPLE" in codes
    assert all(d.get("pair_id") for d in normalized["diagnostics"])
    assert normalized["stats"]["invalid_samples"] >= 4


def test_deterministic_fingerprint():
    sample = Sample("y", "Repeatable prompt", "a", "b", "human", "en")
    first = normalize_sample(sample)["prompt_fingerprint"]
    second = normalize_sample(sample)["prompt_fingerprint"]
    assert first == second


def test_empty_all_three_fields():
    sample = Sample("z", "", "  ", "\t", "web", "en")
    try:
        normalize_sample(sample)
        assert False
    except Exception as exc:
        assert exc.code == "E_INVALID_SAMPLE"
        assert exc.details["pair_id"] == "z"
