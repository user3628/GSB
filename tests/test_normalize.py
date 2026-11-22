from posalign.config import load_governance_config
from posalign.normalize import generate_candidates
from posalign.yamlmini import safe_load
from scripts.generate_samples import GOVERNANCE_YAML


def test_nfkc_whitespace_casefold_and_rule_fallback(sample_config):
    config = load_governance_config(sample_config)
    result = generate_candidates("  ＮＯＵＮ+Custom\t", config)
    assert result.normalized_old_tag == "noun+custom"
    assert [item["tag"] for item in result.candidates] == ["NOUN"]
    assert result.candidates[0]["source"] == "rule"
    codes = {item["code"] for item in result.diagnostics}
    assert "alias_missing" in codes
    assert "alias_missing_rule_fallback" in codes


def test_multiple_alias_candidates_are_never_dropped(sample_config):
    config = load_governance_config(sample_config)
    result = generate_candidates(" 名 ", config)
    assert [item["tag"] for item in result.candidates] == ["NOUN", "VERB"]
    assert all(item["source"] == "alias_table" for item in result.candidates)
    codes = {item["code"] for item in result.diagnostics}
    assert "multiple_alias_candidates" in codes
    assert "multiple_candidates" in codes


def test_missing_alias_without_rule_keeps_diagnostic(sample_config):
    config = load_governance_config(sample_config)
    result = generate_candidates("MysteryTag", config)
    assert result.candidates == ()
    codes = {item["code"] for item in result.diagnostics}
    assert {"alias_missing", "no_candidate"} <= codes


def test_minimal_yaml_fallback_parses_governance_sample():
    parsed = safe_load(GOVERNANCE_YAML)
    assert parsed["standard_tags"][0] == "NOUN"
    assert parsed["aliases"]["名"] == ["NOUN", "VERB"]
    assert parsed["adjudication"]["weights"]["frequency"] == 0.55

