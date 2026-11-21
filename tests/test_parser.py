from __future__ import annotations

import pytest

from factmerge.errors import FactMergeError
from factmerge.parser import parse_chinese_number, parse_claim
from factmerge.units import normalize_claim
from factmerge.util import canonical_json, normalize_text


def test_nfkc_whitespace_offsets_and_fullwidth_digits(config):
    claim = {
        "claim_id": "U-1",
        "entity_id": "E-1",
        "lang": "zh",
        "category": "population",
        "raw_value": "　 １.４５ 亿 人 ",
        "unit": "人",
        "sentence": "全角与空白折叠",
    }
    parsed = parse_claim(claim, config)
    assert parsed["normalized_text"] == " 1.45 亿 人 "
    assert parsed["numeric"]["span"]["text"] == "1.45"
    assert parsed["numeric"]["span"]["start"] == 1
    assert parsed["numeric"]["span"]["source_start"] == 2
    assert parsed["embedded_scale"]["span"]["text"] == "亿"
    assert parsed["unit"]["span"]["field"] == "unit"
    normalized = normalize_text("１２３\t  人")
    assert normalized.normalized == "123 人"


def test_chinese_numeral_and_english_abbreviation(config):
    assert parse_chinese_number("三千五百万") == 35_000_000
    assert parse_chinese_number("一点三八亿") == 138_000_000
    assert parse_chinese_number("一九九八") == 1998

    chinese = parse_claim(
        {
            "claim_id": "N-1",
            "entity_id": "E-1",
            "lang": "zh",
            "category": "population",
            "raw_value": "三千五百万",
            "unit": "人",
        },
        config,
    )
    assert chinese["numeric"]["coefficient"] == "35000000"
    assert chinese["numeric"]["kind"] == "chinese"
    assert normalize_claim(chinese, config)["canonical_value"]["value"] == "35000000"

    english = parse_claim(
        {
            "claim_id": "N-2",
            "entity_id": "E-1",
            "lang": "en",
            "category": "population",
            "raw_value": "3.25M",
            "unit": "people",
        },
        config,
    )
    assert english["embedded_scale"]["token"] == "M"
    assert normalize_claim(english, config)["canonical_value"]["value"] == "3250000"


@pytest.mark.parametrize("raw_value", ["", "1,23.456", "1,234.56.7", "not-a-number"])
def test_parse_errors_have_claim_and_offsets_and_are_deterministic(config, raw_value):
    claim = {
        "claim_id": "BAD-1",
        "entity_id": "E-1",
        "lang": "en",
        "category": "area",
        "raw_value": raw_value,
        "unit": "sq mi",
    }
    errors = []
    for _ in range(2):
        with pytest.raises(FactMergeError) as excinfo:
            parse_claim(claim, config)
        error = excinfo.value
        assert error.code == "E_PARSE_AMBIGUOUS"
        assert error.claim_id == "BAD-1"
        assert error.offsets is not None
        assert "start" in error.offsets and "end" in error.offsets
        errors.append(canonical_json(error.as_dict()))
    assert errors[0] == errors[1]


def test_prose_fields_are_not_mistaken_for_tenure(config):
    claim = {
        "claim_id": "P-1",
        "entity_id": "E-1",
        "lang": "en",
        "category": "population",
        "raw_value": "1.2 million",
        "unit": "people",
        "sentence": "A 2019-03 archived report listed the population.",
    }
    parsed = parse_claim(claim, config)
    assert parsed["kind"] == "scalar"
    assert parsed["category"] == "population"
    assert parsed["field"] == "raw_value"


def test_strict_locale_policy_marks_single_comma_ambiguous(config):
    strict = {**config, "parse": {**config["parse"], "ambiguous_separator_policy": "strict_locale"}}
    claim = {
        "claim_id": "STRICT-1",
        "entity_id": "E-1",
        "lang": "en",
        "category": "population",
        "raw_value": "1,234",
        "unit": "people",
    }
    with pytest.raises(FactMergeError) as excinfo:
        parse_claim(claim, strict)
    assert excinfo.value.code == "E_PARSE_AMBIGUOUS"
    assert excinfo.value.offsets["text"] == "1,234"


def test_sentence_fallback_uses_sentence_offsets(config):
    claim = {
        "claim_id": "P-2",
        "entity_id": "E-1",
        "lang": "en",
        "category": "founded_year",
        "raw_value": "",
        "unit": "year",
        "sentence": "Founded in 1998.",
    }
    parsed = parse_claim(claim, config)
    assert parsed["field"] == "sentence"
    assert parsed["numeric"]["span"]["field"] == "sentence"
    assert parsed["numeric"]["span"]["text"] == "1998"


def test_embedded_unit_can_infer_category_without_separate_unit(config):
    parsed = parse_claim(
        {
            "claim_id": "INFER-1",
            "entity_id": "E-1",
            "lang": "zh",
            "raw_value": "1.38 亿人",
            "unit": None,
        },
        config,
    )
    assert parsed["category"] == "population"
    assert parsed["embedded_unit"]["text"] == "人"
    normalized = normalize_claim(parsed, config)
    assert normalized["status"] == "ok"
    assert normalized["canonical_value"]["value"] == "138000000"
