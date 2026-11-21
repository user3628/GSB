from __future__ import annotations

import copy

from factmerge import FactMergeEngine


def _claim(claim_id, entity_id, category, raw_value, unit, lang="en", source="dataset", observed_at="2024-01-01"):
    return {
        "claim_id": claim_id,
        "entity_id": entity_id,
        "lang": lang,
        "category": category,
        "raw_value": raw_value,
        "unit": unit,
        "sentence": f"{category} statement",
        "source": source,
        "observed_at": observed_at,
        "annotated_confidence": "1.0",
    }


def test_exact_cross_unit_equality_and_demoted_support(config):
    rows = [
        _claim("EQ-ZH", "E-EQ", "population", "1.38亿", "人", "zh", "gov", "2025-01-01"),
        _claim("EQ-EN", "E-EQ", "population", "138 million", "people", "en", "dataset", "2024-01-01"),
    ]
    result = FactMergeEngine(config, rows).run(use_state=False)
    entity = result["entity_results"][0]
    accepted = entity["decision"]["accepted"]
    demoted = entity["decision"]["demoted"]
    assert accepted[0]["claim_id"] == "EQ-ZH"
    assert accepted[0]["value"]["value"] == "138000000"
    assert demoted[0]["claim_id"] == "EQ-EN"
    assert demoted[0]["reason_code"] == "EQUIVALENT_SUPPORT"
    assert entity["decision"]["categories"][0]["conflict_detected"] is False


def test_conflict_confidence_uses_log_domain_and_source_freshness(config):
    rows = [
        _claim("HIGH", "E-C", "population", "140 million", "people", "en", "official_stats", "2025-01-01"),
        _claim("LOW", "E-C", "population", "135 million", "people", "en", "wiki", "2010-01-01"),
    ]
    entity = FactMergeEngine(config, rows).run(use_state=False)["entity_results"][0]
    assert entity["decision"]["accepted"][0]["claim_id"] == "HIGH"
    confidence = entity["decision"]["accepted"][0]["confidence"]
    assert confidence["log_domain"] is True
    assert "source_prior" in confidence["components"]
    assert "language_weight" in confidence["components"]
    assert "freshness" in confidence["components"]
    assert entity["decision"]["demoted"][0]["reason_code"] == "CONFLICT_LOWER_CONFIDENCE"


def test_confidence_tie_uses_claim_id_ascending(config):
    rows = [
        _claim("TIE-B", "E-T", "population", "7.05亿", "人", "zh", "dataset", "2024-01-01"),
        _claim("TIE-A", "E-T", "population", "7.10亿", "人", "zh", "dataset", "2024-01-01"),
    ]
    entity = FactMergeEngine(config, rows).run(use_state=False)["entity_results"][0]
    assert entity["decision"]["accepted"][0]["claim_id"] == "TIE-A"
    tie = entity["decision"]["categories"][0]["tie_break"]
    assert tie["required"] is True
    assert tie["deciding_rule"] == "claim_id_asc"
    assert tie["equal_prefix"] == [
        "confidence_desc",
        "source_priority_desc",
        "language_priority_desc",
        "freshness_desc",
    ]


def test_comparison_is_decimal_exact_not_float_tolerance(config):
    rows = [
        _claim("EXACT-A", "E-X", "area", "100.000000000001", "sq km"),
        _claim("EXACT-B", "E-X", "area", "100.000000000002", "sq km"),
    ]
    entity = FactMergeEngine(config, rows).run(use_state=False)["entity_results"][0]
    category = entity["decision"]["categories"][0]
    assert category["conflict_detected"] is True
    assert category["distinct_value_count"] == 2


def test_reduction_order_does_not_change_entity_evidence(config, rows):
    entity_id = "ENT-ATLAS"
    first = FactMergeEngine(config, rows).run(entity_ids=[entity_id], use_state=False)["entity_results"][0]
    shuffled = copy.deepcopy(rows)
    shuffled.reverse()
    second = FactMergeEngine(config, shuffled).run(entity_ids=[entity_id], use_state=False)["entity_results"][0]
    assert first["chain_digest"] == second["chain_digest"]
    assert first["decision"] == second["decision"]
