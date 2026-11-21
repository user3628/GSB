from __future__ import annotations

from decimal import Decimal

from factmerge.parser import parse_claim
from factmerge.units import normalize_claim
from factmerge.util import dec, quantize


def _normalize(config, claim_id, category, raw_value, unit, lang="zh"):
    parsed = parse_claim(
        {
            "claim_id": claim_id,
            "entity_id": "E-1",
            "lang": lang,
            "category": category,
            "raw_value": raw_value,
            "unit": unit,
        },
        config,
    )
    return normalize_claim(parsed, config)


def test_population_factor_and_recomputation_diagnostics(config):
    normalized = _normalize(config, "P-1", "population", "1.38亿", "人")
    assert normalized["status"] == "ok"
    conversion = normalized["conversion"]
    assert conversion["factor"] == "100000000"
    assert conversion["factor_precision"] == 0
    recomputed = quantize(
        dec(conversion["coefficient"]) * dec(conversion["factor"]),
        conversion["quantum"],
    )
    assert recomputed == dec(conversion["result"]) == Decimal(138_000_000)
    assert conversion["result"] == "138000000"


def test_square_mile_conversion_is_exact_and_recomputable(config):
    normalized = _normalize(config, "A-1", "area", "5,000", "sq mi", "en")
    conversion = normalized["conversion"]
    assert conversion["factor"] == "2.589988110336"
    assert conversion["factor_precision"] == 12
    assert conversion["precision_digits"] == 12
    expected = quantize(Decimal("5000") * Decimal("2.589988110336"), "0.000000000001")
    assert Decimal(conversion["result"]) == expected
    assert normalized["canonical_value"]["value"] == "12949.94055168"


def test_missing_unknown_negative_and_reversed_are_reviewed_not_dropped(config):
    missing = _normalize(config, "M-1", "population", "2,500", None)
    assert missing["status"] == "review"
    assert any(issue["code"] == "E_UNIT_MISSING" for issue in missing["review_issues"])
    assert missing["unit"]["unit_id"] is None

    unknown = _normalize(config, "M-2", "population", "1,800", "hectare-ish", "en")
    assert unknown["status"] == "review"
    assert any(issue["code"] == "E_UNIT_UNKNOWN" for issue in unknown["review_issues"])

    negative = _normalize(config, "M-3", "population", "-2.5万", "人")
    assert negative["status"] == "review"
    assert any(issue["code"] == "E_NEGATIVE_VALUE" for issue in negative["review_issues"])
    assert Decimal(negative["canonical_value"]["value"]) < 0

    reversed_interval = _normalize(config, "M-4", "tenure", "2023-11 to 2019-03", "year-month interval", "en")
    assert reversed_interval["status"] == "review"
    assert any(issue["code"] == "E_INTERVAL_REVERSED" for issue in reversed_interval["review_issues"])


def test_month_ordinals_and_original_closure_semantics(config):
    half_open = _normalize(config, "T-1", "tenure", "[2019-03, 2023-11)", "year-month interval", "en")
    assert half_open["status"] == "ok"
    value = half_open["canonical_value"]
    assert value["start"]["ordinal"] == 2019 * 12 + 2
    assert value["end"]["ordinal"] == 2023 * 12 + 10
    assert value["start_closed"] is True
    assert value["end_closed"] is False
    assert value["value"] == "2019-03..2023-11[closed,open]"

    open_end = _normalize(config, "T-2", "tenure", "2019年3月至今", "年月区间")
    assert open_end["status"] == "ok"
    assert open_end["canonical_value"]["end"] is None
    assert open_end["canonical_value"]["end_closed"] is False

    broken_closure = _normalize(config, "T-3", "tenure", "[2019-03, 2023-11", "year-month interval", "en")
    assert broken_closure["status"] == "review"
    assert any(issue["code"] == "E_INTERVAL_OPEN_CONFLICT" for issue in broken_closure["review_issues"])

    compact_range = _normalize(config, "T-4", "tenure", "2019-03-2023-11", "year-month interval", "en")
    assert compact_range["status"] == "ok"
    assert compact_range["canonical_value"]["value"] == "2019-03..2023-11[closed,closed]"
