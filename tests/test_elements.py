import pytest

from legal_cite.config import default_config
from legal_cite.elements import (normalize_duration, normalize_effective_month,
                                 normalize_penalty)
from legal_cite.errors import E_ELEMENT_INVALID, LegalCiteError

CFG = default_config()


def test_penalty_basis_points_and_fixed_point():
    out = normalize_penalty({"value": "800", "unit": "bp"}, CFG, "C1")
    assert out["value"] == 8.0
    assert out["conversion"]["factor"] == 0.01


def test_penalty_thousands_and_quantization():
    out = normalize_penalty({"value": "1.234567", "unit": "percent"}, CFG, "C2")
    assert out["value"] == 1.2346  # half-up at 4 dp
    assert out["conversion"]["decimal_places"] == 4


def test_penalty_out_of_range():
    with pytest.raises(LegalCiteError) as ei:
        normalize_penalty({"value": "150", "unit": "percent"}, CFG, "C3")
    assert ei.value.code == E_ELEMENT_INVALID
    assert ei.value.details["element"] == "penalty"
    assert ei.value.details["citation_id"] == "C3"


def test_duration_day_to_month_and_inverted_interval():
    out = normalize_duration(
        {"start_months": {"value": 30, "unit": "days"},
         "end_months": {"value": 365, "unit": "days"}}, CFG, "C4")
    assert out["start_months"] == 1
    assert out["end_months"] == 12
    with pytest.raises(LegalCiteError) as ei:
        normalize_duration({"start_months": 12, "end_months": 1,
                            "unit": "month"}, CFG, "C5")
    assert ei.value.code == E_ELEMENT_INVALID
    assert ei.value.details["element"] == "duration"


def test_effective_month_validation():
    assert normalize_effective_month("2026-02", CFG, "C6")["value"] == "2026-02"
    with pytest.raises(LegalCiteError) as ei:
        normalize_effective_month("2026-13", CFG, "C7")
    assert ei.value.code == E_ELEMENT_INVALID
    assert ei.value.details["element"] == "effective_month"


def test_unknown_unit_is_invalid():
    with pytest.raises(LegalCiteError):
        normalize_penalty({"value": "1", "unit": "furlong"}, CFG, "C8")
