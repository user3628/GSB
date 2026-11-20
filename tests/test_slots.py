from __future__ import annotations

import pytest

from qiu.engine import QueryEngine
from qiu.errors import QueryError


def _sample(query_id: str = "slot-q", raw_query: str = "买苹果手机") -> dict[str, object]:
    return {
        "channel": "shop",
        "history_categories": [],
        "locale": "zh-CN",
        "query_id": query_id,
        "raw_query": raw_query,
    }


def test_longest_match_overlap_resolution_and_diagnostics(engine: QueryEngine) -> None:
    result = engine.parse(_sample())
    assert [(slot["type"], slot["text"]) for slot in result["slots"]] == [("category", "苹果手机")]
    suppressed = result["slot_diagnostics"]["suppressed"]
    assert {(item["type"], item["text"]) for item in suppressed} == {
        ("brand", "苹果"),
        ("category", "手机"),
    }
    assert all(item["reason"] == "overlap" for item in suppressed)
    assert all(item["suppressed_by"]["type"] == "category" for item in suppressed)
    assert all(slot["source_dictionary_version"] == "slot-dict-v1" for slot in result["slots"])


def test_injected_overlap_candidate_is_kept_in_diagnostics(engine: QueryEngine) -> None:
    result = engine.parse(
        _sample("slot-fault", "买苹果手机"),
        {"overlap_slot": {"type": "brand", "text": "苹果手机", "start": 1, "end": 5}},
    )
    suppressed = result["slot_diagnostics"]["suppressed"]
    injected = [item for item in suppressed if item["injected"]]
    assert len(injected) == 1
    assert injected[0]["text"] == "苹果手机"
    assert injected[0]["suppressed_by"]["text"] == "苹果手机"


def test_dictionary_version_mismatch_and_missing_dictionary(engine: QueryEngine) -> None:
    with pytest.raises(QueryError) as version_error:
        engine.parse(_sample(), {"dict_version": "slot-dict-v9"})
    assert version_error.value.code == "E_DICT_VERSION"
    assert version_error.value.stage == "slots"
    assert version_error.value.details["expected_dict_version"] == "slot-dict-v1"

    with pytest.raises(QueryError) as missing_error:
        engine.parse(_sample(), {"missing_dictionary": True})
    assert missing_error.value.code == "E_DICT_MISSING"
    assert missing_error.value.stage == "slots"
