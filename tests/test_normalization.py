from __future__ import annotations

import pytest

from qiu.errors import QueryError
from qiu.normalization import normalize_and_tokenize


def test_nfkc_whitespace_case_and_offsets() -> None:
    result = normalize_and_tokenize("  ＡＢＣ　北京!!! ", query_id="q1", max_chars=160)
    assert result["normalized_query"] == "abc 北京!!!"
    assert [token["text"] for token in result["tokens"]] == ["abc", "北", "京"]
    assert [token["kind"] for token in result["tokens"]] == ["latin_alnum", "chinese", "chinese"]
    for token in result["tokens"]:
        assert result["normalized_query"][token["start"] : token["end"]] == token["text"]
    assert result["offset_basis"] == "normalized_query"


def test_punctuation_only_is_empty_query_with_query_id_and_stage() -> None:
    with pytest.raises(QueryError) as caught:
        normalize_and_tokenize(" ！？。 ", query_id="empty-q", max_chars=160)
    error = caught.value
    assert error.code == "E_EMPTY_QUERY"
    assert error.stage == "normalization"
    assert error.query_id == "empty-q"


def test_query_too_long_is_checked_before_tokenization() -> None:
    with pytest.raises(QueryError) as caught:
        normalize_and_tokenize("a" * 11, query_id="long-q", max_chars=10)
    assert caught.value.code == "E_QUERY_TOO_LONG"
    assert caught.value.details == {"actual_chars": 11, "max_query_chars": 10}
