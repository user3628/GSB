from __future__ import annotations

import pytest

from inverted_index.errors import BusinessError
from inverted_index.paths import split_field_path
from inverted_index.text import extract_postings, normalize_term, tokenize


def test_token_normalization_is_fixed_and_casefolded() -> None:
    assert tokenize("  Hello--WORLD_alpha.beta  ") == ["hello", "world", "alpha", "beta"]
    assert normalize_term("HELLO!") == "hello"
    with pytest.raises(BusinessError) as exc:
        normalize_term("two words")
    assert exc.value.code == "E_BAD_TERM"


def test_invalid_paths_have_bad_path_code() -> None:
    for path in ["", ".title", "title.", "a..b", "a.b[]", "a b", "1field"]:
        with pytest.raises(BusinessError) as exc:
            split_field_path(path)
        assert exc.value.code == "E_BAD_PATH"


def test_array_positions_are_gapped_and_phrases_do_not_cross_elements() -> None:
    document = {
        "tags": ["red", "blue"],
        "body": "red blue",
        "sections": [
            {"paragraphs": ["one two", "three"]},
            {"paragraphs": ["four"]},
        ],
    }
    postings = extract_postings(document, ("tags", "body", "sections.paragraphs"))
    assert postings[("tags", "red")] == (0,)
    assert postings[("tags", "blue")] == (2,)
    assert postings[("body", "red")] == (0,)
    assert postings[("body", "blue")] == (1,)
    assert postings[("sections.paragraphs", "one")] == (0,)
    assert postings[("sections.paragraphs", "two")] == (1,)
    assert postings[("sections.paragraphs", "three")] == (3,)
    assert postings[("sections.paragraphs", "four")] == (6,)


def test_same_document_key_is_one_complete_posting() -> None:
    postings = extract_postings({"body": "Red red RED"}, ("body",))
    assert postings[("body", "red")] == (0, 1, 2)
