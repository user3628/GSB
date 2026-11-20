from __future__ import annotations

from xrel_workbench.normalize import extract_tokens, normalize_pair, normalize_text
from xrel_workbench.util import canonical_bytes


def test_nfkc_whitespace_and_offsets() -> None:
    text = normalize_text("  ＩＮＦＯＲＭＡＴＩＯＮ   检索  ")
    assert text == "information 检索"
    tokens = extract_tokens(text)
    by_term = {token["term"]: token for token in tokens}
    assert by_term["information"]["start"] == 0
    assert by_term["information"]["end"] == 11
    assert by_term["检索"]["start"] == 12
    assert by_term["检索"]["lang"] == "zh"


def test_chinese_single_and_bigram_tokens() -> None:
    tokens = extract_tokens("跨语言检索")
    terms = [token["term"] for token in tokens]
    assert terms == ["跨", "跨语", "语", "语言", "言", "言检", "检", "检索", "索"]
    assert all(token["lang"] == "zh" for token in tokens)


def test_token_ids_are_deterministic_and_offset_bearing() -> None:
    pair = {
        "pair_id": "p-test",
        "query": "BM25 风控模型",
        "doc": "Risk Control BM25",
        "lang_query": "zh",
        "lang_doc": "en",
        "source": "unit",
        "human_relevance": 2,
    }
    config = {
        "normalization": {
            "unicode_form": "NFKC",
            "collapse_whitespace": True,
            "cjk_ngram_sizes": [1, 2],
        }
    }
    first = normalize_pair(pair, config, shard="part-001")
    second = normalize_pair(pair, config, shard="part-001")
    assert canonical_bytes(first) == canonical_bytes(second)
    assert [token["term_id"] for token in first["query_tokens"]] == [
        f"q{index:06d}" for index in range(1, len(first["query_tokens"]) + 1)
    ]
    for token in first["query_tokens"] + first["doc_tokens"]:
        assert token["start"] < token["end"]


def test_empty_query_and_doc_diagnostics_are_retained() -> None:
    pair = {
        "pair_id": "p-empty",
        "query": "   ",
        "doc": "",
        "lang_query": "zh",
        "lang_doc": "en",
        "source": "unit",
        "human_relevance": 0,
    }
    config = {
        "normalization": {
            "unicode_form": "NFKC",
            "collapse_whitespace": True,
            "cjk_ngram_sizes": [1, 2],
        }
    }
    record = normalize_pair(pair, config, shard="part-001")
    assert record["query_tokens"] == []
    assert record["doc_tokens"] == []
    assert [item["field"] for item in record["diagnostics"]] == ["query", "doc"]
    assert all(item["code"] == "E_EMPTY_TEXT" for item in record["diagnostics"])
