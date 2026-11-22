import pytest

from legal_cite.errors import E_CITATION_UNPARSED, LegalCiteError
from legal_cite.normalize import (cn_to_int, normalize_text, parse_citation,
                                  parse_locator)


def _rec(cid, raw, fragment, **kw):
    start = raw.index(fragment)
    base = {
        "citation_id": cid, "contract_id": "C", "lang": "zh",
        "channel": "official_filing", "raw_text": raw,
        "citation_text": fragment,
        "citation_span": [start, start + len(fragment)],
        "elements": {},
    }
    base.update(kw)
    return base


def test_cn_numerals():
    assert cn_to_int("三") == 3
    assert cn_to_int("十二") == 12
    assert cn_to_int("二十") == 20
    assert cn_to_int("二十三") == 23


def test_full_width_and_whitespace_offset_mapping():
    raw = "  第　３．２　条  "  # full-width digits/spaces
    normalized, index_map = normalize_text(raw)
    assert "3.2" in normalized
    # every normalized position maps back into the raw string
    assert all(0 <= i < len(raw) for i in index_map)


def test_zh_and_en_locators_share_canonical_form():
    zh = parse_citation(_rec("Z", "根据第三条第二款执行", "第三条第二款", lang="zh"))
    en = parse_citation(_rec("E", "Pursuant to Section 3.2 hereof",
                             "Section 3.2", lang="en"))
    assert zh["location"]["locator"] == en["location"]["locator"] == "A3.S2"
    assert zh["location"]["pattern"] == "cn_tiao_kuan"
    assert en["location"]["pattern"] == "en_section_dotted"
    # raw offsets point at the matched fragment inside the original text
    s, e = zh["location"]["raw_span"]
    assert zh["raw_text"][s:e]


def test_article_only_locator():
    rec = parse_citation(_rec("A", "依据 Article 8 之约定", "Article 8", lang="en"))
    assert rec["location"] == {**rec["location"], "section": 0}
    assert rec["location"]["locator"] == "A8.S0"


def test_unparsed_no_pattern_keeps_id_and_offset():
    rec = _rec("X", "没有任何条款引用", "没有任何条款引用")
    with pytest.raises(LegalCiteError) as ei:
        parse_citation(rec)
    assert ei.value.code == E_CITATION_UNPARSED
    assert ei.value.details["citation_id"] == "X"


def test_unparsed_fragment_mismatch():
    rec = _rec("Y", "根据第三条第一款执行", "第三条第一款")
    rec["citation_text"] = "篡改片段"
    with pytest.raises(LegalCiteError) as ei:
        parse_citation(rec)
    assert ei.value.code == E_CITATION_UNPARSED
    assert ei.value.details["offset"] == rec["citation_span"]


def test_negative_locator_rejected():
    rec = _rec("N", "Pursuant to Section -3.2 hereof", "Section -3.2", lang="en")
    with pytest.raises(LegalCiteError) as ei:
        parse_citation(rec)
    assert ei.value.code == E_CITATION_UNPARSED
