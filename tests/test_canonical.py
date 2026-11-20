from __future__ import annotations

import pytest

from workbench.canonical import build_cache_key, normalize_path, normalize_query
from workbench.errors import WorkbenchError


def test_path_normalization_collapses_slashes_and_dot_segments() -> None:
    assert normalize_path("//assets///./images/../app.js") == "/assets/app.js"
    assert normalize_path("/a/b/.") == "/a/b/"
    assert normalize_path("/../../safe") == "/safe"


def test_path_percent_encoding_is_canonicalized() -> None:
    assert normalize_path("/assets/%7euser/%2f/%41") == "/assets/~user/%2F/A"
    assert normalize_path("/café") == "/caf%C3%A9"


def test_query_is_sorted_and_percent_normalized() -> None:
    assert normalize_query("b=2&a=%7e+space&a=1") == "a=1&a=~%20space&b=2"
    assert normalize_query("?x=%2f&x=%2F") == "x=%2F&x=%2F"


def test_cache_key_merges_logical_url_forms_but_separates_vary() -> None:
    first = build_cache_key(
        "//assets/./app.js",
        "v=1&lang=zh-CN",
        {"Accept-Language": "zh-CN"},
        ["accept-language"],
    )
    second = build_cache_key(
        "/assets/app.js",
        "lang=zh-CN&v=1",
        {"accept-language": "zh-CN"},
        ["Accept-Language"],
    )
    third = build_cache_key(
        "/assets/app.js",
        "lang=zh-CN&v=1",
        {"Accept-Language": "en-US"},
        ["Accept-Language"],
    )
    assert first.digest == second.digest
    assert first.canonical == second.canonical
    assert first.digest != third.digest


@pytest.mark.parametrize("path", ["/bad/%ZZ", "/bad/%2", "/with space", "/back\\slash", "relative"])
def test_invalid_paths_raise_key_invalid(path: str) -> None:
    with pytest.raises(WorkbenchError) as caught:
        normalize_path(path)
    assert caught.value.code == "E_KEY_INVALID"


def test_invalid_query_raises_key_invalid() -> None:
    with pytest.raises(WorkbenchError) as caught:
        normalize_query("a=%GG")
    assert caught.value.code == "E_KEY_INVALID"

