from iqa.vocab import normalize_prompt, tokenize, analyze_prompt, vocab_version
from iqa import semantic
import numpy as np


def test_prompt_normalization_fullwidth_and_whitespace():
    text = "  Ｒed   红色\u3000block  "
    normalized = normalize_prompt(text)
    assert "\u3000" not in normalized
    assert "  " not in normalized
    assert "Red" in normalized


def test_tokenization_cjk_and_latin():
    tokens = tokenize("红色red block 渐变")
    assert "red" in tokens
    assert "红" in tokens
    assert "渐" in tokens
    assert all(token.isalpha() for token in tokens)


def test_unknown_fallback_counted():
    analysis = analyze_prompt("不存在的词 zzzqqq red")
    assert analysis["fallback_count"] >= 2
    assert any(item["primitive"] == "unknown" and item["fallback"] for item in analysis["tokens"])


def test_semantic_score_deterministic_and_bounded():
    pixels = np.random.default_rng(1).random((12, 12, 3))
    metadata = {"image_id": "s", "prompt": "red 红色 smooth noise 渐变"}
    first = semantic.run(pixels, metadata)
    second = semantic.run(pixels, metadata)
    assert first["fingerprint"] == second["fingerprint"]
    assert 0.0 <= first["product"]["score"] <= 1.0
    assert first["product"]["vocab_version"] == vocab_version()


def test_empty_prompt_uses_neutral_score():
    pixels = np.zeros((8, 8, 3))
    record = semantic.run(pixels, {"image_id": "e", "prompt": "123 !!!"})
    assert record["product"]["token_count"] == 0
    assert record["product"]["score"] == 0.5
