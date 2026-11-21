from __future__ import annotations

import pytest

from matgen.errors import E_EMPTY_PROMPT, MatgenError
from matgen.prompt import control_vector_bytes, generate_control_vector, normalize_description


def test_normalization_nfkc_whitespace_width_and_case() -> None:
    normalized = normalize_description("  ＲＯＵＧＨ\t陶土\u3000TEXTURE！ ", material_id="m1")
    assert normalized == "rough 陶土 texture!"
    result = generate_control_vector(
        "  ＲＯＵＧＨ\t陶土\u3000TEXTURE！ ",
        material_id="m1",
        hash_seed=243001,
    )
    assert [entry["token"] for entry in result["tokens"]] == ["rough", "陶土", "texture"]
    assert sum(entry["normalized_weight"] for entry in result["tokens"]) == pytest.approx(1.0)


def test_control_vector_is_byte_reproducible_and_unit_length() -> None:
    first = generate_control_vector("拉丝 Brushed 不锈钢 Steel", material_id="m2", hash_seed=99)
    second = generate_control_vector("拉丝 Brushed 不锈钢 Steel", material_id="m2", hash_seed=99)
    assert control_vector_bytes(first) == control_vector_bytes(second)
    assert first["control_vector_bytes_sha256"] == second["control_vector_bytes_sha256"]
    assert len(first["control_vector"]) == 32
    assert first["norm"] == pytest.approx(1.0, abs=2e-15)
    assert first["error_bound"] >= 0.0
    assert 0 <= first["significant_digits"] <= 15


@pytest.mark.parametrize("description", ["", "   ", "！！！？？？", "...---___"])
def test_empty_or_punctuation_prompt_returns_material_scoped_code(description: str) -> None:
    with pytest.raises(MatgenError) as captured:
        generate_control_vector(description, material_id="empty_material", hash_seed=1)
    assert captured.value.code == E_EMPTY_PROMPT
    assert captured.value.material_id == "empty_material"


def test_token_hash_seed_changes_vector() -> None:
    first = generate_control_vector("metal surface", material_id="m", hash_seed=1)
    second = generate_control_vector("metal surface", material_id="m", hash_seed=2)
    assert first["control_vector_bytes_sha256"] != second["control_vector_bytes_sha256"]

