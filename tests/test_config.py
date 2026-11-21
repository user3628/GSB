from __future__ import annotations

import pytest

from rope_diag.config import RopeConfig, earliest_affected_stage
from rope_diag.errors import RopeError


def test_config_version_is_deterministic() -> None:
    first = RopeConfig(base_frequency=12000.0, scaling_mode="linear", scaling_factor=2.0)
    second = RopeConfig.from_mapping(first.to_mapping())
    assert first.version == second.version
    assert first.with_patch({"base_frequency": 13000.0}).version != first.version


@pytest.mark.parametrize(
    "patch, field",
    [
        ({"scaling_factor": 0.0}, "scaling_factor"),
        ({"rotation_block_size": 0}, "rotation_block_size"),
        ({"rotation_block_size": 3}, "rotation_block_size"),
        ({"scaling_mode": "invalid"}, "scaling_mode"),
        ({"base_frequency": 1.0}, "base_frequency"),
        ({"memory_budget_bytes": 0}, "memory_budget_bytes"),
    ],
)
def test_invalid_config_fields(patch: dict[str, object], field: str) -> None:
    with pytest.raises(RopeError) as caught:
        RopeConfig().with_patch(patch)
    assert caught.value.code == "E_INVALID_CONFIG"
    assert caught.value.details["field"] == field


def test_unknown_config_field_is_rejected() -> None:
    with pytest.raises(RopeError) as caught:
        RopeConfig.from_mapping({**RopeConfig().to_mapping(), "unknown": 1})
    assert caught.value.code == "E_INVALID_CONFIG"
    assert caught.value.details["unknown_fields"] == ["unknown"]


@pytest.mark.parametrize(
    "changed, expected",
    [
        (["base_frequency"], "frequency_generation"),
        (["scaling_mode", "rotation_block_size"], "frequency_generation"),
        (["rotation_block_size"], "block_rotation"),
        (["causal_mask"], "scoring_normalization"),
        (["memory_budget_bytes"], None),
    ],
)
def test_earliest_affected_stage(changed: list[str], expected: str | None) -> None:
    assert earliest_affected_stage(changed) == expected
