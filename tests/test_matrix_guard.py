from __future__ import annotations

from dataclasses import replace

import pytest

from app.codec import reconstruct_shard
from app.errors import BusinessError
from app.layout import parse_layout


def test_rebuild_rejects_out_of_declared_matrix_range(sample_layout) -> None:
    layout = parse_layout(sample_layout)
    obj = layout.objects["obj-alpha"]
    invalid_layout = replace(layout, max_k_plus_m=obj.k + obj.m - 1)
    with pytest.raises(BusinessError) as caught:
        reconstruct_shard(
            layout=invalid_layout,
            obj=obj,
            target_shard=2,
            source_contents={index: b"x" for index in range(obj.k)},
        )
    assert caught.value.code == "E_MATRIX_INVALID"
