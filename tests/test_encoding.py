import os

import pytest

from pkq.encoding import (decode_column, encode_column, read_block, write_block,
                          estimate_decoded_bytes)
from pkq.errors import BlockCorrupt


def test_fixed_roundtrip_with_nulls():
    vals = [1, None, 3, -7, None, 42]
    bitmap, payload = encode_column(vals, "int64", "fixed")
    out = decode_column("int64", "fixed", len(vals), bitmap, payload)
    assert out == vals


def test_rle_roundtrip_deterministic():
    vals = [5, 5, 5, None, None, 9, 9, 1]
    bitmap, payload = encode_column(vals, "int64", "rle")
    assert decode_column("int64", "rle", len(vals), bitmap, payload) == vals
    assert decode_column("int64", "rle", len(vals), bitmap, payload) == vals  # 逐位一致


def test_dict_roundtrip_str_and_int():
    for dtype, vals in [("str", ["a", "bb", "a", None, "ccc", "bb"]),
                        ("int64", [7, 42, 7, None, 99, 42])]:
        bitmap, payload = encode_column(vals, dtype, "dict")
        assert decode_column(dtype, "dict", len(vals), bitmap, payload) == vals


def test_block_header_validation(tmp_path):
    pks = [1, 2, 3]
    cols = {"pk": (pks, "int64", "fixed"), "v": ([10, 20, 30], "int64", "rle")}
    path = tmp_path / "b0.bin"
    write_block(path, 0, pks, cols, decode_cost=10)
    blk = read_block(str(path), expected_block_id=0)
    assert blk.pk_min == 1 and blk.pk_max == 3 and blk.row_count == 3
    assert blk.decode_cost == 10
    # 块号不符
    with pytest.raises(BlockCorrupt):
        read_block(str(path), expected_block_id=1)
    # 篡改 payload -> E_BLOCK_CORRUPT 且指明块号
    raw = bytearray(open(path, "rb").read())
    raw[-1] ^= 0xFF
    open(path, "wb").write(bytes(raw))
    with pytest.raises(BlockCorrupt) as ei:
        read_block(str(path), expected_block_id=0)
    assert ei.value.block_id == 0


def test_estimate_monotonic():
    assert estimate_decoded_bytes(10, 3) > estimate_decoded_bytes(1, 3) > 0
