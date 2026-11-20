from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest

from gemm_lab.blocking import BlockPlan
from gemm_lab.cache import ResultCache, make_cache_key
from gemm_lab.errors import BusinessCode, GemmError


PLAN = BlockPlan(mr=4, nr=8, mc=32, kc=16, nc=32)


def key(input_digest: str = "input-a") -> str:
    return make_cache_key(
        shape_fingerprint="shape-fingerprint",
        plan=PLAN,
        sum_strategy="pairwise",
        input_digest=input_digest,
    )


def publish(cache: ResultCache, cache_key: str, *, expected_seq: int | None = None, config_seq: int = 1):
    result = np.arange(12, dtype=np.float64).reshape(3, 4)
    return cache.publish(
        key=cache_key,
        result=result,
        key_components={"all": "required-components"},
        config_seq=config_seq,
        computation_metadata={"test": True},
        expected_seq=expected_seq,
    )


def test_cache_hit_and_version_mismatch_invalidation(tmp_path: Path):
    cache = ResultCache(tmp_path / "cache")
    cache_key = key()
    published = publish(cache, cache_key)
    hit = cache.lookup(cache_key, current_config_seq=1)
    assert hit.status == "hit"
    assert hit.array is not None and hit.array.shape == (3, 4)
    invalid = cache.lookup(cache_key, current_config_seq=2)
    assert invalid.status == "invalidated"
    assert invalid.reason == "config_version_mismatch"
    assert cache.status()["entry_count"] == 0


def test_duplicate_and_stale_publish(tmp_path: Path):
    cache = ResultCache(tmp_path / "cache")
    cache_key = key()
    publish(cache, cache_key)
    with pytest.raises(GemmError) as duplicate:
        publish(cache, cache_key)
    assert duplicate.value.code == BusinessCode.E_STALE_PUBLISH
    with pytest.raises(GemmError) as stale:
        publish(cache, key("input-b"), expected_seq=0)
    assert stale.value.code == BusinessCode.E_STALE_PUBLISH


def test_result_digest_tamper_invalidates(tmp_path: Path):
    cache = ResultCache(tmp_path / "cache")
    cache_key = key()
    publish(cache, cache_key)
    array_path = cache.entries_dir / f"{cache_key}.npy"
    array_path.write_bytes(b"tampered")
    lookup = cache.lookup(cache_key, current_config_seq=1)
    assert lookup.status == "invalidated"
    assert lookup.reason == "result_digest_mismatch"


def test_cache_key_covers_required_dimensions():
    base = key("a")
    assert base == key("a")
    assert base != key("b")
    other_plan = make_cache_key(
        shape_fingerprint="shape-fingerprint",
        plan=BlockPlan(8, 16, 64, 32, 64),
        sum_strategy="pairwise",
        input_digest="a",
    )
    assert base != other_plan
    other_strategy = make_cache_key(
        shape_fingerprint="shape-fingerprint",
        plan=PLAN,
        sum_strategy="compensated",
        input_digest="a",
    )
    assert base != other_strategy
    other_accumulation = make_cache_key(
        shape_fingerprint="shape-fingerprint",
        plan=PLAN,
        sum_strategy="pairwise",
        input_digest="a",
        accumulation_mode="panel",
    )
    assert base != other_accumulation

