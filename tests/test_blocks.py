from __future__ import annotations

from pathlib import Path

import pytest

from spmm_ops.blocks import select_block_scheme
from spmm_ops.csr import load_csr_npz, load_dense_npy
from spmm_ops.errors import SpMMError


def test_selection_is_reproducible_and_explains_score(sample_dir: Path) -> None:
    matrix = load_csr_npz(sample_dir / "square_40pct.npz")
    dense = load_dense_npy(sample_dir / "square_40pct.dense.npy")
    first = select_block_scheme(matrix, dense.shape[1], cache_budget_bytes=64 * 1024, sum_strategy="compensated")
    second = select_block_scheme(matrix, dense.shape[1], cache_budget_bytes=64 * 1024, sum_strategy="compensated")
    assert first.scheme == second.scheme
    assert first.to_dict()["candidates"] == second.to_dict()["candidates"]
    assert first.candidates
    assert first.chosen.work_set_bytes <= first.cache_budget_bytes
    assert 0.0 <= first.chosen.score < 1.0e6
    assert "predicted_traffic" in first.reason
    assert "empty_row_block_ratio" in first.reason


def test_cache_budget_changes_selected_plan(sample_dir: Path) -> None:
    matrix = load_csr_npz(sample_dir / "square_40pct.npz")
    dense = load_dense_npy(sample_dir / "square_40pct.dense.npy")
    tight = select_block_scheme(matrix, dense.shape[1], cache_budget_bytes=64 * 1024)
    loose = select_block_scheme(matrix, dense.shape[1], cache_budget_bytes=512 * 1024)
    assert tight.scheme != loose.scheme
    assert tight.chosen.work_set_bytes <= 64 * 1024
    assert loose.chosen.work_set_bytes <= 512 * 1024


def test_high_empty_ratio_avoids_empty_spin_plan(sample_dir: Path) -> None:
    matrix = load_csr_npz(sample_dir / "square_1pct.npz")
    dense = load_dense_npy(sample_dir / "square_1pct.dense.npy")
    plan = select_block_scheme(matrix, dense.shape[1], cache_budget_bytes=64 * 1024)
    assert matrix.features()["empty_row_ratio"] > 0.30
    assert plan.chosen.empty_row_block_ratio == 0.0
    assert plan.scheme.row_block == matrix.rows


def test_impossible_cache_budget_returns_memory_budget(sample_dir: Path) -> None:
    matrix = load_csr_npz(sample_dir / "square_10pct.npz")
    dense = load_dense_npy(sample_dir / "square_10pct.dense.npy")
    with pytest.raises(SpMMError) as caught:
        select_block_scheme(matrix, dense.shape[1], cache_budget_bytes=1)
    assert caught.value.code == "E_MEMORY_BUDGET"
    assert caught.value.details["cache_budget_bytes"] == 1
    assert caught.value.details["minimum_candidate_work_set_bytes"] > 1

