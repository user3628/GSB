from __future__ import annotations

import numpy as np
import pytest

from spmm_ops.blocks import BlockScheme
from spmm_ops.csr import CSRMatrix
from spmm_ops.engine import spmm
from spmm_ops.errors import SpMMError
from spmm_ops.memory import plan_execution_schedule


def test_shape_invalid() -> None:
    with pytest.raises(SpMMError) as caught:
        CSRMatrix.from_arrays([0], [], np.asarray([], dtype=np.float64), (0, 3))
    assert caught.value.code == "E_SHAPE_INVALID"


def test_bad_indptr_has_position() -> None:
    with pytest.raises(SpMMError) as caught:
        CSRMatrix.from_arrays([0, 2, 1, 2], [0, 1], np.asarray([1.0, 2.0]), (3, 3))
    assert caught.value.code == "E_BAD_CSR"
    assert caught.value.location == {"array": "indptr", "position": 2, "row": 1}


def test_out_of_bounds_index_has_position() -> None:
    with pytest.raises(SpMMError) as caught:
        CSRMatrix.from_arrays([0, 2], [0, 7], np.asarray([1.0, 2.0]), (1, 3))
    assert caught.value.code == "E_BAD_CSR"
    assert caught.value.location is not None
    assert caught.value.location["position"] == 1
    assert caught.value.location["row"] == 0


def test_duplicate_index_tolerance_is_enforced() -> None:
    matrix = CSRMatrix.from_arrays([0, 2], [1, 1], np.asarray([1.0, 2.0]), (1, 4))
    assert matrix.validate_duplicates(1)["passed"]
    with pytest.raises(SpMMError) as caught:
        matrix.validate_duplicates(0)
    assert caught.value.code == "E_DUP_INDEX"
    assert caught.value.details["violation"]["row"] == 0
    assert caught.value.details["violation"]["column_index"] == 1
    assert caught.value.details["violation"]["nnz_positions"] == [0, 1]


def test_dense_shape_mismatch() -> None:
    matrix = CSRMatrix.from_arrays([0, 1], [0], np.asarray([1.0]), (1, 3))
    dense = np.ones((2, 2), dtype=np.float64)
    with pytest.raises(SpMMError) as caught:
        spmm(matrix, dense, audit=False)
    assert caught.value.code == "E_SHAPE_INVALID"
    assert caught.value.details["csr_shape"] == [1, 3]


def test_memory_budget_reports_breakdown() -> None:
    matrix = CSRMatrix.from_arrays(
        np.asarray([0, 2, 4], dtype=np.int64),
        np.asarray([0, 1, 0, 1], dtype=np.int64),
        np.asarray([1.0, 2.0, 3.0, 4.0], dtype=np.float64),
        (2, 2),
    )
    with pytest.raises(SpMMError) as caught:
        plan_execution_schedule(
            matrix,
            64,
            BlockScheme(2, 2),
            sum_strategy="compensated",
            memory_limit_bytes=100,
        )
    assert caught.value.code == "E_MEMORY_BUDGET"
    assert caught.value.details["shortfall_bytes"] > 0
    assert "formula" in caught.value.details
    assert caught.value.details["suggested_minimum_limit_bytes"] > 100

