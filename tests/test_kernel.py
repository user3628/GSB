from __future__ import annotations

from pathlib import Path

import numpy as np

from spmm_ops.blocks import BlockScheme
from spmm_ops.csr import CSRMatrix, load_csr_npz, load_dense_npy
from spmm_ops.kernel import execute_spmm, naive_spmm
from spmm_ops.memory import plan_execution_schedule


def _sample(sample_dir: Path, name: str) -> tuple[CSRMatrix, np.ndarray]:
    return load_csr_npz(sample_dir / f"{name}.npz"), load_dense_npy(sample_dir / f"{name}.dense.npy")


def test_ascending_kernel_matches_naive_and_is_byte_deterministic(sample_dir: Path) -> None:
    matrix, dense = _sample(sample_dir, "degenerate_struct")
    scheme = BlockScheme(16, 24, "user")
    schedule = plan_execution_schedule(
        matrix, dense.shape[1], scheme, sum_strategy="sequential", memory_limit_bytes=4 * 1024 * 1024
    )
    first = execute_spmm(matrix, dense, schedule, sum_strategy="sequential", order="ascending")
    second = execute_spmm(matrix, dense, schedule, sum_strategy="sequential", order="ascending")
    naive = naive_spmm(matrix, dense, sum_strategy="sequential", order="ascending", col_block=24)
    assert first.output.tobytes() == second.output.tobytes()
    assert np.array_equal(first.output, naive)
    assert first.executed_terms == matrix.nnz


def test_block_kernel_matches_naive_without_memory_split(sample_dir: Path) -> None:
    matrix, dense = _sample(sample_dir, "degenerate_struct")
    scheme = BlockScheme(24, 40, "user")
    schedule = plan_execution_schedule(
        matrix, dense.shape[1], scheme, sum_strategy="compensated", memory_limit_bytes=4 * 1024 * 1024
    )
    result = execute_spmm(matrix, dense, schedule, sum_strategy="compensated", order="block")
    naive = naive_spmm(matrix, dense, sum_strategy="compensated", order="block", col_block=40)
    assert np.array_equal(result.output, naive)


def test_column_order_and_block_order_have_explicit_distinct_semantics() -> None:
    matrix = CSRMatrix.from_arrays(
        np.asarray([0, 3], dtype=np.int64),
        np.asarray([3, 2, 0], dtype=np.int64),
        np.asarray([1.0e16, 1.0, -1.0e16], dtype=np.float64),
        (1, 4),
    )
    dense = np.ones((4, 1), dtype=np.float64)
    scheme = BlockScheme(1, 2, "user")
    schedule = plan_execution_schedule(
        matrix, 1, scheme, sum_strategy="sequential", memory_limit_bytes=1024 * 1024
    )
    ascending = execute_spmm(matrix, dense, schedule, sum_strategy="sequential", order="ascending")
    block = execute_spmm(matrix, dense, schedule, sum_strategy="sequential", order="block")
    assert ascending.output[0, 0] == 0.0
    assert block.output[0, 0] == 1.0
    assert ascending.output.tobytes() != block.output.tobytes()


def test_duplicate_positions_and_explicit_zeros_are_visited() -> None:
    matrix = CSRMatrix.from_arrays(
        np.asarray([0, 3], dtype=np.int64),
        np.asarray([0, 0, 2], dtype=np.int64),
        np.asarray([1.0, 2.0, -0.0], dtype=np.float64),
        (1, 3),
    )
    dense = np.asarray([[0.5, 1.0], [3.0, 4.0], [5.0, 6.0]], dtype=np.float64)
    scheme = BlockScheme(1, 2, "user")
    schedule = plan_execution_schedule(
        matrix, 2, scheme, sum_strategy="sequential", memory_limit_bytes=1024 * 1024
    )
    result = execute_spmm(matrix, dense, schedule, sum_strategy="sequential", order="ascending")
    expected = 1.0 * dense[0] + 2.0 * dense[0] + (-0.0) * dense[2]
    assert np.array_equal(result.output[0], expected)
    assert result.executed_terms == 3


def test_float32_stays_in_float32_domain() -> None:
    matrix = CSRMatrix.from_arrays(
        np.asarray([0, 2], dtype=np.int64),
        np.asarray([0, 1], dtype=np.int64),
        np.asarray([1.25, -0.5], dtype=np.float32),
        (1, 2),
    )
    dense = np.asarray([[2.0], [4.0]], dtype=np.float32)
    scheme = BlockScheme(1, 2)
    schedule = plan_execution_schedule(
        matrix, 1, scheme, sum_strategy="compensated", memory_limit_bytes=1024 * 1024
    )
    result = execute_spmm(matrix, dense, schedule, sum_strategy="compensated", order="ascending")
    assert result.output.dtype == np.dtype("float32")
    assert result.output[0, 0] == np.float32(0.5)

