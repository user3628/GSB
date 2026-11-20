from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest

from spmm_ops.blocks import BlockScheme
from spmm_ops.csr import load_csr_npz, load_dense_npy
from spmm_ops.engine import spmm
from spmm_ops.errors import SpMMError
from spmm_ops.numeric import audit_result, load_high_precision_reference


def test_high_precision_bounds_and_sampling(sample_dir: Path) -> None:
    for strategy, order in (("sequential", "ascending"), ("compensated", "block")):
        result = spmm(
            sample_dir / "square_40pct.npz",
            sample_dir / "square_40pct.dense.npy",
            sum_strategy=strategy,
            order=order,
            block_scheme=(24, 48),
            memory_limit_bytes=4 * 1024 * 1024,
            sampling_count=20,
            self_check_blocks=2,
        )
        report = result.audit_report
        assert report is not None and report["passed"]
        assert report["sampling"]["actual_count"] == 20
        assert report["bound_model"]["n_semantics"].startswith("includes duplicates")
        for block in report["blocks"]:
            assert block["bound_holds"]
            if block["theoretical_abs_error_bound"] is not None:
                assert block["observed_max_abs_error"] <= block["allowable_error"] + 1e-15
        assert all(item["abs_error_from_decimal_reference"] is not None for item in report["sampling"]["positions"])


def test_order_sensitive_sequence_error_is_within_bound(sample_dir: Path) -> None:
    result = spmm(
        sample_dir / "degenerate_struct.npz",
        sample_dir / "degenerate_struct.dense.npy",
        sum_strategy="sequential",
        order="block",
        block_scheme=(16, 24),
        memory_limit_bytes=4 * 1024 * 1024,
        sampling_count=16,
        self_check_blocks=3,
    )
    assert result.audit_report is not None
    assert any(block["observed_max_abs_error"] == 1.0 for block in result.audit_report["blocks"])
    assert all(block["bound_holds"] for block in result.audit_report["blocks"])


def test_nonfinite_sources_are_recorded_for_compensated_kernel(sample_dir: Path) -> None:
    result = spmm(
        sample_dir / "anomaly_nan_inf.npz",
        sample_dir / "anomaly_nan_inf.dense.npy",
        sum_strategy="compensated",
        order="ascending",
        memory_limit_bytes=1024 * 1024,
        sampling_count=12,
        self_check_blocks=2,
    )
    assert result.audit_report is not None
    assert result.kernel_diagnostics["nonfinite_source_count"] > 0
    assert result.kernel_diagnostics["nonfinite_rows"]
    reasons = {
        reason
        for source in result.kernel_diagnostics["nonfinite_sources"]
        for reason in source["reasons"]
    }
    assert "A_NONFINITE" in reasons or "B_NONFINITE" in reasons
    for source in result.kernel_diagnostics["nonfinite_sources"]:
        assert "row" in source
        assert "nnz_position" in source
        assert "column_index" in source


def test_self_check_tolerance_violation_returns_numeric_mismatch(sample_dir: Path) -> None:
    matrix = load_csr_npz(sample_dir / "square_10pct.npz")
    dense = load_dense_npy(sample_dir / "square_10pct.dense.npy")
    reference = load_high_precision_reference(
        sample_dir / "square_10pct.ref.npz", expected_shape=(matrix.rows, dense.shape[1])
    )
    corrupted = reference.values.astype(np.float64, copy=True)
    corrupted[0, 0] += 1.0
    scheme = BlockScheme(24, 24, "user")
    with pytest.raises(SpMMError) as caught:
        audit_result(
            matrix,
            dense,
            corrupted,
            scheme,
            sum_strategy="sequential",
            order="ascending",
            reference=reference,
            sampling_count=4,
            self_check_blocks=4,
            absolute_tolerance=0.0,
            relative_tolerance=0.0,
        )
    assert caught.value.code == "E_NUMERIC_MISMATCH"
    assert caught.value.details["kind"] == "self_check_tolerance"
    assert caught.value.location is not None

