from __future__ import annotations

from pathlib import Path

import numpy as np

from spmm_ops.engine import spmm


def test_split_execution_matches_unsplit_and_records_peak(sample_dir: Path) -> None:
    common = dict(
        block_scheme=(80, 64),
        order="block",
        sum_strategy="compensated",
        audit=False,
    )
    unsplit = spmm(
        sample_dir / "tall_40pct.npz",
        sample_dir / "tall_40pct.dense.npy",
        memory_limit_bytes=4 * 1024 * 1024,
        **common,
    )
    split = spmm(
        sample_dir / "tall_40pct.npz",
        sample_dir / "tall_40pct.dense.npy",
        memory_limit_bytes=36 * 1024,
        **common,
    )
    assert unsplit.schedule.split_count == 0
    assert split.schedule.split_count > 0
    assert split.schedule.estimated_peak_bytes <= 36 * 1024
    assert split.schedule.executed_tile_count == split.schedule.logical_tile_count + split.schedule.split_count
    assert np.allclose(split.output, unsplit.output, rtol=1e-13, atol=1e-13, equal_nan=True)
    assert split.output_digest == unsplit.output_digest

