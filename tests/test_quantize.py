import numpy as np

from xfpipe.quantize import error_bound, quantize


def test_roundtrip_within_quantum():
    rng = np.random.RandomState(7)
    matrix = rng.normal(0, 5, size=(40, 6))
    q = quantize(matrix)
    restored = q.to_float()
    assert np.max(np.abs(restored - matrix)) <= 0.5 / 1_000_000 + 1e-12


def test_error_bound_covers_observed_error():
    rng = np.random.RandomState(11)
    matrix = rng.normal(0, 1, size=(64, 8))
    q = quantize(matrix)
    bound = error_bound(q, matrix)
    assert bound["max_abs_error"] <= bound["total_bound"]
    assert bound["mean_abs_error"] <= bound["max_abs_error"]
    assert bound["scale_per_column"] == [1_000_000.0] * 8


def test_integer_counts_are_exact():
    counts = np.arange(24, dtype=np.float64).reshape(4, 6)
    q = quantize(counts, scale=1.0)
    assert q.to_float().tolist() == counts.tolist()
