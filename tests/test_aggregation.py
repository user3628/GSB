from __future__ import annotations

from eventbus.aggregation import NeumaierSum, combine_aggregates, ordered_aggregates


def test_compensated_sum_handles_mixed_magnitudes():
    acc = NeumaierSum()
    for value in (1e16, 1.0, -1e16):
        acc.add(value)
    assert acc.value == 1.0
    assert acc.compensation == 1.0


def test_batch_boundaries_do_not_change_grouped_result():
    rows = [
        ("alpha", 1e12),
        ("beta", 0.1),
        ("alpha", 1e-6),
        ("alpha", -1e12),
        ("beta", None),
        ("gamma", 3.25),
    ]
    one_batch = ordered_aggregates(rows)
    split = combine_aggregates([rows[:2], rows[2:4], rows[4:]])
    split_reordered_batches = combine_aggregates([[rows[5], rows[2]], rows[:2], rows[3:5]])
    assert one_batch == split == split_reordered_batches
    assert [item["key"] for item in one_batch] == ["alpha", "beta", "gamma"]
    assert {item["key"]: item["count"] for item in one_batch} == {
        "alpha": 3,
        "beta": 2,
        "gamma": 1,
    }
    assert {item["key"]: item["sum"] for item in one_batch}["alpha"] == 0.000001
    assert {item["key"]: item["sum"] for item in one_batch}["beta"] == 0.1
