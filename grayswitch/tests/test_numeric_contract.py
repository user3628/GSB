"""数值契约：容差比较、采样边界、float64 对照、运算轨迹。"""

from app.fixedpoint import Fixed
from app.numeric_contract import (
    SamplingPrng,
    float64_comparison,
    relative_tolerance_compare,
)


def test_tolerance_band_inclusive_boundary():
    limit = Fixed.from_str("0.20")
    tol = Fixed.from_str("0.000001")
    # 恰好等于上限：未越界
    r0 = relative_tolerance_compare(Fixed.from_str("0.20"), limit, tol)
    assert r0["exceeded"] is False
    # 容差带内（含阈值本身）：未越界
    inside = Fixed.from_str("0.2000001")  # 0.2 + 0.5 ulp 级别的小越界
    r1 = relative_tolerance_compare(inside, limit, tol)
    assert r1["exceeded"] is False
    assert r1["boundary"].startswith("inclusive")
    # 明显超出：越界
    r2 = relative_tolerance_compare(Fixed.from_str("0.21"), limit, tol)
    assert r2["exceeded"] is True


def test_tolerance_outputs_fixed_decimals_and_context():
    r = relative_tolerance_compare(
        Fixed.from_str("0.30"), Fixed.from_str("0.20"),
        Fixed.from_str("0.000001"),
    )
    for key in ("usage", "limit", "relative_tolerance", "tolerance_band",
                "threshold_inclusive", "signed_gap_vs_threshold"):
        assert len(r[key].split(".")[1]) == 9
    assert r["comparison"] == "integer comparison of fixed-point numerators"


def test_sampling_deterministic_and_left_closed_right_open():
    prng = SamplingPrng("seed-x")
    d1 = prng.decision(Fixed.from_str("0.5"), 7)
    d2 = prng.decision(Fixed.from_str("0.5"), 7)
    assert d1 == d2  # 同种子同序号逐位一致
    assert d1["interval"] == "left-closed right-open [0, rate)"
    # rate=0 全不采，rate=1 全采
    assert prng.decision(Fixed.zero(), 3)["mirrored"] is False
    assert prng.decision(Fixed.one(), 3)["mirrored"] is True


def test_sampling_distinct_seed_changes_quantile():
    a = SamplingPrng("a").unit_fixed(1)
    b = SamplingPrng("b").unit_fixed(1)
    assert a != b


def test_float64_naive_loses_small_terms():
    sample = [10**18, 10**18, 10**18, 1, 1, 1, 1, 1]
    report = float64_comparison(sample)
    paths = {p["path"]: p for p in report["paths"]}
    # 精确真值与定点 Kahan 完全一致
    assert paths["exact_bigint"]["sum"] == paths["fixed_point_kahan"]["sum"]
    assert paths["fixed_point_kahan"]["abs_error"] == "0"
    # float64 朴素求和把 5 个 1 全部吞掉
    assert paths["float64_naive_order"]["abs_error"] != "0"
    assert "float64_naive_declared_bound" in report


def test_float64_order_can_flip_result():
    # 10 个 1 在朴素顺序下被 10^16 吞掉；从小到大排序后低位被保留，结论翻转。
    sample = [10**16] + [1] * 10
    report = float64_comparison(sample)
    assert report["order_flips_result"] is True
    paths = {p["path"]: p for p in report["paths"]}
    assert paths["float64_naive_order"]["sum"] != paths["float64_sorted_order"]["sum"]
