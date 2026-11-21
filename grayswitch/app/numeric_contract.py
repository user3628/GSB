"""误差可界定的数值契约。

本模块把“每一步怎么舍入、累计误差多少、理论上界多少”变成可持久化、可逐项
核对的数据结构，而不是停留在口头约定。包含四部分：

* :class:`OpTrace` —— 单次定点运算的轨迹（表达式、舍入方式、精确误差、误差上界）；
* :class:`TraceBook` —— 运算轨迹收集器，维护累计绝对误差；
* :func:`relative_tolerance_compare` —— 带相对容差的定点比较（灰度上限契约）；
* :func:`float64_comparison` —— 与朴素 float64 求和的混合量级对照实验；
* :class:`SamplingPrng` / :func:`mirror_decision` —— 采样率定点边界判定。
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from fractions import Fraction
from typing import Any, Dict, List, Sequence

from .fixedpoint import DECIMALS, SCALE, ULP, Fixed, kahan_sum

# 单条乘/除/定点化运算的严格误差上界：半个 ulp（half_* 舍入），截断为 1 ulp。
HALF_ULP = ULP / 2


@dataclass
class OpTrace:
    """一次定点运算的可审计轨迹。"""

    seq: int
    name: str
    expression: str
    rounding: str
    result: str  # 固定小数位字符串
    exact_value: str  # 精确有理值
    abs_error: str  # 本次运算的精确绝对误差
    error_bound: str  # 本次运算声明的误差上界
    cumulative_abs_error: str  # 截至本步的累计绝对误差

    def as_dict(self) -> Dict[str, Any]:
        return {
            "seq": self.seq,
            "name": self.name,
            "expression": self.expression,
            "rounding": self.rounding,
            "result": self.result,
            "exact_value": self.exact_value,
            "abs_error": self.abs_error,
            "error_bound": self.error_bound,
            "cumulative_abs_error": self.cumulative_abs_error,
        }


class TraceBook:
    """收集运算轨迹并维护累计精确绝对误差与宽松理论上界。"""

    def __init__(self, enabled: bool = True) -> None:
        self.enabled = enabled
        self._traces: List[OpTrace] = []
        self._cumulative = Fraction(0)
        self._bound = Fraction(0)

    @property
    def traces(self) -> List[OpTrace]:
        return self._traces

    def record(
        self,
        name: str,
        expression: str,
        rounding: str,
        result: Fixed,
        exact: Fraction,
        bound: Fraction = HALF_ULP,
    ) -> OpTrace:
        err = abs(exact - result.exact())
        self._cumulative += err
        self._bound += bound
        trace = OpTrace(
            seq=len(self._traces) + 1,
            name=name,
            expression=expression,
            rounding=rounding,
            result=result.to_str(),
            exact_value=str(exact),
            abs_error=str(err),
            error_bound=str(bound),
            cumulative_abs_error=str(self._cumulative),
        )
        if self.enabled:
            self._traces.append(trace)
        return trace

    def record_exact(
        self, name: str, expression: str, result: Fixed, exact: Fraction
    ) -> OpTrace:
        """定点间精确运算（加/减/整数倍），误差与上界均为 0。"""
        return self.record(name, expression, "exact", result, exact, Fraction(0))

    def declaration(self) -> Dict[str, Any]:
        """输出数值契约声明（固定小数位 + 分数形式并存）。"""
        return {
            "decimals": DECIMALS,
            "scale": SCALE,
            "ulp": str(ULP),
            "half_ulp": str(HALF_ULP),
            "op_count": len(self._traces),
            "cumulative_abs_error": str(self._cumulative),
            "declared_error_bound": str(self._bound),
            "bound_respected": self._cumulative <= self._bound,
            "rounding_modes_used": sorted({t.rounding for t in self._traces}),
        }


# ---------------------------------------------------------------------------
# 灰度上限：带相对容差的定点比较
# ---------------------------------------------------------------------------

def relative_tolerance_compare(
    usage: Fixed,
    limit: Fixed,
    rel_tol: Fixed,
    rounding: str = "half_even",
) -> Dict[str, Any]:
    """以定点相对容差判断 ``usage`` 是否越过 ``limit``。

    判定规则（边界包含关系显式定义）：
      * 容差带 = ``limit * rel_tol``（一次显式舍入，误差 <= 1/2 ulp）；
      * ``usage <= limit + tol`` 判定为**未越界**（边界包含，含等号）；
      * 仅当 ``usage > limit + tol`` 才判定越界；
      * 完全相等的平局天然落在容差带内（未越界），多版本平局时由调用方按稳定
        字典序打破，本函数不依赖浮点，因此结论逐位可复算。

    输出全部保留固定小数位，并附每一步的舍入说明。
    """
    tol, tol_err = limit.mul(rel_tol, rounding)
    threshold = limit.add(tol)
    exceeded = usage.n > threshold.n  # 纯整数比较，无浮点参与
    gap = usage.sub(threshold)
    return {
        "rounding": rounding,
        "usage": usage.to_str(),
        "limit": limit.to_str(),
        "relative_tolerance": rel_tol.to_str(),
        "tolerance_band": tol.to_str(),
        "tolerance_band_abs_error": str(abs(tol_err)),
        "threshold_inclusive": threshold.to_str(),
        "boundary": "inclusive (usage <= threshold => not exceeded)",
        "exceeded": exceeded,
        "signed_gap_vs_threshold": gap.to_str(),
        "comparison": "integer comparison of fixed-point numerators",
    }


# ---------------------------------------------------------------------------
# 确定性镜像采样（固定种子 + 事件序号，定点边界，包含关系显式定义）
# ---------------------------------------------------------------------------

class SamplingPrng:
    """基于 SHA-256 的确定性伪随机数（无外部随机源，可跨进程逐位复现）。

    取 ``sha256(seed || seq)`` 的前 8 字节构造 [0, 2^64) 内的整数，再映射为
    定点分位 ``u in [0, 1)``。采样判定采用**左闭右开**区间：
    ``mirror := u < rate``（rate=1 全采，rate=0 不采；边界 ``u == rate`` 不采）。
    """

    _MOD = 1 << 64

    def __init__(self, seed: str) -> None:
        self.seed = str(seed)

    def unit_fixed(self, seq: int) -> Fixed:
        digest = hashlib.sha256(f"{self.seed}|{seq}".encode("utf-8")).digest()
        raw = int.from_bytes(digest[:8], "big")
        # 精确映射 raw / 2^64 到定点，half_even 舍入，误差 <= 1/2 ulp。
        value, _ = Fixed.exact_from_int_ratio(raw, self._MOD, "half_even")
        return value

    def decision(self, rate: Fixed, seq: int) -> Dict[str, Any]:
        u = self.unit_fixed(seq)
        mirrored = u.n < rate.n  # 左闭右开：[0, rate)
        return {
            "seq": seq,
            "quantile": u.to_str(),
            "rate": rate.to_str(),
            "interval": "left-closed right-open [0, rate)",
            "mirrored": mirrored,
        }


# ---------------------------------------------------------------------------
# 混合量级样本：定点补偿求和 vs 朴素 float64 求和
# ---------------------------------------------------------------------------

def _float64_ulp(x: float) -> float:
    if x == 0.0:
        return 5e-324
    return abs(x) * 2 ** -52


def float64_comparison(integer_values: Sequence[int]) -> Dict[str, Any]:
    """在同一组“大数 + 小数”混合量级整数上对比三条计算路径。

    * ``exact``：Python 大整数精确和（基准真值）；
    * ``fixed_kahan``：定点 Kahan 补偿求和（整数权重在定点下精确，零误差）；
    * ``float64_naive``：NumPy float64 朴素顺序求和，展示小量被大数吞掉；
    * ``float64_sorted``：按绝对值排序后再朴素求和，展示“运算顺序改变结论”。

    返回每条路径的和、与真值的绝对/相对误差，以及朴素 float64 的理论上界。
    """
    import numpy as np

    exact = sum(int(v) for v in integer_values)
    fixed_values = [Fixed(v * SCALE) for v in integer_values]
    fixed_total, comp = kahan_sum(fixed_values)

    arr = np.asarray(list(integer_values), dtype=np.float64)
    # 显式朴素顺序累加（左结合 ((x0+x1)+x2)+...），不复用 NumPy 的 pairwise 求和，
    # 这样才能真实暴露“大数吞小数”的浮点误差传播。
    naive = 0.0
    for x in arr:
        naive = naive + float(x)
    sorted_values = sorted(abs(int(v)) for v in integer_values)
    sorted_sum = 0.0
    for x in sorted_values:
        sorted_sum = sorted_sum + float(x)

    n = len(integer_values)
    # 标准朴素求和上界：(n-1) * eps * sum(|x_i|)（宽松但可量化）。
    eps = np.finfo(np.float64).eps
    bound = float((n - 1) * eps * sum(abs(int(v)) for v in integer_values)) if n else 0.0

    def pack(label: str, value: Any, exact_value: int, note: str) -> Dict[str, Any]:
        if isinstance(value, Fixed):
            shown = value.to_str()
            abs_err = abs(value.exact() - Fraction(exact_value))
        else:
            shown = repr(value)
            # 不能用 value - float(exact)：exact 转 float 时已丢失精度，
            # 会把误差“减没”。改为 float 精确有理值与整数真值直接相减。
            abs_err = abs(Fraction(value).limit_denominator(10**18)
                          - Fraction(exact_value))
        rel_err = abs_err / Fraction(exact_value) if exact_value else Fraction(0)
        return {
            "path": label,
            "sum": shown,
            "exact_sum": str(exact_value),
            "abs_error": str(abs_err),
            "relative_error": str(rel_err),
            "note": note,
        }

    return {
        "sample_size": n,
        "float64_epsilon": str(eps),
        "float64_naive_declared_bound": repr(bound),
        "paths": [
            pack("exact_bigint", Fixed(exact * SCALE), exact, "Python 任意精度整数，基准真值"),
            pack(
                "fixed_point_kahan",
                fixed_total,
                exact,
                f"定点 Kahan 补偿求和，补偿项={comp.to_str()}，整数路径零舍入",
            ),
            pack("float64_naive_order", naive, exact, "NumPy float64 原始顺序朴素求和"),
            pack(
                "float64_sorted_order",
                sorted_sum,
                exact,
                "按绝对值排序后朴素求和，顺序变化会改变低位结论",
            ),
        ],
        "order_flips_result": naive != sorted_sum,
    }
