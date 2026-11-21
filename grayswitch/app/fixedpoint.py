"""显式定点数与显式舍入运算。

设计要点
--------
* 定点基数 ``SCALE = 10 ** DECIMALS``（默认 9 位小数），所有比例在系统内部只以
  整数分子 ``n``（值为 ``n / SCALE``）表示，不存在隐式二进制浮点。
* 每次乘除都必须显式声明 :class:`Rounding`，运算返回结果与**舍入误差**
  （同为 :class:`Fixed`），并提供基于舍入单位（ulp）的可量化误差上界。
* 提供精确参考值（:class:`fractions.Fraction`）对照，数值契约模块据此逐运算
  统计累计绝对误差，而不是事后估算。
* 从 ``float`` 构造被显式拒绝（:class:`ImplicitFloatError`），外部 JSON 的比例
  必须以字符串或整数传入，避免把不可表示的二进制浮点偷偷带入计算路径。
"""

from __future__ import annotations

from fractions import Fraction
from typing import Iterable, List, Union

DECIMALS = 9
SCALE = 10 ** DECIMALS
HALF = SCALE // 2

# 一个舍入单位（unit in the last place），定点体系下的最小可表示增量。
ULP = Fraction(1, SCALE)


class ImplicitFloatError(TypeError):
    """拒绝未经声明的 float -> 定点转换。"""


class FixedError(ValueError):
    """定点运算非法输入（除零、未知舍入方式等）。"""


Number = Union[int, "Fixed", Fraction, str]


def _round_int(exact_num: int, exact_den: int, rounding: str) -> tuple[int, Fraction]:
    """把有理数 ``exact_num / exact_den`` 按给定方式舍入到整数分子。

    返回 ``(舍入后整数分子, 精确误差 = 精确值 - 舍入值)``，误差用 Fraction 表示，
    单位为“一个定点数”（即分子层面的 1 对应 1/SCALE）。
    """
    if exact_den <= 0:
        raise FixedError("分母必须为正整数")
    q, r = divmod(exact_num, exact_den)
    if r == 0:
        return q, Fraction(0)
    frac = Fraction(r, exact_den)  # 0 < frac < 1
    if rounding == "truncate":
        n = q
    elif rounding == "down":  # 向负无穷
        n = q if exact_num >= 0 else q - 1
    elif rounding == "half_up":
        n = q if frac < Fraction(1, 2) else q + 1
    elif rounding == "half_even":
        if frac < Fraction(1, 2):
            n = q
        elif frac > Fraction(1, 2):
            n = q + 1
        else:
            n = q if q % 2 == 0 else q + 1
    else:
        raise FixedError(f"未知舍入方式: {rounding}")
    # 分子层面的精确误差（真实分子 - 舍入分子），再除以 SCALE 才是值误差。
    numerator_error = Fraction(exact_num, exact_den) - n
    return n, numerator_error / SCALE


class Fixed:
    """不可变定点数，内部只保存整数分子。"""

    __slots__ = ("n",)

    def __init__(self, n: int) -> None:
        if not isinstance(n, int):
            raise ImplicitFloatError(
                "Fixed 只能由整数分子构造；比例请用 Fixed.from_str/from_fraction"
            )
        self.n = n

    # ---------- 构造 ----------
    @classmethod
    def from_str(cls, text: str) -> "Fixed":
        """从十进制字符串构造，例如 ``"0.25"``、``"1"``、``"0.123456789"``。

        超过 ``DECIMALS`` 位的部分按 half_up 显式舍入，并通过返回的误差暴露。
        """
        if isinstance(text, (int,)) and not isinstance(text, bool):
            return cls(int(text) * SCALE)
        if not isinstance(text, str):
            raise ImplicitFloatError("比例输入必须是十进制字符串或整数")
        s = text.strip()
        if not s:
            raise FixedError("空字符串无法构造 Fixed")
        neg = s.startswith("-")
        if neg or s.startswith("+"):
            s = s[1:]
        if not s or s in (".",):
            raise FixedError(f"非法定点字面量: {text!r}")
        if "." in s:
            whole, frac = s.split(".", 1)
            if not whole:
                whole = "0"
            if not frac.isdigit() or not whole.isdigit():
                raise FixedError(f"非法定点字面量: {text!r}")
        else:
            if not s.isdigit():
                raise FixedError(f"非法定点字面量: {text!r}")
            whole, frac = s, ""
        num = int(whole) * SCALE
        if len(frac) >= DECIMALS:
            kept = frac[:DECIMALS]
            rest = frac[DECIMALS:]
            num += int(kept) if kept else 0
            if rest:  # 需要显式舍入
                tail = int(rest)
                tail_den = 10 ** len(rest)
                add, _ = _round_int(tail, tail_den, "half_up")
                num += add
        elif frac:
            num += int(frac) * 10 ** (DECIMALS - len(frac))
        return cls(-num if neg else num)

    @classmethod
    def from_fraction(
        cls, value: Fraction, rounding: str = "half_even"
    ) -> tuple["Fixed", Fraction]:
        """从精确 Fraction 构造，返回 ``(Fixed, 精确舍入误差)``。"""
        n, err = _round_int(value.numerator * SCALE, value.denominator, rounding)
        return cls(n), err

    @classmethod
    def exact_from_int_ratio(
        cls, numerator: int, denominator: int, rounding: str = "half_even"
    ) -> tuple["Fixed", Fraction]:
        """精确计算 ``numerator/denominator`` 并定点化，附带精确误差。"""
        return cls.from_fraction(Fraction(numerator, denominator), rounding)

    @classmethod
    def zero(cls) -> "Fixed":
        return cls(0)

    @classmethod
    def one(cls) -> "Fixed":
        return cls(SCALE)

    # ---------- 精确视图 ----------
    def exact(self) -> Fraction:
        return Fraction(self.n, SCALE)

    # ---------- 比较 / 展示 ----------
    def __eq__(self, other: object) -> bool:
        if isinstance(other, Fixed):
            return self.n == other.n
        return NotImplemented

    def __lt__(self, other: "Fixed") -> bool:
        return self.n < other.n

    def __le__(self, other: "Fixed") -> bool:
        return self.n <= other.n

    def __gt__(self, other: "Fixed") -> bool:
        return self.n > other.n

    def __ge__(self, other: "Fixed") -> bool:
        return self.n >= other.n

    def __hash__(self) -> int:
        return hash(("Fixed", self.n))

    def __repr__(self) -> str:
        return f"Fixed({self.to_str()})"

    def to_str(self, decimals: int = DECIMALS) -> str:
        """固定小数位字符串表示，不足补零（满足“保留固定小数位”输出契约）。"""
        neg = self.n < 0
        num = abs(self.n)
        whole, frac = divmod(num, SCALE)
        s = f"{whole}.{str(frac).zfill(DECIMALS)}"
        if decimals != DECIMALS:
            s = f"{whole}.{str(frac).zfill(DECIMALS)[:decimals]}"
        return ("-" + s) if neg else s

    def to_float(self) -> float:
        """仅用于与朴素 float64 路径做实验对照，业务裁决路径禁止调用。"""
        return self.n / SCALE

    # ---------- 精确运算（定点间加减、整数倍缩放不产生误差） ----------
    def add(self, other: "Fixed") -> "Fixed":
        self._check(other)
        return Fixed(self.n + other.n)

    def sub(self, other: "Fixed") -> "Fixed":
        self._check(other)
        return Fixed(self.n - other.n)

    def neg(self) -> "Fixed":
        return Fixed(-self.n)

    def mul_int(self, factor: int) -> "Fixed":
        if not isinstance(factor, int):
            raise ImplicitFloatError("mul_int 只接受整数因子")
        return Fixed(self.n * factor)

    # ---------- 有舍入运算（必须声明 rounding，并返回精确误差） ----------
    def mul(self, other: "Fixed", rounding: str = "half_even") -> tuple["Fixed", Fraction]:
        """定点乘法：``(a*b)`` 在分子层为 ``n1*n2/SCALE``，需要一次显式舍入。"""
        self._check(other)
        return self.from_fraction(
            Fraction(self.n * other.n, SCALE * SCALE), rounding
        )

    def div(self, denominator: "Fixed", rounding: str = "half_even") -> tuple["Fixed", Fraction]:
        """定点除法：``a/b``，分母必须为正。"""
        self._check(denominator)
        if denominator.n <= 0:
            raise FixedError("定点除法要求正分母")
        return self.from_fraction(
            Fraction(self.n, denominator.n), rounding
        )

    def scale_by(
        self, numerator: int, denominator: int, rounding: str = "half_even"
    ) -> tuple["Fixed", Fraction]:
        """``self * numerator / denominator``，精确有理中间值后一次舍入。"""
        if denominator <= 0:
            raise FixedError("scale_by 要求正分母")
        return self.from_fraction(
            self.exact() * Fraction(numerator, denominator), rounding
        )

    @staticmethod
    def _check(other: object) -> None:
        if not isinstance(other, Fixed):
            raise ImplicitFloatError("Fixed 运算两侧都必须是 Fixed，禁止隐式浮点混用")


def kahan_sum(values: Iterable[Fixed]) -> tuple[Fixed, Fixed]:
    """补偿求和（Kahan）。

    返回 ``(总和, 补偿项 c)``。定点体系下加法本身精确，``c`` 恒为 0；该函数与
    :func:`naive_sum` 的差异在混合量级 float64 对照样本中体现（见 numeric_contract）。
    保留补偿项是为了让“累计分流使用补偿求和”成为代码层面可审计的事实。
    """
    total = Fixed.zero()
    comp = Fixed.zero()
    for v in values:
        y = v.sub(comp)
        new_total = total.add(y)
        comp = new_total.sub(total).sub(y).neg()
        total = new_total
    return total, comp


def naive_sum(values: Iterable[Fixed]) -> Fixed:
    """朴素顺序求和（定点下精确，作为与 Kahan 同口径的基线）。"""
    total = Fixed.zero()
    for v in values:
        total = total.add(v)
    return total


def normalize_weights(
    raw: List[tuple[str, Fixed]], rounding: str = "half_even"
) -> tuple[List[tuple[str, Fixed]], Fixed, List[Fraction]]:
    """把版本权重比例显式归一化为内部定点表示。

    返回 ``(归一化后的 (版本, 比例) 列表, 归一化比例总和, 每版本精确舍入误差)``。

    采用“先定点化、再按最大余数法分配舍入残差”的归一化，保证：
    1. 每个比例都是定点数；
    2. 归一化比例之和**逐位等于** ``Fixed.one()``（残差按精确余数大小分配，
       平局按版本字典序打破，结论稳定可复算）；
    3. 每一步舍入方式与误差都被记录，单版本误差上界为 1 ulp。
    """
    if not raw:
        raise FixedError("权重列表为空，无法归一化")
    negatives = [v for v, w in raw if w.n < 0]
    if negatives:
        raise FixedError(f"存在负权重版本: {negatives}")
    total = Fixed.zero()
    for _, w in raw:
        total = total.add(w)
    if total.n <= 0:
        raise FixedError("版本权重总和为零，无法归一化")

    ordered = sorted(raw, key=lambda item: item[0])  # 稳定字典序
    exact_map = {v: (w.exact() / total.exact()) for v, w in ordered}

    provisional: List[tuple[str, int]] = []
    for v, share in exact_map.items():
        n, _ = _round_int(share.numerator * SCALE, share.denominator, rounding)
        provisional.append((v, n))

    remainder = SCALE - sum(n for _, n in provisional)
    if remainder != 0:
        bump = 1 if remainder > 0 else -1

        def residue(item: tuple[str, int]) -> tuple[Fraction, str]:
            v, n = item
            # 精确分子与当前定点分子之差；越大越应优先 +1（bump=-1 时反向同理）。
            return (exact_map[v] * SCALE - Fraction(n), v)

        ranked = sorted(provisional, key=residue, reverse=(bump == 1))
        i = 0
        while remainder != 0:
            v, _ = ranked[i % len(ranked)]
            for j, (vv, nn) in enumerate(provisional):
                if vv == v:
                    provisional[j] = (vv, nn + bump)
                    break
            remainder -= bump
            i += 1

    result: List[tuple[str, Fixed]] = []
    errors: List[Fraction] = []
    sum_n = 0
    for v, n in provisional:
        result.append((v, Fixed(n)))
        errors.append(exact_map[v] - Fraction(n, SCALE))
        sum_n += n
    assert sum_n == SCALE, "归一化后比例之和必须逐位等于 1"
    return result, Fixed(sum_n), errors
