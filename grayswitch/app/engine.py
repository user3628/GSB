"""灰度路由与镜像裁决引擎（纯函数式状态迁移，便于重放与逐位复算）。

裁决流程（单条事件，状态在事件间显式传递）：

1. **定点累计与补偿求和**：请求权重为正整数，在定点体系下按路由键累计，
   加法本身精确；累计入口统一走 :func:`~grayswitch.app.fixedpoint.kahan_sum`
   语义（补偿项在整数定点路径恒为 0，但“累计分流使用补偿求和”在代码中可审计）。
2. **版本分流（赤字加权路由，deficit routing）**：维护每个路由键的累计权重
   ``T``，按策略归一化定点比例计算各版本的**目标配额**
   ``target_v = T * share_v``（一次显式舍入 + 最大余数法保证配额总和逐位等于
   ``T``），再选择 ``target_v - allocated_v``（赤字）最大的版本承接本事件，
   平局按版本字典序打破。全程整数/定点运算，无二进制浮点，结论逐位可复算，
   且长期实际占比严格收敛到声明比例。
3. **灰度上限**：若命中灰度版本，先在“假设接受”的定点灰度占比上做带相对容差
   的比较，容差带内（含等号）视为未越界；越界则该事件裁决为
   ``E_GRAY_LIMIT_EXCEEDED``，**不计入任何累计、不镜像、不静默回落到基线版**。
4. **镜像采样**：通过灰度检查后，采样与分流读取**同一个策略版本**，用固定种子
   与事件序号生成定点分位，左闭右开 ``[0, rate)`` 判定；再叠加单客户端去重
   窗口（窗口两端均为包含边界）。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from fractions import Fraction
from typing import Any, Dict, List, Optional, Tuple

from .fixedpoint import DECIMALS, SCALE, Fixed, kahan_sum
from .models import PolicySnapshot, RequestEvent
from .numeric_contract import SamplingPrng, relative_tolerance_compare

GRAY_LIMIT_CODE = "E_GRAY_LIMIT_EXCEEDED"


@dataclass
class EngineState:
    """可序列化的引擎累计状态（全部为整数/字符串，持久化无精度损失）。"""

    policy_version: int
    # 每个路由键的累计权重（补偿求和的精确整数结果）。
    route_total: Dict[str, int] = field(default_factory=dict)
    # 每个路由键各版本已承接权重。
    route_version_allocated: Dict[str, Dict[str, int]] = field(default_factory=dict)
    total_weight: int = 0
    gray_weight: int = 0
    version_weight: Dict[str, int] = field(default_factory=dict)
    # 客户端最近一次“采样命中”的毫秒时间戳（去重窗口判定锚点）。
    last_mirror_ts: Dict[str, int] = field(default_factory=dict)
    mirror_count: int = 0
    processed: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "policy_version": self.policy_version,
            "route_total": self.route_total,
            "route_version_allocated": self.route_version_allocated,
            "total_weight": self.total_weight,
            "gray_weight": self.gray_weight,
            "version_weight": self.version_weight,
            "last_mirror_ts": self.last_mirror_ts,
            "mirror_count": self.mirror_count,
            "processed": self.processed,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "EngineState":
        return cls(
            policy_version=int(data["policy_version"]),
            route_total={k: int(v) for k, v in data["route_total"].items()},
            route_version_allocated={
                k: {vv: int(x) for vv, x in alloc.items()}
                for k, alloc in data["route_version_allocated"].items()
            },
            total_weight=int(data["total_weight"]),
            gray_weight=int(data["gray_weight"]),
            version_weight={k: int(v) for k, v in data["version_weight"].items()},
            last_mirror_ts={k: int(v) for k, v in data["last_mirror_ts"].items()},
            mirror_count=int(data["mirror_count"]),
            processed=int(data["processed"]),
        )


def largest_remainder_allocation(
    total_weight: int, normalized: List[Tuple[str, Fixed]]
) -> Dict[str, int]:
    """把整数 ``total_weight`` 按定点比例切成整数目标配额，总和逐位等于输入。

    余数按“精确配额的小数部分大者优先、版本字典序兜底”分配。
    """
    exact_num: Dict[str, Fraction] = {
        v: Fraction(total_weight) * w.exact() for v, w in normalized
    }
    floors = {v: int(x) for v, x in exact_num.items()}
    remainder = total_weight - sum(floors.values())
    ranked = sorted(
        normalized,
        key=lambda item: (exact_num[item[0]] - floors[item[0]], item[0]),
        reverse=True,
    )
    alloc = dict(floors)
    i = 0
    while remainder > 0:
        alloc[ranked[i % len(ranked)][0]] += 1
        remainder -= 1
        i += 1
    assert sum(alloc.values()) == total_weight
    return alloc


def choose_by_deficit(
    event: RequestEvent,
    projected_total: int,
    allocated: Dict[str, int],
    normalized: List[Tuple[str, Fixed]],
    versions: List[str],
) -> Tuple[str, Dict[str, Any]]:
    """赤字加权路由：选择 ``目标配额 - 已承接`` 最大的版本，字典序打破平局。"""
    targets = largest_remainder_allocation(projected_total, normalized)
    deficits = {v: targets[v] - allocated.get(v, 0) for v in versions}
    # 最大赤字；同值时版本字典序升序，保证平局稳定且可复算。
    chosen = sorted(versions, key=lambda v: (-deficits[v], v))[0]
    trace = {
        "targets_after_event": targets,
        "allocated_before_event": {v: allocated.get(v, 0) for v in versions},
        "deficits": deficits,
        "tie_break": "max deficit, then lexicographic version order",
        "chosen": chosen,
    }
    return chosen, trace


def _within_dedup_window(
    client_id: str, ts_ms: int, window_ms: int, last_mirror_ts: Dict[str, int]
) -> Tuple[bool, Optional[int]]:
    """去重窗口判定，边界显式定义为**两端包含**。

    若客户端最近一次镜像发生在 ``t0``，则当前事件时间戳 ``t`` 满足
    ``t0 <= t <= t0 + window_ms`` 时视为窗口内（包含等号），不再镜像。
    """
    last = last_mirror_ts.get(client_id)
    if last is None:
        return False, None
    inside = last <= ts_ms <= last + window_ms
    return inside, last


class RoutingEngine:
    """持有不可变策略快照与可变累计状态的裁决器。"""

    def __init__(self, policy: PolicySnapshot, state: Optional[EngineState] = None) -> None:
        self.policy = policy
        self.state = state or EngineState(policy_version=policy.version)
        if self.state.policy_version != policy.version:
            raise ValueError("引擎状态与策略版本不一致，禁止跨版本混用累计状态")
        self._prng = SamplingPrng(policy.seed)
        self.versions = policy.versions
        self.normalized = sorted(policy.normalized_weights.items())

    def gray_usage_fixed(self, gray_weight: int, total_weight: int) -> Tuple[Fixed, Fraction]:
        """灰度占比定点化（half_even），返回结果与精确舍入误差。"""
        if total_weight <= 0:
            return Fixed.zero(), Fraction(0)
        return Fixed.exact_from_int_ratio(gray_weight, total_weight, "half_even")

    def _accumulate_route(self, route_key: str, weight: int) -> int:
        """按路由键做补偿求和累计（整数定点路径精确，补偿项为 0）。"""
        existing = self.state.route_total.get(route_key, 0)
        total, compensation = kahan_sum(
            [Fixed(existing * SCALE), Fixed(weight * SCALE)]
        )
        assert compensation.n == 0, "整数定点补偿求和必须无残差"
        return total.n // SCALE

    def process(self, event: RequestEvent, event_seq: int) -> Dict[str, Any]:
        """裁决单条事件，返回可持久化的裁决记录（并就地更新状态）。"""
        policy = self.policy
        state = self.state

        if event.target_version not in policy.version_weights:
            # 目标版本不在本策略合法集合内：权重/版本非法，不回落默认版本。
            return {
                "request_id": event.request_id,
                "event_seq": event_seq,
                "timestamp_ms": event.timestamp_ms,
                "route_key": event.route_key,
                "client_id": event.client_id,
                "weight": event.weight,
                "target_version": event.target_version,
                "policy_version": policy.version,
                "status": "rejected",
                "error_code": "E_WEIGHT_INVALID",
                "detail": {
                    "target_version": event.target_version,
                    "legal_versions": self.versions,
                },
            }

        projected_total = self._accumulate_route(event.route_key, event.weight)
        allocated = dict(
            state.route_version_allocated.get(event.route_key, {})
        )
        chosen, deficit_trace = choose_by_deficit(
            event, projected_total, allocated, self.normalized, self.versions
        )

        record: Dict[str, Any] = {
            "request_id": event.request_id,
            "event_seq": event_seq,
            "timestamp_ms": event.timestamp_ms,
            "route_key": event.route_key,
            "client_id": event.client_id,
            "weight": event.weight,
            "target_version": event.target_version,
            "policy_version": policy.version,
            "routed_version": chosen,
            "is_gray": chosen != policy.baseline_version,
            "route_previous_total": state.route_total.get(event.route_key, 0),
            "route_projected_total": projected_total,
            "deficit_trace": deficit_trace,
            "status": "routed",
        }

        # ---- 灰度上限容差判定（仅灰度版本触发） ----
        if record["is_gray"]:
            proj_gray = state.gray_weight + event.weight
            proj_total = state.total_weight + event.weight
            usage, usage_err = self.gray_usage_fixed(proj_gray, proj_total)
            cap_check = relative_tolerance_compare(
                usage, policy.gray_cap, policy.relative_tolerance, "half_even"
            )
            record["gray_check"] = {
                "projected_gray_weight": proj_gray,
                "projected_total_weight": proj_total,
                "usage_abs_error": str(usage_err),
                **cap_check,
            }
            if cap_check["exceeded"]:
                record["status"] = "rejected"
                record["error_code"] = GRAY_LIMIT_CODE
                # 关键：越界事件不更新任何累计状态，也不参与镜像。
                return record

        # ---- 接受事件，提交累计 ----
        state.route_total[event.route_key] = projected_total
        route_alloc = state.route_version_allocated.setdefault(event.route_key, {})
        route_alloc[chosen] = route_alloc.get(chosen, 0) + event.weight
        state.total_weight += event.weight
        state.version_weight[chosen] = state.version_weight.get(chosen, 0) + event.weight
        if record["is_gray"]:
            state.gray_weight += event.weight
        state.processed += 1

        # ---- 镜像采样：同一策略版本 + 同一累计状态，固定种子 + 事件序号 ----
        sample = self._prng.decision(policy.mirror_rate, event_seq)
        # 先读取“本次事件之前”的锚点，保证窗口判定与输出锚点严格对应。
        anchor_ts = state.last_mirror_ts.get(event.client_id)
        inside_window, last_ts = _within_dedup_window(
            event.client_id, event.timestamp_ms, policy.dedup_window_ms,
            state.last_mirror_ts,
        )
        sample_hit = bool(sample["mirrored"])
        mirrored = sample_hit and not inside_window
        record["mirror"] = {
            **sample,
            # 区分“采样命中”与“最终镜像（命中且未被去重）”两个概念。
            "sample_hit": sample_hit,
            "seed": policy.seed,
            "dedup_window_ms": policy.dedup_window_ms,
            "window_boundary": "inclusive both ends: anchor <= t <= anchor + window",
            "inside_dedup_window": inside_window,
            "anchor_ts_ms_before_event": anchor_ts,
            "last_mirror_ts_ms": last_ts,
            "mirrored": mirrored,
        }
        # 锚点 = 最近一次“采样命中”的时间戳（无论该命中是否被去重抑制），
        # 因此窗口随采样命中事件向后滑动，窗口内后续命中的 inside_window 正确。
        if sample_hit:
            state.last_mirror_ts[event.client_id] = event.timestamp_ms
        if mirrored:
            state.mirror_count += 1
        return record

    def current_shares(self) -> Dict[str, Any]:
        """输出当前各版本实际分流占比（定点，固定小数位）与误差说明。"""
        shares: Dict[str, str] = {}
        errors: Dict[str, str] = {}
        for v in self.versions:
            w = self.state.version_weight.get(v, 0)
            if self.state.total_weight:
                fixed, err = Fixed.exact_from_int_ratio(
                    w, self.state.total_weight, "half_even"
                )
                shares[v] = fixed.to_str()
                errors[v] = str(err)
            else:
                shares[v] = Fixed.zero().to_str()
                errors[v] = "0"
        usage, usage_err = self.gray_usage_fixed(
            self.state.gray_weight, self.state.total_weight
        )
        return {
            "policy_version": self.policy.version,
            "total_weight": self.state.total_weight,
            "version_weight": dict(self.state.version_weight),
            "shares": shares,
            "share_abs_errors": errors,
            "gray_weight": self.state.gray_weight,
            "gray_usage": usage.to_str(),
            "gray_usage_abs_error": str(usage_err),
            "gray_cap": self.policy.gray_cap.to_str(),
            "mirror_count": self.state.mirror_count,
            "processed": self.state.processed,
            "decimals": DECIMALS,
        }


def replay(policy: PolicySnapshot, events: List[RequestEvent]) -> Tuple[EngineState, List[Dict[str, Any]]]:
    """从空状态重放整批事件，用于逐位一致性校验。"""
    engine = RoutingEngine(policy)
    records = [engine.process(ev, idx) for idx, ev in enumerate(events)]
    return engine.state, records
