"""请求事件与灰度策略快照的领域模型、加载与归一化。

输入约束
--------
* 比例类字段（版本权重、镜像采样率、灰度上限、相对容差）必须是十进制字符串
  或整数；直接传 JSON float 会被拒绝（定点契约不允许隐式二进制浮点）。
* 请求权重与毫秒时间戳必须是**正/非负整数**，解析失败整批拒绝。
* 快照摘要 :func:`canonical_digest` 对规范化后的 JSON 做 SHA-256，提交时逐字段
  重算并比对，防止快照被局部篡改。
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Tuple

from .errors import BusinessError, E_EVENT_INVALID, E_SNAPSHOT_INVALID, E_WEIGHT_INVALID
from .fixedpoint import Fixed, normalize_weights

# 快照中不参与摘要的字段（摘要字段本身、提交期元数据）。
DIGEST_EXCLUDE = {"digest"}


def canonical_digest(payload: Mapping[str, Any]) -> str:
    """对快照业务字段计算规范化 SHA-256 摘要。"""
    body = {k: v for k, v in payload.items() if k not in DIGEST_EXCLUDE}
    canonical = json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _require_fixed(value: Any, field_name: str) -> Fixed:
    if isinstance(value, bool):
        raise BusinessError(
            E_WEIGHT_INVALID, f"字段 {field_name} 不能是布尔值", {"field": field_name}
        )
    if isinstance(value, (int,)):
        return Fixed(int(value) * 10**9)
    if isinstance(value, str):
        try:
            return Fixed.from_str(value)
        except Exception as exc:  # noqa: BLE001 - 统一翻译为业务码
            raise BusinessError(
                E_WEIGHT_INVALID,
                f"字段 {field_name} 不是合法十进制比例: {value!r}",
                {"field": field_name, "value": value, "detail": str(exc)},
            )
    raise BusinessError(
        E_WEIGHT_INVALID,
        f"字段 {field_name} 必须是十进制字符串或整数，禁止隐式浮点: {value!r}",
        {"field": field_name, "type": type(value).__name__},
    )


@dataclass(frozen=True)
class PolicySnapshot:
    """已归一化、可直接参与裁决的策略快照。"""

    version: int
    generated_at_ms: int
    version_weights: Dict[str, Fixed]
    normalized_weights: Dict[str, Fixed]
    mirror_rate: Fixed
    gray_cap: Fixed
    relative_tolerance: Fixed
    dedup_window_ms: int
    baseline_version: str
    seed: str
    digest: str
    raw: Dict[str, Any] = field(repr=False, default_factory=dict)

    @property
    def versions(self) -> List[str]:
        return sorted(self.version_weights.keys())

    @property
    def gray_versions(self) -> List[str]:
        return [v for v in self.versions if v != self.baseline_version]

    def as_persisted_dict(self) -> Dict[str, Any]:
        return {
            "version": self.version,
            "generated_at_ms": self.generated_at_ms,
            "version_weights": {k: v.to_str() for k, v in self.version_weights.items()},
            "normalized_weights": {k: v.to_str() for k, v in self.normalized_weights.items()},
            "mirror_rate": self.mirror_rate.to_str(),
            "gray_cap": self.gray_cap.to_str(),
            "relative_tolerance": self.relative_tolerance.to_str(),
            "dedup_window_ms": self.dedup_window_ms,
            "baseline_version": self.baseline_version,
            "seed": self.seed,
            "digest": self.digest,
        }


def parse_snapshot(payload: Mapping[str, Any]) -> PolicySnapshot:
    """校验并归一化策略快照；任何失败抛 :class:`BusinessError`。"""
    required = {
        "version",
        "generated_at_ms",
        "version_weights",
        "mirror_rate",
        "gray_cap",
        "dedup_window_ms",
        "baseline_version",
        "seed",
    }
    missing = sorted(required - set(payload.keys()))
    if missing:
        raise BusinessError(
            E_SNAPSHOT_INVALID, "快照缺少必填字段", {"missing": missing}
        )

    try:
        version = int(payload["version"])
        generated_at_ms = int(payload["generated_at_ms"])
        dedup_window_ms = int(payload["dedup_window_ms"])
    except (TypeError, ValueError) as exc:
        raise BusinessError(
            E_SNAPSHOT_INVALID,
            "版本号/时间戳/去重窗口必须是整数",
            {"detail": str(exc)},
        )
    if version < 0 or generated_at_ms < 0 or dedup_window_ms < 0:
        raise BusinessError(
            E_SNAPSHOT_INVALID,
            "版本号、时间戳、去重窗口不能为负",
            {"version": version, "generated_at_ms": generated_at_ms,
             "dedup_window_ms": dedup_window_ms},
        )

    raw_weights = payload["version_weights"]
    if not isinstance(raw_weights, Mapping) or not raw_weights:
        raise BusinessError(
            E_SNAPSHOT_INVALID, "version_weights 必须是非空对象", {}
        )
    weights: List[Tuple[str, Fixed]] = []
    for vid, w in raw_weights.items():
        if not isinstance(vid, str) or not vid:
            raise BusinessError(
                E_SNAPSHOT_INVALID, "版本标识必须是非空字符串", {"value": vid}
            )
        weights.append((vid, _require_fixed(w, f"version_weights.{vid}")))

    mirror_rate = _require_fixed(payload["mirror_rate"], "mirror_rate")
    gray_cap = _require_fixed(payload["gray_cap"], "gray_cap")
    rel_tol = _require_fixed(
        payload.get("relative_tolerance", "0.000001"), "relative_tolerance"
    )
    baseline = str(payload["baseline_version"])
    seed = str(payload["seed"])

    # 比例边界校验（定点整数比较，无浮点）。
    for name, val in (("mirror_rate", mirror_rate), ("gray_cap", gray_cap),
                      ("relative_tolerance", rel_tol)):
        if val.n < 0 or val.n > 10**9:
            raise BusinessError(
                E_WEIGHT_INVALID,
                f"{name} 必须落在 [0, 1] 闭区间内",
                {"field": name, "value": val.to_str()},
            )
    if baseline not in dict(weights):
        raise BusinessError(
            E_SNAPSHOT_INVALID,
            "baseline_version 必须出现在 version_weights 中",
            {"baseline_version": baseline, "known": sorted(dict(weights).keys())},
        )

    # 摘要必须存在且与重算结果一致；不一致时拒绝，调用方保持既有状态不变。
    digest = payload.get("digest")
    if not isinstance(digest, str) or not digest:
        raise BusinessError(
            E_SNAPSHOT_INVALID, "快照缺少 digest 摘要字段", {}
        )
    recomputed = canonical_digest(payload)
    if digest != recomputed:
        raise BusinessError(
            E_SNAPSHOT_INVALID,
            "快照摘要校验失败，内容可能已被篡改",
            {"provided": digest, "recomputed": recomputed},
        )

    try:
        normalized, total, errors = normalize_weights(weights, "half_even")
    except BusinessError:
        raise
    except Exception as exc:  # noqa: BLE001
        # 总和为零 / 负权重 -> E_WEIGHT_INVALID，绝不静默回落默认版本。
        raise BusinessError(
            E_WEIGHT_INVALID,
            "版本权重归一化失败",
            {"detail": str(exc), "weights": {k: v.to_str() for k, v in weights}},
        )

    return PolicySnapshot(
        version=version,
        generated_at_ms=generated_at_ms,
        version_weights=dict(weights),
        normalized_weights=dict(normalized),
        mirror_rate=mirror_rate,
        gray_cap=gray_cap,
        relative_tolerance=rel_tol,
        dedup_window_ms=dedup_window_ms,
        baseline_version=baseline,
        seed=seed,
        digest=digest,
        raw=dict(payload),
    )


@dataclass(frozen=True)
class RequestEvent:
    request_id: str
    timestamp_ms: int
    route_key: str
    client_id: str
    weight: int
    target_version: str


def parse_event_batch(payload: Mapping[str, Any]) -> List[RequestEvent]:
    """解析事件批次；任一条非法则整批拒绝（E_EVENT_INVALID，不入库）。"""
    raw_events = payload.get("events")
    if not isinstance(raw_events, list) or not raw_events:
        raise BusinessError(
            E_EVENT_INVALID, "events 必须是非空数组", {"received_type": type(raw_events).__name__}
        )

    parsed: List[RequestEvent] = []
    seen_ids: Dict[str, int] = {}
    for idx, item in enumerate(raw_events):
        if not isinstance(item, Mapping):
            raise BusinessError(
                E_EVENT_INVALID, f"事件 #{idx} 不是对象", {"index": idx}
            )
        required = ("request_id", "timestamp_ms", "route_key", "client_id", "weight",
                    "target_version")
        missing = [k for k in required if k not in item]
        if missing:
            raise BusinessError(
                E_EVENT_INVALID, f"事件 #{idx} 缺少字段", {"index": idx, "missing": missing}
            )
        rid = item["request_id"]
        if not isinstance(rid, str) or not rid:
            raise BusinessError(
                E_EVENT_INVALID, f"事件 #{idx} request_id 非法", {"index": idx}
            )
        if rid in seen_ids:
            raise BusinessError(
                E_EVENT_INVALID,
                "批次内 request_id 重复",
                {"request_id": rid, "indexes": [seen_ids[rid], idx]},
            )
        seen_ids[rid] = idx

        try:
            ts = int(item["timestamp_ms"])
            weight = int(item["weight"])
        except (TypeError, ValueError) as exc:
            raise BusinessError(
                E_EVENT_INVALID,
                f"事件 {rid} 时间戳或权重不是整数",
                {"request_id": rid, "detail": str(exc)},
            )
        # bool 是 int 的子类，显式排除。
        if isinstance(item["timestamp_ms"], bool) or isinstance(item["weight"], bool):
            raise BusinessError(
                E_EVENT_INVALID, f"事件 {rid} 时间戳/权重不能为布尔值",
                {"request_id": rid},
            )
        if ts < 0:
            raise BusinessError(
                E_EVENT_INVALID, f"事件 {rid} 时间戳为负", {"request_id": rid, "timestamp_ms": ts}
            )
        if weight <= 0:
            # 正整数权重是分流前提；非法权重走 E_EVENT_INVALID（整批不入库），
            # 与“快照比例权重非法 -> E_WEIGHT_INVALID”区分开。
            raise BusinessError(
                E_EVENT_INVALID, f"事件 {rid} 请求权重必须是正整数",
                {"request_id": rid, "weight": weight},
            )
        route_key = item["route_key"]
        client_id = item["client_id"]
        target = item["target_version"]
        if not all(isinstance(x, str) and x for x in (route_key, client_id, target)):
            raise BusinessError(
                E_EVENT_INVALID, f"事件 {rid} 路由键/客户端/目标版本必须是非空字符串",
                {"request_id": rid},
            )
        parsed.append(
            RequestEvent(
                request_id=rid,
                timestamp_ms=ts,
                route_key=route_key,
                client_id=client_id,
                weight=weight,
                target_version=target,
            )
        )
    return parsed
