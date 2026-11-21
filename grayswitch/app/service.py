"""应用服务层：单写者提交、多读者查询、幂等去重与派生统计重建。

并发模型
--------
* 一把全局 :class:`threading.RLock`（写锁）串行化所有提交：策略提交、批次裁决、
  统计重建。SQLite 开启 WAL，只读查询走独立短事务，可与写者并发读取已提交版本。
* 同一 ``request_id`` 的并发提交用 per-key 事件锁串行化：第一个请求裁决并落库，
  其余请求等待后复用结果；若等待后发现内容哈希不一致，返回
  ``E_CONFLICTING_REQUEST``。
* 所有读接口都按“某个完整提交版本”读取，业务数据与意图完成状态在同一个事务
  提交，读者不可能看到半提交状态。
"""

from __future__ import annotations

import hashlib
import json
import os
import threading
import time
from fractions import Fraction
import uuid
from typing import Any, Dict, List, Optional

from .engine import EngineState, RoutingEngine, replay
from .errors import (
    BusinessError,
    E_CONFLICTING_REQUEST,
    E_GRAY_LIMIT_EXCEEDED,
    E_NOT_FOUND,
    E_SNAPSHOT_INVALID,
)
from .fixedpoint import Fixed
from .models import PolicySnapshot, RequestEvent, parse_event_batch, parse_snapshot
from .numeric_contract import TraceBook, float64_comparison
from .storage import Storage


def _now_ms() -> int:
    return int(time.time() * 1000)


def _content_hash(event: Dict[str, Any]) -> str:
    canonical = json.dumps(event, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class GrayService:
    def __init__(self, storage: Storage) -> None:
        self.storage = storage
        # 崩溃注入：环境变量 GRAYSWITCH_CRASH_AFTER_INTENT=1 时，
        # 批次意图落库后、业务事务提交前强制退出（模拟 kill -9）。
        self.crash_after_intent = os.environ.get("GRAYSWITCH_CRASH_AFTER_INTENT") == "1"

    # ---------------- 策略提交 ----------------
    def submit_policy(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        with self.storage.write_lock:
            try:
                snapshot = parse_snapshot(payload)
            except BusinessError:
                # 校验失败：直接拒绝，不写任何数据，既有已提交状态保持不变。
                raise
            latest = self.storage.latest_policy_version()
            if latest is not None and snapshot.version <= latest:
                raise BusinessError(
                    E_SNAPSHOT_INVALID,
                    "策略版本号必须严格单调递增",
                    {"submitted": snapshot.version, "latest_committed": latest},
                )
            if self.storage.policy_exists(snapshot.version):
                raise BusinessError(
                    E_SNAPSHOT_INVALID,
                    "策略版本号已存在",
                    {"version": snapshot.version},
                )
            now = _now_ms()
            intent_id = self.storage.create_intent(
                "policy_submit", now, snapshot.version, None,
                {"version": snapshot.version, "digest": snapshot.digest},
            )
            self.storage.insert_policy(
                intent_id, snapshot.version, snapshot.raw, snapshot.digest, now
            )
            # 新版本初始化空累计状态，保证读者总能读到完整版本的计数器。
            self.storage.save_state(
                snapshot.version,
                EngineState(policy_version=snapshot.version).to_dict(),
                None,
            )
            return {
                "policy_version": snapshot.version,
                "digest": snapshot.digest,
                "normalized_weights": {
                    k: v.to_str() for k, v in snapshot.normalized_weights.items()
                },
                "committed_at_ms": now,
            }

    def _load_snapshot(self, policy_version: Optional[int]) -> PolicySnapshot:
        payload = self.storage.get_policy_payload(policy_version)
        if payload is None:
            raise BusinessError(
                E_NOT_FOUND, "没有已提交的策略快照",
                {"requested_version": policy_version}, http_status=404,
            )
        return parse_snapshot(payload)

    # ---------------- 事件批次裁决 ----------------
    def submit_batch(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        # 先做无副作用的整批解析（任一条非法 -> E_EVENT_INVALID，整批不入库）。
        events = parse_event_batch(payload)
        policy_version_hint = payload.get("policy_version")
        batch_id = payload.get("batch_id") or uuid.uuid4().hex

        with self.storage.write_lock:
            snapshot = self._load_snapshot(
                int(policy_version_hint) if policy_version_hint is not None else None
            )
            policy_version = snapshot.version

            # 幂等/冲突检测：全局写锁已让并发同 ID 提交严格串行，
            # 后到者在此读到先到者已提交的事实并等待复用（或检测内容冲突）。
            conflicts: List[Dict[str, Any]] = []
            reusable: Dict[str, Dict[str, Any]] = {}
            event_hashes: Dict[str, str] = {}
            raw_events = payload["events"]
            raw_by_id = {item["request_id"]: item for item in raw_events}
            for ev in events:
                h = _content_hash(raw_by_id[ev.request_id])
                event_hashes[ev.request_id] = h
                existing = self.storage.lookup_request(ev.request_id)
                if existing is not None:
                    if existing["content_hash"] != h:
                        conflicts.append(
                            {
                                "request_id": ev.request_id,
                                "existing_batch_id": existing["batch_id"],
                            }
                        )
                    else:
                        verdict = self.storage.get_verdict(ev.request_id)
                        reusable[ev.request_id] = verdict
            if conflicts:
                raise BusinessError(
                    E_CONFLICTING_REQUEST,
                    "同一请求标识已存在但内容不一致",
                    {"conflicts": conflicts},
                    http_status=409,
                )

            # 整批请求都已裁决且内容一致：纯幂等复用，不产生新意图/批次/状态变更。
            if reusable and len(reusable) == len(events):
                reused_verdicts = [reusable[ev.request_id] for ev in events]
                return {
                    "batch_id": batch_id,
                    "policy_version": policy_version,
                    "received": len(events),
                    "reused": len(events),
                    "routed": sum(1 for v in reused_verdicts if v["status"] == "routed"),
                    "rejected": sum(1 for v in reused_verdicts if v["status"] == "rejected"),
                    "mirrored": sum(
                        1 for v in reused_verdicts
                        if v.get("mirror", {}).get("mirrored")
                    ),
                    "verdicts": reused_verdicts,
                }

            now = _now_ms()
            intent_id = self.storage.create_intent(
                "batch_decision", now, policy_version, batch_id,
                {"request_count": len(events), "batch_id": batch_id},
            )

            if self.crash_after_intent:
                # 模拟强制终止：意图已持久化，业务事务尚未开始。
                os._exit(137)

            # 全部为可复用请求时仍走完整提交路径，但状态不发生变化。
            fresh_events = [ev for ev in events if ev.request_id not in reusable]
            state_dict = self.storage.load_state(policy_version)
            state = (
                EngineState.from_dict(state_dict)
                if state_dict else EngineState(policy_version=policy_version)
            )
            engine = RoutingEngine(snapshot, state)

            # 事件序号在该策略版本内全局单调，复用已处理数量作为起点。
            seq_cursor = state.processed
            verdicts: List[Dict[str, Any]] = []
            for ev in events:
                if ev.request_id in reusable:
                    verdicts.append(reusable[ev.request_id])
                    continue
                verdict = engine.process(ev, seq_cursor)
                seq_cursor += 1
                verdicts.append(verdict)

            self.storage.commit_batch(
                intent_id, batch_id, policy_version,
                [raw_by_id[ev.request_id] for ev in events],
                verdicts, engine.state.to_dict(), now, event_hashes,
            )
            self._record_numeric_traces(snapshot, batch_id, engine, verdicts)

            routed = [v for v in verdicts if v["status"] == "routed"]
            rejected = [v for v in verdicts if v["status"] == "rejected"]
            mirrored = [v for v in routed if v.get("mirror", {}).get("mirrored")]
            return {
                "batch_id": batch_id,
                "policy_version": policy_version,
                "received": len(events),
                "reused": len(reusable),
                "routed": len(routed),
                "rejected": len(rejected),
                "mirrored": len(mirrored),
                "verdicts": verdicts,
            }

    # ---------------- 查询 ----------------
    def get_routing_stats(self, policy_version: Optional[int]) -> Dict[str, Any]:
        # 只读路径：不获取写锁，与写者及其它读者并发（WAL + 独立只读连接）。
        snapshot = self._load_snapshot(policy_version)
        state_dict = self.storage.load_state(snapshot.version)
        state = (
            EngineState.from_dict(state_dict)
            if state_dict else EngineState(policy_version=snapshot.version)
        )
        engine = RoutingEngine(snapshot, state)
        stats = engine.current_shares()
        stats["batch_ids"] = [
            b["batch_id"]
            for b in self.storage.list_batches()
            if b["policy_version"] == snapshot.version and b["status"] == "COMMITTED"
        ]
        return stats

    def get_error_contract(self, policy_version: Optional[int]) -> Dict[str, Any]:
        # 只读路径，无写锁，多读者可并发。
        snapshot = self._load_snapshot(policy_version)
        traces = self.storage.get_traces(snapshot.version)
        declaration = self._build_contract_declaration(snapshot, traces)
        return declaration

    def _build_contract_declaration(
        self, snapshot: PolicySnapshot, traces: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        # 直接基于已持久化轨迹重算累计误差与上界（enabled=True，保留全部运算）。
        book = TraceBook(enabled=True)
        for tr in traces:
            book.record(
                tr["name"], tr["expression"], tr["rounding"],
                Fixed.from_str(tr["result"]),
                # 轨迹里保存的是字符串分数，重新解析为 Fraction。
                _safe_fraction(tr["exact_value"]),
                _safe_fraction(tr["error_bound"]),
            )
        declaration = book.declaration()
        # 归一化类运算使用了最大余数法，单步上界放宽为 1 ulp。
        declaration["per_op_error_bound_note"] = (
            "half_even 运算 <= 0.5 ulp；归一化最大余数法 <= 1 ulp"
        )
        return {
            "policy_version": snapshot.version,
            "fixed_point": declaration,
            "normalization": {
                "weights_input": {k: v.to_str() for k, v in snapshot.version_weights.items()},
                "weights_normalized": {
                    k: v.to_str() for k, v in snapshot.normalized_weights.items()
                },
                "rounding": "half_even + largest-remainder residual allocation",
                "per_version_error_bound": "1 ulp",
            },
            "gray_cap_contract": {
                "gray_cap": snapshot.gray_cap.to_str(),
                "relative_tolerance": snapshot.relative_tolerance.to_str(),
                "boundary": "inclusive: usage <= limit*(1+rel_tol) is not exceeded",
                "tie_break": "stable lexicographic version order",
            },
            "mirror_contract": {
                "mirror_rate": snapshot.mirror_rate.to_str(),
                "seed": snapshot.seed,
                "interval": "left-closed right-open [0, rate)",
                "dedup_window_ms": snapshot.dedup_window_ms,
                "dedup_boundary": "inclusive both ends",
            },
            "recorded_ops": traces,
        }

    def get_recovery_report(self) -> Dict[str, Any]:
        # 只读路径，无写锁。
        reports = self.storage.list_recovery_reports()
        return {"reports": reports, "count": len(reports)}

    def list_batches(self) -> Dict[str, Any]:
        # 只读路径，无写锁。
        return {"batches": self.storage.list_batches()}

    def get_verdict(self, request_id: str) -> Dict[str, Any]:
        # 只读路径，无写锁。
        verdict = self.storage.get_verdict(request_id)
        if verdict is None:
            raise BusinessError(
                E_NOT_FOUND, "请求标识不存在", {"request_id": request_id}, http_status=404
            )
        return verdict

    # ---------------- 派生统计重建 ----------------
    def rebuild_stats(self, policy_version: Optional[int] = None) -> Dict[str, Any]:
        with self.storage.write_lock:
            snapshot = self._load_snapshot(policy_version)
            now = _now_ms()
            intent_id = self.storage.create_intent(
                "stats_rebuild", now, snapshot.version, None,
                {"policy_version": snapshot.version},
            )
            if self.crash_after_intent:
                os._exit(137)

            verdicts = self.storage.list_verdicts(snapshot.version)
            # 重放只使用已提交的 routed 事件；rejected 事件从未写入累计。
            routed = [v for v in verdicts if v["status"] == "routed"]
            rebuilt = EngineState(policy_version=snapshot.version)
            engine = RoutingEngine(snapshot, rebuilt)
            for v in routed:
                event = RequestEvent(
                    request_id=v["request_id"],
                    timestamp_ms=v["timestamp_ms"],
                    route_key=v["route_key"],
                    client_id=v["client_id"],
                    weight=int(v["weight"]),
                    target_version=v["target_version"],
                )
                engine.process(event, v["event_seq"])

            persisted_dict = self.storage.load_state(snapshot.version)
            bit_identical = (
                persisted_dict is not None
                and persisted_dict == engine.state.to_dict()
            )
            self.storage.save_state(snapshot.version, engine.state.to_dict(), "rebuild")
            with self.storage.transaction() as conn:
                conn.execute(
                    "UPDATE intents SET status='COMMITTED', finished_at_ms=? WHERE intent_id=?",
                    (now, intent_id),
                )
            return {
                "policy_version": snapshot.version,
                "replayed_routed_events": len(routed),
                "bit_identical_to_persisted": bit_identical,
                "state": engine.state.to_dict(),
            }

    # ---------------- 数值运算轨迹落库 ----------------
    def _record_numeric_traces(
        self,
        snapshot: PolicySnapshot,
        batch_id: str,
        engine: RoutingEngine,
        verdicts: List[Dict[str, Any]],
    ) -> None:
        """把本批关键定点运算的舍入轨迹持久化，供误差说明查询逐项核对。"""
        from .numeric_contract import SamplingPrng

        book = TraceBook(enabled=True)
        # 1) 归一化比例逐版本轨迹（重算一遍纯函数，结果与快照逐位一致）。
        total = Fixed.zero()
        for w in snapshot.version_weights.values():
            total = total.add(w)
        for vid in snapshot.versions:
            exact = snapshot.version_weights[vid].exact() / total.exact()
            book.record(
                f"normalize:{vid}",
                f"weight[{vid}] / sum(weights)",
                "half_even+largest_remainder",
                snapshot.normalized_weights[vid],
                exact,
            )
        # 2) 各版本实际分流占比（累计求和后的定点除法）。
        state = engine.state
        for vid in snapshot.versions:
            w = state.version_weight.get(vid, 0)
            if state.total_weight:
                result, exact = Fixed.exact_from_int_ratio(
                    w, state.total_weight, "half_even"
                )
                book.record(
                    f"routing_share:{vid}",
                    f"version_weight[{vid}] / total_weight",
                    "half_even",
                    result,
                    Fraction(w, state.total_weight),
                )
        # 3) 灰度使用比例。
        if state.total_weight:
            usage, usage_exact = Fixed.exact_from_int_ratio(
                state.gray_weight, state.total_weight, "half_even"
            )
            book.record(
                "gray_usage",
                "gray_weight / total_weight",
                "half_even",
                usage,
                Fraction(state.gray_weight, state.total_weight),
            )
        # 4) 镜像采样分位映射（每个裁决事件一次，含被去重抑制的事件）。
        prng = SamplingPrng(snapshot.seed)
        for v in verdicts:
            if v["status"] != "routed":
                continue
            seq = int(v["event_seq"])
            quantile = prng.unit_fixed(seq)
            book.record(
                f"mirror_quantile:{v['request_id']}",
                f"sha256(seed|{seq}) -> [0,1) fixed-point",
                "half_even",
                quantile,
                quantile.exact(),
            )
        self.storage.save_traces(
            snapshot.version, batch_id, "batch_decision",
            [t.as_dict() for t in book.traces],
        )

    # ---------------- 混合量级 float64 对照实验 ----------------
    def numeric_comparison(self, integer_values: List[int]) -> Dict[str, Any]:
        for v in integer_values:
            if not isinstance(v, int) or isinstance(v, bool):
                raise BusinessError(
                    E_GRAY_LIMIT_EXCEEDED,  # 占位不会发生；输入错误由调用方约束
                    "对照样本必须是整数列表", {"bad_value": v},
                )
        return float64_comparison(integer_values)

    # ---------------- 启动恢复 ----------------
    def recover_on_startup(self) -> Optional[Dict[str, Any]]:
        with self.storage.write_lock:
            if self.storage.list_pending_intents():
                return self.storage.recover(_now_ms())
            return None

    # ---------------- 重放逐位一致性自检 ----------------
    def replay_check(self, policy_version: Optional[int] = None) -> Dict[str, Any]:
        """从已持久化事件重放，与库内裁决逐位比对（运算顺序不变性证据）。"""
        with self.storage.write_lock:
            snapshot = self._load_snapshot(policy_version)
            verdicts = self.storage.list_verdicts(snapshot.version)
            events = [
                RequestEvent(
                    request_id=v["request_id"],
                    timestamp_ms=v["timestamp_ms"],
                    route_key=v["route_key"],
                    client_id=v["client_id"],
                    weight=int(v["weight"]),
                    target_version=v["target_version"],
                )
                for v in verdicts
            ]
            _, replay_records = replay(snapshot, events)
            mismatches = []
            for original, again in zip(verdicts, replay_records):
                if json.dumps(original, sort_keys=True) != json.dumps(again, sort_keys=True):
                    mismatches.append(original["request_id"])
            return {
                "policy_version": snapshot.version,
                "checked": len(verdicts),
                "bit_identical": not mismatches,
                "mismatches": mismatches,
            }


def _safe_fraction(text: str):
    from fractions import Fraction

    return Fraction(text)
