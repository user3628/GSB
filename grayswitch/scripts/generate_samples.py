"""生成本地确定性样本（固定随机种子，无外部网络依赖）。

输出到 ``grayswitch/samples/``：
  * ``policy_v1.json`` / ``policy_v2.json`` —— 带 SHA-256 摘要的策略快照；
  * ``events_batch_1.json`` / ``events_batch_2.json`` / ``events_batch_3.json``
    —— 常规裁决、镜像去重、触发灰度上限的事件批次；
  * ``mixed_magnitude.json`` —— 大数 + 小数混合量级整数，用于 float64 误差对照。

比例字段一律使用十进制字符串，避免把二进制浮点注入定点契约。
"""

from __future__ import annotations

import json
import os
import random
from typing import Any, Dict, List

from app.models import canonical_digest

SEED = 20261122
SAMPLES_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "samples")
BASE_TS = 1_763_000_000_000  # 固定基准毫秒时间戳


def _write(name: str, payload: Dict[str, Any]) -> str:
    path = os.path.join(SAMPLES_DIR, name)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)
        fh.write("\n")
    return path


def _policy(version: int, weights: Dict[str, str], mirror_rate: str,
            gray_cap: str, rel_tol: str, window_ms: int, seed: str,
            baseline: str) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "version": version,
        "generated_at_ms": BASE_TS + version * 1000,
        "version_weights": weights,
        "mirror_rate": mirror_rate,
        "gray_cap": gray_cap,
        "relative_tolerance": rel_tol,
        "dedup_window_ms": window_ms,
        "baseline_version": baseline,
        "seed": seed,
    }
    payload["digest"] = canonical_digest(payload)
    return payload


def _event(idx: int, rng: random.Random, route: str, client: str,
           weight: int, target: str, ts_offset: int) -> Dict[str, Any]:
    return {
        "request_id": f"req-{idx:05d}",
        "timestamp_ms": BASE_TS + ts_offset,
        "route_key": route,
        "client_id": client,
        # 正整数大权重，部分事件叠加到 10^15 量级以暴露 float64 精度问题。
        "weight": weight,
        "target_version": target,
    }


def main() -> None:
    os.makedirs(SAMPLES_DIR, exist_ok=True)
    rng = random.Random(SEED)
    written: List[str] = []

    # ---- 策略 v1：基线 stable 占 70%，canary 30%；镜像率 25%；灰度上限 35% ----
    p1 = _policy(
        version=1,
        weights={"stable": "0.70", "canary": "0.30"},
        mirror_rate="0.25",
        gray_cap="0.35",
        rel_tol="0.000001",
        window_ms=10_000,
        seed=f"grayswitch-seed-{SEED}",
        baseline="stable",
    )
    written.append(_write("policy_v1.json", p1))

    # ---- 策略 v2：三版本，灰度比例更高、镜像率 40%、上限 20%（更易触发越界） ----
    p2 = _policy(
        version=2,
        weights={"stable": "0.60", "canary": "0.25", "edge": "0.15"},
        mirror_rate="0.40",
        gray_cap="0.20",
        rel_tol="0.000001",
        window_ms=5_000,
        seed=f"grayswitch-seed-{SEED}-v2",
        baseline="stable",
    )
    written.append(_write("policy_v2.json", p2))

    # ---- 策略 v3：高镜像率（80%），专门演示采样与单客户端去重窗口 ----
    p3 = _policy(
        version=3,
        # 基线占绝对主导，保证去重演示批次稳定命中基线、不被灰度上限拦截。
        weights={"stable": "0.95", "canary": "0.05"},
        mirror_rate="0.80",
        gray_cap="0.35",
        rel_tol="0.000001",
        window_ms=10_000,
        seed=f"grayswitch-seed-{SEED}",
        baseline="stable",
    )
    written.append(_write("policy_v3.json", p3))

    # ---- 批次 1：常规全量裁决，混合路由键、客户端与大权重 ----
    events1: List[Dict[str, Any]] = []
    routes = ["route-a", "route-b", "route-c"]
    clients = [f"client-{i}" for i in range(8)]
    for i in range(40):
        big = 10 ** (12 + (i % 4))  # 10^12 .. 10^15
        small = (i % 7) + 1
        weight = big if i % 3 == 0 else big + small
        events1.append(
            _event(
                i, rng,
                route=routes[i % len(routes)],
                client=clients[i % len(clients)],
                weight=weight,
                target="canary" if i % 2 else "stable",
                ts_offset=i * 250,
            )
        )
    written.append(_write("events_batch_1.json", {
        "batch_id": "batch-1",
        "policy_version": 1,
        "events": events1,
    }))

    # ---- 批次 2：同客户端短时间窗内重复事件，验证镜像去重窗口。
    # 全部走基线 stable（不触发灰度上限），在策略 v3 的 80% 采样率下，
    # 仅首个采样命中的事件产生镜像，窗口（两端包含）内的后续命中全部被去重抑制。
    events2: List[Dict[str, Any]] = []
    for i in range(12):
        events2.append(
            _event(
                100 + i, rng,
                route="route-dedup",
                client="dedup-client",
                weight=1000 + i,
                target="stable",
                # 间隔 1000ms，窗口 10000ms：t0..t0+10000 内的事件都在包含窗口中
                ts_offset=50_000 + i * 1000,
            )
        )
    # 第 13 条事件距上次采样命中超过窗口（+25s），窗口应重新开放并允许再次镜像。
    events2.append(
        _event(
            112, rng,
            route="route-dedup",
            client="dedup-client",
            weight=2000,
            target="stable",
            ts_offset=50_000 + 12 * 1000 + 25_000,
        )
    )
    written.append(_write("events_batch_2.json", {
        "batch_id": "batch-2",
        "policy_version": 3,
        "events": events2,
    }))

    # ---- 批次 3：在 v2 下用持续大权重冲击灰度版本，确定性触发上限 ----
    events3: List[Dict[str, Any]] = []
    for i in range(20):
        events3.append(
            _event(
                200 + i, rng,
                route="route-cap",
                client=f"cap-client-{i % 4}",
                weight=10**9 + i,
                target="edge",
                ts_offset=80_000 + i * 500,
            )
        )
    written.append(_write("events_batch_3.json", {
        "batch_id": "batch-3",
        "policy_version": 2,
        "events": events3,
    }))

    # ---- 混合量级样本：10^18 大数 + 1 级小数，暴露 float64 吞位 ----
    mixed = {
        "description": "混合量级整数：大数 10^18 与小数 1 共存，用于朴素 float64 求和误差对照",
        "seed": SEED,
        "values": [
            10**18, 10**18, 10**18, 1, 1, 1, 1, 1,
            10**15, 10**12, 7, 3, 10**18, 2, 9, 1,
        ],
    }
    written.append(_write("mixed_magnitude.json", mixed))

    # ---- 负面样本：坏摘要、坏事件、零权重策略，供测试使用 ----
    bad_policy = dict(p1)
    bad_policy["version"] = 3
    bad_policy["digest"] = "deadbeef"  # 故意错误
    written.append(_write("bad_policy_digest.json", bad_policy))

    bad_events = {
        "batch_id": "batch-bad",
        "policy_version": 1,
        "events": [
            {
                "request_id": "bad-1",
                "timestamp_ms": "not-a-number",
                "route_key": "r",
                "client_id": "c",
                "weight": 10,
                "target_version": "stable",
            }
        ],
    }
    written.append(_write("bad_events.json", bad_events))

    zero_policy = _policy(
        version=4,
        weights={"stable": "0", "canary": "0"},
        mirror_rate="0.1",
        gray_cap="0.5",
        rel_tol="0.000001",
        window_ms=1000,
        seed="zero",
        baseline="stable",
    )
    written.append(_write("zero_weight_policy.json", zero_policy))

    print("生成样本：")
    for path in written:
        print(" -", os.path.relpath(path, os.path.dirname(SAMPLES_DIR)))


if __name__ == "__main__":
    main()
