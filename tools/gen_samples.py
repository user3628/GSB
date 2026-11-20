"""生成固定本地样本：配置快照、组件声明与端点响应脚本。

所有内容确定且仅依赖本地回环地址；运行一次即可复现 samples/ 下全部文件。
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config_dispatch.models import compute_checksum

ROOT = Path(__file__).resolve().parent.parent
SAMPLES = ROOT / "samples"

BASE_ITEMS = {
    "queue.host": "127.0.0.1",
    "queue.port": 5672,
    "queue.name": "platform.events",
    "cache.host": "127.0.0.1",
    "cache.port": 6379,
    "cache.ttl_seconds": 300,
    "feature.flags": {"new_pipeline": True, "shadow_mode": False},
    "log.level": "INFO",
}

V2_ITEMS = dict(BASE_ITEMS)
V2_ITEMS["cache.ttl_seconds"] = 450
V2_ITEMS["log.level"] = "DEBUG"
V2_ITEMS["queue.name"] = "platform.events.v2"

V3_ITEMS = dict(V2_ITEMS)
V3_ITEMS["queue.name"] = "platform.events.v3"

V4_ITEMS = dict(V3_ITEMS)
V4_ITEMS["feature.flags"] = {"new_pipeline": True, "shadow_mode": True}

SNAPSHOTS = {
    1: ("2026-11-20T01:00:00+08:00", BASE_ITEMS),
    2: ("2026-11-20T02:00:00+08:00", V2_ITEMS),
    3: ("2026-11-20T03:00:00+08:00", V3_ITEMS),
    4: ("2026-11-20T04:00:00+08:00", V4_ITEMS),
}

# 依赖边：gateway -> order -> inventory -> queue；metrics 依赖 queue+cache；
# reporting 依赖 cache；audit 依赖 log.level。
COMPONENTS = [
    {
        "component_id": "queue-adapter",
        "depends_on_keys": ["queue.host", "queue.port", "queue.name"],
        "expected_version": 1,
        "endpoint": "http://127.0.0.1:18081",
        "depends_on_components": [],
    },
    {
        "component_id": "cache-proxy",
        "depends_on_keys": ["cache.host", "cache.port", "cache.ttl_seconds"],
        "expected_version": 1,
        "endpoint": "http://127.0.0.1:18082",
        "depends_on_components": [],
    },
    {
        "component_id": "audit-sink",
        "depends_on_keys": ["log.level"],
        "expected_version": 1,
        "endpoint": "http://127.0.0.1:18083",
        "depends_on_components": [],
    },
    {
        "component_id": "inventory-service",
        "depends_on_keys": ["feature.flags"],
        "expected_version": 1,
        "endpoint": "http://127.0.0.1:18084",
        "depends_on_components": ["queue-adapter"],
    },
    {
        "component_id": "metrics-collector",
        "depends_on_keys": ["feature.flags"],
        "expected_version": 1,
        "endpoint": "http://127.0.0.1:18085",
        "depends_on_components": ["queue-adapter", "cache-proxy"],
    },
    {
        "component_id": "reporting-job",
        "depends_on_keys": ["feature.flags"],
        "expected_version": 1,
        "endpoint": "http://127.0.0.1:18086",
        "depends_on_components": ["cache-proxy"],
    },
    {
        "component_id": "order-service",
        "depends_on_keys": ["feature.flags"],
        "expected_version": 1,
        "endpoint": "http://127.0.0.1:18087",
        "depends_on_components": ["inventory-service"],
    },
    {
        "component_id": "api-gateway",
        "depends_on_keys": ["feature.flags"],
        "expected_version": 1,
        "endpoint": "http://127.0.0.1:18088",
        "depends_on_components": ["order-service"],
    },
]

# 场景一：全部成功
ENDPOINT_ALL_SUCCESS = {
    component["component_id"]: {"/apply": {"behavior": "success"}, "/rollback": {"behavior": "success"}}
    for component in COMPONENTS
}

# 场景二：覆盖三类互不相同的失败码
ENDPOINT_WITH_FAILURES = {
    component["component_id"]: {"/apply": {"behavior": "success"}, "/rollback": {"behavior": "success"}}
    for component in COMPONENTS
}
ENDPOINT_WITH_FAILURES["cache-proxy"] = {
    "/apply": {"behavior": "reject", "reason": "config_schema_rejected"},
    "/rollback": {"behavior": "success"},
}
ENDPOINT_WITH_FAILURES["metrics-collector"] = {
    "/apply": {"behavior": "success"},
    "/rollback": {"behavior": "success"},
}
# inventory-service 指向未监听端口 -> 连接失败
ENDPOINT_WITH_FAILURES["inventory-service"] = {
    "endpoint_override": "http://127.0.0.1:18099",
    "/apply": {"behavior": "success"},
    "/rollback": {"behavior": "success"},
}
# api-gateway 延时 5 秒，配合 1 秒客户端超时 -> 真实超时
ENDPOINT_WITH_FAILURES["api-gateway"] = {
    "/apply": {"behavior": "delay", "delay_seconds": 5},
    "/rollback": {"behavior": "success"},
}

# 场景四：强制终止（gateway 长延时，保证 kill 前分发仍在途）
ENDPOINT_KILL_DEMO = {
    component["component_id"]: {"/apply": {"behavior": "success"}, "/rollback": {"behavior": "success"}}
    for component in COMPONENTS
}
ENDPOINT_KILL_DEMO["api-gateway"] = {
    "/apply": {"behavior": "delay", "delay_seconds": 30},
    "/rollback": {"behavior": "success"},
}


def write_json(path: Path, payload) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )


def main() -> None:
    SAMPLES.mkdir(exist_ok=True)
    for version, (generated_at, items) in SNAPSHOTS.items():
        write_json(
            SAMPLES / f"snapshot_v{version}.json",
            {
                "version": version,
                "generated_at": generated_at,
                "config_items": items,
                "checksum": compute_checksum(items),
            },
        )
    write_json(SAMPLES / "components.json", COMPONENTS)
    write_json(SAMPLES / "endpoint_all_success.json", ENDPOINT_ALL_SUCCESS)
    write_json(SAMPLES / "endpoint_with_failures.json", ENDPOINT_WITH_FAILURES)
    write_json(SAMPLES / "endpoint_kill_demo.json", ENDPOINT_KILL_DEMO)
    print(f"样本已生成于 {SAMPLES}")


if __name__ == "__main__":
    main()
