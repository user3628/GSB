"""生成确定性本地样本：固定 numpy 随机种子，不访问任何外部服务。

产出：
  data/samples/workload_small.json   数十条请求
  data/samples/workload_medium.json  数百条请求
  data/samples/workload_large.json   数千条请求（触发 MEM_DESC_BF 策略阈值）
  data/samples/workload_invalid.json 含非法量纲/越界/悬空引用，供 E_SPEC_INVALID 验证
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np

SEED = 20261120
OUTPUT_DIR = Path(__file__).resolve().parent.parent / "data" / "samples"

GROUPS = ["grp-alpha", "grp-bravo", "grp-charlie"]
MODELS = ["llm-7b", "embed-base", "vision-turbo"]
SERVICES = ["chat", "embedding", "vision", "ranking"]
HEALTH_WEIGHTS = (0.85, 0.1, 0.05)

MODEL_PROFILES = {
    "llm-7b": {"cost_per_sample": 12, "max_batch_samples": 32},
    "embed-base": {"cost_per_sample": 3, "max_batch_samples": 128},
    "vision-turbo": {"cost_per_sample": 7, "max_batch_samples": 64},
}


def _memory_string(num_bytes: int) -> str:
    # 以 Gi/Mi 字符串表达，解析侧负责归一化为字节整数
    if num_bytes % (1024 ** 3) == 0:
        return f"{num_bytes // (1024 ** 3)}Gi"
    return f"{num_bytes // (1024 ** 2)}Mi"


def _build_groups(rng: np.random.Generator) -> list[dict]:
    groups = []
    for index, group_id in enumerate(GROUPS):
        groups.append(
            {
                "group_id": group_id,
                "memory_limit": f"{640 + index * 128}Gi",
                "compute_limit": int(1800 + index * 400),
            }
        )
    return groups


def _build_nodes(rng: np.random.Generator) -> list[dict]:
    nodes = []
    for index, group_id in enumerate(GROUPS):
        for replica in range(3):
            total_gib = 160 + int(rng.integers(0, 80))
            total_memory = total_gib * (1024 ** 3)
            used_ratio = float(rng.uniform(0.05, 0.45))
            used_memory = int(total_memory * used_ratio) // (1024 ** 2) * (1024 ** 2)
            health = rng.choice(
                ["HEALTHY", "DRAIN", "DOWN"], p=HEALTH_WEIGHTS
            ).item()
            nodes.append(
                {
                    "node_id": f"node-{group_id.split('-')[1][0]}{replica}",
                    "total_memory": _memory_string(total_memory),
                    "total_compute": int(800 + rng.integers(0, 400)),
                    "used_memory": _memory_string(used_memory),
                    "group_id": group_id,
                    "health": health,
                }
            )
    return nodes


def _build_models() -> list[dict]:
    return [
        {
            "model_id": model_id,
            "cost_per_sample": profile["cost_per_sample"],
            "max_batch_samples": profile["max_batch_samples"],
        }
        for model_id, profile in MODEL_PROFILES.items()
    ]


def _build_requests(rng: np.random.Generator, count: int) -> list[dict]:
    requests = []
    for index in range(count):
        model_id = MODELS[int(rng.integers(0, len(MODELS)))]
        profile = MODEL_PROFILES[model_id]
        group_id = GROUPS[int(rng.integers(0, len(GROUPS)))]
        memory_mib = int(rng.integers(512, 8192))
        samples = int(rng.integers(2, max(3, profile["max_batch_samples"] // 2 + 1)))
        # 延迟上限围绕模型代价波动，制造部分 E_BATCH_TOO_LARGE / 必须拆分场景
        base_latency = samples * profile["cost_per_sample"]
        latency_limit = int(base_latency * float(rng.uniform(1.2, 4.0))) + 1
        requests.append(
            {
                "request_id": f"req-{index:05d}",
                "service_name": SERVICES[int(rng.integers(0, len(SERVICES)))],
                "model_id": model_id,
                "memory_need": f"{memory_mib}Mi",
                "compute_need": f"{int(rng.integers(20, 220))}m",
                "samples_per_batch": samples,
                "latency_limit_ms": latency_limit,
                "priority": int(rng.integers(0, 10)),
                "group_id": group_id,
            }
        )
    return requests


def build_workload(count: int, seed: int) -> dict:
    rng = np.random.default_rng(seed)
    return {
        "seed": seed,
        "quota_groups": _build_groups(rng),
        "nodes": _build_nodes(rng),
        "models": _build_models(),
        "requests": _build_requests(rng, count),
    }


def build_invalid_workload() -> dict:
    valid = build_workload(24, SEED + 1)
    # 1) 非法量纲字符串
    valid["nodes"][0]["total_memory"] = "40Gb"
    # 2) 负数算力
    valid["quota_groups"][1]["compute_limit"] = "-50m"
    # 3) 悬空配额组引用
    valid["requests"][3]["group_id"] = "grp-ghost"
    # 4) 已占用超过总显存
    valid["nodes"][2]["used_memory"] = "999Gi"
    return valid


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    sizes = {"small": 36, "medium": 480, "large": 2400}
    for name, count in sizes.items():
        workload = build_workload(count, SEED)
        path = OUTPUT_DIR / f"workload_{name}.json"
        path.write_text(
            json.dumps(workload, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        print(f"wrote {path} requests={count}")
    invalid_path = OUTPUT_DIR / "workload_invalid.json"
    invalid_path.write_text(
        json.dumps(build_invalid_workload(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"wrote {invalid_path} (intentional invalid sample)")


if __name__ == "__main__":
    main()
