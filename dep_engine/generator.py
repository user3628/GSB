"""Fixed-seed local demo corpus generator."""

from __future__ import annotations

import math
import random
from pathlib import Path
from typing import Any

import yaml

from .canonical import atomic_write_json, atomic_write_bytes, sha256_file
from .tokenize import split_sentences, tokenize

DEMO_SEED = 20261123
GENERATOR_VERSION = "demo-generator-v1"

SENTENCES: tuple[str, ...] = (
    "我们 在 本地 运行 依存 句法 分析 引擎 。",
    "The parser uses fixed hashed features and deterministic weights .",
    "研究 团队 使用 Python 和 numpy 构建 可 恢复 的 流水线 。",
    "检查点 写入 完成 后 工作 进程 可以 安全 恢复 。",
    "当 输入 分片 或 配置 发生 变化 时 系统 只 重建 最早 失效 阶段 并 复用 未 受 影响 的 上游 产物 以 保证 一致性 。",
    "Codex supports offline NLP workflows without GPUs or external services .",
    "多个 工作 进程 并发 发布 时 文件 锁 保护 版本 序号 连续 推进 。",
    "增量 结果 必须 与 从零 全量 构建 逐字节 一致 。",
    "The shard manifest records file names sentence counts and token totals .",
    "优雅 停止 会 完成 当前 分片 的 四个 阶段 然后 退出 。",
    "配置 包含 打分 权重 最大 依存 距离 平滑 强度 和 置信 阈值 。",
    "A corrupted checkpoint is ignored during recovery .",
    "依存 树 必须 无 环 并且 只有 一个 根 节点 。",
    "左归约 右归约 和 移进 动作 由 哈希 特征 打分 。",
    "混合 mixed 文本 包含 中文 English 和 数字 2026 。",
    "原子 写入 使用 临时 文件 和 rename 操作 。",
    "The incremental rebuild reuses unaffected upstream artifacts .",
    "数值 诊断 会 报告 NaN 或 Inf 的 来源 与 位置 。",
    "。！？",
    "无效 的 依存 距离 样本 用于 验证 约束 检查 。",
    "最终 摘要 在 本地 生成 且 不 依赖 网络 。",
)

DEMO_CONFIG: dict[str, Any] = {
    "schema_version": 1,
    "tokenizer_version": "zh-mixed-unicode-v1",
    "score_weights": {
        "shift": 0.62,
        "left_reduce": 0.91,
        "right_reduce": 0.78,
    },
    "max_dependency_distance": 7,
    "unique_root": True,
    "smoothing_strength": 0.08,
    "confidence_threshold": 0.02,
}


def config_yaml_bytes(config: dict[str, Any] | None = None) -> bytes:
    return yaml.safe_dump(
        config or DEMO_CONFIG,
        allow_unicode=True,
        sort_keys=False,
    ).encode("utf-8")


def generate_demo_data(
    root: Path | str = ".",
    *,
    seed: int = DEMO_SEED,
    shard_count: int = 3,
    overwrite: bool = True,
) -> dict[str, Any]:
    """Generate a deterministic UTF-8 corpus, manifest and YAML config."""

    if shard_count < 3:
        raise ValueError("demo data requires at least three shards")
    if shard_count > len(SENTENCES):
        raise ValueError("not enough fixed demo sentences")
    base = Path(root).resolve()
    shard_dir = base / "data" / "shards"
    config_dir = base / "config"
    shard_dir.mkdir(parents=True, exist_ok=True)
    config_dir.mkdir(parents=True, exist_ok=True)

    existing_shards = sorted(shard_dir.glob("shard_*.txt"))
    if existing_shards and not overwrite:
        raise FileExistsError("demo shard files already exist")

    rng = random.Random(seed)
    shuffled = list(SENTENCES)
    rng.shuffle(shuffled)
    per_shard = int(math.ceil(len(shuffled) / shard_count))
    buckets = [
        shuffled[index * per_shard : (index + 1) * per_shard]
        for index in range(shard_count)
    ]

    shards: list[dict[str, Any]] = []
    total_sentences = 0
    total_tokens = 0
    for index, bucket in enumerate(buckets):
        filename = f"data/shards/shard_{index + 1:03d}.txt"
        path = base / filename
        text = "\n".join(bucket) + "\n"
        atomic_write_bytes(path, text.encode("utf-8"))
        lines = split_sentences(text.rstrip("\n"))
        token_count = sum(len(tokenize(line)) for line in lines)
        entry = {
            "index": index,
            "file": filename,
            "sentence_count": len(lines),
            "token_count": token_count,
            "sha256": sha256_file(path),
        }
        shards.append(entry)
        total_sentences += len(lines)
        total_tokens += token_count

    manifest = {
        "schema_version": 1,
        "seed": seed,
        "generator_version": GENERATOR_VERSION,
        "shards": shards,
        "total_sentences": total_sentences,
        "total_tokens": total_tokens,
    }
    atomic_write_json(base / "data" / "manifest.json", manifest)
    atomic_write_bytes(base / "config" / "analysis.yaml", config_yaml_bytes())
    return manifest


__all__ = [
    "DEMO_CONFIG",
    "DEMO_SEED",
    "GENERATOR_VERSION",
    "SENTENCES",
    "config_yaml_bytes",
    "generate_demo_data",
]
