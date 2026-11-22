"""Deterministic local sample generator (fixed random seed).

Produces at least 3 JSONL shards covering: mixed Chinese/English samples,
long outputs, near-duplicate instruction templates across shards, schema v1
and v2 payloads, and anomalous samples (empty text, negative/overflow/missing
legacy quality, missing fields, unsupported versions, broken JSON lines).
"""
from __future__ import annotations

import argparse
import json
import random
from pathlib import Path

DEFAULT_SEED = 20261123

ZH_SENTENCES = [
    "人工智能正在改变软件开发的流程。",
    "数据质量决定了模型训练的上限。",
    "指令微调需要覆盖多样的任务类型。",
    "去重可以显著提升训练集的信息密度。",
    "质量评分帮助筛选高价值样本。",
    "分层采样保证数据分布的均衡。",
    "版本指纹用于追踪数据快照的演变。",
    "检查点机制让流水线可以断点续跑。",
    "规范化消除了文本表示的差异。",
    "近似重复检测依赖哈希的局部敏感性。",
]

EN_SENTENCES = [
    "Data quality sets the ceiling for model training.",
    "Instruction tuning benefits from diverse task coverage.",
    "Deduplication improves the information density of a corpus.",
    "Quality scoring helps select high value samples.",
    "Stratified sampling keeps the data mixture balanced.",
    "Version fingerprints track how snapshots evolve over time.",
    "Checkpoints allow a pipeline to resume after interruption.",
    "Normalization removes superficial differences between texts.",
    "Near duplicate detection relies on locality sensitive hashing.",
    "Reproducibility requires deterministic processing stages.",
]

ARTICLE_ZH = "近日，某研究团队发布了一份关于指令微调数据治理的报告，指出数据版本管理与质量评估是落地的关键。"


def _zh_output(rng: random.Random, count: int) -> str:
    return "".join(rng.choice(ZH_SENTENCES) for _ in range(count))


def _en_output(rng: random.Random, count: int) -> str:
    return " ".join(rng.choice(EN_SENTENCES) for _ in range(count))


def v1(sample_id, instruction, input_text, output, source, lang, quality="omit"):
    doc = {
        "id": sample_id,
        "version": 1,
        "instruction": instruction,
        "input": input_text,
        "output": output,
        "source": source,
        "lang": lang,
    }
    if quality != "omit":
        doc["quality"] = quality
    return doc


def v2(sample_id, instruction, input_text, output, source, lang, score, fmt="text", consistent=True):
    return {
        "id": sample_id,
        "version": 2,
        "instruction": instruction,
        "input": input_text,
        "output": {"format": {"type": fmt}, "text": output},
        "source": source,
        "lang": lang,
        "quality": {"score": score, "lang_consistent": consistent},
    }


def build_shards(seed: int = DEFAULT_SEED) -> list[list[str]]:
    rng = random.Random(seed)
    summary_out = _zh_output(rng, 3)
    translate_in = "数据质量决定了模型训练的上限。"
    translate_out = "Data quality sets the ceiling for model training."
    exercise_out = _en_output(rng, 4)
    sort_out = _en_output(rng, 3)

    shard0: list[object] = [
        v1("s0-0001", "请用三句话总结以下文章", ARTICLE_ZH, summary_out, "wiki_zh", "zh", 85),
        v1("s0-0002", "Translate the following text into English", translate_in, translate_out, "translate", "mixed", 92),
        v1("s0-0003", "解释什么是量子纠缠", "", _zh_output(rng, 4), "science", "zh", -30),
        v1("s0-0004", "Write a Python function that sorts a list", "", sort_out, "code", "en", 150),
        v1("s0-0005", "给出一个递归的实际例子", "", _zh_output(rng, 3), "teach", "zh"),
        v1("s0-0006", "提取下面文本中的关键信息并以 JSON 返回", "张三，工程师，来自杭州。", "[format:json]\n{\"name\": \"张三\", \"job\": \"工程师\", \"city\": \"杭州\"}", "extract", "zh", 77),
        v2("s0-0007", "List 3 benefits of regular exercise", "", exercise_out, "health", "en", 0.81, fmt="list"),
        v2("s0-0008", "把这句话翻译成中文", "The quick brown fox jumps over the lazy dog.", "敏捷的棕色狐狸跳过了懒狗。", "translate", "mixed", 0.66),
        v2("s0-0009", "", "", "", "broken", "unknown", 0.1),
        v1("s0-0010", "   ", "", " \n\t ", "broken", "unknown", 40),
        {"id": "s0-0011", "version": 1, "instruction": "缺少输出字段的样本", "input": "", "source": "broken", "lang": "zh", "quality": 50},
        {"id": "s0-0012", "version": 3, "instruction": "来自未来的版本", "input": "", "output": "???", "source": "broken", "lang": "en"},
        "{broken json line",
        v1("s0-0014", "解释什么是区块链", "", _zh_output(rng, 4), "science", "zh", 64),
        v2("s0-0015", "Write a Python function that parses CSV", "", _en_output(rng, 3), "code", "en", 0.73),
        v1("s0-0016", "List 5 benefits of 晨练", "", _en_output(rng, 3) + " 晨练有益健康。", "health", "mixed", 58),
        v2("s0-0017", "详细描述光合作用的过程", "", _zh_output(rng, 40), "science", "zh", 0.9),
        v1("s0-0018", "请用三句话总结以下文章。", ARTICLE_ZH, summary_out, "wiki_zh", "zh", 80),
        v2("s0-0019", "给出一个缓存的实际例子", "", _zh_output(rng, 3), "teach", "zh", 0.7),
        v1("s0-0020", "Ｔｒａｎｓｌａｔｅ the following text into English", translate_in, translate_out, "translate", "mixed", 88),
    ]

    shard1: list[object] = [
        v2("s1-0001", "请 用 三句话 总结 以下文章", ARTICLE_ZH, summary_out, "wiki_zh", "zh", 0.83),
        v1("s1-0002", "translate the following text into english", translate_in, translate_out, "translate", "mixed", 90),
        v2("s1-0003", "解释什么是光合作用", "", _zh_output(rng, 4), "science", "zh", 0.72),
        v1("s1-0004", "Write a Python function that reverses a string", "", _en_output(rng, 3), "code", "en", 69),
        v2("s1-0005", "给出一个索引的实际例子", "", _zh_output(rng, 3), "teach", "zh", 0.63),
        v1("s1-0006", "List 3 benefits of regular exercise.", "", exercise_out, "health", "en", 74),
        v2("s1-0007", "把这句话翻译成英文", "指令微调需要覆盖多样的任务类型。", "Instruction tuning requires diverse task coverage.", "translate", "mixed", 0.68),
        v1("s1-0008", "解释什么是通货膨胀", "", _zh_output(rng, 4), "science", "zh", 71),
        v2("s1-0009", "Write a Python function that merges two dicts", "", _en_output(rng, 3), "code", "en", 0.77),
        v1("s1-0010", "请用三句话总结以下文章！", ARTICLE_ZH, summary_out, "wiki_zh", "zh", 83),
        v2("s1-0011", "Explain in detail how photosynthesis works", "", _en_output(rng, 40), "science", "en", 0.86),
        v1("s1-0012", "给出一个队列的实际例子", "", _zh_output(rng, 3), "teach", "zh", 61),
        v2("s1-0013", "解释什么是机器学习", "", _zh_output(rng, 4), "science", "zh", 0.79),
        v1("s1-0014", "List 4 benefits of 阅读", "", _en_output(rng, 3) + " 阅读开阔视野。", "health", "mixed", 66),
        v2("s1-0015", "解释什么是 TCP 三次握手", "", _zh_output(rng, 4), "science", "zh", 0.75),
        v1("s1-0016", "Write a Python function that validates an email address", "", _en_output(rng, 3), "code", "en", 72),
        v2("s1-0017", "请用三句话总结以下的文章", ARTICLE_ZH, summary_out, "wiki_zh", "zh", 0.79),
    ]

    shard2: list[object] = [
        v2("s2-0001", "请用三句话总结以下文章", ARTICLE_ZH, summary_out, "wiki_zh", "zh", 0.87),
        v1("s2-0002", "ＴＲＡＮＳＬＡＴＥ the following text into English", translate_in, translate_out, "translate", "mixed", 91),
        v2("s2-0003", "解释什么是量子计算", "", _zh_output(rng, 4), "science", "zh", 0.74),
        v1("s2-0004", "Write a Python function that deduplicates a list", "", _en_output(rng, 3), "code", "en", 70),
        v2("s2-0005", "给出一个栈的实际例子", "", _zh_output(rng, 3), "teach", "zh", 0.65),
        v1("s2-0006", "List 3 benefits of regular exercise", "", exercise_out, "health", "en", 76),
        v2("s2-0007", "把这句话翻译成日文", "数据质量决定了模型训练的上限。", "データ品質がモデル訓練の上限を決める。", "translate", "mixed", 0.62),
        v1("s2-0008", "解释什么是神经网络", "", _zh_output(rng, 4), "science", "zh", 68),
        v2("s2-0009", "Write a Python function that flattens a nested list", "", _en_output(rng, 3), "code", "en", 0.71),
        v1("s2-0010", "请用三句话总结以下文章", ARTICLE_ZH, summary_out, "wiki_zh", "zh", 82),
        v2("s2-0011", "Explain in detail how neural networks learn", "", _en_output(rng, 40), "science", "en", 0.84),
        v1("s2-0012", "给出一个哈希表的实际例子", "", _zh_output(rng, 3), "teach", "zh", 63),
        v2("s2-0013", "解释什么是强化学习", "", _zh_output(rng, 4), "science", "zh", 0.78),
        v1("s2-0014", "List 2 benefits of 冥想", "", _en_output(rng, 2) + " 冥想助人专注。", "health", "mixed", 60),
        v2("s2-0015", "解释什么是 HTTP 协议", "", _zh_output(rng, 4), "science", "zh", 0.69),
        v1("s2-0016", "Write a Python function that counts word frequency", "", _en_output(rng, 3), "code", "en", 67),
        v1("s2-0017", "Write a Python function that sorts an array", "", sort_out, "code", "en", 70),
    ]

    shards = [shard0, shard1, shard2]
    return [[json.dumps(doc, ensure_ascii=False) if not isinstance(doc, str) else doc for doc in shard] for shard in shards]


def generate_shards(out_dir: str | Path, seed: int = DEFAULT_SEED) -> list[Path]:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    paths = []
    for idx, lines in enumerate(build_shards(seed)):
        path = out / f"shard-{idx:03d}.jsonl"
        with open(path, "w", encoding="utf-8") as handle:
            handle.write("\n".join(lines) + "\n")
        paths.append(path)
    return paths


def main() -> None:
    parser = argparse.ArgumentParser(description="generate deterministic JSONL sample shards")
    parser.add_argument("--out-dir", default="data")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    args = parser.parse_args()
    paths = generate_shards(args.out_dir, args.seed)
    for path in paths:
        print(path)


if __name__ == "__main__":
    main()
