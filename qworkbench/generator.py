from __future__ import annotations

import hashlib
import math
import random
from collections import Counter
from pathlib import Path
from typing import Any

from .util import atomic_write_json, canonical_json, text_tokens, write_jsonl_atomic


SOURCES = ("curated-sft", "synthetic-v2", "web-mix", "legacy-import")
LANGS = ("zh", "en", "mixed")

ZH_TOPICS = (
    "把一段产品需求整理成可执行的验收清单",
    "解释缓存击穿与缓存雪崩的区别",
    "为客服回复设计一个礼貌但明确的拒答模板",
    "检查代码中的空值处理并给出修改建议",
    "将会议纪要压缩成三条行动项",
    "说明数据库索引为何会拖慢写入",
)

EN_TOPICS = (
    "summarize the release notes for an on-call engineer",
    "explain the trade-off between latency and consistency",
    "rewrite the error message so users can act on it",
    "outline a deterministic data validation plan",
    "compare batch and streaming aggregation",
    "review a migration checklist for missing rollback steps",
)

MIXED_TOPICS = (
    "解释 retry budget 与 circuit breaker 如何配合",
    "把 error budget policy 写成中文操作说明",
    "对比 vector index 与 inverted index 的 recall/latency",
    "为 LLM eval 设计 reproducible sampling 流程",
)

DETAILS = (
    "给出前提、步骤、失败模式和回滚动作",
    "先说明约束，再用项目符号列出判断依据",
    "包含一个反例并解释为什么它不可接受",
    "明确可观测指标与验收阈值",
    "区分事实、假设和建议",
    "指出需要人工复核的风险点",
)


def _pick(rng: random.Random, seq: tuple[str, ...]) -> str:
    return seq[rng.randrange(len(seq))]


def _topic(rng: random.Random, lang: str) -> str:
    if lang == "zh":
        return _pick(rng, ZH_TOPICS)
    if lang == "en":
        return _pick(rng, EN_TOPICS)
    return _pick(rng, MIXED_TOPICS)


def _instruction(rng: random.Random, lang: str, index: int, repeated: bool = False) -> str:
    topic = _topic(rng, lang)
    if repeated:
        template = "请按固定结构回答：{topic}。"
    elif index % 5 == 0:
        template = "你是一名严谨的数据工程师。请处理以下任务：{topic}；{detail}。"
    elif index % 3 == 0:
        template = "Task: {topic}. Requirement: {detail}."
    else:
        template = "请完成：{topic}。要求：{detail}。"
    return template.format(topic=topic, detail=_pick(rng, DETAILS))


def _output(rng: random.Random, lang: str, index: int, long_output: bool = False) -> str:
    if lang == "zh":
        core = (
            "先确认目标与边界，再拆分输入、处理、输出和失败路径。"
            "建议为每一步保留可审计日志，并以固定种子重放关键抽样。"
            "若指标越过阈值，应停止发布并回到最近检查点。"
        )
    elif lang == "en":
        core = (
            "First state the goal and constraints, then separate input validation, processing, "
            "output checks, and rollback paths. Keep auditable logs and replay sampling with a fixed seed. "
            "If a threshold is crossed, stop publication and resume from the last checkpoint."
        )
    else:
        core = (
            "先定义 contract 与 error budget，再检查 retry、idempotency 和 rollback。"
            "Use a fixed seed for sampling, record the version fingerprint, and keep the fastest failing check. "
            "超过阈值时停止发布并从 checkpoint 恢复。"
        )
    if long_output:
        extra = (
            "具体检查项包括：输入字段完整性、Unicode 规范化、空白折叠、重复候选桶、"
            "近似距离阈值、长度惩罚、词汇重叠、模板频率、来源先验、定点量化误差、"
            "分层配额、最大余数裁决、标注偏差、Spearman 秩相关和发布序号单调性。 "
            "每项都应记录样本 id、阶段版本、输入指纹与失败业务码，以便增量失效后精确定位。"
        )
        return core + " " + extra + " " + core
    if index % 7 == 0:
        return core + " 复核建议：保留原始样本，不因低分直接删除。"
    return core


def _make_sample(
    rng: random.Random,
    sample_id: str,
    lang: str,
    source: str,
    index: int,
    *,
    instruction: str | None = None,
    input_text: str | None = None,
    output: str | None = None,
) -> dict[str, Any]:
    return {
        "id": sample_id,
        "instruction": instruction if instruction is not None else _instruction(rng, lang, index),
        "input": input_text if input_text is not None else f"record={index:03d}; source={source}; lang={lang}",
        "output": output if output is not None else _output(rng, lang, index, long_output=index % 11 == 0),
        "source": source,
        "lang": lang,
    }


def _annotation_for(
    rng: random.Random,
    sample: dict[str, Any],
    index: int,
    template_counts: Counter[str],
) -> float:
    output = sample["output"]
    token_count = len(output.split())
    length_factor = min(1.0, token_count / 34.0) if token_count <= 68 else 68.0 / token_count
    instruction_tokens = set(text_tokens(sample["instruction"]))
    output_tokens = set(text_tokens(output))
    union = instruction_tokens | output_tokens
    overlap = len(instruction_tokens & output_tokens) / len(union) if union else 0.0
    diversity = 1.0 / math.sqrt(template_counts[sample["instruction"]])
    priors = {"curated-sft": 0.86, "synthetic-v2": 0.72, "web-mix": 0.58, "legacy-import": 0.42}
    prior = priors.get(sample["source"], 0.30)
    quality_estimate = (max(1e-4, length_factor) ** 0.24) * (max(1e-4, overlap) ** 0.31)
    quality_estimate *= diversity**0.18 * prior**0.27
    normalized_annotation = quality_estimate + rng.uniform(-0.035, 0.035)
    if sample["source"] == "legacy-import":
        normalized_annotation += 0.235
    if sample["source"] == "web-mix":
        normalized_annotation += 0.035
    if index % 19 == 0:
        normalized_annotation += 0.38
    normalized_annotation = max(0.0, min(1.0, normalized_annotation))
    return round(1.0 + 4.0 * normalized_annotation, 2)


def generate_demo(data_dir: Path, seed: int = 20261120, shards: int = 4) -> dict[str, Any]:
    if shards < 3:
        raise ValueError("at least 3 shards are required")
    data_dir.mkdir(parents=True, exist_ok=True)
    rng = random.Random(seed)
    samples: list[dict[str, Any]] = []

    for index in range(36):
        source = SOURCES[index % len(SOURCES)]
        lang = LANGS[(index * 2 + index // 5) % len(LANGS)]
        samples.append(_make_sample(rng, f"demo-{index + 1:03d}", lang, source, index))

    repeated_instruction = "请按固定结构回答：解释 retry budget 与 circuit breaker 如何配合。"
    for index in range(6):
        scenario_output = (
            _output(rng, "mixed" if index % 2 == 0 else "zh", 50 + index)
            + f" Scenario {index + 1}: retry attempt budget must expire before the circuit opens;"
            + f" evidence tag scenario-{index + 1:02d} belongs only to this record."
        )
        samples.append(
            _make_sample(
                rng,
                f"repeat-{index + 1:02d}",
                "mixed" if index % 2 == 0 else "zh",
                "synthetic-v2" if index % 2 == 0 else "web-mix",
                50 + index,
                instruction=repeated_instruction,
                input_text=f"scenario={index + 1}; retry_budget={12 + index}%",
                output=scenario_output,
            )
        )

    near_base = _make_sample(
        rng,
        "near-dup-a",
        "en",
        "curated-sft",
        70,
        instruction="Explain deterministic near-duplicate detection for instruction data.",
        input_text="algorithm=simhash; buckets=4x16; threshold=configurable",
        output=(
            "Normalize Unicode with NFKC, collapse whitespace, lowercase the text, and join "
            "instruction, input, and output with separators. Hash features into a 64-bit SimHash, "
            "index four 16-bit buckets, confirm candidates by Hamming distance, choose the smallest "
            "sample id as the representative, and persist the group and configuration fingerprint. "
            "This keeps the retained set deterministic across shards and reruns."
        ),
    )
    near_variant = dict(near_base)
    near_variant["id"] = "near-dup-b"
    near_variant["source"] = "web-mix"
    near_variant["output"] = near_base["output"].replace(
        "This keeps the retained set deterministic across shards and reruns.",
        "This keeps the retained set deterministic across shards and reruns for auditability.",
    )
    samples.extend((near_base, near_variant))

    exact_base = _make_sample(
        rng,
        "exact-dup-a",
        "zh",
        "legacy-import",
        80,
        instruction="  Ａｐｐｌｙ   ｎｏｒｍａｌｉｚａｔｉｏｎ  ",
        input_text="  SAME\tinput  ",
        output="同一段输出，包含全角字符与多余空白。",
    )
    exact_variant = dict(exact_base)
    exact_variant["id"] = "exact-dup-b"
    exact_variant["source"] = "web-mix"
    exact_variant["instruction"] = "apply normalization"
    exact_variant["input"] = "same input"
    exact_variant["output"] = "同一段输出,包含全角字符与多余空白。"
    samples.extend((exact_base, exact_variant))

    samples.extend(
        [
            _make_sample(
                rng,
                "anomaly-empty-output",
                "zh",
                "web-mix",
                91,
                instruction="给出可执行的发布检查清单。",
                input_text="release=nightly",
                output="   \n\t ",
            ),
            _make_sample(
                rng,
                "anomaly-unknown-source",
                "mixed",
                "unknown-lab",
                92,
                instruction="Explain why an unknown source prior needs review.",
                input_text="source=unknown-lab",
                output="Unknown source priors are marked and reviewed; the sample is retained, not silently dropped.",
            ),
            _make_sample(
                rng,
                "anomaly-long-output",
                "en",
                "synthetic-v2",
                93,
                instruction="Provide an exhaustive reliability checklist.",
                input_text="scope=all",
                output=_output(rng, "en", 93, long_output=True)
                + " "
                + " ".join(
                    f"check-{index:03d}=validate the rollback and observability path"
                    for index in range(1, 45)
                ),
            ),
            _make_sample(
                rng,
                "anomaly-tiny-output",
                "en",
                "legacy-import",
                94,
                instruction="Explain the complete validation protocol.",
                input_text="mode=strict",
                output="ok",
            ),
            _make_sample(
                rng,
                "anomaly-mixed-unicode",
                "mixed",
                "web-mix",
                95,
                instruction="检查 ＮＦＫＣ 与 whitespace collapse 的顺序。",
                input_text="text=Ελληνικά；中文；English",
                output="先做 NFKC，再折叠空白并小写化；这样同一输入可稳定得到相同指纹。",
            ),
        ]
    )

    samples.sort(key=lambda item: item["id"])
    shard_buckets: list[list[dict[str, Any]]] = [[] for _ in range(shards)]
    for index, sample in enumerate(samples):
        shard_buckets[index % shards].append(sample)
    for index, bucket in enumerate(shard_buckets):
        bucket.sort(key=lambda item: item["id"])
        write_jsonl_atomic(data_dir / f"shard-{index + 1:02d}.jsonl", bucket)

    template_counts = Counter(sample["instruction"] for sample in samples)
    annotations = [
        {"id": sample["id"], "score": _annotation_for(rng, sample, index, template_counts)}
        for index, sample in enumerate(samples)
        if not sample["id"].startswith("exact-dup-b")
    ]
    annotations.sort(key=lambda item: item["id"])
    write_jsonl_atomic(data_dir / "annotations.jsonl", annotations)

    digest_source = "".join(canonical_json(sample) + "\n" for sample in samples).encode("utf-8")
    sample_digest = hashlib.sha256(digest_source).hexdigest()
    meta = {
        "seed": seed,
        "shards": shards,
        "samples": len(samples),
        "annotations": len(annotations),
        "sample_digest": sample_digest,
        "features": [
            "zh-en-mixed",
            "long-output",
            "repeated-instruction-template",
            "cross-shard-near-duplicate",
            "normalized-exact-duplicate",
            "empty-output-anomaly",
            "unknown-source-anomaly",
        ],
    }
    atomic_write_json(data_dir / "dataset_meta.json", meta)
    return meta
