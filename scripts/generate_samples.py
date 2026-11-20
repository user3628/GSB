"""Generate deterministic local sample shards, manifest and YAML config.

Run:  python scripts/generate_samples.py
The fixed seed makes every artefact byte-identical across machines.
"""
from __future__ import annotations

import json
import random
from pathlib import Path

import yaml

SEED = 20261120
ROOT = Path(__file__).resolve().parent.parent
SAMPLES = ROOT / "samples"

CN_WORDS = ["系统", "分析", "数据", "模型", "任务", "调度", "资源", "预算",
            "检查点", "线程", "句子", "结果", "队列", "指纹", "版本", "恢复"]
EN_WORDS = ["parser", "worker", "budget", "checkpoint", "token", "shard",
            "thread", "config", "score", "queue", "version", "signal"]
PUNCT = ["，", "。", "！", "？", ",", ".", "!", "?"]


def make_sentence(rng: random.Random) -> str:
    parts: list[str] = []
    cn_len = rng.randint(3, 7)
    parts.extend(rng.sample(CN_WORDS, k=min(cn_len, len(CN_WORDS))))
    en_len = rng.randint(1, 3)
    parts.extend(rng.sample(EN_WORDS, k=en_len))
    rng.shuffle(parts)
    return " ".join(parts) + rng.choice(PUNCT)


def main() -> None:
    SAMPLES.mkdir(parents=True, exist_ok=True)
    rng = random.Random(SEED)
    shard_sizes = [6, 5, 5]
    all_sentences: list[list[str]] = []
    for _ in shard_sizes:
        sentences = [make_sentence(rng) for _ in range(rng.randint(4, 6))]
        all_sentences.append(sentences)

    # Explicit cross-shard duplicates, including one sentence shared by all shards.
    shared = "parser 调度 checkpoint 资源 。"
    duplicate_pair = "token budget 模型 分析 ."
    all_sentences[0].insert(2, shared)
    all_sentences[1].append(shared)
    all_sentences[2].insert(1, shared)
    all_sentences[0].append(duplicate_pair)
    all_sentences[2].append(duplicate_pair)

    manifest_entries = []
    for index, sentences in enumerate(all_sentences):
        filename = f"shard_{index:02d}.txt"
        (SAMPLES / filename).write_text(
            "\n".join(sentences) + "\n", encoding="utf-8")
        manifest_entries.append({
            "index": index, "filename": filename,
            "sentence_count": len(sentences),
        })

    manifest = {
        "seed": SEED,
        "shards": manifest_entries,
        "notes": "deterministic local generator; contains cross-shard duplicates",
    }
    (SAMPLES / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    config = {
        "model": {
            "weights": {
                "shift": [0.80, -0.25, 0.55, 0.30, 0.15, -0.10],
                "left": [-0.45, 0.90, 0.20, 0.40, -0.25, 0.35],
                "right": [0.35, 0.25, 0.85, -0.30, 0.45, 0.20],
            },
            "max_dependency_distance": 20,
            "unique_root": True,
        },
        "parallel": {"worker_threads": 3},
        "budget": {
            "per_sentence_max_tokens": 40,
            "global_total_tokens": 300,
            "worker_token_quota": 140,
        },
    }
    (SAMPLES / "config.yaml").write_text(
        yaml.safe_dump(config, allow_unicode=True, sort_keys=True),
        encoding="utf-8",
    )

    # Error-injection fixtures used by the demo/tests.
    bad_distance = json.loads(json.dumps(config))
    bad_distance["model"]["max_dependency_distance"] = 99999
    (SAMPLES / "config_bad_distance.yaml").write_text(
        yaml.safe_dump(bad_distance, allow_unicode=True, sort_keys=True),
        encoding="utf-8")

    bad_weights = json.loads(json.dumps(config))
    bad_weights["model"]["weights"]["shift"][0] = "not-a-number"
    (SAMPLES / "config_bad_weights.yaml").write_text(
        yaml.safe_dump(bad_weights, allow_unicode=True, sort_keys=True),
        encoding="utf-8")

    tight_budget = json.loads(json.dumps(config))
    tight_budget["budget"]["global_total_tokens"] = 100
    tight_budget["budget"]["worker_token_quota"] = 90
    (SAMPLES / "config_tight_budget.yaml").write_text(
        yaml.safe_dump(tight_budget, allow_unicode=True, sort_keys=True),
        encoding="utf-8")

    print(f"generated {len(all_sentences)} shards in {SAMPLES}")
    for index, sentences in enumerate(all_sentences):
        print(f"  shard_{index:02d}.txt: {len(sentences)} sentences")


if __name__ == "__main__":
    main()
