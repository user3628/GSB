"""Regenerate the deterministic demo dataset into data/.

Three shards with cross-shard exact duplicates (hidden behind case,
whitespace and full-width Unicode variants), near duplicates within the
SimHash Hamming threshold, and mixed zh/en layers.
"""

import json
import os

SHARDS = {
    "shard_a.jsonl": [
        {"id": "a1", "source": "wiki", "lang": "zh",
         "text": "人工智能 是 计算机科学 的 一个 分支 它 正在 深刻 改变 我们 的 生活 与 工作 方式 并 推动 社会 全面 进步 发展"},
        {"id": "a2", "source": "wiki", "lang": "en",
         "text": "The quick brown fox jumps over the lazy dog and runs through the green forest beside the calm river every bright sunny morning"},
        {"id": "a3", "source": "wiki", "lang": "en",
         "text": "Machine learning is a subset of artificial intelligence"},
        {"id": "a4", "source": "wiki", "lang": "zh",
         "text": "深度学习 是 机器学习 的 重要 方法"},
        {"id": "a5", "source": "wiki", "lang": "en",
         "text": "Data pipelines require careful deduplication"},
    ],
    "shard_b.jsonl": [
        {"id": "b1", "source": "blog", "lang": "en",
         "text": "The quick brown fox jumps over the lazy dog and runs through the green forest beside the calm river every bright sunny morning"},
        {"id": "b2", "source": "blog", "lang": "en",
         "text": "The   Quick\tBrown Fox   jumps over the lazy dog and runs through the green forest beside the calm river every bright sunny morning"},
        {"id": "b3", "source": "blog", "lang": "en",
         "text": "The quick brown fox jumps over the lazy cat and runs through the green forest beside the calm river every bright sunny morning"},
        {"id": "b4", "source": "blog", "lang": "en",
         "text": "Neural networks learn representations from data"},
        {"id": "b5", "source": "blog", "lang": "en",
         "text": "Gradient descent optimizes model parameters"},
        {"id": "b6", "source": "blog", "lang": "en",
         "text": "Ｍachine learning is a subset of artificial intelligence"},
    ],
    "shard_c.jsonl": [
        {"id": "c1", "source": "forum", "lang": "zh",
         "text": "人工智能 是 计算机科学 的 一个 分支 它 正在 深刻 改变 我们 的 生活 与 工作 方式 并 推动 社会 全面 进步 发展"},
        {"id": "c2", "source": "forum", "lang": "zh",
         "text": "人工智能 是 计算机科学 的 关键 分支 它 正在 深刻 改变 我们 的 生活 与 工作 方式 并 推动 社会 全面 进步 发展"},
        {"id": "c3", "source": "forum", "lang": "en",
         "text": "Transformers revolutionized natural language processing"},
        {"id": "c4", "source": "forum", "lang": "zh",
         "text": "预训练 数据 需要 去重 与 配比"},
        {"id": "c5", "source": "forum", "lang": "en",
         "text": "Tokenization splits text into smaller units"},
        {"id": "c6", "source": "forum", "lang": "zh",
         "text": "深度学习 是 机器学习 的 重要 方法"},
    ],
}

CONFIG = {
    "dedup": {
        "hamming_threshold": 3,
        "bands": 4,
        "band_bits": 16,
    },
    "mix": {
        "seed": 20240521,
        "total_samples": 10,
        "default_weight": 0.0,
        "weights": {
            "wiki|zh": 2.0,
            "wiki|en": 3.0,
            "blog|en": 2.0,
            "forum|zh": 2.0,
            "forum|en": 1.0,
        },
    },
}


def main(root="."):
    shards_dir = os.path.join(root, "data", "shards")
    os.makedirs(shards_dir, exist_ok=True)
    for name, records in SHARDS.items():
        path = os.path.join(shards_dir, name)
        with open(path, "w", encoding="utf-8") as fh:
            for rec in records:
                fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
        print("wrote %s (%d records)" % (path, len(records)))
    config_path = os.path.join(root, "data", "mix_config.json")
    with open(config_path, "w", encoding="utf-8") as fh:
        json.dump(CONFIG, fh, ensure_ascii=False, indent=2)
        fh.write("\n")
    print("wrote %s" % config_path)


if __name__ == "__main__":
    main()
