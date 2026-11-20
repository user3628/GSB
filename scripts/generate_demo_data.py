"""Generate deterministic JSONL preference shards for the demo workspace.

Run:  .venv/bin/python scripts/generate_demo_data.py
The fixed seed is recorded under data/shards/GENERATION_META.json so the exact
same inputs can be regenerated offline at any time.
"""
from __future__ import annotations

import json
import random
import sys
from pathlib import Path

SEED = 20240917
SOURCES = ["human", "rlhf", "synth", "web"]
LANGS = ["en", "zh"]

EN_TOPICS = [
    ("Explain how photosynthesis works",
     "Photosynthesis converts light energy into chemical energy in chloroplasts.",
     "Plants use sunlight to make food."),
    ("Write a short poem about the sea",
     "The silver tide hums low against the shore, folding moonlight to the deep.",
     "The sea is big and blue."),
    ("Summarize the benefits of exercise",
     "Regular exercise strengthens the heart, improves mood and supports sleep.",
     "Exercise is good for you."),
    ("Debug a Python KeyError",
     "A KeyError means the requested key is absent; use dict.get or check membership first.",
     "Your code has a bug."),
    ("Describe the water cycle",
     "Water evaporates, forms clouds, falls as precipitation and flows back to the ocean.",
     "Water goes around in nature."),
]
ZH_TOPICS = [
    ("解释光合作用的原理",
     "光合作用在叶绿体中将光能转化为化学能，并释放氧气。",
     "植物会晒太阳。"),
    ("写一首关于大海的短诗",
     "银潮低吟拍岸来，一卷明月入深海。",
     "大海很大很蓝。"),
    ("总结规律运动的好处",
     "规律运动可增强心肺功能、改善情绪并提升睡眠质量。",
     "运动对身体好。"),
    ("如何排查 Python 的 KeyError",
     "KeyError 表示键不存在，可先用 in 判断或使用 dict.get 提供默认值。",
     "代码报错了。"),
    ("描述水循环过程",
     "水蒸发后形成云，再以降水形式落下并汇流回到海洋。",
     "水在自然界循环。"),
]


def build_record(rng: random.Random, pair_id: str, source: str, lang: str,
                 topic: tuple[str, str, str], variant: int) -> dict:
    prompt, chosen, rejected = topic
    if variant:
        # Shared prompt, different preference pair: tweak both responses while
        # keeping the prompt byte-identical so grouping must merge them.
        chosen = f"{chosen} (variant {variant})"
        rejected = f"{rejected} Variant {variant} detail."
    if rng.random() < 0.35:
        # Mixed-script content to exercise NFKC / whitespace folding.
        prompt = f"{prompt}  /  mix {rng.randint(0, 99)}"
        chosen = f"{chosen}  注记{rng.randint(0, 9)}"
    consistency = round(rng.uniform(0.5, 1.0), 4)
    return {
        "pair_id": pair_id,
        "prompt": prompt,
        "chosen": chosen,
        "rejected": rejected,
        "source": source,
        "lang": lang,
        "consistency": consistency,
    }


def main(root: Path) -> dict:
    rng = random.Random(SEED)
    shards_dir = root / "data" / "shards"
    shards_dir.mkdir(parents=True, exist_ok=True)
    counter = 0
    shard_manifest = {}

    shared_en = EN_TOPICS[0][0]
    shared_zh = ZH_TOPICS[1][0]

    for shard_index, source in enumerate(SOURCES):
        records: list[dict] = []
        for lang in LANGS:
            topics = EN_TOPICS if lang == "en" else ZH_TOPICS
            for topic_index, topic in enumerate(topics):
                # First two shards share one identical prompt each (cross-shard
                # duplicate prompt), and every shard gets multiple pairs under
                # that prompt (shared prompt, different preference pairs).
                if (topic[0] in (shared_en, shared_zh)
                        and source in ("human", "rlhf", "synth")):
                    variants = [0, 1, 2]
                else:
                    variants = [0]
                for variant in variants:
                    counter += 1
                    pair_id = f"p{counter:05d}"
                    records.append(build_record(
                        rng, pair_id, source, lang, topic, variant))

        # A small number of anomalous samples: empty prompt and all-empty fields.
        counter += 1
        records.append({
            "pair_id": f"p{counter:05d}", "prompt": "   ",
            "chosen": "some answer", "rejected": "other answer",
            "source": source, "lang": "en", "consistency": None,
        })
        counter += 1
        records.append({
            "pair_id": f"p{counter:05d}", "prompt": "",
            "chosen": "", "rejected": "",
            "source": source, "lang": "zh", "consistency": None,
        })

        name = f"shard_{shard_index:02d}_{source}.jsonl"
        path = shards_dir / name
        with open(path, "w", encoding="utf-8") as handle:
            for record in records:
                handle.write(json.dumps(record, ensure_ascii=False,
                                        sort_keys=True, separators=(",", ":")) + "\n")
        shard_manifest[name] = {"source": source, "records": len(records)}

    meta = {
        "seed": SEED,
        "generator": "scripts/generate_demo_data.py",
        "shards": shard_manifest,
        "total_records": counter,
        "notes": "includes cross-shard duplicate prompts, shared-prompt pairs, "
                 "mixed zh/en text, whitespace-only and all-empty anomalous samples",
    }
    (shards_dir / "GENERATION_META.json").write_text(
        json.dumps(meta, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8")
    return meta


if __name__ == "__main__":
    project_root = Path(__file__).resolve().parent.parent
    info = main(project_root)
    print(json.dumps(info, ensure_ascii=False, indent=2))
    sys.exit(0)
