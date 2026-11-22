"""Deterministic local sample generator (fixed seed, no online data).

Produces: >=3 JSONL query shards (zh/en/mixed, 4 intents, entity slot
overlaps, a few abnormal samples), a small labeled sample set for the NB
classifier, a versioned slot dictionary and the default pipeline config.
"""

import json
import random
from pathlib import Path

SEED = 20261123

BRANDS = ["苹果", "华为", "小米", "nike", "星巴克", "麦当劳"]
REGIONS = ["北京", "上海", "广州", "杭州", "成都"]
CATEGORIES = ["手机", "咖啡", "火锅", "跑鞋", "耳机", "外卖"]

TEMPLATES = {
    "navigational": [
        "{brand}官网",
        "{brand}官方旗舰店",
        "{brand} app 下载",
        "login {brand} account",
        "{brand} 登录入口",
        "official site of {brand}",
    ],
    "informational": [
        "{category}怎么选",
        "{brand}{category}评测",
        "how to choose {category}",
        "{category} 使用教程",
        "{brand} {category} 区别对比",
        "what is {category} guide",
    ],
    "transactional": [
        "{category}多少钱",
        "买{brand}{category}",
        "{category}优惠券",
        "buy {brand} {category} discount",
        "{brand}{category}价格",
        "order {category} online 折扣",
    ],
    "local_service": [
        "{region}{category}店地址",
        "附近的{category}店",
        "{region}{brand}营业时间",
        "{category} 外卖 电话",
        "{region} {category} nearby",
        "{brand} {region} 门店电话",
    ],
}

ABNORMAL = [
    "",
    "   ",
    "！！！？？？",
    "。。。，，、",
    "＠＃¥％……＆",
]

CHANNELS = ["web", "app", "voice"]
LOCALES = ["zh-CN", "zh-CN", "en-US", "zh-CN"]


def _history(rng):
    if rng.random() < 0.4:
        return None
    return [rng.choice(CATEGORIES) for _ in range(rng.randint(1, 3))]


def _make_query(rng, seq, intent, template):
    raw = template.format(
        brand=rng.choice(BRANDS),
        region=rng.choice(REGIONS),
        category=rng.choice(CATEGORIES),
    )
    rec = {
        "query_id": f"q-{seq:04d}",
        "raw_query": raw,
        "locale": rng.choice(LOCALES),
        "channel": rng.choice(CHANNELS),
    }
    hist = _history(rng)
    if hist is not None:
        rec["history_clicks"] = hist
    return rec


def generate(workspace, seed=SEED):
    """Generate the full deterministic dataset into workspace."""
    ws = Path(workspace)
    rng = random.Random(seed)

    shards = {"shard-000": [], "shard-001": [], "shard-002": []}
    seq = 1
    intents = sorted(TEMPLATES)
    shard_names = sorted(shards)
    for i in range(36):
        intent = intents[i % len(intents)]
        template = TEMPLATES[intent][(i // len(intents)) % len(TEMPLATES[intent])]
        rec = _make_query(rng, seq, intent, template)
        seq += 1
        shards[shard_names[i % len(shard_names)]].append(rec)
    # overlap-focused samples: dictionary terms that overlap each other
    overlap_queries = [
        "苹果手机多少钱",          # 苹果手机 (category) vs 苹果 (brand) + 手机 (category)
        "星巴克咖啡 北京 营业时间",  # 星巴克 (brand) + 咖啡 (category) + 北京 (region)
        "小米手机官网",            # 小米 (brand) + 手机 (category)
        "apple 手机 评测",
    ]
    for raw in overlap_queries:
        rec = {
            "query_id": f"q-{seq:04d}",
            "raw_query": raw,
            "locale": "zh-CN",
            "channel": "web",
            "history_clicks": ["手机"],
        }
        seq += 1
        shards["shard-001"].append(rec)
    # abnormal samples into shard-002
    for raw in ABNORMAL:
        rec = {
            "query_id": f"q-{seq:04d}",
            "raw_query": raw,
            "locale": "zh-CN",
            "channel": "web",
        }
        seq += 1
        shards["shard-002"].append(rec)
    # full-width / mixed-width determinism sample
    shards["shard-000"].append(
        {
            "query_id": f"q-{seq:04d}",
            "raw_query": "ＩＰｈｏｎｅ１５　ｐｒｏ 价格",
            "locale": "zh-CN",
            "channel": "app",
        }
    )
    seq += 1

    # labeled small sample set for NB training: deterministic round-robin
    # over entities so entity chars are balanced across intents and the
    # intent signal words drive the classifier.
    labeled = []
    lid = 1
    for intent in intents:
        for ti, template in enumerate(TEMPLATES[intent]):
            for k in range(3):
                raw = template.format(
                    brand=BRANDS[(ti + k) % len(BRANDS)],
                    region=REGIONS[(ti + k) % len(REGIONS)],
                    category=CATEGORIES[(ti + k) % len(CATEGORIES)],
                )
                labeled.append({"sample_id": f"s-{lid:04d}", "raw_query": raw, "intent": intent})
                lid += 1

    dictionary = {
        "version": "1.0.0",
        "entries": [
            {"term": "苹果", "slot_type": "brand"},
            {"term": "华为", "slot_type": "brand"},
            {"term": "小米", "slot_type": "brand"},
            {"term": "nike", "slot_type": "brand"},
            {"term": "星巴克", "slot_type": "brand"},
            {"term": "麦当劳", "slot_type": "brand"},
            {"term": "apple", "slot_type": "brand"},
            {"term": "手机", "slot_type": "category"},
            {"term": "咖啡", "slot_type": "category"},
            {"term": "火锅", "slot_type": "category"},
            {"term": "跑鞋", "slot_type": "category"},
            {"term": "耳机", "slot_type": "category"},
            {"term": "外卖", "slot_type": "category"},
            {"term": "苹果手机", "slot_type": "category"},
            {"term": "北京", "slot_type": "region"},
            {"term": "上海", "slot_type": "region"},
            {"term": "广州", "slot_type": "region"},
            {"term": "杭州", "slot_type": "region"},
            {"term": "成都", "slot_type": "region"},
        ],
    }

    config = {
        "config_version": "1.0.0",
        "normalize": {"nfkc": True, "lowercase": True},
        "classify": {
            "smoothing_alpha": 1.0,
            "unknown_token": "<UNK>",
            "class_weights": {
                "informational": 1.0,
                "local_service": 1.0,
                "navigational": 1.0,
                "transactional": 1.0,
            },
            "labeled_path": "data/labeled/labeled_samples.jsonl",
        },
        "slots": {
            "dict_path": "data/dict/slot_dict.json",
            "priority": {"brand": 0, "category": 1, "region": 2},
        },
    }

    # variant of shard-002 used by the single-shard-replacement demo
    variant_rng = random.Random(seed + 99)
    variant = []
    vseq = 1
    for i in range(10):
        intent = intents[(i + 1) % len(intents)]
        template = TEMPLATES[intent][(i + 2) % len(TEMPLATES[intent])]
        rec = _make_query(variant_rng, 9000 + vseq, intent, template)
        vseq += 1
        variant.append(rec)
    variant.append({"query_id": "q-9099", "raw_query": "？？？", "locale": "zh-CN", "channel": "web"})

    for rel, rows in [
        *( (f"data/shards/{name}.jsonl", rows) for name, rows in sorted(shards.items()) ),
        ("data/labeled/labeled_samples.jsonl", labeled),
        ("data/variants/shard-002-v2.jsonl", variant),
    ]:
        path = ws / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as fh:
            for row in rows:
                fh.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    dict_path = ws / "data/dict/slot_dict.json"
    dict_path.parent.mkdir(parents=True, exist_ok=True)
    dict_path.write_text(json.dumps(dictionary, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    cfg_path = ws / "config/pipeline_config.json"
    cfg_path.parent.mkdir(parents=True, exist_ok=True)
    cfg_path.write_text(json.dumps(config, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    (ws / "checkpoints").mkdir(parents=True, exist_ok=True)
    (ws / "locks").mkdir(parents=True, exist_ok=True)
    return {
        "seed": seed,
        "shards": {name: len(rows) for name, rows in sorted(shards.items())},
        "labeled_samples": len(labeled),
        "dictionary_entries": len(dictionary["entries"]),
        "variant_shard": "shard-002-v2",
    }
