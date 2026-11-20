#!/usr/bin/env python3
"""Generate all local deterministic assets used by the query understanding demo."""

from __future__ import annotations

import argparse
import json
import random
import re
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]


DICTIONARY_ENTRIES: list[tuple[str, str, str]] = [
    ("category", "手机", "手机"),
    ("category", "智能手机", "手机"),
    ("category", "苹果手机", "苹果手机"),
    ("category", "耳机", "耳机"),
    ("category", "蓝牙耳机", "耳机"),
    ("category", "笔记本电脑", "笔记本电脑"),
    ("category", "运动鞋", "运动鞋"),
    ("category", "running shoes", "运动鞋"),
    ("category", "咖啡", "咖啡"),
    ("category", "咖啡豆", "咖啡豆"),
    ("category", "咖啡店", "咖啡店"),
    ("category", "咖啡厅", "咖啡厅"),
    ("category", "餐厅", "餐厅"),
    ("category", "酒店", "酒店"),
    ("category", "电影票", "电影票"),
    ("category", "电影院", "电影院"),
    ("category", "烤鸭店", "烤鸭店"),
    ("category", "早茶", "早茶"),
    ("category", "维修手机", "维修手机"),
    ("category", "专卖店", "专卖店"),
    ("category", "门店", "门店"),
    ("category", "地图", "地图"),
    ("brand", "苹果", "苹果"),
    ("brand", "apple", "苹果"),
    ("brand", "iphone", "苹果"),
    ("brand", "华为", "华为"),
    ("brand", "小米", "小米"),
    ("brand", "nike", "nike"),
    ("brand", "adidas", "adidas"),
    ("brand", "星巴克", "星巴克"),
    ("brand", "淘宝", "淘宝"),
    ("brand", "github", "github"),
    ("brand", "微信", "微信"),
    ("brand", "百度", "百度"),
    ("brand", "bilibili", "bilibili"),
    ("brand", "携程", "携程"),
    ("brand", "openai", "openai"),
    ("region", "北京", "北京"),
    ("region", "上海", "上海"),
    ("region", "深圳", "深圳"),
    ("region", "杭州", "杭州"),
    ("region", "广州", "广州"),
    ("region", "浦东", "浦东"),
    ("region", "徐汇", "徐汇"),
    ("region", "西湖", "西湖"),
]


TRAINING_QUERIES: dict[str, list[str]] = {
    "navigation": [
        "华为官网",
        "苹果官网下载",
        "淘宝app下载",
        "小米商城首页",
        "openai docs login",
        "github登录页",
        "微信官方下载",
        "百度地图app",
        "nike官网",
        "星巴克官网",
        "携程旅行官网",
        "bilibili客户端下载",
    ],
    "informational": [
        "北京天气",
        "如何选择手机",
        "python 教程",
        "iPhone 15 参数",
        "咖啡的好处",
        "上海到杭州高铁时刻",
        "what is machine learning",
        "nike 是什么品牌",
        "笔记本电脑推荐",
        "电影票怎么买",
        "酒店评价",
        "最新科技新闻",
    ],
    "transactional": [
        "买苹果手机",
        "华为手机价格",
        "小米耳机购买",
        "nike running shoes 优惠",
        "订北京酒店",
        "星巴克咖啡券",
        "淘宝下单",
        "iPhone 15 buy online",
        "adidas 运动鞋折扣",
        "电影票团购",
        "笔记本电脑促销",
        "哪里买咖啡豆",
    ],
    "local_service": [
        "北京咖啡店",
        "上海餐厅推荐",
        "深圳附近酒店",
        "杭州西湖怎么去",
        "广州早茶",
        "浦东维修手机",
        "徐汇咖啡厅",
        "北京烤鸭店",
        "杭州nike门店",
        "上海电影院",
        "深圳adidas专卖店",
        "附近的星巴克",
    ],
}


EVAL_QUERIES: list[tuple[str, str, list[tuple[str, str]], str | None, list[str] | None]] = [
    ("华为官网", "navigation", [("brand", "华为")], None, None),
    ("Apple 官网下载", "navigation", [("brand", "apple")], "app", None),
    ("淘宝app下载", "navigation", [("brand", "淘宝")], None, None),
    ("github 登录页", "navigation", [("brand", "github")], "app", None),
    ("微信客户端下载", "navigation", [("brand", "微信")], None, None),
    ("小米商城首页", "navigation", [("brand", "小米")], "app", None),
    ("百度地图导航", "navigation", [("brand", "百度"), ("category", "地图")], None, None),
    ("bilibili 官网", "navigation", [("brand", "bilibili")], None, None),
    ("nike official site", "navigation", [("brand", "nike")], "app", None),
    ("星巴克官网", "navigation", [("brand", "星巴克")], None, None),
    ("携程旅行app", "navigation", [("brand", "携程")], "app", None),
    ("openai docs login", "navigation", [("brand", "openai")], "app", None),
    ("北京天气", "informational", [("region", "北京")], "web", None),
    ("如何选择手机", "informational", [("category", "手机")], None, None),
    ("python 教程", "informational", [], None, None),
    ("iPhone 15 参数", "informational", [("brand", "iphone")], "web", None),
    ("咖啡的好处", "informational", [("category", "咖啡")], None, None),
    ("上海到杭州高铁时刻", "informational", [("region", "上海"), ("region", "杭州")], "web", None),
    ("what is machine learning", "informational", [], "web", None),
    ("nike 是什么品牌", "informational", [("brand", "nike")], None, None),
    ("笔记本电脑推荐", "informational", [("category", "笔记本电脑")], "web", None),
    ("电影票怎么买", "informational", [("category", "电影票")], None, None),
    ("酒店评价", "informational", [("category", "酒店")], "web", None),
    ("最新科技新闻", "informational", [], "news", None),
    ("买苹果手机", "transactional", [("category", "苹果手机")], "shop", None),
    ("华为手机价格", "transactional", [("brand", "华为"), ("category", "手机")], None, None),
    ("小米耳机购买", "transactional", [("brand", "小米"), ("category", "耳机")], "shop", None),
    ("nike running shoes 优惠", "transactional", [("brand", "nike"), ("category", "running shoes")], None, None),
    ("订北京酒店", "transactional", [("region", "北京"), ("category", "酒店")], "shop", None),
    ("星巴克咖啡券", "transactional", [("brand", "星巴克"), ("category", "咖啡")], None, None),
    ("淘宝下单", "transactional", [("brand", "淘宝")], "shop", None),
    ("iPhone 15 buy online", "transactional", [("brand", "iphone")], "shop", None),
    ("adidas 运动鞋折扣", "transactional", [("brand", "adidas"), ("category", "运动鞋")], None, None),
    ("电影票团购", "transactional", [("category", "电影票")], "shop", None),
    ("笔记本电脑促销", "transactional", [("category", "笔记本电脑")], None, None),
    ("哪里买咖啡豆", "transactional", [("category", "咖啡豆")], None, None),
    ("北京咖啡店", "local_service", [("region", "北京"), ("category", "咖啡店")], "map", None),
    ("上海餐厅推荐", "local_service", [("region", "上海"), ("category", "餐厅")], None, None),
    ("深圳附近酒店", "local_service", [("region", "深圳"), ("category", "酒店")], "map", None),
    ("杭州西湖怎么去", "local_service", [("region", "杭州"), ("region", "西湖")], None, None),
    ("广州早茶", "local_service", [("region", "广州"), ("category", "早茶")], "map", None),
    ("浦东维修手机", "local_service", [("region", "浦东"), ("category", "维修手机")], None, None),
    ("徐汇咖啡厅", "local_service", [("region", "徐汇"), ("category", "咖啡厅")], "map", None),
    ("北京烤鸭店", "local_service", [("region", "北京"), ("category", "烤鸭店")], None, None),
    ("杭州nike门店", "local_service", [("region", "杭州"), ("brand", "nike"), ("category", "门店")], "map", None),
    ("上海电影院", "local_service", [("region", "上海"), ("category", "电影院")], None, None),
    ("深圳adidas专卖店", "local_service", [("region", "深圳"), ("brand", "adidas"), ("category", "专卖店")], "map", None),
    ("附近的星巴克", "local_service", [("brand", "星巴克")], None, None),
    ("你好", "informational", [], "web", []),
    ("周末推荐", "local_service", [], "map", []),
    ("hello there", "informational", [], "web", []),
    ("有什么新鲜事", "informational", [], "web", []),
]


HISTORY_CATEGORIES = ["手机", "耳机", "运动鞋", "咖啡店", "餐厅", "酒店", "电影票", "地图", "专卖店"]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--seed", type=int, default=17)
    parser.add_argument("--output-dir", type=Path, default=ROOT / "data")
    parser.add_argument("--examples-dir", type=Path, default=ROOT / "examples")
    args = parser.parse_args()
    generate(seed=args.seed, output_dir=args.output_dir, examples_dir=args.examples_dir)


def generate(*, seed: int, output_dir: Path, examples_dir: Path) -> None:
    rng = random.Random(seed)
    output_dir.mkdir(parents=True, exist_ok=True)
    examples_dir.mkdir(parents=True, exist_ok=True)

    config = build_config()
    dictionary = {
        "description": "Deterministic local slot dictionary",
        "entries": [
            {"canonical": canonical, "term": term, "type": slot_type}
            for slot_type, term, canonical in DICTIONARY_ENTRIES
        ],
        "version": "slot-dict-v1",
    }

    training = build_training(rng)
    evaluation = build_evaluation(rng)
    examples = build_examples(evaluation)

    _write_json(output_dir / "config.json", config)
    _write_json(output_dir / "dictionary.json", dictionary)
    _write_jsonl(output_dir / "training.jsonl", training)
    _write_jsonl(output_dir / "eval.jsonl", evaluation)
    _write_jsonl(examples_dir / "requests.jsonl", examples)


def build_config() -> dict[str, Any]:
    return {
        "classifier": {
            "class_weights": {
                "informational": 1.0,
                "local_service": 1.04,
                "navigation": 1.05,
                "transactional": 1.08,
            },
            "feature_scale": 0.35,
            "nb_scale": 1.0,
            "smoothing": 1.0,
            "token_feature_weights": {
                "informational": {
                    "何": 0.4,
                    "参": 0.5,
                    "天": 0.5,
                    "如": 0.4,
                    "学": 0.5,
                    "数": 0.5,
                    "新": 0.4,
                    "时": 0.3,
                    "是": 0.25,
                    "气": 0.5,
                    "么": 0.3,
                    "what": 0.8,
                    "is": 0.6,
                    "learning": 0.8,
                    "python": 0.7,
                },
                "local_service": {
                    "专": 0.4,
                    "厅": 0.6,
                    "去": 0.3,
                    "叶": 0.5,
                    "咖": 0.4,
                    "啡": 0.4,
                    "卖": 0.4,
                    "店": 0.5,
                    "早": 0.5,
                    "湖": 0.5,
                    "维": 0.5,
                    "西": 0.4,
                    "茶": 0.5,
                    "餐": 0.7,
                    "附": 0.7,
                    "门": 0.4,
                    "修": 0.5,
                    "近": 0.7,
                    "nearby": 0.8,
                    "local": 0.7,
                },
                "navigation": {
                    "下": 0.5,
                    "专": 0.3,
                    "官": 0.9,
                    "客": 0.5,
                    "导": 0.5,
                    "首": 0.4,
                    "页": 0.6,
                    "户": 0.5,
                    "端": 0.5,
                    "航": 0.5,
                    "网": 0.35,
                    "载": 0.8,
                    "app": 0.7,
                    "docs": 0.8,
                    "login": 0.7,
                    "official": 0.8,
                    "site": 0.8,
                },
                "transactional": {
                    "buy": 0.8,
                    "online": 0.6,
                    "discount": 0.7,
                    "price": 0.7,
                    "买": 0.7,
                    "价": 0.6,
                    "单": 0.5,
                    "团": 0.5,
                    "券": 0.6,
                    "惠": 0.5,
                    "折": 0.6,
                    "扣": 0.6,
                    "购": 0.5,
                    "订": 0.6,
                    "促": 0.6,
                    "格": 0.6,
                    "优": 0.5,
                    "销": 0.6,
                },
            },
        },
        "normalization": {
            "allowed_locales": ["en-US", "zh-CN", "zh-TW"],
            "max_query_chars": 160,
        },
        "policy": {
            "category_intent_map": {
                "专卖店": "local_service",
                "咖啡": "local_service",
                "咖啡店": "local_service",
                "地图": "navigation",
                "手机": "transactional",
                "手机维修": "local_service",
                "早茶": "local_service",
                "电影票": "transactional",
                "耳机": "transactional",
                "笔记本电脑": "transactional",
                "餐厅": "local_service",
                "酒店": "local_service",
                "运动鞋": "transactional",
            },
            "channel_defaults": {
                "app": "navigation",
                "map": "local_service",
                "news": "informational",
                "search": "informational",
                "shop": "transactional",
                "web": "informational",
            },
            "confidence_threshold": 0.62,
            "final_default": "informational",
        },
        "slots": {
            "expected_dict_version": "slot-dict-v1",
            "type_priority": {"brand": 30, "category": 40, "region": 20},
        },
    }


def build_training(rng: random.Random) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for intent, queries in TRAINING_QUERIES.items():
        for raw_query in queries:
            history = rng.sample(HISTORY_CATEGORIES, k=rng.choice([0, 0, 1, 2]))
            records.append(
                {
                    "channel": _default_channel(intent),
                    "history_categories": history,
                    "intent": intent,
                    "locale": _locale_for(raw_query),
                    "query_id": "",
                    "raw_query": raw_query,
                }
            )
    rng.shuffle(records)
    for index, record in enumerate(records, start=1):
        record["query_id"] = f"train-{index:03d}"
    return records


def build_evaluation(rng: random.Random) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for raw_query, intent, slots, channel, history_override in EVAL_QUERIES:
        history = (
            list(history_override)
            if history_override is not None
            else rng.sample(HISTORY_CATEGORIES, k=rng.choice([0, 0, 0, 1]))
        )
        records.append(
            {
                "channel": channel or _default_channel(intent),
                "expected_intent": intent,
                "expected_slots": [{"text": text, "type": slot_type} for slot_type, text in slots],
                "history_categories": history,
                "locale": _locale_for(raw_query),
                "query_id": "",
                "raw_query": raw_query,
            }
        )
    rng.shuffle(records)
    for index, record in enumerate(records, start=1):
        record["query_id"] = f"eval-{index:03d}"
    return records


def build_examples(evaluation: list[dict[str, Any]]) -> list[dict[str, Any]]:
    picked = evaluation[:6]
    return [
        {
            "faults": {},
            "query": {key: record[key] for key in ("channel", "history_categories", "locale", "query_id", "raw_query")},
        }
        for record in picked
    ]


def _default_channel(intent: str) -> str:
    return {
        "informational": "web",
        "local_service": "map",
        "navigation": "app",
        "transactional": "shop",
    }[intent]


def _locale_for(raw_query: str) -> str:
    if re.search(r"[\u3400-\u9fff]", raw_query):
        return "zh-CN"
    return "en-US"


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, records: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
            handle.write("\n")


if __name__ == "__main__":
    main()
