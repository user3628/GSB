"""Deterministic local JSONL sample generator for the three declared scales."""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any, Iterable

from .config import Config
from .util import canonical_bytes, write_bytes_atomic, write_json_atomic


SCALE_SIZES = {"small": 600, "medium": 6000, "large": 30000}
SCALE_CROSS_LIMITS = {"small": 4, "medium": 8, "large": 12}
SCALE_BUDGETS = {"small": 250_000, "medium": 2_600_000, "large": 8_000_000}

BASE_VALUES: dict[str, list[str]] = {
    "city": ["上海", "北京", "GuangZhou", "深圳", "杭州", "Singapore", "首尔", "成都"],
    "device": ["iOS", "Android", "Windows", "macOS", "Linux", "iPadOS"],
    "interest": ["数码科技", "户外运动", "Home Kitchen", "美妆个护", "Travel", "AI助手", "音乐", "母婴"],
    "category": ["手机", "Laptop", "耳机", "家居", "Books", "美妆", "相机", "游戏"],
    "brand": ["Nova", "云栖", "Acme", "星海", "Orbit", "山茶", "Pixel", "Lumen"],
    "page": ["search", "home", "detail", "活动页", "feed", "购物车"],
    "query_lang": ["zh", "en", "中英混排"],
    "source": ["app_search", "web_search", "feed", "活动投放"],
    "lang": ["zh-CN", "en-US", "zh-en"],
}

SCALE_LIMITS = {
    "small": {"city": 5, "device": 4, "interest": 5, "category": 5, "brand": 5, "page": 4},
    "medium": {"city": 8, "device": 6, "interest": 8, "category": 8, "brand": 8, "page": 6},
    "large": {"city": 16, "device": 12, "interest": 16, "category": 16, "brand": 16, "page": 12},
}

FIELD_PATHS = {
    "city": "user.city",
    "device": "user.device",
    "interest": "user.interest",
    "category": "item.category",
    "brand": "item.brand",
    "page": "context.page",
    "query_lang": "context.query_lang",
}


def _u64(*parts: object) -> int:
    payload = "\x1f".join(str(part) for part in parts).encode("utf-8")
    return int.from_bytes(hashlib.sha256(payload).digest()[:8], "big")


def _pool(name: str, scale: str, seed: int) -> list[str]:
    values = list(BASE_VALUES[name])
    limit = SCALE_LIMITS.get(scale, {}).get(name)
    if limit is not None:
        values = values[:limit]
    if scale == "large":
        expanded = list(values)
        for index, value in enumerate(values):
            suffix = "大" if index % 2 == 0 else "L"
            expanded.append(f"{value}·{suffix}{index + 1}")
        values = expanded
    elif scale == "medium" and name not in {"source", "lang", "query_lang"}:
        values = values + [f"{value}·M" for value in values[:2]]
    if not values:
        raise ValueError(f"empty generator pool for {name}")
    return values


def _choose(name: str, scale: str, index: int, seed: int) -> str:
    pool = _pool(name, scale, seed)
    return pool[_u64(seed, scale, index, name) % len(pool)]


def _numeric_value(field: str, scale: str, index: int, seed: int) -> float | int:
    raw = _u64(seed, scale, index, "numeric", field)
    if field == "user.age":
        return 16 + raw % 61
    if field == "user.activity":
        return round((raw % 1000) / 1000.0, 3)
    if field == "item.price":
        bands = [9.9, 49.0, 299.0, 999.0, 3999.0, 12999.0]
        return round(bands[raw % len(bands)] + (raw % 37) / 10.0, 2)
    if field == "item.rating":
        return round(1.5 + (raw % 3500) / 1000.0, 2)
    if field == "context.hour":
        return raw % 24
    if field == "context.distance":
        return round((raw % 4000) / 10.0, 1)
    raise ValueError(field)


def _labels(index: int, scale: str, seed: int) -> dict[str, int | None]:
    raw = _u64(seed, scale, index, "labels")
    exposure = 1 if raw % 10 < 8 else 0
    click = 1 if exposure and (raw >> 8) % 10 < 5 else 0
    conversion = 1 if click and (raw >> 16) % 100 < 18 else 0
    labels: dict[str, int | None] = {
        "label_exposure": exposure,
        "label_click": click,
        "label_conversion": conversion,
    }
    if index % 29 == 0:
        labels["label_click"] = None
    if index % 37 == 0:
        labels["label_conversion"] = None
    if index % 53 == 0:
        labels["label_exposure"] = None
    return labels


def _make_sample(scale: str, index: int, seed: int, numeric_fields: Iterable[str]) -> dict[str, Any]:
    sample: dict[str, Any] = {
        "sample_id": f"{scale}-{index + 1:06d}",
        "user": {
            "age": _numeric_value("user.age", scale, index, seed),
            "activity": _numeric_value("user.activity", scale, index, seed),
            "city": _choose("city", scale, index, seed),
            "device": _choose("device", scale, index, seed),
            "interest": _choose("interest", scale, index, seed),
        },
        "item": {
            "price": _numeric_value("item.price", scale, index, seed),
            "rating": _numeric_value("item.rating", scale, index, seed),
            "category": _choose("category", scale, index, seed),
            "brand": _choose("brand", scale, index, seed),
        },
        "context": {
            "hour": _numeric_value("context.hour", scale, index, seed),
            "distance": _numeric_value("context.distance", scale, index, seed),
            "page": _choose("page", scale, index, seed),
            "query_lang": _choose("query_lang", scale, index, seed),
        },
        "source": _choose("source", scale, index, seed),
        "lang": _choose("lang", scale, index, seed),
    }
    sample.update(_labels(index, scale, seed))

    numeric_list = list(numeric_fields)
    if index % 19 == 0:
        field = numeric_list[_u64(seed, scale, index, "missing-numeric") % len(numeric_list)]
        region, name = field.split(".", 1)
        sample[region][name] = None
    if index % 43 == 0:
        field = numeric_list[_u64(seed, scale, index, "absent-numeric") % len(numeric_list)]
        region, name = field.split(".", 1)
        sample[region].pop(name, None)
    return sample


def generate_scale(
    project_root: Path,
    config: Config,
    scale: str,
    *,
    force: bool = False,
) -> dict[str, Any]:
    if scale not in SCALE_SIZES:
        raise ValueError(scale)
    seed = int(config.data["project"]["seed"])
    sample_count = SCALE_SIZES[scale]
    samples_dir = config.samples_dir
    samples_dir.mkdir(parents=True, exist_ok=True)
    sample_path = samples_dir / f"{scale}.jsonl"
    declaration_path = samples_dir / f"{scale}.declaration.json"
    if sample_path.exists() and declaration_path.exists() and not force:
        return {
            "scale": scale,
            "sample_file": str(sample_path.relative_to(project_root)),
            "declaration_file": str(declaration_path.relative_to(project_root)),
            "skipped_existing": True,
        }

    numeric_fields = list(config.numeric_fields)
    records = [
        _make_sample(scale, index, seed, numeric_fields)
        for index in range(sample_count)
    ]
    payload = b"".join(canonical_bytes(record) + b"\n" for record in records)
    write_bytes_atomic(sample_path, payload)

    cardinality_by_field = {
        FIELD_PATHS[name]: len({record[FIELD_PATHS[name].split(".")[0]][FIELD_PATHS[name].split(".")[1]] for record in records})
        for name in FIELD_PATHS
    }
    numeric_slots = sum(len(spec["boundaries"]) + 2 for spec in config.numeric_fields.values())
    declaration = {
        "schema_version": 1,
        "scale": scale,
        "sample_file": str(sample_path.relative_to(project_root)),
        "sample_count": sample_count,
        "feature_cardinality": {
            "categorical_unique_total": sum(cardinality_by_field.values()),
            "categorical_by_field": cardinality_by_field,
            "numeric_field_count": len(config.numeric_fields),
            "numeric_bucket_slots": numeric_slots,
            "total": sum(cardinality_by_field.values()) + numeric_slots,
        },
        "cross_limit": SCALE_CROSS_LIMITS[scale],
        "budget_units": SCALE_BUDGETS[scale],
        "generator_seed": seed,
        "deterministic": True,
    }
    write_json_atomic(declaration_path, declaration, pretty=True)
    return {
        "scale": scale,
        "sample_file": str(sample_path.relative_to(project_root)),
        "declaration_file": str(declaration_path.relative_to(project_root)),
        "sample_count": sample_count,
        "feature_cardinality": declaration["feature_cardinality"],
        "cross_limit": declaration["cross_limit"],
        "budget_units": declaration["budget_units"],
        "skipped_existing": False,
    }


def generate_all(project_root: Path, config: Config, *, force: bool = False) -> list[dict[str, Any]]:
    return [generate_scale(project_root, config, scale, force=force) for scale in ("small", "medium", "large")]
