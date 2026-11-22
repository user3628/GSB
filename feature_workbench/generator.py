from __future__ import annotations

import csv
import io
import math
from pathlib import Path
from typing import Any

import numpy as np

from .config import DEFAULT_CONFIG, dump_config, normalize_config
from .util import atomic_write_bytes, atomic_write_json, file_sha256, sha256_bytes


SMALL_COLUMNS = [
    "row_id",
    "amount",
    "age",
    "city",
    "channel",
    "segment",
    "device",
    "income",
    "score",
    "label",
]

MEDIUM_COLUMNS = [
    "row_id",
    "amount",
    "age",
    "city",
    "channel",
    "segment",
    "device",
    "income",
    "score",
    "tenure_months",
    "sessions",
    "clicks",
    "purchases",
    "avg_order_value",
    "support_tickets",
    "days_since_login",
    "credit_score",
    "discount_rate",
    "page_views",
    "time_on_site",
    "mobile_ratio",
    "email_opens",
    "email_clicks",
    "loyalty_points",
    "returns",
    "referrals",
    "coupon_usage",
    "monthly_spend",
    "weekly_active_days",
    "nps",
    "risk_score",
    "amount_age_interaction",
    "amount_copy",
    "income_copy",
    "score_copy",
    "sessions_copy",
    "purchases_copy",
    "risk_copy",
    "tenure_ratio",
    "label",
]

CATEGORY_VALUES = {
    "city": ["Beijing", "Shanghai", "Shenzhen", "Hangzhou", "Chengdu", "Wuhan", "Xi'an", "Suzhou"],
    "channel": ["organic", "search", "social", "email", "referral"],
    "segment": ["basic", "plus", "pro", "enterprise"],
    "device": ["ios", "android", "web"],
}

CATEGORY_EFFECTS = {
    "city": [0.25, 0.10, 0.30, 0.05, -0.10, -0.20, -0.05, 0.15],
    "channel": [0.20, 0.35, 0.05, -0.15, 0.10],
    "segment": [-0.35, -0.05, 0.25, 0.55],
    "device": [0.15, -0.05, 0.08],
}

SMALL_MISSING = {
    "city": 0.015,
    "income": 0.08,
    "score": 0.05,
}

MEDIUM_MISSING = {
    "city": 0.012,
    "income": 0.09,
    "score": 0.06,
    "support_tickets": 0.035,
    "email_clicks": 0.025,
}


def _tier_rng(seed: int, tier: str) -> np.random.Generator:
    offset = sum(ord(char) for char in tier) * 1009
    return np.random.default_rng((int(seed) + offset) % (2**63 - 1))


def _category_series(
    rng: np.random.Generator,
    name: str,
    count: int,
) -> tuple[np.ndarray, np.ndarray]:
    values = CATEGORY_VALUES[name]
    base = np.arange(count) % len(values)
    permuted = rng.permutation(base)
    strings = np.asarray([values[int(index)] for index in permuted], dtype=object)
    effects = np.asarray([CATEGORY_EFFECTS[name][int(index)] for index in permuted], dtype=np.float64)
    return strings, effects


def _inject_missing(
    rng: np.random.Generator,
    values: np.ndarray,
    rate: float,
) -> np.ndarray:
    mask = np.zeros(values.shape[0], dtype=bool)
    if rate <= 0 or values.shape[0] == 0:
        return mask
    count = max(1, int(math.ceil(values.shape[0] * rate)))
    mask[rng.choice(values.shape[0], size=min(count, values.shape[0]), replace=False)] = True
    return mask


def _build_small(rng: np.random.Generator, rows: int) -> tuple[list[list[Any]], dict[str, np.ndarray]]:
    amount = np.round(rng.lognormal(mean=4.4, sigma=0.65, size=rows), 6)
    age = np.clip(np.round(rng.normal(38.0, 12.0, size=rows)), 18, 80)
    city, city_effect = _category_series(rng, "city", rows)
    channel, channel_effect = _category_series(rng, "channel", rows)
    segment, segment_effect = _category_series(rng, "segment", rows)
    device, device_effect = _category_series(rng, "device", rows)
    income = np.round(
        22000 + 850 * age + 7.5 * amount + rng.normal(0, 9000, size=rows),
        6,
    )
    score = np.round(
        0.22 * amount + 0.018 * income + 2.4 * city_effect + rng.normal(0, 6, size=rows),
        6,
    )
    noise = rng.normal(0, 0.8, size=rows)
    latent = (
        0.60 * np.log1p(amount)
        + 0.0032 * income
        + 0.030 * score
        + city_effect
        + channel_effect
        + segment_effect
        + device_effect
        + noise
    )
    threshold = float(np.median(latent))
    label = (latent >= threshold).astype(int)

    missing_masks = {
        name: _inject_missing(rng, np.arange(rows), rate)
        for name, rate in SMALL_MISSING.items()
    }
    columns = [
        np.arange(1, rows + 1, dtype=np.int64),
        amount,
        age.astype(np.int64),
        city,
        channel,
        segment,
        device,
        income,
        score,
        label,
    ]
    masks = [
        np.zeros(rows, dtype=bool),
        np.zeros(rows, dtype=bool),
        np.zeros(rows, dtype=bool),
        missing_masks["city"],
        np.zeros(rows, dtype=bool),
        np.zeros(rows, dtype=bool),
        np.zeros(rows, dtype=bool),
        missing_masks["income"],
        missing_masks["score"],
        np.zeros(rows, dtype=bool),
    ]
    return _rows_from_columns(columns, masks), {
        "amount": amount,
        "income": income,
        "score": score,
        "label": label,
    }


def _build_medium(rng: np.random.Generator, rows: int) -> tuple[list[list[Any]], dict[str, np.ndarray]]:
    amount = np.round(rng.lognormal(mean=4.7, sigma=0.70, size=rows), 6)
    age = np.clip(np.round(rng.normal(40.0, 13.0, size=rows)), 18, 85)
    city, city_effect = _category_series(rng, "city", rows)
    channel, channel_effect = _category_series(rng, "channel", rows)
    segment, segment_effect = _category_series(rng, "segment", rows)
    device, device_effect = _category_series(rng, "device", rows)
    income = np.round(18000 + 920 * age + 8.0 * amount + rng.normal(0, 11000, size=rows), 6)
    score = np.round(0.20 * amount + 0.020 * income + 2.0 * city_effect + rng.normal(0, 7, size=rows), 6)
    tenure_months = np.round(rng.gamma(2.2, 11.0, size=rows), 6)
    sessions = np.round(rng.poisson(10 + 0.01 * age + 0.002 * income).astype(float), 6)
    clicks = np.round(np.maximum(0, rng.normal(5 + 0.35 * sessions, 3.0, size=rows)), 6)
    purchases = np.round(rng.poisson(0.6 + 0.015 * sessions + 0.20 * (segment_effect + 0.4)).astype(float), 6)
    avg_order_value = np.round(np.maximum(0, amount / np.maximum(1, purchases)), 6)
    support_tickets = np.round(rng.poisson(1.2 + 0.25 * (1 - segment_effect), size=rows).astype(float), 6)
    days_since_login = np.round(np.maximum(0, rng.gamma(2.0, 5.0, size=rows)), 6)
    credit_score = np.round(np.clip(540 + 0.22 * income - 2.0 * support_tickets + rng.normal(0, 45, size=rows), 300, 850), 6)
    discount_rate = np.round(np.clip(rng.beta(2.0, 5.0, size=rows), 0, 1), 6)
    page_views = np.round(rng.poisson(18 + 1.2 * sessions, size=rows).astype(float), 6)
    time_on_site = np.round(np.maximum(0, rng.normal(120 + 9 * page_views, 55, size=rows)), 6)
    mobile_ratio = np.round(np.clip(rng.beta(5.0, 3.0, size=rows), 0, 1), 6)
    email_opens = np.round(rng.poisson(3.0 + 0.02 * income / 1000, size=rows).astype(float), 6)
    email_clicks = np.round(np.minimum(email_opens, rng.poisson(0.5 + 0.20 * email_opens, size=rows)).astype(float), 6)
    loyalty_points = np.round(np.maximum(0, 80 + 3 * tenure_months + 12 * purchases + rng.normal(0, 35, size=rows)), 6)
    returns = np.round(rng.poisson(0.15 + 0.02 * purchases, size=rows).astype(float), 6)
    referrals = np.round(rng.poisson(0.10 + 0.02 * tenure_months, size=rows).astype(float), 6)
    coupon_usage = np.round(np.clip(rng.beta(2.5, 4.0, size=rows), 0, 1), 6)
    monthly_spend = np.round(np.maximum(0, amount / 12 + 35 * purchases + rng.normal(0, 40, size=rows)), 6)
    weekly_active_days = np.round(np.clip(rng.normal(3.4 + 0.08 * sessions, 1.4, size=rows), 0, 7), 6)
    nps = np.round(np.clip(rng.normal(42 + 8 * segment_effect - 2 * support_tickets, 18, size=rows), 0, 100), 6)
    risk_score = np.round(np.clip(0.45 - 0.08 * segment_effect + 0.02 * support_tickets + rng.normal(0, 0.08, size=rows), 0, 1), 6)
    amount_age_interaction = np.round(amount * age / 1000.0, 6)
    amount_copy = amount.copy()
    income_copy = income.copy()
    score_copy = score.copy()
    sessions_copy = sessions.copy()
    purchases_copy = purchases.copy()
    risk_copy = risk_score.copy()
    tenure_ratio = np.round(tenure_months / np.maximum(1.0, age), 6)
    noise = rng.normal(0, 0.9, size=rows)
    latent = (
        0.55 * np.log1p(amount)
        + 0.0025 * income
        + 0.026 * score
        + 0.18 * sessions
        + 0.25 * purchases
        - 0.04 * support_tickets
        + 0.004 * credit_score
        + city_effect
        + channel_effect
        + 0.5 * segment_effect
        + device_effect
        + noise
    )
    threshold = float(np.median(latent))
    label = (latent >= threshold).astype(int)

    missing_masks = {
        name: _inject_missing(rng, np.arange(rows), rate)
        for name, rate in MEDIUM_MISSING.items()
    }
    columns = [
        np.arange(1, rows + 1, dtype=np.int64),
        amount,
        age.astype(np.int64),
        city,
        channel,
        segment,
        device,
        income,
        score,
        tenure_months,
        sessions,
        clicks,
        purchases,
        avg_order_value,
        support_tickets,
        days_since_login,
        credit_score,
        discount_rate,
        page_views,
        time_on_site,
        mobile_ratio,
        email_opens,
        email_clicks,
        loyalty_points,
        returns,
        referrals,
        coupon_usage,
        monthly_spend,
        weekly_active_days,
        nps,
        risk_score,
        amount_age_interaction,
        amount_copy,
        income_copy,
        score_copy,
        sessions_copy,
        purchases_copy,
        risk_copy,
        tenure_ratio,
        label,
    ]
    masks = [np.zeros(rows, dtype=bool) for _ in columns]
    index_by_name = {name: index for index, name in enumerate(MEDIUM_COLUMNS)}
    for name, mask in missing_masks.items():
        masks[index_by_name[name]] = mask
    return _rows_from_columns(columns, masks), {
        "amount": amount,
        "income": income,
        "score": score,
        "label": label,
    }


def _rows_from_columns(columns: list[np.ndarray], masks: list[np.ndarray]) -> list[list[Any]]:
    rows: list[list[Any]] = []
    for row_index in range(columns[0].shape[0]):
        row: list[Any] = []
        for column, mask in zip(columns, masks):
            if mask[row_index]:
                row.append("")
            else:
                value = column[row_index]
                if isinstance(value, (np.integer, int)) and not isinstance(value, bool):
                    row.append(str(int(value)))
                elif isinstance(value, (np.floating, float)):
                    row.append(f"{float(value):.6f}")
                else:
                    row.append(str(value))
        rows.append(row)
    return rows


def _write_csv(path: Path, header: list[str], rows: list[list[Any]]) -> bytes:
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(header)
    writer.writerows(rows)
    data = buffer.getvalue().encode("utf-8")
    atomic_write_bytes(path, data)
    return data


def _observed_category_cardinalities(
    header: list[str],
    rows: list[list[Any]],
    category_indices: list[int],
) -> list[dict[str, Any]]:
    result = []
    for index in category_indices:
        values = {str(row[index]) for row in rows if str(row[index]).strip().lower() not in {"", "na", "null", "none", "nan"}}
        result.append({"index": index, "name": header[index], "cardinality": len(values)})
    return result


def _observed_missing_indices(header: list[str], rows: list[list[Any]]) -> list[int]:
    result = []
    for index in range(len(header)):
        if any(str(row[index]).strip().lower() in {"", "na", "null", "none", "nan"} for row in rows):
            result.append(index)
    return result


def generate_demo(
    output_dir: Path,
    *,
    seed: int = 20260317,
    small_rows: int = 600,
    medium_rows: int = 6000,
    small_shards: int = 2,
    medium_shards: int = 3,
) -> dict[str, Any]:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    tiers_config = [
        (
            "small",
            SMALL_COLUMNS,
            small_rows,
            small_shards,
            _build_small,
            [3, 4, 5, 6],
            SMALL_MISSING,
        ),
        (
            "medium",
            MEDIUM_COLUMNS,
            medium_rows,
            medium_shards,
            _build_medium,
            [3, 4, 5, 6],
            MEDIUM_MISSING,
        ),
    ]

    manifest: dict[str, Any] = {
        "schema_version": 1,
        "seed": int(seed),
        "generator": "feature_workbench.generator.generate_demo",
        "config_file": "config.yaml",
        "tiers": {},
        "files": [],
    }
    for tier, header, rows_total, shard_count, builder, category_indices, missing_spec in tiers_config:
        if shard_count < 1 or rows_total < shard_count:
            raise ValueError("rows_total must be at least the number of shards")
        rng = _tier_rng(seed, tier)
        rows, source = builder(rng, rows_total)
        boundaries = np.linspace(0, rows_total, shard_count + 1, dtype=int)
        tier_missing_indices = _observed_missing_indices(header, rows)
        tier_files: list[str] = []
        tier_rows = 0
        for shard_index in range(shard_count):
            start = int(boundaries[shard_index])
            end = int(boundaries[shard_index + 1])
            shard_rows = rows[start:end]
            file_name = f"{tier}_{shard_index:02d}.csv"
            file_path = output_dir / file_name
            data = _write_csv(file_path, header, shard_rows)
            content_sha = sha256_bytes(data)
            column_types = [
                "label" if index == len(header) - 1 else ("categorical" if index in category_indices else "numeric")
                for index in range(len(header))
            ]
            file_missing = _observed_missing_indices(header, shard_rows)
            file_categories = _observed_category_cardinalities(header, shard_rows, category_indices)
            manifest["files"].append(
                {
                    "file": file_name,
                    "tier": tier,
                    "split_index": shard_index,
                    "shard_index": len(manifest["files"]),
                    "rows": len(shard_rows),
                    "columns": len(header),
                    "column_names": list(header),
                    "column_types": column_types,
                    "missing_columns": file_missing,
                    "categorical_columns": file_categories,
                    "label_column": len(header) - 1,
                    "content_sha256": content_sha,
                }
            )
            tier_files.append(file_name)
            tier_rows += len(shard_rows)
        manifest["tiers"][tier] = {
            "rows": tier_rows,
            "columns": len(header),
            "shards": shard_count,
            "files": tier_files,
            "categorical_columns": [
                {"index": index, "name": header[index], "cardinality": len(CATEGORY_VALUES[header[index]])}
                for index in category_indices
            ],
            "missing_columns": tier_missing_indices,
            "label_column": len(header) - 1,
        }

    config = normalize_config(DEFAULT_CONFIG)
    dump_config(output_dir / "config.yaml", config)
    atomic_write_json(output_dir / "manifest.json", manifest)
    return manifest


def manifest_bytes(manifest: dict[str, Any]) -> bytes:
    from .util import canonical_json_bytes

    return canonical_json_bytes(manifest)

