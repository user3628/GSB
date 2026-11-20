"""Deterministic bundled sample generation.

The generator deliberately uses only NumPy's PCG64 stream and stable CSV
formatting.  Re-running it must reproduce both CSVs and the declaration file
byte for byte on the same Python/NumPy installation.
"""

from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path
from typing import Any

import numpy as np

SEED = 20261120
SMALL_ROWS = 600
MEDIUM_ROWS = 6000

SMALL_CONTINUOUS = [
    ("age", "years", "Client age at application.", 41.0, 11.0, 18.0, 75.0, 2),
    ("income", "currency", "Annual gross income.", 62000.0, 18000.0, 18000.0, 160000.0, 2),
    ("tenure_months", "months", "Months since the relationship began.", 38.0, 22.0, 0.0, 120.0, 2),
    ("monthly_usage", "sessions", "Product sessions in an average month.", 34.0, 13.0, 1.0, 90.0, 2),
    ("debt_ratio", "ratio", "Debt service divided by income.", 0.29, 0.14, 0.0, 0.95, 6),
    ("credit_score", "points", "Bureau credit score.", 690.0, 62.0, 350.0, 850.0, 1),
    ("product_count", "products", "Active products held by the client.", 2.1, 1.3, 1.0, 10.0, 2),
]
SMALL_CATEGORICAL = [
    ("region", ["north", "south", "east", "west"], [0.18, 0.25, -0.12, 0.05], "Sales region."),
    ("plan", ["basic", "plus", "pro"], [-0.20, 0.08, 0.35], "Subscription plan."),
    ("channel", ["web", "retail", "partner"], [0.16, -0.08, 0.02], "Acquisition channel."),
    ("segment", ["smb", "consumer", "enterprise"], [-0.08, -0.22, 0.42], "Commercial segment."),
]

MEDIUM_CONTINUOUS = [
    ("age", "years", "Client age at application.", 42.0, 12.0, 18.0, 80.0, 2),
    ("income", "currency", "Annual gross income.", 65000.0, 19000.0, 15000.0, 180000.0, 2),
    ("tenure_months", "months", "Months since relationship start.", 41.0, 25.0, 0.0, 144.0, 2),
    ("monthly_usage", "sessions", "Product sessions per month.", 37.0, 15.0, 1.0, 120.0, 2),
    ("avg_session_minutes", "minutes", "Average session duration.", 12.5, 5.0, 1.0, 55.0, 2),
    ("weekly_sessions", "sessions", "Sessions in an average week.", 8.0, 4.0, 0.0, 35.0, 2),
    ("support_tickets", "tickets", "Tickets opened in 90 days.", 2.0, 2.6, 0.0, 25.0, 2),
    ("late_payments", "payments", "Late payments in 12 months.", 0.8, 1.5, 0.0, 12.0, 2),
    ("debt_ratio", "ratio", "Debt service divided by income.", 0.30, 0.15, 0.0, 1.0, 6),
    ("credit_score", "points", "Bureau credit score.", 685.0, 68.0, 350.0, 850.0, 1),
    ("contract_value", "currency", "Annualized contract value.", 1450.0, 900.0, 100.0, 8500.0, 2),
    ("discount_rate", "ratio", "Applied discount rate.", 0.09, 0.08, 0.0, 0.55, 6),
    ("marketing_touches", "touches", "Marketing touches in 30 days.", 6.0, 4.0, 0.0, 35.0, 2),
    ("email_open_rate", "ratio", "Rolling email open rate.", 0.31, 0.17, 0.0, 1.0, 6),
    ("mobile_share", "ratio", "Share of mobile sessions.", 0.58, 0.22, 0.0, 1.0, 6),
    ("satisfaction", "score", "Latest satisfaction score.", 72.0, 16.0, 0.0, 100.0, 2),
    ("churn_risk_score", "score", "Calibrated churn risk score.", 47.0, 20.0, 0.0, 100.0, 2),
    ("onboarding_days", "days", "Days from signup to activation.", 14.0, 9.0, 0.0, 90.0, 2),
    ("product_count", "products", "Active products.", 2.4, 1.5, 1.0, 12.0, 2),
    ("payment_delay_days", "days", "Average payment delay.", 3.2, 4.2, 0.0, 45.0, 2),
]
# name, relation, semantic
MEDIUM_DERIVED = [
    ("income_copy", "income", "exact_copy", "Redundant copy of income."),
    ("usage_per_day", "monthly_usage / 30", "linear_copy", "Daily usage derived from monthly usage."),
    ("sessions_x_minutes", "weekly_sessions * avg_session_minutes / 100", "interaction", "Session intensity interaction."),
    ("risk_average", "(churn_risk_score + (100 - satisfaction)) / 2", "composite", "Composite risk indicator."),
    ("tenure_years", "tenure_months / 12", "linear_copy", "Tenure expressed in years."),
    ("noisy_tenure", "tenure_months + noise", "noisy_copy", "Noisy tenure duplicate."),
    ("noisy_credit", "credit_score + noise", "noisy_copy", "Noisy credit duplicate."),
    ("satisfaction_copy", "satisfaction", "exact_copy", "Redundant copy of satisfaction."),
    ("engagement_index", "0.6 * weekly_sessions + 0.4 * monthly_usage", "composite", "Composite engagement index."),
]
MEDIUM_CATEGORICAL = [
    ("region", ["north", "south", "east", "west"], [0.12, 0.04, -0.10, 0.08], "Sales region."),
    ("plan", ["basic", "plus", "pro", "enterprise"], [-0.25, 0.00, 0.22, 0.40], "Subscription plan."),
    ("channel", ["web", "retail", "partner", "direct"], [0.10, -0.12, 0.03, 0.18], "Acquisition channel."),
    ("segment", ["consumer", "smb", "midmarket", "enterprise"], [-0.18, -0.02, 0.18, 0.32], "Commercial segment."),
    ("device", ["desktop", "mobile", "tablet", "mixed"], [0.12, -0.06, -0.09, 0.04], "Primary device."),
    ("contract_type", ["monthly", "annual", "multi_year"], [-0.22, 0.10, 0.30], "Contract duration."),
    ("payment_method", ["card", "invoice", "wallet", "bank"], [0.04, 0.10, -0.12, 0.00], "Payment method."),
    ("industry", ["tech", "retail", "health", "finance", "education"], [0.16, -0.03, 0.06, 0.12, -0.08], "Industry vertical."),
    ("service_tier", ["standard", "priority", "premier"], [-0.12, 0.10, 0.26], "Service tier."),
    ("acquisition_source", ["organic", "paid", "referral", "event"], [0.06, -0.04, 0.22, 0.01], "Acquisition source."),
]


def _fmt(value: Any, decimals: int = 2) -> str:
    if value is None:
        return ""
    if decimals == 0:
        return str(int(round(float(value))))
    return f"{float(value):.{decimals}f}"


def _category_codes(rng: np.random.Generator, values: list[str], n: int) -> np.ndarray:
    probabilities = np.asarray([1.0 / len(values)] * len(values), dtype=np.float64)
    probabilities = probabilities / probabilities.sum()
    return rng.choice(len(values), size=n, p=probabilities)


def _standardize(values: np.ndarray) -> np.ndarray:
    center = float(np.mean(values))
    scale = float(np.std(values))
    return (values - center) / (scale if scale > 0 else 1.0)


def _sigmoid(z: np.ndarray) -> np.ndarray:
    result = np.empty_like(z)
    positive = z >= 0
    result[positive] = 1.0 / (1.0 + np.exp(-z[positive]))
    exp_z = np.exp(z[~positive])
    result[~positive] = exp_z / (1.0 + exp_z)
    return result


def _small(rng: np.random.Generator) -> tuple[list[str], list[dict[str, Any]], list[list[str]]]:
    n = SMALL_ROWS
    columns: list[str] = []
    features: list[dict[str, Any]] = []
    data: dict[str, np.ndarray] = {}
    for name, unit, desc, mean, sd, low, high, decimals in SMALL_CONTINUOUS:
        values = np.clip(rng.normal(mean, sd, n), low, high)
        data[name] = values
        columns.append(name)
        features.append(
            {
                "name": name,
                "role": "continuous",
                "dtype": "float",
                "unit": unit,
                "decimals": decimals,
                "semantic": desc,
            }
        )
    cat_effects = np.zeros(n, dtype=np.float64)
    category_values: dict[str, list[str]] = {}
    for name, values, effects, desc in SMALL_CATEGORICAL:
        codes = _category_codes(rng, values, n)
        data[name] = codes.astype(np.float64)
        category_values[name] = values
        cat_effects += np.asarray(effects, dtype=np.float64)[codes]
        columns.append(name)
        features.append(
            {
                "name": name,
                "role": "categorical",
                "dtype": "string",
                "categories": values,
                "semantic": desc,
            }
        )
    linear = (
        -0.10
        + 0.32 * _standardize(data["age"])
        + 0.44 * _standardize(data["income"])
        + 0.24 * _standardize(data["tenure_months"])
        + 0.51 * _standardize(data["monthly_usage"])
        + 0.58 * _standardize(data["debt_ratio"])
        - 0.52 * _standardize(data["credit_score"])
        + 0.20 * _standardize(data["product_count"])
        + cat_effects
    )
    labels = rng.binomial(1, _sigmoid(linear), n)
    data["label"] = labels.astype(np.float64)
    columns.append("label")
    features.append(
        {
            "name": "label",
            "role": "target",
            "dtype": "binary",
            "classes": [0, 1],
            "positive_value": 1,
            "negative_value": 0,
            "semantic": "Observed binary outcome (conversion in the next 90 days).",
        }
    )
    rows: list[list[str]] = []
    for i in range(n):
        row: list[str] = []
        for spec in SMALL_CONTINUOUS:
            row.append(_fmt(data[spec[0]][i], spec[7]))
        for name, values, _, _ in SMALL_CATEGORICAL:
            row.append(values[int(data[name][i])])
        row.append(str(int(labels[i])))
        rows.append(row)
    return columns, features, rows


def _medium(rng: np.random.Generator) -> tuple[list[str], list[dict[str, Any]], list[list[str]]]:
    n = MEDIUM_ROWS
    columns: list[str] = []
    features: list[dict[str, Any]] = []
    data: dict[str, np.ndarray] = {}
    for name, unit, desc, mean, sd, low, high, decimals in MEDIUM_CONTINUOUS:
        values = np.clip(rng.normal(mean, sd, n), low, high)
        data[name] = values
        columns.append(name)
        features.append(
            {
                "name": name,
                "role": "continuous",
                "dtype": "float",
                "unit": unit,
                "decimals": decimals,
                "semantic": desc,
            }
        )
    derived = {
        "income_copy": data["income"].copy(),
        "usage_per_day": data["monthly_usage"] / 30.0,
        "sessions_x_minutes": data["weekly_sessions"] * data["avg_session_minutes"] / 100.0,
        "risk_average": (data["churn_risk_score"] + (100.0 - data["satisfaction"])) / 2.0,
        "tenure_years": data["tenure_months"] / 12.0,
        "noisy_tenure": data["tenure_months"] + rng.normal(0, 0.7, n),
        "noisy_credit": data["credit_score"] + rng.normal(0, 2.0, n),
        "satisfaction_copy": data["satisfaction"].copy(),
        "engagement_index": 0.6 * data["weekly_sessions"] + 0.4 * data["monthly_usage"],
    }
    for name, source, relation, desc in MEDIUM_DERIVED:
        data[name] = derived[name]
        columns.append(name)
        features.append(
            {
                "name": name,
                "role": "continuous",
                "dtype": "float",
                "unit": "derived",
                "decimals": 5,
                "semantic": desc,
                "relation": {"type": relation, "source": source},
            }
        )
    cat_effects = np.zeros(n, dtype=np.float64)
    category_values: dict[str, list[str]] = {}
    for name, values, effects, desc in MEDIUM_CATEGORICAL:
        codes = _category_codes(rng, values, n)
        data[name] = codes.astype(np.float64)
        category_values[name] = values
        cat_effects += np.asarray(effects, dtype=np.float64)[codes]
        columns.append(name)
        features.append(
            {
                "name": name,
                "role": "categorical",
                "dtype": "string",
                "categories": values,
                "semantic": desc,
            }
        )
    linear = (
        -0.08
        + 0.22 * _standardize(data["age"])
        + 0.31 * _standardize(data["income"])
        + 0.16 * _standardize(data["tenure_months"])
        + 0.35 * _standardize(data["monthly_usage"])
        + 0.12 * _standardize(data["avg_session_minutes"])
        + 0.09 * _standardize(data["weekly_sessions"])
        + 0.18 * _standardize(data["support_tickets"])
        + 0.19 * _standardize(data["late_payments"])
        + 0.38 * _standardize(data["debt_ratio"])
        - 0.37 * _standardize(data["credit_score"])
        + 0.11 * _standardize(data["contract_value"])
        + 0.10 * _standardize(data["discount_rate"])
        + 0.07 * _standardize(data["marketing_touches"])
        + 0.12 * _standardize(data["email_open_rate"])
        + 0.11 * _standardize(data["mobile_share"])
        - 0.30 * _standardize(data["satisfaction"])
        + 0.24 * _standardize(data["churn_risk_score"])
        + 0.08 * _standardize(data["onboarding_days"])
        + 0.13 * _standardize(data["product_count"])
        + 0.16 * _standardize(data["payment_delay_days"])
        + cat_effects
    )
    labels = rng.binomial(1, _sigmoid(linear), n)
    data["label"] = labels.astype(np.float64)
    columns.append("label")
    features.append(
        {
            "name": "label",
            "role": "target",
            "dtype": "binary",
            "classes": [0, 1],
            "positive_value": 1,
            "negative_value": 0,
            "semantic": "Observed binary renewal outcome used as the training label.",
        }
    )

    missing_idx = {
        "income": rng.choice(n, size=max(1, int(n * 0.03)), replace=False),
        "credit_score": rng.choice(n, size=max(1, int(n * 0.018)), replace=False),
        "satisfaction": rng.choice(n, size=max(1, int(n * 0.012)), replace=False),
        "industry": rng.choice(n, size=max(1, int(n * 0.04)), replace=False),
        "payment_method": rng.choice(n, size=max(1, int(n * 0.02)), replace=False),
    }
    rows: list[list[str]] = []
    for i in range(n):
        row: list[str] = []
        for spec in MEDIUM_CONTINUOUS:
            name, decimals = spec[0], spec[7]
            row.append("" if i in missing_idx.get(name, ()) else _fmt(data[name][i], decimals))
        for name, source, _, _ in MEDIUM_DERIVED:
            decimals = 5
            row.append(_fmt(data[name][i], decimals))
        for name, values, _, _ in MEDIUM_CATEGORICAL:
            if i in missing_idx.get(name, ()):
                row.append("")
            else:
                row.append(values[int(data[name][i])])
        row.append(str(int(labels[i])))
        rows.append(row)
    return columns, features, rows


def _missing_columns(
    columns: list[str], features: list[dict[str, Any]], rows: list[list[str]]
) -> list[dict[str, Any]]:
    result = []
    roles = {item["name"]: item["role"] for item in features}
    for index, column in enumerate(columns):
        count = sum(1 for row in rows if row[index].strip() == "")
        if count:
            result.append(
                {
                    "column": column,
                    "role": roles.get(column),
                    "missing_count": count,
                    "missing_rate": count / len(rows),
                    "handling": "configured imputation during preprocessing",
                }
            )
    return result


def _write_csv(path: Path, columns: list[str], rows: list[list[str]]) -> str:
    with open(path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(columns)
        writer.writerows(rows)
    payload = path.read_bytes()
    return hashlib.sha256(payload).hexdigest()


def _dataset_declaration(
    dataset_id: str,
    file_name: str,
    sha: str,
    rows: list[list[str]],
    columns: list[str],
    features: list[dict[str, Any]],
    seed: int,
    notes: list[str],
) -> dict[str, Any]:
    redundant = [
        {"column": item["name"], **item["relation"], "semantic": item["semantic"]}
        for item in features
        if "relation" in item
    ]
    return {
        "dataset_id": dataset_id,
        "file": file_name,
        "sha256": sha,
        "rows": len(rows),
        "columns": len(columns),
        "column_names": columns,
        "target": next(item for item in features if item["role"] == "target"),
        "features": [item for item in features if item["role"] != "target"],
        "missing_columns": _missing_columns(columns, features, rows),
        "redundant_columns": redundant,
        "class_counts": _class_counts(rows, columns, features),
        "seed": seed,
        "notes": notes,
    }


def _class_counts(
    rows: list[list[str]], columns: list[str], features: list[dict[str, Any]]
) -> dict[str, int]:
    target = next(item["name"] for item in features if item["role"] == "target")
    position = columns.index(target)
    counts: dict[str, int] = {}
    for row in rows:
        counts[row[position]] = counts.get(row[position], 0) + 1
    return dict(sorted(counts.items()))


def generate_samples(output_dir: str | Path) -> dict[str, Any]:
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    rng = np.random.Generator(np.random.PCG64(SEED))
    small_columns, small_features, small_rows = _small(rng)
    medium_columns, medium_features, medium_rows = _medium(rng)
    small_sha = _write_csv(output / "small.csv", small_columns, small_rows)
    medium_sha = _write_csv(output / "medium.csv", medium_columns, medium_rows)
    small = _dataset_declaration(
        "small",
        "small.csv",
        small_sha,
        small_rows,
        small_columns,
        small_features,
        SEED,
        [
            "600 rows and 12 columns including the target.",
            "Seven continuous, four categorical and one binary target column.",
            "No missing values; intended for fast interactive experiments.",
        ],
    )
    medium = _dataset_declaration(
        "medium",
        "medium.csv",
        medium_sha,
        medium_rows,
        medium_columns,
        medium_features,
        SEED,
        [
            "6000 rows and 40 columns including the target.",
            "Twenty-nine continuous, ten categorical and one binary target column.",
            "Contains exact copies, linear derivatives, composites and two noisy redundant columns.",
            "Five declared columns contain small deterministic missing blocks.",
        ],
    )
    declaration = {
        "schema_version": 1,
        "declaration_type": "deterministic-training-datasets",
        "generator": "scripts/generate_samples.py",
        "seed": SEED,
        "datasets": [small, medium],
    }
    (output / "dataset_declaration.json").write_bytes(
        (json.dumps(declaration, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode(
            "utf-8"
        )
    )
    (output / "DATASETS.md").write_text(_markdown(declaration), encoding="utf-8")
    return declaration


def _markdown(declaration: dict[str, Any]) -> str:
    lines = [
        "# 数据集声明",
        "",
        f"固定生成种子：`{declaration['seed']}`。生成脚本：`{declaration['generator']}`。",
        "所有 CSV 使用 UTF-8、LF 换行和固定列顺序；缺失值以空字段表示。",
        "",
    ]
    for dataset in declaration["datasets"]:
        lines.extend(
            [
                f"## {dataset['dataset_id']}",
                "",
                f"- 文件：`{dataset['file']}`",
                f"- SHA-256：`{dataset['sha256']}`",
                f"- 行数：{dataset['rows']}",
                f"- 列数：{dataset['columns']}",
                f"- 目标列：`{dataset['target']['name']}`",
                f"- 类别计数：`{json.dumps(dataset['class_counts'], ensure_ascii=False)}`",
                f"- 缺失列：{', '.join('`' + item['column'] + '`' for item in dataset['missing_columns']) or '无'}",
                f"- 冗余列：{', '.join('`' + item['column'] + '`' for item in dataset['redundant_columns']) or '无'}",
                "",
                "| 列 | 角色 | 语义 |",
                "| --- | --- | --- |",
            ]
        )
        for feature in dataset["features"]:
            lines.append(
                f"| `{feature['name']}` | {feature['role']} | {feature['semantic']} |"
            )
        lines.append("")
        for note in dataset["notes"]:
            lines.append(f"- {note}")
        lines.append("")
    return "\n".join(lines)


if __name__ == "__main__":
    generated = generate_samples(Path(__file__).resolve().parent.parent / "data")
    for item in generated["datasets"]:
        print(f"{item['dataset_id']}: {item['rows']}x{item['columns']} sha256={item['sha256']}")
