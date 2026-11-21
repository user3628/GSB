#!/usr/bin/env python3
"""Generate deterministic local corpus shards, expected values and legacy v1 evidence.

The script is intentionally offline and uses a fixed NumPy PCG64 seed. It is the
only place where sample values are generated; runtime code never draws random data.
"""

from __future__ import annotations

import argparse
import hashlib
import sys
from decimal import Decimal
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from factmerge.util import canonical_json, decimal_text, dec, quantize  # noqa: E402


SQ_MI_TO_KM2 = dec("2.589988110336")
MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]


def claim(
    claim_id: str,
    entity_id: str,
    lang: str,
    category: str | None,
    raw_value: str,
    unit: str | None,
    sentence: str,
    source: str,
    observed_at: str,
    confidence: str = "1.0",
) -> dict:
    value = {
        "claim_id": claim_id,
        "entity_id": entity_id,
        "lang": lang,
        "raw_value": raw_value,
        "unit": unit,
        "sentence": sentence,
        "source": source,
        "observed_at": observed_at,
        "annotated_confidence": confidence,
    }
    if category is not None:
        value["category"] = category
    return value


def generate(seed: int) -> tuple[list[dict], dict, list[dict], list[dict], list[dict]]:
    rng = np.random.default_rng(seed)
    records: list[dict] = []
    expected: dict[str, dict] = {}

    # 1. Population conflict plus a conflicting area value.
    pop_million = int(rng.choice([132, 136, 138, 142]))
    conflict_million = int(rng.choice([3, 4, 5, 7]))
    records.extend(
        [
            claim(
                "C-POP-ATLAS-ZH",
                "ENT-ATLAS",
                "zh",
                "population",
                f"{Decimal(pop_million) / Decimal(100):.2f}亿",
                "人",
                f"阿特拉斯市人口约为{Decimal(pop_million) / Decimal(100):.2f}亿人。",
                "official_stats",
                "2025-01-01",
                "0.98",
            ),
            claim(
                "C-POP-ATLAS-EN-EQ",
                "ENT-ATLAS",
                "en",
                "population",
                f"{pop_million} million",
                "people",
                f"Atlas has a population of {pop_million} million people.",
                "academic",
                "2023-06-15",
                "1.0",
            ),
            claim(
                "C-POP-ATLAS-EN-CONFLICT",
                "ENT-ATLAS",
                "en",
                "population",
                f"{pop_million + conflict_million} million",
                "people",
                f"An older estimate gives {pop_million + conflict_million} million residents.",
                "wiki",
                "2018-03-01",
                "0.7",
            ),
        ]
    )
    expected.setdefault("ENT-ATLAS", {})["population"] = {
        "value": str(pop_million * 1_000_000),
        "canonical_unit": "person",
        "conflict": True,
    }

    area_km2 = int(rng.choice([16850, 20500, 7420, 31500]))
    area_sqmi_conflict = int((Decimal(area_km2) / SQ_MI_TO_KM2).quantize(Decimal("0.01"))) + int(
        rng.choice([2, 3, 5])
    )
    records.extend(
        [
            claim(
                "C-AREA-ATLAS-ZH",
                "ENT-ATLAS",
                "zh",
                "area",
                f"{area_km2:,}",
                "平方公里",
                f"阿特拉斯市面积为{area_km2:,}平方公里。",
                "gov",
                "2024-10-01",
                "0.95",
            ),
            claim(
                "C-AREA-ATLAS-EN",
                "ENT-ATLAS",
                "en",
                "area",
                f"{area_sqmi_conflict:,}",
                "sq mi",
                f"Atlas covers {area_sqmi_conflict:,} square miles.",
                "wiki",
                "2017-09-01",
                "0.65",
            ),
        ]
    )
    expected["ENT-ATLAS"]["area"] = {
        "value": str(area_km2),
        "canonical_unit": "km2",
        "conflict": True,
    }

    # 2. Founded-year conflict and tenure interval conflict.
    founded = int(rng.integers(1880, 2019))
    records.extend(
        [
            claim(
                "C-YEAR-BOREAL-ZH",
                "ENT-BOREAL",
                "zh",
                "founded_year",
                f"{founded}年",
                "年",
                f"北方研究所成立于{founded}年。",
                "gov",
                "2024-08-01",
                "0.96",
            ),
            claim(
                "C-YEAR-BOREAL-EN",
                "ENT-BOREAL",
                "en",
                "founded_year",
                f"{founded - 1}",
                "year",
                f"The Boreal Institute was founded in {founded - 1}.",
                "wiki",
                "2014-01-01",
                "0.55",
            ),
        ]
    )
    expected["ENT-BOREAL"] = {
        "founded_year": {"value": str(founded), "canonical_unit": "year", "conflict": True}
    }

    start_year = int(rng.integers(2012, 2018))
    start_month = int(rng.integers(1, 8))
    end_year = start_year + int(rng.integers(3, 6))
    end_month = int(rng.integers(1, 13))
    if (end_year, end_month) <= (start_year, start_month):
        end_year, end_month = start_year + 1, start_month
    conflict_end_month = end_month + 1
    conflict_end_year = end_year
    if conflict_end_month == 13:
        conflict_end_month = 1
        conflict_end_year += 1
    records.extend(
        [
            claim(
                "C-TENURE-BOREAL-ZH",
                "ENT-BOREAL",
                "zh",
                "tenure",
                f"{start_year}年{start_month}月至{end_year}年{end_month}月",
                "年月区间",
                "北方研究所所长任期为该年月区间。",
                "gov",
                "2025-01-01",
                "0.94",
            ),
            claim(
                "C-TENURE-BOREAL-EN-EQ",
                "ENT-BOREAL",
                "en",
                "tenure",
                f"{MONTHS[start_month - 1]} {start_year} to {MONTHS[end_month - 1]} {end_year}",
                "year-month interval",
                "The director served from the first month to the second month.",
                "dataset",
                "2023-02-01",
                "1.0",
            ),
            claim(
                "C-TENURE-BOREAL-EN-CONFLICT",
                "ENT-BOREAL",
                "en",
                "tenure",
                f"{start_year}-{start_month:02d} to {conflict_end_year}-{conflict_end_month:02d}",
                "year-month interval",
                "A later news source reports a different end month.",
                "news",
                "2020-05-01",
                "0.8",
            ),
        ]
    )
    expected["ENT-BOREAL"]["tenure"] = {
        "type": "month_interval",
        "start": f"{start_year:04d}-{start_month:02d}",
        "end": f"{end_year:04d}-{end_month:02d}",
        "conflict": True,
    }

    # 3. Cross-language equivalents: 万, M and an exact square-mile conversion.
    pop2_million = int(rng.choice([32, 48, 65]))
    records.extend(
        [
            claim(
                "C-POP-CEDAR-ZH",
                "ENT-CEDAR",
                "zh",
                "population",
                f"{pop2_million * 100}万",
                "人",
                "雪松市人口保持稳定。",
                "gov",
                "2024-04-01",
                "0.93",
            ),
            claim(
                "C-POP-CEDAR-EN",
                "ENT-CEDAR",
                "en",
                "population",
                f"{pop2_million}M",
                "people",
                "Cedar's population is unchanged in the English dataset.",
                "dataset",
                "2024-04-01",
                "1.0",
            ),
        ]
    )
    expected["ENT-CEDAR"] = {
        "population": {
            "value": str(pop2_million * 1_000_000),
            "canonical_unit": "person",
            "conflict": False,
        }
    }

    sqmi_exact = int(rng.choice([1000, 5000, 7200]))
    km2_exact = quantize(dec(sqmi_exact) * SQ_MI_TO_KM2, "0.000000000001")
    records.extend(
        [
            claim(
                "C-AREA-CEDAR-ZH",
                "ENT-CEDAR",
                "zh",
                "area",
                decimal_text(km2_exact),
                "平方公里",
                "雪松市面积按平方公里记录。",
                "gov",
                "2024-04-01",
                "0.9",
            ),
            claim(
                "C-AREA-CEDAR-EN",
                "ENT-CEDAR",
                "en",
                "area",
                f"{sqmi_exact:,}",
                "sq mi",
                "Cedar's area is recorded in square miles.",
                "dataset",
                "2024-04-01",
                "1.0",
            ),
        ]
    )
    expected["ENT-CEDAR"]["area"] = {
        "value": decimal_text(km2_exact),
        "canonical_unit": "km2",
        "conflict": False,
    }

    # 4. Review anomalies, including missing/unknown units, negative and reversed range.
    valid_pop = int(rng.choice([9, 12, 15])) * 1_000_000
    records.extend(
        [
            claim(
                "C-POP-DELTA-VALID",
                "ENT-DELTA",
                "zh",
                "population",
                f"{valid_pop // 10_000}万",
                "人",
                "三角洲地区人口采用有效统计值。",
                "official_stats",
                "2025-01-01",
                "0.99",
            ),
            claim(
                "C-POP-DELTA-MISSING",
                "ENT-DELTA",
                "zh",
                "population",
                "2,500",
                None,
                "另一份材料只写人口约2,500，却没有单位。",
                "news",
                "2022-01-01",
                "0.7",
            ),
            claim(
                "C-POP-DELTA-UNKNOWN",
                "ENT-DELTA",
                "en",
                "population",
                "1,800",
                "hectare-ish",
                "A malformed source describes the population with an unknown unit.",
                "wiki",
                "2021-01-01",
                "0.4",
            ),
            claim(
                "C-POP-DELTA-NEGATIVE",
                "ENT-DELTA",
                "zh",
                "population",
                "-2.5万",
                "人",
                "异常样本出现了负人口值。",
                "unknown",
                "2020-01-01",
                "0.3",
            ),
            claim(
                "C-TENURE-DELTA-VALID",
                "ENT-DELTA",
                "en",
                "tenure",
                "2020-01 to 2024-01",
                "year-month interval",
                "A valid tenure interval is retained.",
                "dataset",
                "2024-02-01",
                "1.0",
            ),
            claim(
                "C-TENURE-DELTA-REVERSED",
                "ENT-DELTA",
                "en",
                "tenure",
                "2023-11 to 2019-03",
                "year-month interval",
                "This interval is accidentally reversed.",
                "news",
                "2023-01-01",
                "0.6",
            ),
        ]
    )
    expected["ENT-DELTA"] = {
        "population": {"value": str(valid_pop), "canonical_unit": "person", "conflict": False},
        "tenure": {
            "type": "month_interval",
            "start": "2020-01",
            "end": "2024-01",
            "conflict": False,
        },
    }

    # 5. Parse ambiguity, empty fragment, and unit-only category inference.
    area_epsilon = dec(rng.choice(["450.5", "612.25", "980.75"]))
    records.extend(
        [
            claim(
                "C-YEAR-EPSILON-AMBIGUOUS",
                "ENT-EPSILON",
                "zh",
                "founded_year",
                "1,23.456",
                "年",
                "成立年份含冲突千分位和小数点。",
                "wiki",
                "2019-01-01",
                "0.5",
            ),
            claim(
                "C-AREA-EPSILON-EMPTY",
                "ENT-EPSILON",
                "zh",
                "area",
                "",
                "平方公里",
                "面积数值缺失。",
                "news",
                "2019-01-01",
                "0.5",
            ),
            claim(
                "C-AREA-EPSILON-VALID",
                "ENT-EPSILON",
                "zh",
                "area",
                decimal_text(area_epsilon),
                "平方公里",
                "有效面积值。",
                "gov",
                "2025-01-01",
                "0.92",
            ),
            claim(
                "C-POP-EPSILON-ZH",
                "ENT-EPSILON",
                "zh",
                None,
                "1.25",
                "亿人",
                "该地区人口为一点二五亿人。",
                "gov",
                "2024-12-01",
                "0.95",
            ),
            claim(
                "C-POP-EPSILON-EN",
                "ENT-EPSILON",
                "en",
                None,
                "125 million",
                "people",
                "The population is 125 million people.",
                "dataset",
                "2024-12-01",
                "1.0",
            ),
        ]
    )
    expected["ENT-EPSILON"] = {
        "area": {"value": decimal_text(area_epsilon), "canonical_unit": "km2", "conflict": False},
        "population": {"value": "125000000", "canonical_unit": "person", "conflict": False},
    }

    # 6. Unicode full-width digits and the 亿 alias.
    fullwidth_pop = int(rng.choice([141, 145, 153]))
    records.extend(
        [
            claim(
                "C-POP-FJORD-ZH-FULLWIDTH",
                "ENT-FJORD",
                "zh",
                "population",
                f"１.{(fullwidth_pop - 100):02d}亿".replace("1.", "１.", 1),
                "人",
                "峡湾地区人口使用全角数字录入。",
                "gov",
                "2024-06-01",
                "0.9",
            ),
            claim(
                "C-POP-FJORD-EN",
                "ENT-FJORD",
                "en",
                "population",
                f"{fullwidth_pop} million",
                "people",
                "The English record uses millions.",
                "academic",
                "2023-06-01",
                "1.0",
            ),
        ]
    )
    expected["ENT-FJORD"] = {
        "population": {
            "value": str(fullwidth_pop * 1_000_000),
            "canonical_unit": "person",
            "conflict": False,
        }
    }

    # 7. Exact confidence tie, resolved by claim_id ascending.
    records.extend(
        [
            claim(
                "C-TIE-A",
                "ENT-GROVE",
                "zh",
                "population",
                "7.10亿",
                "人",
                "并列可信度声明 A。",
                "dataset",
                "2024-01-01",
                "1.0",
            ),
            claim(
                "C-TIE-B",
                "ENT-GROVE",
                "zh",
                "population",
                "7.05亿",
                "人",
                "并列可信度声明 B。",
                "dataset",
                "2024-01-01",
                "1.0",
            ),
        ]
    )
    expected["ENT-GROVE"] = {
        "population": {"value": "710000000", "canonical_unit": "person", "conflict": True}
    }

    legacy = [
        {
            "schema_version": "1.0",
            "record_id": "L001",
            "claim_id": "L001",
            "entity_id": "ENT-LEGACY-A",
            "lang": "zh",
            "category": "population",
            "value": "138,000,000",
            "unit": "人",
            "decision": "accepted",
            "source": "gov",
            "observed_at": "2024-01-01",
        },
        {
            "schema_version": "1.0",
            "record_id": "L002",
            "claim_id": "L002",
            "entity_id": "ENT-LEGACY-B",
            "lang": "zh",
            "category": "population",
            "value": "1.38",
            "unit": "亿人",
            "decision": "accepted",
            "source": "official_stats",
            "observed_at": "2024-01-01",
        },
        {
            "schema_version": "1.0",
            "record_id": "L003",
            "claim_id": "L003",
            "entity_id": "ENT-LEGACY-C",
            "lang": "en",
            "category": "area",
            "value": "6,510",
            "unit": "square miles",
            "decision": "accepted",
            "source": "dataset",
            "observed_at": "2023-01-01",
        },
        {
            "schema_version": "1.0",
            "record_id": "L004",
            "claim_id": "L004",
            "entity_id": "ENT-LEGACY-D",
            "lang": "zh",
            "category": "founded_year",
            "value": "1998",
            "unit": "年",
            "decision": "accepted",
            "source": "gov",
            "observed_at": "2022-01-01",
        },
        {
            "schema_version": "1.0",
            "record_id": "L005",
            "claim_id": "L005",
            "entity_id": "ENT-LEGACY-E",
            "lang": "zh",
            "category": "tenure",
            "value": "2021-04",
            "unit": "任期（年月）",
            "decision": "accepted",
            "source": "dataset",
            "observed_at": "2023-01-01",
        },
    ]
    invalid_legacy = [
        {
            "schema_version": "1.0",
            "record_id": "L900",
            "claim_id": "L900",
            "entity_id": "ENT-LEGACY-INVALID",
            "lang": "en",
            "category": "area",
            "value": "100",
            "unit": "furlongs",
            "decision": "accepted",
        }
    ]
    edge_cases = [
        {
            "claim_id": "EDGE-EMPTY",
            "entity_id": "ENT-EDGE",
            "lang": "zh",
            "category": "population",
            "raw_value": "",
            "unit": "人",
            "sentence": "空数值",
            "source": "unknown",
        },
        {
            "claim_id": "EDGE-AMBIGUOUS",
            "entity_id": "ENT-EDGE",
            "lang": "en",
            "category": "area",
            "raw_value": "1,234.56.7",
            "unit": "sq mi",
            "sentence": "ambiguous separators",
            "source": "unknown",
        },
    ]
    return records, expected, legacy, invalid_legacy, edge_cases


def write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(canonical_json(row) + "\n" for row in rows), encoding="utf-8")


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=int, default=20261122)
    parser.add_argument("--out", default=str(ROOT / "samples"))
    args = parser.parse_args(argv)
    out = Path(args.out)
    records, expected, legacy, invalid_legacy, edge_cases = generate(args.seed)

    rng = np.random.default_rng(args.seed + 1)
    order = rng.permutation(len(records)).tolist()
    shuffled = [records[index] for index in order]
    split_points = [0, len(shuffled) // 3, (2 * len(shuffled)) // 3, len(shuffled)]
    shard_paths: list[Path] = []
    for index in range(3):
        path = out / "corpus" / f"shard-{index + 1:02d}.jsonl"
        write_jsonl(path, shuffled[split_points[index] : split_points[index + 1]])
        shard_paths.append(path)

    expected_path = out / "expected.json"
    expected_path.write_text(canonical_json(expected) + "\n", encoding="utf-8")
    legacy_path = out / "legacy_evidence_v1.jsonl"
    write_jsonl(legacy_path, legacy)
    invalid_path = out / "legacy_evidence_v1_invalid.jsonl"
    write_jsonl(invalid_path, invalid_legacy)
    edge_path = out / "edge_cases.jsonl"
    write_jsonl(edge_path, edge_cases)

    provenance = {
        "generator": "scripts/generate_samples.py",
        "seed": args.seed,
        "numpy_version": np.__version__,
        "claim_count": len(records),
        "shards": [
            {"path": path.relative_to(out).as_posix(), "sha256": sha256_file(path), "rows": len(shuffled[split_points[i] : split_points[i + 1]])}
            for i, path in enumerate(shard_paths)
        ],
        "legacy_evidence_v1.jsonl": sha256_file(legacy_path),
        "expected.json": sha256_file(expected_path),
        "edge_cases.jsonl": sha256_file(edge_path),
    }
    (out / "generator_manifest.json").write_text(canonical_json(provenance) + "\n", encoding="utf-8")
    print(canonical_json({"status": "generated", "out": str(out), "seed": args.seed, "claim_count": len(records)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
