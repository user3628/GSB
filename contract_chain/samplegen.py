"""Deterministic local layout-sample generator (fixed seed, NumPy RNG only)."""
from __future__ import annotations

import numpy as np

from .codec import write_json
from .config import PipelineConfig

CATEGORIES = ("clause", "heading", "signature", "table")


def _jittered_region(rng, region_id, category, x, y, w, h):
    jitter = rng.normal(0.0, 0.004, size=4)
    xywh = np.array([x, y, w, h], dtype=float) + jitter
    xywh = np.clip(xywh, 0.02, 0.95)
    xywh[2] = min(float(xywh[2]), 1.0 - float(xywh[0]) - 0.01)
    xywh[3] = min(float(xywh[3]), 1.0 - float(xywh[1]) - 0.01)
    return {
        "region_id": region_id,
        "category": category,
        "xywh": [round(float(v), 6) for v in xywh],
        "refs": [],
    }


def _ref(ref_id, contract_id, lang, raw_text, raw_terms, source, crawl_month, struct_version=1):
    return {
        "ref_id": ref_id,
        "contract_id": contract_id,
        "lang": lang,
        "raw_text": raw_text,
        "raw_terms": dict(raw_terms),
        "source": source,
        "crawl_month": crawl_month,
        "struct_version": struct_version,
    }


def build_sample(seed: int = 20241123) -> dict:
    """Produce a fully deterministic multi-page, multi-language layout sample."""
    rng = np.random.default_rng(seed)
    pages = []

    # Page 1: Chinese version of contract C100 with the core conflict cluster.
    page_zh = {
        "page_id": "p-zh-1",
        "width": 1240,
        "height": 1754,
        "regions": [],
    }
    r1 = _jittered_region(rng, "r-zh-1", "clause", 0.08, 0.12, 0.84, 0.10)
    r1["refs"].append(
        _ref(
            "ref-0001",
            "C100",
            "zh",
            "第3.2条 适用期限与违约责任",
            {"duration": "12个月", "penalty": "5%", "effective_month": "2024-03"},
            "official",
            "2024-05",
        )
    )
    r2 = _jittered_region(rng, "r-zh-2", "clause", 0.08, 0.28, 0.84, 0.11)
    r2["refs"].append(
        _ref(
            "ref-0002",
            "C100",
            "zh",
            "依据第三条第二款 约定执行",
            {"duration": "12 个月", "penalty": "5.00%", "effective_month": "2024年3月"},
            "archive",
            "2024-01",
        )
    )
    r3 = _jittered_region(rng, "r-zh-3", "clause", 0.08, 0.44, 0.84, 0.09)
    r3["refs"].append(
        _ref(
            "ref-0003",
            "C100",
            "zh",
            "第四条（参见 Section 7.1）保留事项",
            {"duration": "6个月", "penalty": "2%", "effective_month": "2024-04"},
            "official",
            "2024-04",
        )
    )
    r4 = _jittered_region(rng, "r-zh-4", "clause", 0.08, 0.58, 0.84, 0.09)
    r4["refs"].append(
        _ref(
            "ref-0004",
            "C100",
            "zh",
            "条款详见附件清单",
            {"duration": "3个月", "penalty": "1%", "effective_month": "2024-05"},
            "web",
            "2024-06",
        )
    )
    r5 = _jittered_region(rng, "r-zh-5", "table", 0.08, 0.72, 0.84, 0.12)
    r5["refs"].append(
        _ref(
            "ref-0005",
            "C100",
            "zh",
            "第五条 费用表",
            {"duration": "若干个月", "penalty": "5", "effective_month": "2024-07"},
            "web",
            "2024-07",
        )
    )
    page_zh["regions"] = [r1, r2, r3, r4, r5]
    pages.append(page_zh)

    # Page 2: English version with conflicting term values for the same location.
    page_en = {
        "page_id": "p-en-1",
        "width": 1240,
        "height": 1754,
        "regions": [],
    }
    e1 = _jittered_region(rng, "r-en-1", "clause", 0.10, 0.14, 0.80, 0.10)
    e1["refs"].append(
        _ref(
            "ref-0006",
            "C100",
            "en",
            "Section 3.2 Term and Liquidated Damages",
            {"duration": "18 months", "penalty": "4.5%", "effective_month": "2024-02"},
            "broker",
            "2024-06",
        )
    )
    e2 = _jittered_region(rng, "r-en-2", "clause", 0.10, 0.30, 0.80, 0.10)
    e2["refs"].append(
        _ref(
            "ref-0007",
            "C100",
            "en",
            "Article 9 Governing Language",
            {"duration": "24 months", "penalty": "3%", "effective_month": "2024-01"},
            "official",
            "2024-03",
        )
    )
    e3 = _jittered_region(rng, "r-en-3", "clause", 0.10, 0.46, 0.80, 0.10)
    e3["refs"].append(
        _ref(
            "ref-0008",
            "C100",
            "en",
            "Section 7.1 Reserved Matters",
            {"duration": "6 months", "penalty": "2.0%", "effective_month": "2024-04"},
            "broker",
            "2024-05",
        )
    )
    page_en["regions"] = [e1, e2, e3]
    pages.append(page_en)

    # Page 3: second contract, inverted interval and unknown-unit failure cases.
    page_c2 = {
        "page_id": "p-zh-2",
        "width": 1240,
        "height": 1754,
        "regions": [],
    }
    c1 = _jittered_region(rng, "r-c2-1", "clause", 0.09, 0.16, 0.82, 0.10)
    c1["refs"].append(
        _ref(
            "ref-0009",
            "C200",
            "zh",
            "第九条 保证期间",
            {"duration": "（24, 12）个月", "penalty": "8%", "effective_month": "2025-01"},
            "official",
            "2025-02",
        )
    )
    c2 = _jittered_region(rng, "r-c2-2", "clause", 0.09, 0.32, 0.82, 0.10)
    c2["refs"].append(
        _ref(
            "ref-0010",
            "C200",
            "zh",
            "第九条第二款 补充保证",
            {"duration": "12个月", "penalty": "8.0%", "effective_month": "2025年1月"},
            "archive",
            "2024-12",
        )
    )
    c3 = _jittered_region(rng, "r-c2-3", "heading", 0.09, 0.50, 0.82, 0.06)
    c3["refs"].append(
        _ref(
            "ref-0011",
            "C200",
            "en",
            "Article 9 Warranty",
            {"duration": "1 year", "penalty": "7 percent", "effective_month": "2025-02"},
            "web",
            "2025-03",
        )
    )
    page_c2["regions"] = [c1, c2, c3]
    pages.append(page_c2)

    return {
        "sample_id": "layout-seed-%d" % seed,
        "seed": seed,
        "generator": "contract_chain.samplegen.build_sample",
        "pages": pages,
    }


def build_evidence_template() -> dict:
    return {
        "manifest_version": 1,
        "kind": "contract_chain_evidence_manifest",
        "records": [],
    }


def write_workspace_bundle(directory, seed: int = 20241123, config: PipelineConfig | None = None):
    """Write sample, default config and an empty evidence-list template."""
    config = config or PipelineConfig()
    sample_path = "%s/sample.json" % directory
    config_path = "%s/config.json" % directory
    template_path = "%s/evidence_template.json" % directory
    write_json(sample_path, build_sample(seed))
    write_json(config_path, config.to_dict())
    write_json(template_path, build_evidence_template())
    return {
        "sample": sample_path,
        "config": config_path,
        "evidence_template": template_path,
    }
