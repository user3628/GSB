"""Deterministic synthetic citation shard generator (fixed seed, no I/O deps).

Produces three UTF-8 JSONL shards plus a seed gold-annotation shard.  The
data covers mixed Chinese/English phrasing, cross-shard duplicate citations,
multi-version citations of the same clause, deliberate conflicts and a small
set of anomalous records exercising every business error path.
"""

import json
from pathlib import Path

import numpy as np

SEED = 20261123

CHANNELS = ["official_filing", "counsel_review",
            "counterparty_draft", "ocr_extract"]


def _zh(article, section):
    cn_num = {1: "一", 2: "二", 3: "三", 4: "四", 5: "五",
              6: "六", 7: "七", 8: "八", 9: "九", 10: "十"}
    text = f"根据第{cn_num[article]}条"
    if section:
        text += f"第{cn_num[section]}款"
    return text + "之约定"


def _en(article, section):
    if section:
        return f"Pursuant to Section {article}.{section} of this Agreement"
    return f"Pursuant to Article {article} of this Agreement"


def _span(raw_text, citation_text):
    start = raw_text.index(citation_text)
    return [start, start + len(citation_text)]


def _elements(template):
    """Return a fresh element dict from a compact template."""
    return json.loads(json.dumps(template))


def _record(cid, contract_id, lang, channel, raw_text, citation_text,
            elements, issued_date, shard_id):
    span = _span(raw_text, citation_text)
    return {
        "citation_id": cid,
        "contract_id": contract_id,
        "lang": lang,
        "channel": channel,
        "raw_text": raw_text,
        "citation_text": citation_text,
        "citation_span": span,
        "issued_date": issued_date,
        "shard_id": shard_id,
        "elements": elements,
    }


DURATION_TEMPLATES = [
    {"start_months": 1, "end_months": 12, "unit": "month"},
    {"start_months": {"value": 30, "unit": "days"},
     "end_months": {"value": 365, "unit": "days"}},
    {"start_months": 3, "end_months": 24, "unit": "month"},
]
PENALTY_TEMPLATES = [
    {"value": "5.00", "unit": "percent"},
    {"value": "800", "unit": "bp"},
    {"value": "7.5", "unit": "percent"},
]
MONTHS = ["2026-02", "2026-03", "2026-06"]


def generate(output_dir):
    rng = np.random.RandomState(SEED)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    shards = [[], [], []]
    gold = []
    seq = 0

    def next_id():
        nonlocal seq
        seq += 1
        return f"CITE-{seq:04d}"

    # Six contracts, each with a clean same-clause bilingual pair plus a
    # deliberate conflicting multi-version group.
    for ci in range(1, 7):
        contract_id = f"CTR-{ci:03d}"
        article = int(rng.randint(1, 9))
        section = int(rng.randint(0, 4))
        section = section  # 0 means article-only
        zh_phrase = _zh(article, section)
        en_phrase = _en(article, section)
        tmpl_d = DURATION_TEMPLATES[ci % 3]
        tmpl_p = PENALTY_TEMPLATES[ci % 3]
        month = MONTHS[ci % 3]
        base_elements = {
            "duration": _elements(tmpl_d),
            "penalty": _elements(tmpl_p),
            "effective_month": month,
        }
        # Bilingual pair (same normalized values) -> no conflict.
        zh_raw = f"{zh_phrase}，违约金及期限按下述执行。"
        en_raw = f"{en_phrase}, the penalty and term shall apply as below."
        zh_id = next_id()
        en_id = next_id()
        zh_rec = _record(zh_id, contract_id, "zh", "official_filing",
                         zh_raw, zh_phrase, _elements(base_elements),
                         "2025-11-01", "shard-1")
        en_rec = _record(en_id, contract_id, "en", "counsel_review",
                         en_raw, en_phrase, _elements(base_elements),
                         "2025-12-15", "shard-2")
        shards[0].append(zh_rec)
        shards[1].append(en_rec)
        # Exact cross-shard duplicate of the English citation.
        dup = json.loads(json.dumps(en_rec))
        dup["shard_id"] = "shard-3"
        shards[2].append(dup)

        # Conflicting version: different penalty + duration on same locator.
        conflict_elements = {
            "duration": _elements(DURATION_TEMPLATES[(ci + 1) % 3]),
            "penalty": _elements(PENALTY_TEMPLATES[(ci + 1) % 3]),
            "effective_month": MONTHS[(ci + 1) % 3],
        }
        conflict_raw = f"{en_phrase}; conflicting commercial terms are noted."
        conflict_id = next_id()
        conflict_rec = _record(conflict_id, contract_id, "en",
                               "counterparty_draft", conflict_raw, en_phrase,
                               conflict_elements, "2025-10-05", "shard-2")
        shards[1].append(conflict_rec)
        gold.append({
            "contract_id": contract_id,
            "locator_article": article,
            "locator_section": section,
            "citation_ids": [zh_id, en_id, conflict_id],
            "conflict": True,
            # Higher prior official_filing wins over counterparty_draft.
            "expected_adopted_citation_id": zh_id,
        })

    # ---- Anomalous samples ---------------------------------------------
    # 1) Unparsed: no locator pattern at all.
    bad1 = _record(next_id(), "CTR-001", "zh", "ocr_extract",
                   "本文件缺少明确条款引用。", "本文件缺少明确条款引用。",
                   {"penalty": {"value": "1", "unit": "percent"}},
                   "2025-09-01", "shard-3")
    shards[2].append(bad1)
    # 2) Unparsed: fragment mismatch.
    raw2 = "根据第三条第一款，适用下述比例。"
    bad2 = _record(next_id(), "CTR-002", "zh", "ocr_extract", raw2,
                   "第三条第一款",
                   {"penalty": {"value": "2", "unit": "percent"}},
                   "2025-09-02", "shard-1")
    # Keep the original span but point the fragment at non-matching text.
    bad2["citation_text"] = "不存在的片段"
    shards[0].append(bad2)
    # 3) Invalid element: inverted duration interval.
    raw3 = "Pursuant to Section 4.1 the interval is inverted."
    bad3 = _record(next_id(), "CTR-003", "en", "ocr_extract", raw3,
                   "Section 4.1",
                   {"duration": {"start_months": 12, "end_months": 1,
                                 "unit": "month"}},
                   "2025-09-03", "shard-2")
    shards[1].append(bad3)
    # 4) Invalid element: penalty above representable range.
    raw4 = "Pursuant to Article 5 an excessive penalty is stated."
    bad4 = _record(next_id(), "CTR-004", "en", "ocr_extract", raw4,
                   "Article 5",
                   {"penalty": {"value": "150", "unit": "percent"}},
                   "2025-09-04", "shard-3")
    shards[2].append(bad4)
    # 5) Invalid element: illegal effective month.
    raw5 = "根据第七条之约定，生效月份非法。"
    bad5 = _record(next_id(), "CTR-005", "zh", "ocr_extract", raw5,
                   "第七条",
                   {"effective_month": "2026-13"},
                   "2025-09-05", "shard-1")
    shards[0].append(bad5)
    # 6) Manual-review group: all citations from low-trust OCR channel.
    review_elements = {
        "duration": {"start_months": 2, "end_months": 10, "unit": "month"},
        "penalty": {"value": "4.0", "unit": "percent"},
        "effective_month": "2026-04",
    }
    r1 = _record(next_id(), "CTR-006", "en", "ocr_extract",
                 "Pursuant to Section 8.2 a low-trust term is extracted.",
                 "Section 8.2", _elements(review_elements),
                 "2024-01-01", "shard-2")
    r2_elements = _elements(review_elements)
    r2_elements["penalty"] = {"value": "9.0", "unit": "percent"}
    r2 = _record(next_id(), "CTR-006", "en", "ocr_extract",
                 "Pursuant to Section 8.2 another low-trust term appears.",
                 "Section 8.2", r2_elements, "2024-02-01", "shard-3")
    shards[1].append(r1)
    shards[2].append(r2)
    gold.append({
        "contract_id": "CTR-006",
        "locator_article": 8,
        "locator_section": 2,
        "citation_ids": [r1["citation_id"], r2["citation_id"]],
        "conflict": True,
        "expected_manual_review": True,
    })

    paths = []
    for idx, rows in enumerate(shards, start=1):
        path = output_dir / f"citations_shard_{idx}.jsonl"
        with open(path, "w", encoding="utf-8", newline="\n") as fh:
            for row in rows:
                fh.write(json.dumps(row, ensure_ascii=False,
                                    sort_keys=True) + "\n")
        paths.append(path)
    gold_path = output_dir / "gold_annotations.jsonl"
    with open(gold_path, "w", encoding="utf-8", newline="\n") as fh:
        for row in gold:
            fh.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    return {"shards": [str(p) for p in paths],
            "gold": str(gold_path), "seed": SEED,
            "record_counts": [len(s) for s in shards]}
