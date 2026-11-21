#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from contract_elements.canonical import atomic_write_json, atomic_write_jsonl


def _annotation(element_type: str, text: str, surface: str, normalized_value: str) -> dict[str, Any]:
    start = text.index(surface)
    end = start + len(surface)
    return {
        "element_type": element_type,
        "start": start,
        "end": end,
        "value": surface,
        "normalized_value": normalized_value,
    }


def _record(
    record_id: str,
    contract_id: str,
    template_id: str,
    clause_no: int,
    clause_text: str,
    effective_date: str,
    annotations: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "record_id": record_id,
        "contract_id": contract_id,
        "template_id": template_id,
        "clause_no": clause_no,
        "clause_text": clause_text,
        "effective_date": effective_date,
        "structure_version": 2,
        "annotations": annotations,
    }


def contracts() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    def add(record: dict[str, Any]) -> None:
        rows.append(record)

    alpha_date = "2025-03-15"
    text = "第1条 甲方应于2025年1月15日前支付人民币1,200,000.00元。"
    add(_record("A01", "C-ALPHA-01", "TPL-MASTER", 1, text, alpha_date, [
        _annotation("date", text, "2025年1月15日", "2025-01-15"),
        _annotation("amount", text, "人民币1,200,000.00元", "CNY:1200000.00"),
    ]))
    text = "第2条 乙方应在30个工作日内完成交付，逾期每日按万分之五支付违约金。"
    add(_record("A02", "C-ALPHA-01", "TPL-MASTER", 2, text, alpha_date, [
        _annotation("term", text, "30个工作日", "business_day:30"),
        _annotation("penalty_ratio", text, "万分之五", "0.000500"),
    ]))
    text = "Article 3: The total contract price is RMB 1,250,000.00, payable within 45 calendar days."
    add(_record("A03", "C-ALPHA-01", "TPL-MASTER", 3, text, alpha_date, [
        _annotation("amount", text, "RMB 1,250,000.00", "CNY:1250000.00"),
        _annotation("term", text, "45 calendar days", "day:45"),
    ]))
    text = "第4条 本合同的生效日期为2025年3月1日。"
    add(_record("A04", "C-ALPHA-01", "TPL-MASTER", 4, text, alpha_date, [
        _annotation("date", text, "2025年3月1日", "2025-03-01"),
    ]))

    beta_date = "2025-02-01"
    text = "第1条 服务费为人民币880,000.50元，分两期支付。"
    add(_record("B01", "C-BETA-02", "TPL-SERVICE", 1, text, beta_date, [
        _annotation("amount", text, "人民币880,000.50元", "CNY:880000.50"),
    ]))
    text = "第2条 The service period is 12 months from April 30, 2025."
    add(_record("B02", "C-BETA-02", "TPL-SERVICE", 2, text, beta_date, [
        _annotation("term", text, "12 months", "month:12"),
        _annotation("date", text, "April 30, 2025", "2025-04-30"),
    ]))
    text = "第3条 违约金比例为0.25%。"
    add(_record("B03", "C-BETA-02", "TPL-SERVICE", 3, text, beta_date, [
        _annotation("penalty_ratio", text, "0.25%", "0.002500"),
    ]))
    text = "第4条 The delivery deadline is 2025-05-15."
    add(_record("B04", "C-BETA-02", "TPL-SERVICE", 4, text, beta_date, [
        _annotation("date", text, "2025-05-15", "2025-05-15"),
    ]))

    gamma_date = "2025-04-01"
    text = "第1条 采购金额共计1,000,000.00元。"
    add(_record("G01", "C-GAMMA-03", "TPL-PURCHASE", 1, text, gamma_date, [
        _annotation("amount", text, "1,000,000.00元", "CNY:1000000.00"),
    ]))
    text = "第2条 交货期限为60日。"
    add(_record("G02", "C-GAMMA-03", "TPL-PURCHASE", 2, text, gamma_date, [
        _annotation("term", text, "60日", "day:60"),
    ]))
    text = "第3条 逾期付款违约金为千分之三。"
    add(_record("G03", "C-GAMMA-03", "TPL-PURCHASE", 3, text, gamma_date, [
        _annotation("penalty_ratio", text, "千分之三", "0.003000"),
    ]))
    text = "第4条 交付日期为2025-05-10。"
    add(_record("G04", "C-GAMMA-03", "TPL-PURCHASE", 4, text, gamma_date, [
        _annotation("date", text, "2025-05-10", "2025-05-10"),
    ]))

    delta_date = "2025-05-01"
    text = "第1条 年度服务费为人民币500,000.00元。"
    add(_record("D01", "C-DELTA-04", "TPL-RENEW", 1, text, delta_date, [
        _annotation("amount", text, "人民币500,000.00元", "CNY:500000.00"),
    ]))
    text = "第2条 续约期限为2年。"
    add(_record("D02", "C-DELTA-04", "TPL-RENEW", 2, text, delta_date, [
        _annotation("term", text, "2年", "year:2"),
    ]))
    text = "第3条 提前解约违约金为百分之五。"
    add(_record("D03", "C-DELTA-04", "TPL-RENEW", 3, text, delta_date, [
        _annotation("penalty_ratio", text, "百分之五", "0.050000"),
    ]))
    text = "第4条 通知发出日期不得晚于2025年6月30日。"
    add(_record("D04", "C-DELTA-04", "TPL-RENEW", 4, text, delta_date, [
        _annotation("date", text, "2025年6月30日", "2025-06-30"),
    ]))

    epsilon_date = "2025-06-01"
    text = "第1条 合同金额为人民币2,000,000.00元。"
    add(_record("E01", "C-EPSILON-05", "TPL-MASTER", 1, text, epsilon_date, [
        _annotation("amount", text, "人民币2,000,000.00元", "CNY:2000000.00"),
    ]))
    text = "第2条 履约期限为90日。"
    add(_record("E02", "C-EPSILON-05", "TPL-MASTER", 2, text, epsilon_date, [
        _annotation("term", text, "90日", "day:90"),
    ]))
    text = "第3条 延迟履行违约金为0.1%。"
    add(_record("E03", "C-EPSILON-05", "TPL-MASTER", 3, text, epsilon_date, [
        _annotation("penalty_ratio", text, "0.1%", "0.001000"),
    ]))
    text = "第4条 交付日期为2025-07-01。"
    add(_record("E04", "C-EPSILON-05", "TPL-MASTER", 4, text, epsilon_date, [
        _annotation("date", text, "2025-07-01", "2025-07-01"),
    ]))

    zeta_date = "2025-06-15"
    text = "第1条 首付款为人民币300,000.00元。"
    add(_record("Z01", "C-ZETA-06", "TPL-MASTER", 1, text, zeta_date, [
        _annotation("amount", text, "人民币300,000.00元", "CNY:300000.00"),
    ]))
    text = "第2条 尾款为人民币400,000.00元。"
    add(_record("Z02", "C-ZETA-06", "TPL-MASTER", 2, text, zeta_date, [
        _annotation("amount", text, "人民币400,000.00元", "CNY:400000.00"),
    ]))
    text = "第3条 Payment shall be completed within 15 business days after April 15, 2025."
    add(_record("Z03", "C-ZETA-06", "TPL-MASTER", 3, text, zeta_date, [
        _annotation("term", text, "15 business days", "business_day:15"),
        _annotation("date", text, "April 15, 2025", "2025-04-15"),
    ]))
    text = "第4条 逾期违约金为每日0.05%。"
    add(_record("Z04", "C-ZETA-06", "TPL-MASTER", 4, text, zeta_date, [
        _annotation("penalty_ratio", text, "0.05%", "0.000500"),
    ]))

    eta_date = "2025-07-01"
    text = "第1条 合同金额为人民币1,500,000.00元。"
    add(_record("T01", "C-ETA-07", "TPL-PURCHASE", 1, text, eta_date, [
        _annotation("amount", text, "人民币1,500,000.00元", "CNY:1500000.00"),
    ]))
    text = "第2条 The performance period is 18 months."
    add(_record("T02", "C-ETA-07", "TPL-PURCHASE", 2, text, eta_date, [
        _annotation("term", text, "18 months", "month:18"),
    ]))
    text = "第3条 迟延履行违约金为万分之十。"
    add(_record("T03", "C-ETA-07", "TPL-PURCHASE", 3, text, eta_date, [
        _annotation("penalty_ratio", text, "万分之十", "0.001000"),
    ]))
    text = "第4条 乙方应于2025年8月31日前完成验收。"
    add(_record("T04", "C-ETA-07", "TPL-PURCHASE", 4, text, eta_date, [
        _annotation("date", text, "2025年8月31日", "2025-08-31"),
    ]))

    theta_date = "2025-08-01"
    text = "第1条 暂定金额为人民币900,000.00元。"
    add(_record("H01", "C-THETA-08", "TPL-PURCHASE", 1, text, theta_date, [
        _annotation("amount", text, "人民币900,000.00元", "CNY:900000.00"),
    ]))
    text = "第2条 调整后金额为人民币950,000.00元。"
    add(_record("H02", "C-THETA-08", "TPL-PURCHASE", 2, text, theta_date, [
        _annotation("amount", text, "人民币950,000.00元", "CNY:950000.00"),
    ]))
    text = "第3条 服务期限为6个月。"
    add(_record("H03", "C-THETA-08", "TPL-PURCHASE", 3, text, theta_date, [
        _annotation("term", text, "6个月", "month:6"),
    ]))
    text = "第4条 合同有效期为12个月。"
    add(_record("H04", "C-THETA-08", "TPL-PURCHASE", 4, text, theta_date, [
        _annotation("term", text, "12个月", "month:12"),
    ]))

    iota_date = "2025-09-01"
    text = "第1条 合同金额为人民币1,100,000.00元。"
    add(_record("I01", "C-IOTA-09", "TPL-RENEW", 1, text, iota_date, [
        _annotation("amount", text, "人民币1,100,000.00元", "CNY:1100000.00"),
    ]))
    text = "第2条 最终验收日期为2025年12月31日。"
    add(_record("I02", "C-IOTA-09", "TPL-RENEW", 2, text, iota_date, [
        _annotation("date", text, "2025年12月31日", "2025-12-31"),
    ]))
    text = "第3条 质保期为半年。"
    add(_record("I03", "C-IOTA-09", "TPL-RENEW", 3, text, iota_date, [
        _annotation("term", text, "半年", "month:6"),
    ]))
    text = "第4条 逾期违约金为0.5%。"
    add(_record("I04", "C-IOTA-09", "TPL-RENEW", 4, text, iota_date, [
        _annotation("penalty_ratio", text, "0.5%", "0.005000"),
    ]))
    return rows


def seeds() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    def add(seed_id: str, clause_no: int, text: str, annotations: list[dict[str, Any]]) -> None:
        rows.append({
            "seed_id": seed_id,
            "contract_id": f"SEED-{seed_id}",
            "clause_no": clause_no,
            "clause_text": text,
            "structure_version": 2,
            "annotations": annotations,
        })

    text = "第1条 甲方应支付人民币1,000,000.00元。"
    add("AMT-CNY", 1, text, [_annotation("amount", text, "人民币1,000,000.00元", "CNY:1000000.00")])
    text = "第2条 合同总价为680,000.00元。"
    add("AMT-UNIT", 2, text, [_annotation("amount", text, "680,000.00元", "CNY:680000.00")])
    text = "Article 3: The deposit is USD 50,000.00."
    add("AMT-USD", 3, text, [_annotation("amount", text, "USD 50,000.00", "USD:50000.00")])
    text = "第4条 定金为￥20,000.00。"
    add("AMT-SYMBOL", 4, text, [_annotation("amount", text, "￥20,000.00", "CNY:20000.00")])
    text = "第5条 生效日为2025年2月3日。"
    add("DATE-CN", 5, text, [_annotation("date", text, "2025年2月3日", "2025-02-03")])
    text = "第6条 Deadline: 2025-03-31."
    add("DATE-ISO", 6, text, [_annotation("date", text, "2025-03-31", "2025-03-31")])
    text = "第7条 Delivery: April 5, 2025."
    add("DATE-EN", 7, text, [_annotation("date", text, "April 5, 2025", "2025-04-05")])
    text = "第8条 交付期为30个工作日。"
    add("TERM-WORKDAY", 8, text, [_annotation("term", text, "30个工作日", "business_day:30")])
    text = "第9条 The warranty period is 24 months."
    add("TERM-MONTH", 9, text, [_annotation("term", text, "24 months", "month:24")])
    text = "第10条 续期为半年。"
    add("TERM-DICT", 10, text, [_annotation("term", text, "半年", "month:6")])
    text = "第11条 违约金为万分之五。"
    add("RATIO-WAN", 11, text, [_annotation("penalty_ratio", text, "万分之五", "0.000500")])
    text = "第12条 Late fee is 0.3% per day."
    add("RATIO-PERCENT", 12, text, [_annotation("penalty_ratio", text, "0.3%", "0.003000")])
    text = "第13条 违约金为千分之三。"
    add("RATIO-QIAN", 13, text, [_annotation("penalty_ratio", text, "千分之三", "0.003000")])
    text = "第14条 Annual rate is 5.5 percent."
    add("RATIO-EN", 14, text, [_annotation("penalty_ratio", text, "5.5 percent", "0.055000")])
    return rows


def dictionary_entries() -> dict[str, Any]:
    entries: list[dict[str, Any]] = []

    def evidence(entry_id: str, element_type: str, surface: str) -> None:
        entries.append({"entry_id": entry_id, "element_type": element_type, "surface": surface, "mode": "evidence"})

    for entry_id, surface in [
        ("DICT_AMT_CNY", "人民币"), ("DICT_AMT_RMB", "RMB"), ("DICT_AMT_CNY_CODE", "CNY"),
        ("DICT_AMT_USD", "USD"), ("DICT_AMT_HKD", "HKD"), ("DICT_AMT_YUAN", "元"),
        ("DICT_AMT_WAN", "万元"), ("DICT_AMT_YI", "亿元"), ("DICT_AMT_USD_CN", "美元"),
        ("DICT_AMT_HKD_CN", "港元"), ("DICT_AMT_SYMBOL", "￥"), ("DICT_AMT_SYMBOL2", "¥"),
    ]:
        evidence(entry_id, "amount", surface)
    for entry_id, surface in [
        ("DICT_DATE_YEAR", "年"), ("DICT_DATE_MONTH", "月"), ("DICT_DATE_DAY", "日"),
        ("DICT_DATE_JAN", "January"), ("DICT_DATE_FEB", "February"), ("DICT_DATE_MAR", "March"),
        ("DICT_DATE_APR", "April"), ("DICT_DATE_MAY", "May"), ("DICT_DATE_JUN", "June"),
        ("DICT_DATE_JUL", "July"), ("DICT_DATE_AUG", "August"), ("DICT_DATE_SEP", "September"),
        ("DICT_DATE_OCT", "October"), ("DICT_DATE_NOV", "November"), ("DICT_DATE_DEC", "December"),
    ]:
        evidence(entry_id, "date", surface)
    for entry_id, surface in [
        ("DICT_TERM_WORKDAY", "工作日"), ("DICT_TERM_DAY", "日"), ("DICT_TERM_DAY_CN", "天"),
        ("DICT_TERM_WEEK", "周"), ("DICT_TERM_MONTH", "个月"), ("DICT_TERM_YEAR", "年"),
        ("DICT_TERM_BUSINESS_DAY", "business day"), ("DICT_TERM_CALENDAR_DAY", "calendar day"),
        ("DICT_TERM_DAYS", "days"), ("DICT_TERM_MONTHS", "months"), ("DICT_TERM_YEARS", "years"),
    ]:
        evidence(entry_id, "term", surface)
    for entry_id, surface in [
        ("DICT_RATIO_PERCENT", "%"), ("DICT_RATIO_PERMILLE", "‰"), ("DICT_RATIO_WAN", "万分之"),
        ("DICT_RATIO_QIAN", "千分之"), ("DICT_RATIO_BAI", "百分之"), ("DICT_RATIO_EN", "percent"),
    ]:
        evidence(entry_id, "penalty_ratio", surface)
    entries.extend([
        {"entry_id": "DICT_TERM_HALF_YEAR", "element_type": "term", "surface": "半年", "normalized_value": "month:6", "mode": "candidate"},
        {"entry_id": "DICT_RATIO_WANFEN_WU", "element_type": "penalty_ratio", "surface": "万分之五", "normalized_value": "0.000500", "mode": "candidate"},
    ])
    entries.sort(key=lambda entry: (entry["element_type"], entry["entry_id"]))
    return {"schema_version": 1, "entries": entries}


def v1_rows() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    text = "第1条 乙方应在2025年1月20日前支付人民币1,250,000.00元。"
    rows.append({
        "structure_version": 1,
        "contract_id": "C-V1-ALPHA",
        "template_id": "TPL-V1",
        "effective_date": "2025-01-10",
        "clauses": [
            {"clause_text": text, "elements": [
                {"type": "amount", "start": text.index("1,250,000.00"), "end": text.index("1,250,000.00") + len("1,250,000.00"), "value": "1,250,000.00"},
                {"type": "date", "start": text.index("2025年1月20日"), "end": text.index("2025年1月20日") + len("2025年1月20日"), "value": "2025-01-20"},
            ]},
            {"clause_text": "The advance payment is RMB 80,000.00.", "elements": [
                {"type": "amount", "start": 27, "end": 36, "value": "80,000.00"},
            ]},
        ],
    })
    text = "第3条 保证金为￥15,000.00。"
    rows.append({
        "structure_version": 1,
        "contract_id": "C-V1-BETA",
        "template_id": "TPL-V1",
        "clauses": [
            {"clause_text": text, "elements": [
                {"type": "amount", "start": text.index("15,000.00"), "end": text.index("15,000.00") + len("15,000.00"), "value": "15,000.00"},
            ]},
        ],
    })
    return rows


def gold_conflicts() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "conflicts": [
            {"contract_id": "C-ALPHA-01", "element_type": "amount", "winner": "CNY:1200000.00"},
            {"contract_id": "C-ALPHA-01", "element_type": "date", "winner": "2025-01-15"},
            {"contract_id": "C-ALPHA-01", "element_type": "term", "winner": "business_day:30"},
            {"contract_id": "C-BETA-02", "element_type": "date", "winner": "2025-04-30"},
            {"contract_id": "C-ZETA-06", "element_type": "amount", "winner": "CNY:300000.00"},
            {"contract_id": "C-THETA-08", "element_type": "amount", "winner": "CNY:900000.00"},
            {"contract_id": "C-THETA-08", "element_type": "term", "winner": "month:6"},
        ],
    }


def injections() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "injections": [
            {"injection_id": "INJ-CLAUSE-NO", "kind": "invalid_clause_no", "contract_id": "C-IOTA-09", "clause_no": 1, "value": "第X条"},
            {"injection_id": "INJ-TEXT-LONG", "kind": "text_too_long", "contract_id": "C-DELTA-04", "clause_no": 1},
            {"injection_id": "INJ-ELEMENT", "kind": "unknown_element", "contract_id": "C-EPSILON-05", "clause_no": 1, "value": "payment_method"},
            {"injection_id": "INJ-DATE", "kind": "invalid_effective_date", "contract_id": "C-ETA-07", "clause_no": 1, "value": "2025/13/40"},
        ],
    }


def main() -> None:
    data = ROOT / "data"
    data.mkdir(parents=True, exist_ok=True)
    atomic_write_jsonl(data / "contracts.jsonl", contracts())
    atomic_write_jsonl(data / "seeds.jsonl", seeds())
    atomic_write_json(data / "elements.json", dictionary_entries())
    atomic_write_jsonl(data / "annotations_v1.jsonl", v1_rows())
    atomic_write_json(data / "gold_conflicts.json", gold_conflicts())
    atomic_write_json(data / "injections.json", injections())
    print(json.dumps({"contract_records": len(contracts()), "seed_records": len(seeds()), "v1_records": len(v1_rows())}, sort_keys=True))


if __name__ == "__main__":
    main()

