"""Deterministic local contract-clause generator (no model, no network)."""
from __future__ import annotations

import json
import os
import random

from .util import atomic_write

TEMPLATES = {
    "obligation": [
        ("zh", "乙方应当按照约定标准履行全部义务，并保证服务质量。"),
        ("zh", "供应商必须在交付前完成检验，不得迟延履行。"),
        ("en", "The supplier shall fully ensure compliance and must undertake each obligation."),
        ("mixed", "The Party B 应当 ensure all obligations 完全 met before delivery。"),
    ],
    "disclaimer": [
        ("zh", "因不可抗力导致的损失，甲方概不负责并免除责任。"),
        ("zh", "乙方对间接损失不承担责任，法律另有规定的除外。"),
        ("en", "The party bears no liability for force majeure and shall not indemnify indirect damages."),
        ("mixed", "Party A 不承担责任 for indirect damages under force majeure。"),
    ],
    "breach": [
        ("zh", "任何一方违约的，应支付违约金并赔偿全部损失。"),
        ("zh", "迟延履行超过十五日的，构成重大违约并触发损失赔偿。"),
        ("en", "A material breach triggers liquidated damages and compensation for all damages."),
        ("mixed", "If Party B 违约, it shall pay 违约金 and damages promptly。"),
    ],
    "payment": [
        ("zh", "甲方应在收到发票后三十日内支付价款，逾期付款产生滞纳金。"),
        ("zh", "乙方应先开具发票，甲方再安排付款，部分款项可略微延后。"),
        ("en", "The buyer shall make each payment upon invoice; overdue amounts accrue a late fee."),
        ("mixed", "Party A shall pay the 价款 within 30 days after receiving the invoice。"),
    ],
    "termination": [
        ("zh", "一方严重违约的，另一方有权提前终止并解除合同。"),
        ("zh", "合同到期或被撤销后，双方可终止后续合作。"),
        ("en", "Either party may terminate the agreement and rescind it upon a material breach."),
        ("mixed", "Party A may 提前终止 the contract and 解除合同 after written notice。"),
    ],
    "neutral": [
        ("zh", "本条款用于记录一般性沟通安排，不涉及具体风险事项。"),
        ("en", "This clause records general coordination notes without specific risk wording."),
        ("mixed", "This note 记录一般安排 and contains no risk wording。"),
    ],
}

CATEGORY_RISK_HINT = {
    "obligation": 1,
    "disclaimer": 2,
    "breach": 3,
    "payment": 2,
    "termination": 3,
    "neutral": 0,
}

EDGE_CASES = [
    {"lang": "zh", "text": "", "template_id": "edge_empty"},
    {"lang": "en", "text": "   ...!!! ？？ ", "template_id": "edge_punct"},
    {"lang": "zh", "text": "　Ｆｕｌｌ－ｗｉｄｔｈ　全角文字 与  half  width 混排。", "template_id": "edge_width"},
]


def _pseudo_level(rng: random.Random, category: str, text: str) -> int:
    base = CATEGORY_RISK_HINT[category]
    weak = ("轻微", "略微", "slightly", "部分", "partially", "不", "not", "without")
    strong = ("严重", "重大", "完全", "全部", "material", "fully", "completely")
    if any(marker in text for marker in strong):
        base = min(3, base + 1)
    if any(marker in text for marker in weak):
        base = max(0, base - 1)
    if rng.random() < 0.08:
        base = max(0, min(3, base + rng.choice([-1, 1])))
    return base


def generate_dataset(seed: int, sizes: dict[str, int], out_dir: str, shard_count: int = 3) -> dict:
    rng = random.Random(seed)
    os.makedirs(out_dir, exist_ok=True)
    categories = list(CATEGORY_RISK_HINT.keys())
    all_clauses: list[dict] = []
    clause_seq = 0
    for profile, count in sizes.items():
        for _ in range(count):
            category = rng.choices(categories, weights=[22, 16, 18, 18, 16, 10], k=1)[0]
            lang, text = rng.choice(TEMPLATES[category])
            if rng.random() < 0.25:
                prefix = rng.choice(["", "此外，", "Furthermore, ", "据此，"])
                text = prefix + text
            clause_seq += 1
            all_clauses.append(
                {
                    "clause_id": f"C{clause_seq:06d}",
                    "contract_id": f"K-{profile}-{clause_seq % 17:03d}",
                    "text": text,
                    "lang": lang,
                    "template_id": f"tpl_{category}",
                    "human_level": _pseudo_level(rng, category, text),
                    "_profile": profile,
                }
            )
    # Deterministic edge cases exercise E_EMPTY_TEXT and width normalization.
    for edge in EDGE_CASES:
        clause_seq += 1
        all_clauses.append(
            {
                "clause_id": f"C{clause_seq:06d}",
                "contract_id": "K-edge-000",
                "text": edge["text"],
                "lang": edge["lang"],
                "template_id": edge["template_id"],
                "human_level": 0,
                "_profile": "edge",
            }
        )

    # Deterministic round-robin distribution across shards.
    shards: list[list[dict]] = [[] for _ in range(shard_count)]
    for idx, clause in enumerate(all_clauses):
        shards[idx % shard_count].append(clause)

    paths = []
    for idx, shard in enumerate(shards, start=1):
        path = os.path.join(out_dir, f"shard_{idx:02d}.jsonl")
        lines = []
        for clause in shard:
            clause = dict(clause)
            clause.pop("_profile", None)
            lines.append(json.dumps(clause, ensure_ascii=False, sort_keys=True))
        atomic_write(path, ("\n".join(lines) + "\n").encode("utf-8"))
        paths.append(path)
    manifest = {
        "seed": seed,
        "sizes": sizes,
        "shard_count": shard_count,
        "total_clauses": len(all_clauses),
        "shards": [os.path.basename(p) for p in paths],
    }
    atomic_write(
        os.path.join(out_dir, "manifest.json"),
        (json.dumps(manifest, ensure_ascii=False, sort_keys=True) + "\n").encode("utf-8"),
    )
    return {"paths": paths, "manifest": manifest}
