"""Generate deterministic local JSONL shards and a governance YAML sample."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
SAMPLES = ROOT / "samples"


GOVERNANCE_YAML = '''version: "gov-2026-11-23"
standard_tags:
  - NOUN
  - PROPN
  - VERB
  - AUX
  - ADJ
  - ADV
  - PRON
  - DET
  - ADP
  - NUM
  - PUNCT
  - PART
  - CCONJ
  - SCONJ
  - INTJ
  - X

aliases:
  noun: [NOUN]
  n: [NOUN]
  "名": [NOUN, VERB]
  "标题": [NOUN, PROPN]
  verb: [VERB]
  v: [VERB]
  "动": [VERB, AUX]
  aux: [AUX]
  adj: [ADJ]
  a: [ADJ]
  adv: [ADV]
  d: [ADV]
  pron: [PRON]
  pronoun: [PRON]
  det: [DET]
  adp: [ADP]
  num: [NUM]
  punct: [PUNCT]
  part: [PART]
  cconj: [CCONJ]
  sconj: [SCONJ]
  intj: [INTJ]
  propn: [PROPN]
  "专名": [PROPN]
  x: [X]
  "，": [PUNCT]

candidate_rules:
  standard_casefold: true
  stem_separators: ["+", "/", "@", ":", "-", "_"]

alignment:
  min_rate: 0.72
  token_comparison: nfkc_casefold
  allow_unpaired: true

adjudication:
  strategy_order:
    - frequency
    - annotator_reliability
    - candidate_priority
  weights:
    frequency: 0.55
    annotator_reliability: 0.30
    candidate_priority: 0.15
  candidate_priority:
    - NOUN
    - PROPN
    - VERB
    - AUX
    - ADJ
    - ADV
    - PRON
    - DET
    - ADP
    - NUM
    - PUNCT
    - PART
    - CCONJ
    - SCONJ
    - INTJ
    - X
  annotator_reliability:
    annotator-a: 0.95
    annotator-b: 0.60
    annotator-c: 0.82
    legacy-annotator: 0.72
  default_annotator_reliability: 0.50
  confidence_threshold: 0.55
  tie_breaker: strategy_then_lexicographic

structure_v1:
  confidence_scale: percent
  zero_tokens: unsegmented
  preserve_original: true
'''


def record(
    sent_id: str,
    tokens: list[str],
    old_tags: list[str],
    new_tags: list[str] | None,
    *,
    annotator: str,
    version: int = 2,
    confidence: int | float | None = None,
    new_tokens: list[str] | None = None,
) -> dict[str, Any]:
    row: dict[str, Any] = {
        "sent_id": sent_id,
        "tokens": tokens,
        "old_tags": old_tags,
        "annotator_id": annotator,
        "structure_version": version,
    }
    if new_tags is not None:
        row["new_tags"] = new_tags
    if new_tokens is not None:
        row["new_tokens"] = new_tokens
    if confidence is not None:
        row["confidence"] = confidence
    return row


def shard_v2_a() -> list[dict[str, Any]]:
    return [
        record(
            "v2-001",
            ["I", "爱", "NLP", "。"],
            ["  Pronoun  ", "verB\tGerund", "Noun+Custom", "，"],
            ["PRON", "VERB", "NOUN", "PUNCT"],
            annotator="annotator-a",
            confidence=0.94,
        ),
        record(
            "v2-002",
            ["The", "quick", "brown", "fox", "jumps"],
            ["DET", "adj", "ADJ", "noun", "v"],
            ["DET", "ADV", "ADJ", "ADJ", "NOUN", "VERB"],
            annotator="annotator-a",
            confidence=0.91,
            new_tokens=["The", "very", "quick", "brown", "fox", "jumps"],
        ),
        record(
            "v2-003",
            ["我们", "open", "source", "模型"],
            ["ｐｒｏｎ", "ＶＥＲＢ", "noun", "NOUN"],
            ["PRON", "VERB", "NOUN", "NOUN"],
            annotator="annotator-c",
            confidence=0.88,
        ),
        record(
            "v2-004",
            ["mystery", "token"],
            ["mysterytag", "X"],
            ["X", "X"],
            annotator="annotator-b",
            confidence=0.73,
        ),
    ]


def shard_v1_b() -> list[dict[str, Any]]:
    return [
        record(
            "v1-001",
            ["Open", "source", "引擎", "。"],
            ["VERB", "  NOUN ", "名词", "，"],
            ["VERB", "NOUN", "NOUN", "PUNCT"],
            annotator="legacy-annotator",
            version=1,
            confidence=85,
        ),
        record(
            "v1-002",
            [],
            ["整句", "未切分"],
            None,
            annotator="legacy-annotator",
            version=1,
            confidence=100,
        ),
        record(
            "v1-003",
            ["数据", "quality", "matters"],
            ["noun", "NOUN", "v"],
            ["NOUN", "NOUN", "VERB"],
            annotator="legacy-annotator",
            version=1,
            confidence=92,
        ),
    ]


def shard_v2_c() -> list[dict[str, Any]]:
    return [
        record(
            "v2-101",
            ["名", "词", "test"],
            ["名", "名", "noun"],
            ["NOUN", "NOUN", "NOUN"],
            annotator="annotator-a",
            confidence=0.96,
        ),
        record(
            "v2-102",
            ["这个", "名字", "works"],
            ["DET", "名", "v"],
            ["DET", "NOUN", "VERB"],
            annotator="annotator-b",
            confidence=0.81,
        ),
        record(
            "v2-103",
            ["名", "动"],
            ["名", "动"],
            ["VERB", "AUX"],
            annotator="annotator-b",
            confidence=0.68,
        ),
        record(
            "v2-104",
            ["I", "really", "like", "green", "tea"],
            ["ｐｒｏｎ", "adv", "v", "adj", "noun"],
            ["PRON", "ADV", "VERB", "ADJ", "NOUN"],
            annotator="annotator-a",
            confidence=0.93,
            new_tokens=["I", "very", "like", "green", "tea"],
        ),
        record(
            "v2-105",
            ["标题", "是", "Title"],
            ["标题", "v", "标题"],
            ["PROPN", "VERB", "PROPN"],
            annotator="annotator-c",
            confidence=0.86,
        ),
        record(
            "v2-106",
            ["动", "词"],
            ["动", "noun"],
            ["VERB", "NOUN"],
            annotator="annotator-a",
            confidence=0.9,
        ),
    ]


def invalid_alignment_shard() -> list[dict[str, Any]]:
    return [
        record(
            "bad-align-001",
            ["I", "like", "green", "tea"],
            ["pron", "v", "adj", "noun"],
            ["PRON", "VERB", "NOUN"],
            annotator="annotator-a",
            confidence=0.9,
            new_tokens=["You", "love", "coffee"],
        )
    ]


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    text = "".join(
        json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
        for row in rows
    )
    path.write_bytes(text.encode("utf-8"))


def main() -> None:
    SAMPLES.mkdir(parents=True, exist_ok=True)
    (SAMPLES / "governance.yaml").write_bytes(GOVERNANCE_YAML.encode("utf-8"))
    write_jsonl(SAMPLES / "shard_01_v2.jsonl", shard_v2_a())
    write_jsonl(SAMPLES / "shard_02_v1.jsonl", shard_v1_b())
    write_jsonl(SAMPLES / "shard_03_v2.jsonl", shard_v2_c())
    write_jsonl(SAMPLES / "injected_alignment_failed.jsonl", invalid_alignment_shard())
    print(f"wrote deterministic samples to {SAMPLES}")


if __name__ == "__main__":
    main()

