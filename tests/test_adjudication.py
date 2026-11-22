from __future__ import annotations

from posalign.adjudication import adjudicate
from posalign.config import load_governance_config
from posalign.engine import Engine


def test_conflict_is_resolved_with_evidence_and_rejections(tmp_path, sample_config, sample_shards):
    result = Engine(tmp_path / "work").run(
        sample_shards, config_path=sample_config, reset=True
    )
    engine = Engine(tmp_path / "work")
    mapping = {item["old_tag"]: item for item in engine.mapping_resolved()["resolved"]}
    assert mapping["名"]["new_tag"] == "NOUN"
    assert mapping["名"]["arbitration"]["rejected_candidates"][0]["tag"] == "VERB"
    candidates = engine.mapping_candidates("名")["candidates"]
    assert [item["tag"] for item in candidates] == ["NOUN", "VERB"]
    assert candidates[1]["decision"] == "rejected"
    assert candidates[1]["rejection_reasons"]


def test_confidence_tie_uses_lexicographic_tag_order(tmp_path, sample_config):
    text = sample_config.read_text(encoding="utf-8")
    text = text.replace("frequency: 0.55", "frequency: 0.50")
    text = text.replace("annotator_reliability: 0.30", "annotator_reliability: 0.50")
    text = text.replace("candidate_priority: 0.15", "candidate_priority: 0.0")
    path = tmp_path / "tie.yaml"
    path.write_text(text, encoding="utf-8")
    config = load_governance_config(path)
    pairs = [
        {
            "sent_id": "s1",
            "old_tag": "名",
            "old_tag_raw": "名",
            "old_candidates": [
                {"tag": "NOUN", "source": "alias_table", "sources": ["alias_table"], "rules": []},
                {"tag": "VERB", "source": "alias_table", "sources": ["alias_table"], "rules": []},
            ],
            "new_tag": "NOUN",
            "new_tag_diagnostic": None,
            "annotator": "same",
            "confidence": 1.0,
        },
        {
            "sent_id": "s2",
            "old_tag": "名",
            "old_tag_raw": "名",
            "old_candidates": [
                {"tag": "NOUN", "source": "alias_table", "sources": ["alias_table"], "rules": []},
                {"tag": "VERB", "source": "alias_table", "sources": ["alias_table"], "rules": []},
            ],
            "new_tag": "VERB",
            "new_tag_diagnostic": None,
            "annotator": "same",
            "confidence": 1.0,
        },
    ]
    payload = {
        "shard_id": "x.jsonl",
        "sentences": [{"sent_id": "s", "pairs": pairs}],
    }
    result = adjudicate([payload], config)
    label = result["labels"][0]
    assert label["resolved"]["new_tag"] == "NOUN"
    assert label["candidates"][1]["rejection_reasons"] == [
        "tie_lost_lexicographic_tag_order"
    ]

