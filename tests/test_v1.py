from __future__ import annotations

import json

import pytest

from posalign.config import load_governance_config
from posalign.engine import Engine
from posalign.errors import BusinessError, E_INVALID_RECORD
from posalign.parse import parse_shard


def test_v1_upgrade_confidence_and_unsplit(sample_config):
    config = load_governance_config(sample_config)
    payload = parse_shard("samples/shard_02_v1.jsonl", config=config)
    records = {record["sent_id"]: record for record in payload["records"]}
    first = records["v1-001"]
    assert first["confidence"] == 0.85
    assert first["confidence_conversion"]["from"] == 85
    assert "divided by 100" in first["confidence_conversion"]["basis"]
    assert first["compatibility"]["degraded"] is True
    assert first["normalized_version"] == 2
    assert first["alignable"] is True
    unsplit = records["v1-002"]
    assert unsplit["unsplit"] is True
    assert unsplit["alignable"] is False
    assert "zero_tokens_marked_unsegmented_not_alignable" in unsplit["compatibility"]["reasons"]
    assert payload["stats"]["v1_unsegmented"] == 1


def test_v1_reverse_export_is_strict_v1_consumable(tmp_path, sample_config):
    engine = Engine(tmp_path / "work")
    target = tmp_path / "roundtrip.jsonl"
    result = engine.export_v1(
        ["samples/shard_02_v1.jsonl"],
        config_path=sample_config,
        output_path=target,
    )
    rows = [json.loads(line) for line in target.read_text(encoding="utf-8").splitlines()]
    source = [
        json.loads(line)
        for line in (sample_config.parent / "shard_02_v1.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    assert result["records"] == len(rows) == 3
    assert rows == source
    assert all(row["structure_version"] == 1 for row in rows)
    assert all(isinstance(row["confidence"], int) for row in rows)


def test_v1_confidence_must_be_integer_percentage(tmp_path, sample_config):
    shard = tmp_path / "bad_v1.jsonl"
    shard.write_text(
        json.dumps(
            {
                "sent_id": "bad-v1",
                "tokens": ["x"],
                "old_tags": ["noun"],
                "new_tags": ["NOUN"],
                "annotator_id": "a",
                "structure_version": 1,
                "confidence": 85.5,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    config = load_governance_config(sample_config)
    with pytest.raises(BusinessError) as caught:
        parse_shard(shard, config=config)
    assert caught.value.code == E_INVALID_RECORD


def test_optional_new_tags_become_unpaired(tmp_path, sample_config):
    shard = tmp_path / "unpaired.jsonl"
    shard.write_text(
        json.dumps(
            {
                "sent_id": "unpaired-1",
                "tokens": ["hello"],
                "old_tags": ["intj"],
                "annotator": "a",
                "structure_version": 2,
            },
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )
    config = load_governance_config(sample_config)
    payload = parse_shard(shard, config=config)
    assert payload["records"][0]["alignable"] is False
    assert payload["stats"]["unpaired_records"] == 1

