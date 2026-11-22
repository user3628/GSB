from __future__ import annotations

import json

import pytest

from posalign.alignment import align_shard
from posalign.config import load_governance_config
from posalign.errors import BusinessError, E_ALIGNMENT_FAILED
from posalign.parse import parse_shard


def _parse(tmp_path, config, rows):
    shard = tmp_path / "align.jsonl"
    shard.write_text(
        "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows),
        encoding="utf-8",
    )
    return parse_shard(shard, config=config)


def test_lcs_records_insertion_deletion_and_misalignment(tmp_path, sample_config):
    config = load_governance_config(sample_config)
    payload = _parse(
        tmp_path,
        config,
        [
            {
                "sent_id": "insert",
                "tokens": ["The", "fox"],
                "new_tokens": ["The", "red", "fox"],
                "old_tags": ["det", "noun"],
                "new_tags": ["DET", "ADJ", "NOUN"],
                "annotator": "annotator-a",
                "structure_version": 2,
            },
            {
                "sent_id": "misalign",
                "tokens": ["I", "really", "like", "tea"],
                "new_tokens": ["I", "very", "like", "tea"],
                "old_tags": ["pron", "adv", "v", "noun"],
                "new_tags": ["PRON", "ADV", "VERB", "NOUN"],
                "annotator": "annotator-a",
                "structure_version": 2,
            },
        ],
    )
    aligned = align_shard(payload, config=config)
    by_id = {sentence["sent_id"]: sentence for sentence in aligned["sentences"]}
    assert by_id["insert"]["alignment_rate"] == 0.8
    assert by_id["insert"]["insertions"][0]["new_token"] == "red"
    assert by_id["misalign"]["alignment_rate"] == 0.75
    assert by_id["misalign"]["misaligned_spans"][0]["misaligned"] is True
    assert by_id["misalign"]["misaligned_spans"][0]["old_start"] == 1
    assert aligned["stats"]["matched_pairs"] == 5


def test_low_alignment_rate_raises_with_sent_id_and_rate(tmp_path, sample_config):
    config = load_governance_config(sample_config)
    payload = _parse(
        tmp_path,
        config,
        [
            {
                "sent_id": "bad",
                "tokens": ["I", "like", "tea"],
                "new_tokens": ["You", "love", "coffee"],
                "old_tags": ["pron", "v", "noun"],
                "new_tags": ["PRON", "VERB", "NOUN"],
                "annotator": "annotator-a",
                "structure_version": 2,
            }
        ],
    )
    with pytest.raises(BusinessError) as caught:
        align_shard(payload, config=config)
    assert caught.value.code == E_ALIGNMENT_FAILED
    failure = caught.value.details["failures"][0]
    assert failure["sent_id"] == "bad"
    assert failure["alignment_rate"] == 0.0
    assert caught.value.details["wrote_alignment_stage"] is False


def test_explicit_empty_new_tokens_fails_instead_of_falling_back(tmp_path, sample_config):
    config = load_governance_config(sample_config)
    payload = _parse(
        tmp_path,
        config,
        [
            {
                "sent_id": "empty-new-side",
                "tokens": ["I", "like", "tea"],
                "new_tokens": [],
                "old_tags": ["pron", "v", "noun"],
                "new_tags": [],
                "annotator": "annotator-a",
                "structure_version": 2,
            }
        ],
    )
    with pytest.raises(BusinessError) as caught:
        align_shard(payload, config=config)
    assert caught.value.code == E_ALIGNMENT_FAILED
    failure = caught.value.details["failures"][0]
    assert failure["sent_id"] == "empty-new-side"
    assert failure["alignment_rate"] == 0.0
    assert failure["new_tokens"] == []

