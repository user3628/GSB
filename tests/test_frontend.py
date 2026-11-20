from __future__ import annotations

from pathlib import Path

import pytest

from stream_tts.config import SplitConfig, load_config
from stream_tts.errors import TTSError
from stream_tts.frontend import PhonemeTable, front_end, front_end_single_chunk, normalize_text
from stream_tts.inputs import load_samples


def test_normalization_and_chunk_offsets(workspace: Path):
    assert normalize_text("  Ａ\tB\u3000你好  ") == " A B 你好 "
    config = load_config(workspace / "config/default.yaml", workspace=workspace)
    table = PhonemeTable.load(config.resolved_path("phoneme_table"))
    utt = load_samples(config.resolved_path("samples"))[0]
    result = front_end(
        utt_id=utt.utt_id,
        text=utt.text,
        language=utt.language,
        split=config.split,
        phoneme_table=table,
    )
    assert len(result.chunks) >= 2
    assert "".join(chunk.text for chunk in result.chunks) == result.normalized_text
    assert result.chunks[0].char_start == 0
    assert result.chunks[-1].char_end == len(result.normalized_text)
    for left, right in zip(result.chunks, result.chunks[1:]):
        assert left.char_end == right.char_start
    assert result.chunks[0].visible_chars >= config.split.first_chunk_min_chars
    assert result.chunks[0].text.endswith("。")
    assert all(phone.global_index == index for index, phone in enumerate(result.phonemes))


def test_single_chunk_has_identical_global_phone_stream(workspace: Path):
    config = load_config(workspace / "config/default.yaml", workspace=workspace)
    table = PhonemeTable.load(config.resolved_path("phoneme_table"))
    for utt in load_samples(config.resolved_path("samples")):
        chunked = front_end(
            utt_id=utt.utt_id,
            text=utt.text,
            language=utt.language,
            split=config.split,
            phoneme_table=table,
        )
        one_shot = front_end_single_chunk(
            utt_id=utt.utt_id,
            text=utt.text,
            language=utt.language,
            split=config.split,
            phoneme_table=table,
        )
        assert [(p.global_index, p.phone, p.frames, p.marker) for p in chunked.phonemes] == [
            (p.global_index, p.phone, p.frames, p.marker) for p in one_shot.phonemes
        ]


def test_empty_unknown_and_invalid_split_errors(workspace: Path):
    config = load_config(workspace / "config/default.yaml", workspace=workspace)
    table = PhonemeTable.load(config.resolved_path("phoneme_table"))
    with pytest.raises(TTSError) as empty:
        front_end(
            utt_id="empty",
            text=" \t\n ",
            language="zh",
            split=config.split,
            phoneme_table=table,
        )
    assert empty.value.code == "E_INVALID_TEXT"
    assert empty.value.context["utt_id"] == "empty"
    assert empty.value.context["chunk_index"] == 0
    assert empty.value.context["char_position"] == 0

    with pytest.raises(TTSError) as unknown:
        front_end(
            utt_id="unknown",
            text="你好，龘。",
            language="zh",
            split=config.split,
            phoneme_table=table,
        )
    assert unknown.value.code == "E_INVALID_TEXT"
    assert unknown.value.context["char_position"] == 3
    assert unknown.value.context["character"] == "龘"

    bad = SplitConfig(
        first_chunk_min_chars=-1,
        subsequent_chunk_chars=8,
        punctuation_forced=True,
        max_budget_splits=1,
    )
    with pytest.raises(TTSError) as invalid:
        front_end(
            utt_id="bad-split",
            text="你好",
            language="zh",
            split=bad,
            phoneme_table=table,
        )
    assert invalid.value.code == "E_INVALID_TEXT"
    assert invalid.value.context["field"] == "split.first_chunk_min_chars"
