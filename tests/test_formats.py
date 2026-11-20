from __future__ import annotations

import re

import pytest

from transcript_workbench.formats import (
    decode_json,
    decode_text,
    encode_json,
    encode_srt,
    encode_webvtt,
    format_timestamp,
)
from transcript_workbench.models import validate_timeline


def _word_signature(words):
    return [word.to_dict() for word in words]


def test_json_srt_json_roundtrip_preserves_every_field(boot_workspace):
    state = boot_workspace.load_state()
    for chunk_id in state.chunks:
        chunks = {chunk_id: state.chunks[chunk_id]}
        words = [word for word in state.words if word.chunk_id == chunk_id]
        encoded_json = encode_json(chunks, words)
        decoded_json = decode_json(encoded_json, known_chunks=chunks)
        assert _word_signature(decoded_json.words) == _word_signature(words)

        encoded_srt = encode_srt(chunks, words)
        assert re.search(r"\d{2}:\d{2}:\d{2},\d{3} --> \d{2}:\d{2}:\d{2},\d{3}", encoded_srt)
        decoded_srt = decode_text(
            encoded_srt, "srt", known_chunks=chunks, default_chunk_id=chunk_id
        )
        assert _word_signature(decoded_srt.words) == _word_signature(words)
        assert decoded_srt.loss_report == []
        assert all(word.language == state.chunks[chunk_id].language for word in decoded_srt.words)
        assert all(0.0 <= word.confidence <= 1.0 for word in decoded_srt.words)
        validate_timeline(decoded_srt.words, decoded_srt.chunks)


def test_json_webvtt_json_roundtrip_preserves_every_field(boot_workspace):
    state = boot_workspace.load_state()
    for chunk_id in state.chunks:
        chunks = {chunk_id: state.chunks[chunk_id]}
        words = [word for word in state.words if word.chunk_id == chunk_id]
        encoded = encode_webvtt(chunks, words)
        assert encoded.startswith("WEBVTT\n\n")
        assert re.search(r"\d{2}:\d{2}:\d{2}\.\d{3} --> \d{2}:\d{2}:\d{2}\.\d{3}", encoded)
        decoded = decode_text(
            encoded, "webvtt", known_chunks=chunks, default_chunk_id=chunk_id
        )
        assert _word_signature(decoded.words) == _word_signature(words)
        assert decoded.loss_report == []
        final_json = encode_json(decoded.chunks, decoded.words)
        final = decode_json(final_json, known_chunks=chunks)
        assert _word_signature(final.words) == _word_signature(words)


def test_roundtrip_check_compares_gaps_and_overlaps(boot_workspace):
    state = boot_workspace.load_state()
    report = boot_workspace.roundtrip_check(version=state.version)
    assert report["all_exact"] is True
    assert len(report["checks"]) == len(state.chunks) * 2
    for check in report["checks"]:
        assert check["exact"] is True
        assert check["timeline_regions_exact"] is True
        assert check["loss_report"] == []


def test_third_party_webvtt_reports_metadata_loss_explicitly(boot_workspace):
    state = boot_workspace.load_state()
    chunk = state.chunks["chunk-02"]
    raw = "WEBVTT\n\n00:00:00.100 --> 00:00:00.500\nhello\n"
    result = decode_text(
        raw,
        "webvtt",
        known_chunks={chunk.chunk_id: chunk},
        default_language="en-US",
        default_chunk_id=chunk.chunk_id,
    )
    fields = {item["field"] for item in result.loss_report}
    assert {"confidence", "language", "entry_id"} <= fields
    assert result.words[0].confidence == 0.5
    assert result.words[0].language == "en-US"
    assert result.words[0].text == "hello"


def test_timestamp_format_and_long_hours():
    assert format_timestamp(3_723_045, ",") == "01:02:03,045"
    assert format_timestamp(3_723_045, ".") == "01:02:03.045"
    assert format_timestamp(360_000_000, ",") == "100:00:00,000"


def test_empty_timeline_roundtrips_through_all_formats(boot_workspace):
    state = boot_workspace.load_state()
    chunk = state.chunks["chunk-03"]
    chunks = {chunk.chunk_id: chunk}
    for fmt in ("srt", "webvtt"):
        encoded = (
            encode_srt(chunks, [])
            if fmt == "srt"
            else encode_webvtt(chunks, [])
        )
        decoded = decode_text(
            encoded,
            fmt,
            known_chunks=chunks,
            default_chunk_id=chunk.chunk_id,
        )
        assert decoded.words == []
        assert decoded.chunks[chunk.chunk_id].duration_ms == chunk.duration_ms
