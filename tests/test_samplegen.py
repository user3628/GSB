from __future__ import annotations

import json
import wave
from pathlib import Path

from cabin_voice.manifest import validate_manifest


def test_demo_contains_three_chunks_and_required_observations(generated_demo: dict) -> None:
    manifest = json.loads(Path(generated_demo["manifest"]).read_text(encoding="utf-8"))
    assert len(manifest["chunks"]) >= 3
    assert {zone["zone_id"] for zone in manifest["zones"]} == {
        "driver",
        "passenger",
        "rear_left",
        "rear_right",
    }
    assert all("priority" in zone for zone in manifest["zones"])
    for chunk in manifest["chunks"]:
        with wave.open(str(Path(generated_demo["output"]) / chunk["wav"]), "rb") as handle:
            assert handle.getframerate() == 16000
            assert handle.getnchannels() == 1
            assert handle.getsampwidth() == 2
        assert len(chunk["segments"]) == 4
        assert any(segment["reference_active_ms"] for segment in chunk["segments"])
        assert any(segment["lexicon_hits"] for segment in chunk["segments"])
        for segment in chunk["segments"]:
            assert "lexicon_hits" in segment

    overlap = json.loads(Path(generated_demo["overlap_manifest"]).read_text(encoding="utf-8"))
    validate_manifest(overlap, Path(generated_demo["output"]))
    terms = [
        hit["term"]
        for segment in overlap["chunks"][0]["segments"]
        for hit in segment["lexicon_hits"]
    ]
    assert terms.count("open_window") == 2


def test_invalid_offset_manifest_is_injected(generated_demo: dict) -> None:
    invalid = json.loads(Path(generated_demo["invalid_offset_manifest"]).read_text(encoding="utf-8"))
    segment = invalid["chunks"][0]["segments"][0]
    with wave.open(str(Path(generated_demo["output"]) / invalid["chunks"][0]["wav"]), "rb") as handle:
        assert segment["offset_samples"] > handle.getnframes()
