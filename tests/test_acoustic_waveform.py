from __future__ import annotations

import io
import wave
from pathlib import Path

import numpy as np
import pytest
import yaml

from stream_tts.acoustic import AcousticPredictor, AcousticResult, Prosody, assert_finite
from stream_tts.config import SplitConfig, load_config
from stream_tts.errors import TTSError
from stream_tts.frontend import PhonemeTable, front_end, front_end_single_chunk
from stream_tts.inputs import load_samples
from stream_tts.waveform import synth_chunk, wav_bytes


def _splits() -> list[SplitConfig]:
    return [
        SplitConfig(5, 7, True, 2),
        SplitConfig(20, 25, True, 2),
    ]


def test_acoustic_and_pcm_are_invariant_to_chunking(workspace: Path):
    config = load_config(workspace / "config/default.yaml", workspace=workspace)
    table = PhonemeTable.load(config.resolved_path("phoneme_table"))
    predictor = AcousticPredictor(config)
    utt = load_samples(config.resolved_path("samples"))[0]
    prosody = Prosody.from_dict(utt.prosody)

    results = []
    for split in _splits():
        frontend = front_end(
            utt_id=utt.utt_id,
            text=utt.text,
            language=utt.language,
            split=split,
            phoneme_table=table,
        )
        pieces = [
            predictor.predict_phones(
                utt_id=utt.utt_id,
                phones=chunk.phonemes,
                speaker=utt.speaker,
                prosody=prosody,
                frame_start=sum(item.frame_count for item in frontend.chunks[: index]),
            )
            for index, chunk in enumerate(frontend.chunks)
        ]
        values = np.concatenate([item.values for item in pieces], axis=0)
        markers = [marker for item in pieces for marker in item.markers]
        pcm = b"".join(synth_chunk(utt_id=utt.utt_id, acoustic=item, config=config).pcm for item in pieces)
        results.append((values, markers, pcm, len(frontend.chunks)))

    one_shot_frontend = front_end_single_chunk(
        utt_id=utt.utt_id,
        text=utt.text,
        language=utt.language,
        split=config.split,
        phoneme_table=table,
    )
    full = predictor.predict_all(frontend=one_shot_frontend, speaker=utt.speaker, prosody=prosody)
    full_pcm = synth_chunk(utt_id=utt.utt_id, acoustic=full, config=config).pcm
    assert results[0][3] != results[1][3]
    for values, markers, pcm, _ in results:
        assert values.astype("<f8").tobytes() == full.values.astype("<f8").tobytes()
        assert markers == list(full.markers)
        assert pcm == full_pcm


def test_numeric_error_prevents_pcm(workspace: Path):
    config = load_config(workspace / "config/default.yaml", workspace=workspace)
    table = PhonemeTable.load(config.resolved_path("phoneme_table"))
    predictor = AcousticPredictor(config)
    utt = load_samples(config.resolved_path("samples"))[0]
    full_frontend = front_end_single_chunk(
        utt_id=utt.utt_id,
        text=utt.text,
        language=utt.language,
        split=config.split,
        phoneme_table=table,
    )
    acoustic = predictor.predict_all(
        frontend=full_frontend,
        speaker=utt.speaker,
        prosody=Prosody.from_dict(utt.prosody),
    )
    broken = acoustic.values.copy()
    broken[7, 0] = np.nan
    with pytest.raises(TTSError) as exc:
        assert_finite(utt_id=utt.utt_id, values=broken)
    assert exc.value.code == "E_NUMERIC"
    assert exc.value.context["utt_id"] == utt.utt_id
    assert exc.value.context["frame_index"] == 7
    assert exc.value.context["parameter_name"] == "band_0"

    bad_result = AcousticResult(
        utt_id=utt.utt_id,
        frame_start=0,
        frame_end=broken.shape[0],
        values=broken,
        markers=acoustic.markers,
    )
    with pytest.raises(TTSError) as exc2:
        synth_chunk(utt_id=utt.utt_id, acoustic=bad_result, config=config)
    assert exc2.value.code == "E_NUMERIC"


def test_stereo_wav_and_clipping(workspace: Path):
    raw = yaml.safe_load((workspace / "config/default.yaml").read_text(encoding="utf-8"))
    raw["output"]["channels"] = 2
    stereo_path = workspace / "config/stereo.yaml"
    stereo_path.write_text(yaml.safe_dump(raw, allow_unicode=True, sort_keys=False), encoding="utf-8")
    config = load_config(stereo_path, workspace=workspace)
    values = np.asarray(
        [[80.0, 80.0, 80.0, 80.0, 80.0, 80.0, 80.0, 80.0, 220.0, 4.0]] * 3,
        dtype="<f8",
    )
    acoustic = AcousticResult(
        utt_id="stereo",
        frame_start=0,
        frame_end=3,
        values=values,
        markers=("ZH", "ZH", "ZH"),
    )
    result = synth_chunk(utt_id="stereo", acoustic=acoustic, config=config)
    assert result.clip_count > 0
    assert result.sample_count_per_channel == 3 * config.hop_samples
    wav_data = wav_bytes(pcm=result.pcm, config=config)
    with wave.open(io.BytesIO(wav_data), "rb") as wav:
        assert wav.getnchannels() == 2
        assert wav.getsampwidth() == 2
        assert wav.getframerate() == 16000
        assert wav.getnframes() == 3 * config.hop_samples


def test_changed_model_structure_regenerates_seeded_weights(workspace: Path):
    raw = yaml.safe_load((workspace / "config/default.yaml").read_text(encoding="utf-8"))
    raw["model"]["n_bands"] = 9
    path = workspace / "config/nine-bands.yaml"
    path.write_text(yaml.safe_dump(raw, allow_unicode=True, sort_keys=False), encoding="utf-8")
    config = load_config(path, workspace=workspace)
    predictor = AcousticPredictor(config)
    assert predictor.weights[f"W{predictor.layer_count - 1}"].shape[1] == 11
    table = PhonemeTable.load(config.resolved_path("phoneme_table"))
    utt = load_samples(config.resolved_path("samples"))[0]
    frontend = front_end_single_chunk(
        utt_id=utt.utt_id,
        text=utt.text,
        language=utt.language,
        split=config.split,
        phoneme_table=table,
    )
    acoustic = predictor.predict_all(
        frontend=frontend,
        speaker=utt.speaker,
        prosody=Prosody.from_dict(utt.prosody),
    )
    assert acoustic.values.shape[1] == 11
