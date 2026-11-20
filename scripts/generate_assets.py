#!/usr/bin/env python3
"""Generate every deterministic local asset used by the experiment.

Run from the repository root:
    .venv/bin/python scripts/generate_assets.py
"""

from __future__ import annotations

import hashlib
import json
import random
import sys
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from stream_tts.acoustic import AcousticPredictor, Prosody, assert_finite, generate_weights, weights_digest
from stream_tts.calibration import ensure_calibration
from stream_tts.config import load_config
from stream_tts.frontend import PhonemeTable, front_end_single_chunk
from stream_tts.inputs import Utterance, samples_to_jsonl
from stream_tts.utils import atomic_write_json, atomic_write_text, sha256_bytes, sha256_json
from stream_tts.waveform import synth_chunk, wav_bytes


PINYIN = {
    "你": "ni3", "好": "hao3", "欢": "huan1", "迎": "ying2", "使": "shi3",
    "用": "yong4", "流": "liu2", "式": "shi4", "语": "yu3", "音": "yin1",
    "合": "he2", "成": "cheng2", "今": "jin1", "天": "tian1", "气": "qi4",
    "很": "hen3", "我": "wo3", "们": "men5", "去": "qu4", "公": "gong1",
    "园": "yuan2", "散": "san4", "步": "bu4", "吧": "ba5", "请": "qing3",
    "播": "bo1", "放": "fang4", "然": "ran2", "后": "hou4", "打": "da3",
    "开": "kai1", "页": "ye4", "面": "mian4", "这": "zhe4", "是": "shi4",
    "一": "yi1", "条": "tiao2", "较": "jiao4", "长": "chang2", "的": "de5",
    "测": "ce4", "试": "shi4", "句": "ju4", "包": "bao1", "含": "han2",
    "数": "shu4", "字": "zi4", "和": "he2", "标": "biao1", "点": "dian3",
    "符": "fu2", "号": "hao4", "完": "wan2", "整": "zheng3", "处": "chu3",
    "理": "li3", "每": "mei3", "个": "ge4", "块": "kuai4", "并": "bing4",
    "保": "bao3", "证": "zheng4", "拼": "pin1", "接": "jie1", "结": "jie2",
    "果": "guo3", "与": "yu3", "体": "ti3", "推": "tui1", "全": "quan2",
    "致": "zhi4",
}


LATIN_VOWELS = set("aeiou")
LATIN_PHONE = {
    "a": "AA", "b": "B", "c": "K", "d": "D", "e": "EH", "f": "F", "g": "G",
    "h": "HH", "i": "IH", "j": "JH", "k": "K", "l": "L", "m": "M", "n": "N",
    "o": "OW", "p": "P", "q": "K", "r": "R", "s": "S", "t": "T", "u": "UW",
    "v": "V", "w": "W", "x": "KS", "y": "Y", "z": "Z",
}

PUNCTUATION = {
    "，": ("SIL", 2, "PAUSE"), "。": ("SIL", 3, "PAUSE"), "、": ("SIL", 2, "PAUSE"),
    "；": ("SIL", 2, "PAUSE"), "：": ("SIL", 2, "PAUSE"), "？": ("SIL", 3, "PAUSE"),
    "！": ("SIL", 3, "PAUSE"), "“": ("SIL", 1, "PAUSE"), "”": ("SIL", 1, "PAUSE"),
    "‘": ("SIL", 1, "PAUSE"), "’": ("SIL", 1, "PAUSE"), "（": ("SIL", 1, "PAUSE"),
    "）": ("SIL", 1, "PAUSE"), "《": ("SIL", 1, "PAUSE"), "》": ("SIL", 1, "PAUSE"),
    "…": ("SIL", 2, "PAUSE"), "—": ("SIL", 2, "PAUSE"),
    ",": ("SIL", 2, "PAUSE"), ".": ("SIL", 3, "PAUSE"), "?": ("SIL", 3, "PAUSE"),
    "!": ("SIL", 3, "PAUSE"), ";": ("SIL", 2, "PAUSE"), ":": ("SIL", 2, "PAUSE"),
    "'": ("SIL", 1, "PAUSE"), '"': ("SIL", 1, "PAUSE"), "-": ("SIL", 1, "PAUSE"),
    "(": ("SIL", 1, "PAUSE"), ")": ("SIL", 1, "PAUSE"), "/": ("SIL", 1, "PAUSE"),
    "%": ("SIL", 1, "PAUSE"), "+": ("SIL", 1, "PAUSE"), "=": ("SIL", 1, "PAUSE"),
    "@": ("SIL", 1, "PAUSE"), "#": ("SIL", 1, "PAUSE"), "&": ("SIL", 1, "PAUSE"),
}


def phoneme_table() -> dict:
    chars = {
        char: {"phone": phone, "frames": 4, "marker": "ZH"}
        for char, phone in PINYIN.items()
    }
    latin = {
        char: {
            "phone": phone,
            "frames": 3 if char in LATIN_VOWELS else 2,
            "marker": "EN",
        }
        for char, phone in LATIN_PHONE.items()
    }
    digits = {
        str(digit): {"phone": f"DIG{digit}", "frames": 2, "marker": "NUM"}
        for digit in range(10)
    }
    punct = {
        char: {"phone": phone, "frames": frames, "marker": marker}
        for char, (phone, frames, marker) in PUNCTUATION.items()
    }
    return {
        "schema_version": 1,
        "description": "Explicit synthetic symbol table; unknown characters are errors, never dropped.",
        "chars": dict(sorted(chars.items())),
        "latin": dict(sorted(latin.items())),
        "digits": digits,
        "punctuation": dict(sorted(punct.items())),
        "whitespace": {"phone": "SP", "frames": 1, "marker": "SIL"},
    }


def make_samples(seed: int) -> list[Utterance]:
    rng = random.Random(seed)
    texts = [
        (
            "utt_mix_01",
            "你好，欢迎使用 Stream TTS 流式语音合成。今天天气很好，我们去 park 散步吧。",
            "zh",
        ),
        ("utt_mix_02", "请播放 English news，然后打开 weather forecast 页面。", "en"),
        (
            "utt_budget_stress_03",
            "这是一条较长的测试语句，包含 English words、数字 2026 和标点符号；请完整处理每个块，并保证拼接结果与整体推理完全一致。",
            "zh",
        ),
    ]
    samples: list[Utterance] = []
    for index, (utt_id, text, language) in enumerate(texts):
        speaker = 1 + rng.randrange(4)
        rate = round(0.92 + rng.random() * 0.16, 3)
        energy = round(0.96 + rng.random() * 0.12, 3)
        pitch = round(0.96 + rng.random() * 0.10, 3)
        complexity = 1.0 if index < 2 else 10.0
        samples.append(
            Utterance(
                utt_id=utt_id,
                text=text,
                language=language,
                speaker=speaker,
                prosody={
                    "rate": rate,
                    "energy": energy,
                    "pitch": pitch,
                    "complexity": complexity,
                },
            )
        )
    return samples


def main() -> int:
    config = load_config(ROOT / "config/default.yaml", workspace=ROOT)
    data_dir = ROOT / "data"
    reference_dir = data_dir / "reference"
    data_dir.mkdir(parents=True, exist_ok=True)
    reference_dir.mkdir(parents=True, exist_ok=True)

    table = phoneme_table()
    atomic_write_json(config.resolved_path("phoneme_table"), table)

    arrays = generate_weights(config)
    weights_path = config.resolved_path("acoustic_weights")
    weights_path.parent.mkdir(parents=True, exist_ok=True)
    np.savez_compressed(weights_path, **arrays)

    samples = make_samples(config.model.seed)
    samples_text = samples_to_jsonl(samples)
    atomic_write_text(config.resolved_path("samples"), samples_text)

    invalid_samples = [
        {
            "utt_id": "invalid_empty",
            "text": "   ",
            "language": "zh",
            "speaker": 1,
            "prosody": {},
        },
        {
            "utt_id": "invalid_unknown_char",
            "text": "你好，龘。",
            "language": "zh",
            "speaker": 1,
            "prosody": {},
        },
        {
            "utt_id": "invalid_emoji",
            "text": "hello 😀",
            "language": "en",
            "speaker": 1,
            "prosody": {},
        },
    ]
    atomic_write_text(
        data_dir / "invalid_samples.jsonl",
        "".join(json.dumps(item, ensure_ascii=False) + "\n" for item in invalid_samples),
    )

    predictor = AcousticPredictor(config)
    calibration = ensure_calibration(config, predictor)
    loaded_table = PhonemeTable.load(config.resolved_path("phoneme_table"))

    references: dict[str, dict] = {}
    for utt in samples:
        frontend = front_end_single_chunk(
            utt_id=utt.utt_id,
            text=utt.text,
            language=utt.language,
            split=config.split,
            phoneme_table=loaded_table,
        )
        acoustic = predictor.predict_all(
            frontend=frontend,
            speaker=utt.speaker,
            prosody=Prosody.from_dict(utt.prosody),
        )
        assert_finite(utt_id=utt.utt_id, values=acoustic.values)
        wave = synth_chunk(utt_id=utt.utt_id, acoustic=acoustic, config=config)
        wav = wav_bytes(pcm=wave.pcm, config=config)
        reference_path = reference_dir / f"{utt.utt_id}.wav"
        ref_tmp = reference_path.with_suffix(".wav.tmp")
        ref_tmp.write_bytes(wav)
        ref_tmp.replace(reference_path)
        references[utt.utt_id] = {
            "path": str(reference_path.relative_to(ROOT)),
            "sha256": sha256_bytes(wav),
            "byte_count": len(wav),
            "frame_count": acoustic.n_frames,
            "sample_count_per_channel": wave.sample_count_per_channel,
            "acoustic_sha256": acoustic.digest,
            "marker_count": len(acoustic.markers),
        }

    checksums = {
        "schema_version": 1,
        "model_seed": config.model.seed,
        "samples_sha256": hashlib.sha256(samples_text.encode("utf-8")).hexdigest(),
        "phoneme_table_sha256": sha256_json(table),
        "weights_sha256": weights_digest(arrays),
        "calibration_sha256": sha256_json(calibration),
        "calibration_unit_ms": calibration["unit_ms"],
        "references": references,
    }
    atomic_write_json(data_dir / "checksums.json", checksums)

    print(
        json.dumps(
            {
                "samples": len(samples),
                "weights_digest": checksums["weights_sha256"],
                "phoneme_table_digest": checksums["phoneme_table_sha256"],
                "calibration_unit_ms": calibration["unit_ms"],
                "references": {key: value["sha256"] for key, value in references.items()},
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
