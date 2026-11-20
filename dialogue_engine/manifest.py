"""Dataset import and strict manifest/WAV consistency validation."""

from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
from scipy.io import wavfile

from .config import DIALOGUE_ACTS, EMOTIONS
from .errors import (
    E_INPUT_LENGTH,
    E_INVALID_TURN,
    as_error,
)
from .serialization import digest_json, read_json, sha256_bytes


@dataclass(frozen=True)
class TurnAnnotation:
    index: int
    speaker_id: str
    start_ms: float
    end_ms: float
    start_sample: int
    end_sample: int
    ref_emotion: str
    ref_dialogue_act: str
    text: str = ""

    @property
    def duration_ms(self) -> float:
        return self.end_ms - self.start_ms

    def truth_dict(self) -> dict[str, Any]:
        return {
            "turn_index": self.index,
            "speaker_id": self.speaker_id,
            "start_ms": self.start_ms,
            "end_ms": self.end_ms,
            "start_sample": self.start_sample,
            "end_sample": self.end_sample,
            "ref_emotion": self.ref_emotion,
            "ref_dialogue_act": self.ref_dialogue_act,
            "text": self.text,
        }


@dataclass(frozen=True)
class ShardData:
    shard_id: str
    wav_path: Path
    wav_file: str
    sample_rate: int
    samples: np.ndarray
    turns: tuple[TurnAnnotation, ...]
    declared_sample_count: int
    manifest_metadata: dict[str, Any]

    @property
    def sample_count(self) -> int:
        return int(self.samples.shape[0])

    @property
    def samples_digest(self) -> str:
        return sha256_bytes(np.ascontiguousarray(self.samples, dtype="<i2").tobytes())

    @property
    def turn_count(self) -> int:
        return len(self.turns)

    @property
    def manifest_digest(self) -> str:
        return digest_json(
            {
                "sample_rate": self.sample_rate,
                "sample_count": self.sample_count,
                "turns": [turn.truth_dict() for turn in self.turns],
                "metadata": self.manifest_metadata,
            }
        )

    def turn(self, index: int) -> TurnAnnotation:
        if index < 0 or index >= len(self.turns):
            raise as_error(
                E_INVALID_TURN,
                f"turn index {index} is outside [0, {len(self.turns)})",
                shard_id=self.shard_id,
                turn_index=index,
                stage="import",
            )
        return self.turns[index]


@dataclass(frozen=True)
class Dataset:
    data_dir: Path
    manifest_path: Path
    manifest: dict[str, Any]
    shards: tuple[ShardData, ...]

    @property
    def shard_map(self) -> dict[str, ShardData]:
        return {shard.shard_id: shard for shard in self.shards}

    def shard(self, shard_id: str) -> ShardData:
        for shard in self.shards:
            if shard.shard_id == shard_id:
                return shard
        raise as_error(
            E_INVALID_TURN,
            f"unknown shard_id {shard_id!r}",
            shard_id=shard_id,
            stage="import",
        )


def _require_number(value: Any, field: str, *, shard_id: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise as_error(
            E_INPUT_LENGTH,
            f"{field} must be numeric",
            shard_id=shard_id,
            stage="import",
        )
    number = float(value)
    if not math.isfinite(number):
        raise as_error(
            E_INPUT_LENGTH,
            f"{field} must be finite",
            shard_id=shard_id,
            stage="import",
        )
    return number


def _load_wav(path: Path, shard_id: str) -> tuple[int, np.ndarray]:
    if not path.is_file():
        raise as_error(
            E_INPUT_LENGTH,
            f"WAV file does not exist: {path}",
            shard_id=shard_id,
            stage="import",
        )
    try:
        sample_rate, samples = wavfile.read(path, mmap=False)
    except Exception as exc:  # scipy raises several format-specific exceptions
        raise as_error(
            E_INPUT_LENGTH,
            f"cannot read WAV file: {exc}",
            shard_id=shard_id,
            stage="import",
        ) from exc
    if samples.ndim != 1:
        raise as_error(
            E_INPUT_LENGTH,
            f"expected mono PCM, got shape {samples.shape}",
            shard_id=shard_id,
            stage="import",
        )
    if samples.dtype != np.int16:
        raise as_error(
            E_INPUT_LENGTH,
            f"expected 16-bit PCM, got {samples.dtype}",
            shard_id=shard_id,
            stage="import",
        )
    if sample_rate != 16_000:
        raise as_error(
            E_INPUT_LENGTH,
            f"expected 16000 Hz, got {sample_rate} Hz",
            shard_id=shard_id,
            stage="import",
        )
    return int(sample_rate), samples


def _normalize_turns(raw_shard: dict[str, Any], shard_id: str) -> list[dict[str, Any]]:
    raw_turns = raw_shard.get("turns")
    if not isinstance(raw_turns, list) or not raw_turns:
        raise as_error(
            E_INVALID_TURN,
            "turns must be a non-empty list",
            shard_id=shard_id,
            stage="import",
        )
    return raw_turns


def _validate_turns(
    raw_shard: dict[str, Any],
    *,
    shard_id: str,
    sample_rate: int,
    sample_count: int,
) -> tuple[TurnAnnotation, ...]:
    raw_turns = _normalize_turns(raw_shard, shard_id)
    declared_turn_count = raw_shard.get("turn_count")
    if declared_turn_count != len(raw_turns):
        raise as_error(
            E_INPUT_LENGTH,
            f"declared turn_count={declared_turn_count!r} != actual {len(raw_turns)}",
            shard_id=shard_id,
            stage="import",
        )

    duration_ms = sample_count * 1000.0 / sample_rate
    turns: list[TurnAnnotation] = []
    previous_end_sample = -1
    previous_end_ms: float | None = None

    for position, raw_turn in enumerate(raw_turns):
        if not isinstance(raw_turn, dict):
            raise as_error(
                E_INVALID_TURN,
                f"turn at position {position} is not an object",
                shard_id=shard_id,
                turn_index=position,
                stage="import",
            )
        declared_index = raw_turn.get("turn_index", position)
        if declared_index != position or isinstance(declared_index, bool):
            raise as_error(
                E_INVALID_TURN,
                f"turn_index must be sequential; expected {position}, got {declared_index!r}",
                shard_id=shard_id,
                turn_index=position,
                stage="import",
            )
        start_ms = _require_number(raw_turn.get("start_ms"), "start_ms", shard_id=shard_id)
        end_ms = _require_number(raw_turn.get("end_ms"), "end_ms", shard_id=shard_id)
        speaker_id = raw_turn.get("speaker_id")
        if speaker_id is None or str(speaker_id).strip() == "":
            raise as_error(
                E_INVALID_TURN,
                "speaker_id must be non-empty",
                shard_id=shard_id,
                turn_index=position,
                stage="import",
            )
        speaker_id = str(speaker_id)
        ref_emotion = raw_turn.get("ref_emotion")
        ref_act = raw_turn.get("ref_dialogue_act")
        if ref_emotion not in EMOTIONS:
            raise as_error(
                E_INVALID_TURN,
                f"ref_emotion must be one of {EMOTIONS}, got {ref_emotion!r}",
                shard_id=shard_id,
                turn_index=position,
                stage="import",
            )
        if ref_act not in DIALOGUE_ACTS:
            raise as_error(
                E_INVALID_TURN,
                f"ref_dialogue_act must be one of {DIALOGUE_ACTS}, got {ref_act!r}",
                shard_id=shard_id,
                turn_index=position,
                stage="import",
            )
        if start_ms < 0.0 or end_ms > duration_ms + 1e-9 or start_ms >= end_ms:
            raise as_error(
                E_INVALID_TURN,
                f"invalid turn boundary [{start_ms}, {end_ms}) for {duration_ms:.3f} ms audio",
                shard_id=shard_id,
                turn_index=position,
                stage="import",
            )
        start_sample = int(round(start_ms * sample_rate / 1000.0))
        end_sample = int(round(end_ms * sample_rate / 1000.0))
        if end_sample > sample_count:
            end_sample = sample_count
        if start_sample < 0 or start_sample >= end_sample:
            raise as_error(
                E_INVALID_TURN,
                f"turn maps to empty sample range [{start_sample}, {end_sample})",
                shard_id=shard_id,
                turn_index=position,
                stage="import",
            )
        if start_sample < previous_end_sample:
            raise as_error(
                E_INVALID_TURN,
                (
                    f"turn starts at sample {start_sample}, before previous end "
                    f"{previous_end_sample}"
                ),
                shard_id=shard_id,
                turn_index=position,
                stage="import",
            )
        if previous_end_ms is not None and start_ms < previous_end_ms:
            raise as_error(
                E_INVALID_TURN,
                f"overlapping milliseconds: {start_ms} < previous end {previous_end_ms}",
                shard_id=shard_id,
                turn_index=position,
                stage="import",
            )
        turns.append(
            TurnAnnotation(
                index=position,
                speaker_id=speaker_id,
                start_ms=start_ms,
                end_ms=end_ms,
                start_sample=start_sample,
                end_sample=end_sample,
                ref_emotion=str(ref_emotion),
                ref_dialogue_act=str(ref_act),
                text=str(raw_turn.get("text", "")),
            )
        )
        previous_end_sample = end_sample
        previous_end_ms = end_ms
    return tuple(turns)


def load_dataset(data_dir: str | Path, *, manifest_name: str = "manifest.json") -> Dataset:
    """Load a manifest and every WAV shard, validating declared facts."""

    root = Path(data_dir).expanduser().resolve()
    manifest_path = root / manifest_name if root.is_dir() else root
    if not manifest_path.is_file():
        raise as_error(
            E_INPUT_LENGTH,
            f"manifest does not exist: {manifest_path}",
            stage="import",
        )
    try:
        manifest = read_json(manifest_path)
    except Exception as exc:
        raise as_error(
            E_INPUT_LENGTH,
            f"cannot parse manifest JSON: {exc}",
            stage="import",
        ) from exc
    if not isinstance(manifest, dict):
        raise as_error(
            E_INPUT_LENGTH,
            "manifest root must be an object",
            stage="import",
        )
    raw_shards = manifest.get("shards")
    if not isinstance(raw_shards, list) or not raw_shards:
        raise as_error(
            E_INPUT_LENGTH,
            "manifest.shards must be a non-empty list",
            stage="import",
        )

    base_dir = manifest_path.parent
    shards: list[ShardData] = []
    seen_shards: set[str] = set()
    for position, raw_shard in enumerate(raw_shards):
        if not isinstance(raw_shard, dict):
            raise as_error(
                E_INPUT_LENGTH,
                f"shard entry {position} is not an object",
                stage="import",
            )
        shard_id = raw_shard.get("shard_id")
        if shard_id is None or str(shard_id).strip() == "":
            raise as_error(
                E_INPUT_LENGTH,
                f"shard entry {position} has no shard_id",
                stage="import",
            )
        shard_id = str(shard_id)
        if shard_id in seen_shards:
            raise as_error(
                E_INPUT_LENGTH,
                f"duplicate shard_id {shard_id!r}",
                shard_id=shard_id,
                stage="import",
            )
        seen_shards.add(shard_id)
        wav_file = raw_shard.get("wav_file")
        if not isinstance(wav_file, str) or not wav_file:
            raise as_error(
                E_INPUT_LENGTH,
                "wav_file must be a non-empty string",
                shard_id=shard_id,
                stage="import",
            )
        wav_path = (base_dir / wav_file).resolve()
        sample_rate, samples = _load_wav(wav_path, shard_id)
        declared_sample_count = raw_shard.get("sample_count")
        if isinstance(declared_sample_count, bool) or not isinstance(
            declared_sample_count, int
        ):
            raise as_error(
                E_INPUT_LENGTH,
                f"sample_count must be an integer, got {declared_sample_count!r}",
                shard_id=shard_id,
                stage="import",
            )
        actual_sample_count = int(samples.shape[0])
        if declared_sample_count != actual_sample_count:
            raise as_error(
                E_INPUT_LENGTH,
                (
                    f"declared sample_count={declared_sample_count} != actual "
                    f"{actual_sample_count}"
                ),
                shard_id=shard_id,
                stage="import",
            )
        turns = _validate_turns(
            raw_shard,
            shard_id=shard_id,
            sample_rate=sample_rate,
            sample_count=actual_sample_count,
        )
        metadata = {
            key: value
            for key, value in raw_shard.items()
            if key not in {"wav_file", "sample_count", "turn_count", "turns"}
        }
        shards.append(
            ShardData(
                shard_id=shard_id,
                wav_path=wav_path,
                wav_file=wav_file,
                sample_rate=sample_rate,
                samples=samples,
                turns=turns,
                declared_sample_count=declared_sample_count,
                manifest_metadata=metadata,
            )
        )

    manifest_rate = manifest.get("sample_rate", 16_000)
    if manifest_rate != 16_000:
        raise as_error(
            E_INPUT_LENGTH,
            f"manifest sample_rate must be 16000, got {manifest_rate!r}",
            stage="import",
        )
    return Dataset(
        data_dir=base_dir,
        manifest_path=manifest_path,
        manifest=manifest,
        shards=tuple(shards),
    )

