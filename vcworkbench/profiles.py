from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SpeakerProfile:
    speaker_id: str
    pitch_factor: float
    gain: float
    phase_cycles: float


TARGET_SPEAKERS: dict[str, SpeakerProfile] = {
    "T1": SpeakerProfile("T1", pitch_factor=1.08, gain=0.92, phase_cycles=0.17),
    "T2": SpeakerProfile("T2", pitch_factor=0.92, gain=1.00, phase_cycles=0.31),
    "T3": SpeakerProfile("T3", pitch_factor=1.02, gain=0.88, phase_cycles=0.47),
}

SOURCE_SPEAKERS: dict[str, dict[str, float | str]] = {
    "S1": {"base_f0_hz": 180.0, "brightness": 1.00, "formant_scale": 1.00},
    "S2": {"base_f0_hz": 230.0, "brightness": 1.12, "formant_scale": 1.06},
    "S3": {"base_f0_hz": 160.0, "brightness": 0.90, "formant_scale": 0.94},
}


def target_profile(speaker_id: str) -> SpeakerProfile | None:
    return TARGET_SPEAKERS.get(speaker_id)
