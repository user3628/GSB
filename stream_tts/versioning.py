"""Content-addressed fingerprints for stage invalidation."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from .calibration import calibration_digest
from .config import EngineConfig
from .scheduler import schedule_fingerprint
from .utils import sha256_json


@dataclass(frozen=True)
class FingerprintSet:
    frontend: str
    schedule: str
    acoustic: str
    waveform: str
    validation: str
    overall: str

    def to_dict(self) -> dict[str, str]:
        return asdict(self)

    @classmethod
    def compute(
        cls,
        *,
        config: EngineConfig,
        text: str,
        language: str,
        speaker: int,
        prosody: dict[str, Any],
        phoneme_table_digest: str,
        weights_digest: str,
        calibration: dict[str, Any],
    ) -> "FingerprintSet":
        frontend = sha256_json(
            {
                "text": text,
                "language": language,
                "split": config.split_digest(),
                "phoneme_table": phoneme_table_digest,
            }
        )
        schedule = schedule_fingerprint(
            frontend_fingerprint=frontend, config=config, calibration=calibration
        )
        acoustic = sha256_json(
            {
                "frontend": frontend,
                "schedule": schedule,
                "model_structure": config.model_structure_digest(),
                "weights": weights_digest,
                "speaker": speaker,
                "prosody": prosody,
                "calibration": calibration_digest(calibration),
            }
        )
        waveform = sha256_json(
            {
                "acoustic": acoustic,
                "synthesis": config.synthesis_digest(),
                "output": config.output.__dict__,
            }
        )
        validation = sha256_json(
            {
                "waveform": waveform,
                "schedule": schedule,
                "mode": config.validation.mode,
                "reference_tolerance": config.validation.reference_tolerance,
                "algorithm": "strict-chunk-vs-oneshot-v1",
            }
        )
        overall = sha256_json(
            {
                "frontend": frontend,
                "schedule": schedule,
                "acoustic": acoustic,
                "waveform": waveform,
                "validation": validation,
            }
        )
        return cls(
            frontend=frontend,
            schedule=schedule,
            acoustic=acoustic,
            waveform=waveform,
            validation=validation,
            overall=overall,
        )
